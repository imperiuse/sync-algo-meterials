package program

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	mrc "github.com/BoskyWSMFN/go-chan-broadcast/pkg/broadcast"
	pb "gitlab-tc.dev.codd.local/theseus/rc-client/external/proto-core-rc/v1"
	"gitlab-tc.dev.codd.local/theseus/rc-client/internal/domain/clientstate/dump"
	gp "gitlab-tc.dev.codd.local/theseus/rc-client/internal/domain/program/generatedprogram"
	"gitlab-tc.dev.codd.local/theseus/rc-client/internal/domain/program/phasescounter"
	"gitlab-tc.dev.codd.local/theseus/rc-client/internal/domain/program/programvalidator"
	"gitlab-tc.dev.codd.local/theseus/rc-client/internal/logger"
	"gitlab-tc.dev.codd.local/theseus/rc-client/internal/logger/field"
	"gitlab-tc.dev.codd.local/theseus/rc-client/tools/ctxtimer"
	"gitlab-tc.dev.codd.local/theseus/rc-client/tools/deepcopy"
	"gitlab-tc.dev.codd.local/theseus/rc-client/tools/minmax"
	tst "gitlab-tc.dev.codd.local/theseus/rc-client/tools/tstimestamp"
)

type (
	RCProgramManager interface {
		BufferProgram(prog *Program, startTime time.Time, nowTime time.Time, forced bool) error
		ForwardNextPhase(currentPhaseID uint32, nowTime time.Time) error

		SubscribeToProgramApplySignal() mrc.Subscriber[signal]
		SubscribeToCountedPhasesSignal() mrc.Subscriber[signal]
		SubscribeToForwardNextPhaseSignal() mrc.Subscriber[signal]

		CopyLocalProgramTo(*Program)
		CopyCurrentProgramTo(*Program)

		GetDeltaSecondsForPhase(phaseID uint32, nowTime time.Time) uint32
		GetTimeLeftForCurrentPhase(nowTime time.Time) time.Duration
		GetTimeLeftForBufferedProgramToApply(nowTime time.Time) time.Duration
		GetMinimalPossibleStartTime(nowTime time.Time) time.Time

		ResetLocalProgramCounter()
	}

	coordinatorI interface {
		SubscribeToSnapshots() mrc.Subscriber[*dump.Snapshot]
		UpdateSignalGroups([]uint32, time.Time)
	}

	expectedStater interface {
		GetExpectedStatus() Status
		GetExpectedPhaseID() uint32
		GetExpectedPhaseStart() time.Time
	}

	coordinationStater interface {
		IsCoordinated() bool
		IsPhaseKept() bool
	}

	LocalProgramStorageI interface {
		StoreProgram(ctx context.Context, startedAt time.Time, finishedAt time.Time, prog *pb.RC_Program)
		GetActualProgram(ctx context.Context, nowTime time.Time) (*pb.RC_Program, error)
	}

	rcProgramState struct { //nolint: govet
		coordinator      coordinatorI
		eStater          expectedStater
		cStater          coordinationStater
		programGenerator gp.LocalProgramGenerator
		programCache     LocalProgramStorageI

		newProgramSignal     mrc.Broadcaster[signal]
		countedPhasesSignal  mrc.Broadcaster[signal]
		phaseForwardedSignal mrc.Broadcaster[signal]

		phaseForwardedLocalSignal chan signal
		newBufProgSignal          chan signal

		sendPhaseForwardedSignalFunc func()
		sendNewBufProgSignalFunc     func()

		log *logger.Logger

		programApplyTimer *ctxtimer.TimerCtx

		localProgram *atomic.Pointer[Program]

		currentProgram          *atomic.Pointer[Program]
		currentProgramStartTime *timestamp

		bufferedProgram           *atomic.Pointer[Program]
		bufferedProgramStartTime  *timestamp
		isBufferedProgramFromCore *atomic.Bool

		maxPhasesLen *atomic.Uint32

		cyclesToAddToKeepInTimeSpan uint32
	}

	Status    = pb.RC_StatusMsg_StatusRC
	Program   = pb.RC_Program
	timestamp = tst.Timestamp

	signal = struct{}
)

func NewRCProgramManager(
	globalCtx context.Context,
	subscr coordinatorI,
	eStater expectedStater,
	cStater coordinationStater,
	programCache LocalProgramStorageI,
	log *logger.Logger,
	localProgramInErrorSeconds int,
	cyclesToAddToKeepInTimeSpan int,
) RCProgramManager {
	const initStartPhaseID = 1

	p := createRCProgramState(
		globalCtx, subscr, eStater,
		cStater, programCache, log,
		localProgramInErrorSeconds,
		cyclesToAddToKeepInTimeSpan,
	)
	nowTime := time.Now().UTC()

	p.currentProgramStartTime.SetTime(nowTime)

	localProgram, err := programCache.GetActualProgram(globalCtx, nowTime)
	if err != nil {
		localProgram = &pb.RC_Program{
			Phases: []*pb.RC_Phase{
				{
					Id: initStartPhaseID,
				},
			},
			StartPhaseId: initStartPhaseID,
		}
	}

	p.localProgram.Store(localProgram)

	currentProgram := new(Program)
	deepcopy.RCProgram(currentProgram, localProgram)
	p.currentProgram.Store(currentProgram)

	p.sendPhaseForwardedSignalFunc = func() {
		select {
		case <-globalCtx.Done():
		case p.phaseForwardedLocalSignal <- signal{}:
		}
	}

	p.sendNewBufProgSignalFunc = func() {
		select {
		case <-globalCtx.Done():
		case p.newBufProgSignal <- signal{}:
		}
	}

	p.startWorkers(globalCtx)

	return p
}

func createRCProgramState(
	globalCtx context.Context,
	subscr coordinatorI,
	eStater expectedStater,
	cStater coordinationStater,
	programCache LocalProgramStorageI,
	log *logger.Logger,
	localProgramInErrorSeconds int,
	cyclesToAddToKeepInTimeSpan int,
) *rcProgramState {
	const defaultCyclesToAdd = 3

	if cyclesToAddToKeepInTimeSpan < defaultCyclesToAdd {
		cyclesToAddToKeepInTimeSpan = defaultCyclesToAdd
	}

	return &rcProgramState{
		coordinator:      subscr,
		eStater:          eStater,
		cStater:          cStater,
		programGenerator: gp.NewLocalProgramGenerator(globalCtx, localProgramInErrorSeconds),
		programCache:     programCache,

		newProgramSignal:     mrc.New[signal](),
		countedPhasesSignal:  mrc.New[signal](),
		phaseForwardedSignal: mrc.New[signal](),

		phaseForwardedLocalSignal: make(chan signal),
		newBufProgSignal:          make(chan signal),

		log: log.With(field.Module("Program Manager")),

		programApplyTimer: ctxtimer.NewTimerCtxWithTimeout(globalCtx, time.Second),

		localProgram: new(atomic.Pointer[Program]),

		currentProgram:          new(atomic.Pointer[Program]),
		currentProgramStartTime: tst.NewTimestamp(),

		bufferedProgram:           new(atomic.Pointer[Program]),
		bufferedProgramStartTime:  tst.NewTimestamp(),
		isBufferedProgramFromCore: new(atomic.Bool),

		maxPhasesLen:                new(atomic.Uint32),
		cyclesToAddToKeepInTimeSpan: uint32(cyclesToAddToKeepInTimeSpan),
	}
}

func (p *rcProgramState) BufferProgram(prog *Program, startTime time.Time, nowTime time.Time, forced bool) error {
	if !programvalidator.ValidateProgram(prog.GetTCycle(), prog.GetPhases()) {
		return fmt.Errorf("RCProgramManager.BufferProgram(): bad program")
	}

	if !p.checkPhasesAmount(prog) {
		return fmt.Errorf("RCProgramManager.BufferProgram(): too many phases for RC")
	}

	if !forced {
		if !p.checkStartTime(startTime, nowTime) {
			return fmt.Errorf("RCProgramManager.BufferProgram(): need more time to apply program")
		}
	} else {
		startTime = p.GetMinimalPossibleStartTime(nowTime)
		p.log.Debug("minimal possible start time",
			field.String("duration", startTime.Sub(nowTime).String()))
	}

	defer p.log.Debug("program from Core buffered", field.ID(prog.Id))

	newProg := new(Program)
	deepcopy.RCProgram(newProg, prog)

	p.bufferedProgram.Store(newProg)
	p.bufferedProgramStartTime.SetTime(startTime)
	p.isBufferedProgramFromCore.Store(true)

	p.programApplyTimer.ResetWithDeadline(startTime)

	if !forced {
		p.sendNewBufProgSignalFunc()
	}

	return nil
}

func (p *rcProgramState) ForwardNextPhase(receivedPhaseID uint32, nowTime time.Time) error {
	if !p.cStater.IsCoordinated() {
		return fmt.Errorf(
			"RCProgramManager.ForwardNextPhase(): have to be in coordination mode",
		)
	}

	if p.cStater.IsPhaseKept() {
		return fmt.Errorf(
			"RCProgramManager.ForwardNextPhase(): keeping current phase",
		)
	}

	currentPhaseID := p.eStater.GetExpectedPhaseID()
	if receivedPhaseID != currentPhaseID {
		return fmt.Errorf(
			"RCProgramManager.ForwardNextPhase(): bad phase - received %d, current %d",
			receivedPhaseID,
			currentPhaseID,
		)
	}

	timeSpent := uint32(
		math.Round(
			nowTime.Sub(p.eStater.GetExpectedPhaseStart()).Seconds(),
		),
	)

	for _, ph := range p.currentProgram.Load().Phases {
		if currentPhaseID != ph.Id {
			continue
		}

		if timeSpent >= ph.TMin {
			p.sendPhaseForwardedSignalFunc()

			return nil
		}

		return fmt.Errorf(
			"RCProgramManager.ForwardNextPhase(): in T minimum, send again after %d seconds",
			ph.TMin-timeSpent,
		)
	}

	return fmt.Errorf(
		"RCProgramManager.ForwardNextPhase(): phase %d not found in current program",
		currentPhaseID,
	) // this error should never appear
}

func (p *rcProgramState) SubscribeToProgramApplySignal() mrc.Subscriber[signal] {
	return p.newProgramSignal.Subscribe()
}

func (p *rcProgramState) SubscribeToCountedPhasesSignal() mrc.Subscriber[signal] {
	return p.countedPhasesSignal.Subscribe()
}

func (p *rcProgramState) SubscribeToForwardNextPhaseSignal() mrc.Subscriber[signal] {
	return p.phaseForwardedSignal.Subscribe()
}

func (p *rcProgramState) CopyLocalProgramTo(prog *Program) {
	p.copyProgramToFrom(prog, p.localProgram)
}

func (p *rcProgramState) CopyCurrentProgramTo(prog *Program) {
	p.copyProgramToFrom(prog, p.currentProgram)
}

func (p *rcProgramState) GetDeltaSecondsForPhase(phaseID uint32, nowTime time.Time) uint32 {
	if p.bufferedProgram.Load() == nil {
		return 0
	}

	bufProgStartTime := p.bufferedProgramStartTime.GetTime()

	if bufProgStartTime.IsZero() || nowTime.After(bufProgStartTime) {
		return 0
	}

	deadlineSeconds := bufProgStartTime.Unix() - nowTime.Unix()

	curProg := p.currentProgram.Load()
	phasesLeft, phasesLeftSeconds := phasescounter.GetPhasesLeft(
		deadlineSeconds,
		curProg.Phases,
		curProg.TCycle,
		phaseID,
		p.bufferedProgram.Load().GetStartPhaseId(),
	)

	if phasesLeft == 0 || phasesLeftSeconds == 0 {
		return 0
	}

	// actual delta. Should always be same for each phase in program.
	deltaSeconds := uint32(deadlineSeconds-phasesLeftSeconds) / uint32(phasesLeft)

	// to add leap second to first phase in program, usually - 1.
	// there will always should be at least one phase in phases slice.
	if int(deadlineSeconds-phasesLeftSeconds)%phasesLeft > 0 {
		for _, ph := range curProg.Phases {
			if ph.IsHidden {
				continue
			}

			if phaseID == ph.Id { // first non-hidden phase of program, usually - 1, but...
				deltaSeconds++
			}

			break
		}
	}

	return deltaSeconds
}

func (p *rcProgramState) GetTimeLeftForCurrentPhase(nowTime time.Time) time.Duration {
	if nowTime.IsZero() {
		return 0
	}

	var (
		expectedPhaseID    = p.eStater.GetExpectedPhaseID()
		expectedPhaseStart = p.eStater.GetExpectedPhaseStart()
		phaseSeconds       uint32
	)

	for _, ph := range p.currentProgram.Load().Phases {
		if ph.Id == expectedPhaseID {
			phaseSeconds = ph.TOsn + ph.TProm

			break
		}
	}

	return expectedPhaseStart.Add(time.Duration(phaseSeconds) * time.Second).Sub(nowTime)
}

func (p *rcProgramState) GetTimeLeftForBufferedProgramToApply(nowTime time.Time) time.Duration {
	bufProgStartTime := p.bufferedProgramStartTime.GetTime()
	if bufProgStartTime.IsZero() {
		return 0
	}

	return bufProgStartTime.Sub(nowTime).Round(time.Second)
}

func (p *rcProgramState) GetMinimalPossibleStartTime(nowTime time.Time) time.Time {
	curProg := p.currentProgram.Load()

	curProgTCycle := curProg.TCycle
	if curProgTCycle == 0 {
		return nowTime
	}

	var minimalDuration uint32

	expectedPhaseID := p.eStater.GetExpectedPhaseID()

	for _, ph := range curProg.Phases {
		if expectedPhaseID == ph.Id {
			minimalDuration = minmax.GetMaxValue(ph.TProm, ph.TMin)
		}
	}

	minimalStartTime := p.eStater.GetExpectedPhaseStart().Add(time.Duration(minimalDuration) * time.Second)
	if minimalStartTime.After(nowTime) {
		return minimalStartTime
	}

	return nowTime
}

func (p *rcProgramState) ResetLocalProgramCounter() {
	p.programGenerator.Reset()
}
