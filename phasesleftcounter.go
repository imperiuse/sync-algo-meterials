package phasescounter

import (
	pb "gitlab-tc.dev.codd.local/theseus/rc-client/external/proto-core-rc/v1"
)

const oneHourInSeconds = 3600

type (
	Phase  = pb.RC_Phase
	Phases = []*pb.RC_Phase
)

func getPhasesLeftRaw(
	phasesLeft int,
	phasesLeftSeconds int64,
	deadlineSeconds int64,
	phases Phases,
	curPhaseNumber int,
	startPhaseNumber int,
) (int, int64) {
	tMain := phases[curPhaseNumber].TOsn
	tInterim := phases[curPhaseNumber].TProm
	phaseT := int64(tMain + tInterim)

	if !phases[curPhaseNumber].IsHidden {
		deadlineSeconds -= phaseT
		if deadlineSeconds <= 0 && curPhaseNumber == startPhaseNumber {
			return phasesLeft, phasesLeftSeconds
		}

		phasesLeftSeconds += phaseT
		phasesLeft++
	}

	curPhaseNumber++
	if curPhaseNumber >= len(phases) {
		curPhaseNumber = 0
	}

	return getPhasesLeftRaw(
		phasesLeft,
		phasesLeftSeconds,
		deadlineSeconds,
		phases,
		curPhaseNumber,
		startPhaseNumber)
}

func GetPhasesLeft(
	deadlineSeconds int64,
	phases Phases,
	currentProgramTCycle uint32,
	currentPhaseID uint32,
	startPhaseID uint32,
) (phasesLeft int, phasesLeftSeconds int64) {
	if len(phases) == 0 ||
		currentProgramTCycle == 0 ||
		deadlineSeconds <= 0 ||
		deadlineSeconds > oneHourInSeconds {
		return
	}

	var (
		startPhaseNumberFound bool
		startPhaseNumber      int

		curPhaseNumberFound bool
		curPhaseNumber      int

		lenPhasesNonHidden int
	)

	for i, ph := range phases {
		if ph == nil {
			return 0, 0
		} else if !ph.IsHidden {
			lenPhasesNonHidden++

			if ph.Id == currentPhaseID {
				curPhaseNumberFound = true
				curPhaseNumber = i
			}

			if ph.Id == startPhaseID {
				startPhaseNumberFound = true
				startPhaseNumber = i
			}
		}
	}

	if !curPhaseNumberFound || !startPhaseNumberFound {
		return
	}

	phasesLeft, phasesLeftSeconds = getPhasesLeftRaw(
		0,
		0,
		deadlineSeconds,
		phases,
		curPhaseNumber,
		startPhaseNumber)

	if phasesLeftSeconds > deadlineSeconds {
		phasesLeft -= lenPhasesNonHidden
		phasesLeftSeconds -= int64(currentProgramTCycle)
	}

	return
}
