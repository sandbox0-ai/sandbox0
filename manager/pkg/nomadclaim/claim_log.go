package nomadclaim

import (
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"go.uber.org/zap/zapcore"
)

// claimPhaseLogs reuses the planner's same-request observations. Correlated
// phases distinguish control-plane and readiness waits from nested node timings
// without another clock, metric label, or copy of the claim state machine.
type claimPhaseLogs []runtimeslotclaim.PhaseObservation

func (phases claimPhaseLogs) MarshalLogArray(encoder zapcore.ArrayEncoder) error {
	for _, phase := range phases {
		if err := encoder.AppendObject(claimPhaseLog(phase)); err != nil {
			return err
		}
	}
	return nil
}

type claimPhaseLog runtimeslotclaim.PhaseObservation

func (phase claimPhaseLog) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("phase", phase.Phase)
	encoder.AddInt64("durationMicros", phase.Duration.Microseconds())
	encoder.AddBool("succeeded", phase.Succeeded)
	return nil
}
