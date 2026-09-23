package runtimeslot

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// CheckpointRestoreAuthority binds an independently scheduled use of a retained
// execution image. The regional lifecycle persists this before dispatch. It
// grants image custody only; the writer-bound restore command grants execution.
// Keeping it separate from publication allows many forks to share image bytes.
type CheckpointRestoreAuthority struct {
	Assignment     runtimecontrol.CheckpointRestoreAssignment `json:"assignment"`
	LifecycleEpoch int64                                      `json:"lifecycle_epoch"`
	Retained       CheckpointRetained                         `json:"retained"`
}

func (a CheckpointRestoreAuthority) ValidateFor(publication MigrationPublicationRequest, receipt MigrationPublication) error {
	if publication.CheckpointSource == nil || a.LifecycleEpoch <= 0 {
		return fmt.Errorf("checkpoint restore requires its independent lifecycle and source")
	}
	if err := a.Assignment.Validate(); err != nil {
		return err
	}
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment(publication.Capture.Request.OperationID, *publication.CheckpointSource)
	if err != nil || a.Assignment.Capture != capture || a.Assignment.OperationID == capture.OperationID {
		return fmt.Errorf("checkpoint restore changed capture identity or reused its operation")
	}
	if a.Retained.CheckpointID != capture.OperationID {
		return fmt.Errorf("checkpoint restore changed retained image owner")
	}
	return a.Retained.ValidateFor(publication, receipt)
}
