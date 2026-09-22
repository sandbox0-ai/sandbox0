package nomadruntime

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// MigrationCaptureRecovery has no driver dependency and no checkpoint/start
// method. A recovered intent is still only intent, never permission to replay.
type MigrationCaptureRecovery interface {
	RecoverMigrationCapture(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
}

func (d *nodeRuntime) RecoverMigrationCapture(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	digest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	custody, err := d.GetMigrationCapture(ctx, request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if custody == nil {
		return nil, errdefs.ErrNotFound
	}
	if custody.Capture.RequestDigest != digest || custody.Capture.Request != request {
		return nil, errdefs.ErrAlreadyExists
	}
	if custody.Capture.State == protocol.MigrationCaptureComplete && custody.Capture.RootFS == nil {
		// SealMigrationRootFS owns per-slot reconciliation and verifies that
		// execution is stopped before freezing/checkpointing the exact writer.
		// If the source ran again, it invalidates the image instead of killing
		// the source to manufacture a consistent cut.
		if _, err := d.SealMigrationRootFS(ctx, request); err != nil {
			return nil, err
		}
		custody, err = d.GetMigrationCapture(ctx, request.Target.SlotID)
		if err != nil {
			return nil, err
		}
		if custody == nil || custody.Capture.RequestDigest != digest || custody.Capture.Request != request {
			return nil, fmt.Errorf("migration recovery changed source custody: %w", errdefs.ErrFailedPrecondition)
		}
	}
	capture := custody.Capture
	if err := capture.Validate(); err != nil {
		return nil, err
	}
	return &capture, nil
}
