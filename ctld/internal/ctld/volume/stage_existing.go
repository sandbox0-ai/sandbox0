package volume

import (
	"context"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

type existingStageProvider struct {
	controller *Controller
}

func (p existingStageProvider) EnsureStaged(_ context.Context, req ctldapi.VolumeAttachRequest) (string, error) {
	return p.controller.stagedVolumePath(req.SandboxVolumeID)
}

func (p existingStageProvider) Release(_ context.Context, _ ctldapi.VolumeDetachRequest) error {
	return nil
}
