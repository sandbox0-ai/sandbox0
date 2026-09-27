package nomadclaim

import (
	"context"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
)

type resourceResizePauseStore interface {
	RequestNomadSandboxResourceResizePause(context.Context, string, string) (*sandboxstore.NomadSandboxPauseCandidate, error)
}

// PauseSandboxForResourceResize uses planned filesystem retirement. It never
// restores memory or changes the live source's immutable resource lease.
func (s *Service) PauseSandboxForResourceResize(ctx context.Context, sandboxID, operationID string) error {
	store, ok := s.store.(resourceResizePauseStore)
	if !ok {
		return fmt.Errorf("%w: resource resize pause authority is unavailable", service.ErrSandboxRuntimeUpdateUnavailable)
	}
	candidate, err := store.RequestNomadSandboxResourceResizePause(ctx, sandboxID, operationID)
	if err != nil {
		return mapNomadSandboxPauseError(sandboxID, err)
	}
	if candidate.AlreadyPaused && candidate.SlotID == "" {
		return nil
	}
	s.enqueueNomadSandboxPause(sandboxID)
	if err := s.CompletePausingSandboxRuntime(ctx, sandboxID); err != nil && !errors.Is(err, errNomadSandboxPausePending) {
		return err
	}
	return nil
}
