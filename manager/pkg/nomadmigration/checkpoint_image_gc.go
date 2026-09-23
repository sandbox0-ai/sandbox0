package nomadmigration

import (
	"context"
	"errors"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

type CheckpointImageGCStore interface {
	ListNomadCheckpointImageGC(context.Context, string, int) ([]string, error)
	AuthorizeNomadCheckpointImageGC(context.Context, string) (*runtimecheckpoint.Binding, error)
	CompleteNomadCheckpointImageGC(context.Context, runtimecheckpoint.Binding) error
}

// Forks share the captured object prefix. The database retains it until the
// last paused owner, active restore and fork has released its reference.
func NewCheckpointImageGC(store CheckpointImageGCStore, objects ImageCollector) (*Coordinator, error) {
	if store == nil || objects == nil {
		return nil, errors.New("checkpoint image GC authorities are required")
	}
	return &Coordinator{list: store.ListNomadCheckpointImageGC, step: func(ctx context.Context, id string) (bool, error) {
		binding, err := store.AuthorizeNomadCheckpointImageGC(ctx, id)
		if err != nil || binding == nil {
			return false, err
		}
		if binding.Validate() != nil || binding.OperationID != id {
			return false, errors.New("checkpoint image GC changed binding")
		}
		done, err := objects.Collect(ctx, *binding)
		if err != nil || !done {
			return false, err
		}
		err = store.CompleteNomadCheckpointImageGC(ctx, *binding)
		return err == nil, err
	}, idleDelay: 30 * time.Second}, nil
}
