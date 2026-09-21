package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

type ImageGCStore interface {
	ListNomadMigrationImageGC(context.Context, string, int) ([]string, error)
	AuthorizeNomadMigrationImageGC(context.Context, string) (*runtimecheckpoint.Binding, error)
	CompleteNomadMigrationImageGC(context.Context, runtimecheckpoint.Binding) error
}

type ImageCollector interface {
	Collect(context.Context, runtimecheckpoint.Binding) (bool, error)
}

// NewImageGC reuses terminal migration receipts; age alone never authorizes
// deletion. Each pass removes a bounded page, and retries reuse the same binding.
func NewImageGC(store ImageGCStore, objects ImageCollector) (*Coordinator, error) {
	if store == nil || objects == nil {
		return nil, errors.New("migration image GC authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationImageGC, step: func(ctx context.Context, id string) (bool, error) {
		binding, err := store.AuthorizeNomadMigrationImageGC(ctx, id)
		if err != nil || binding == nil {
			return false, err
		}
		if binding.Validate() != nil || binding.OperationID != id {
			return false, errors.New("migration image GC changed binding")
		}
		done, err := objects.Collect(ctx, *binding)
		if err != nil || !done {
			return false, err
		}
		err = store.CompleteNomadMigrationImageGC(ctx, *binding)
		return err == nil, err
	}}, nil
}
