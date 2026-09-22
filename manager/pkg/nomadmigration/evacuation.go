package nomadmigration

import (
	"context"
	"errors"
)

// EvacuationStore discovers current workloads on regionally fenced draining
// nodes. The store owns source revalidation, destination selection and bounded
// retry admission; there is no user-selected node or mutable work queue.
type EvacuationStore interface {
	ListNomadMigrationEvacuations(context.Context, string, int) ([]string, error)
	ReserveNomadMigrationEvacuation(context.Context, string) (bool, error)
}

func NewEvacuation(store EvacuationStore) (*Coordinator, error) {
	if store == nil {
		return nil, errors.New("migration evacuation store is required")
	}
	c := &Coordinator{list: store.ListNomadMigrationEvacuations, step: store.ReserveNomadMigrationEvacuation, progress: &Progress{}}
	c.drainWatcher, _ = store.(drainWatcher)
	return c, nil
}
