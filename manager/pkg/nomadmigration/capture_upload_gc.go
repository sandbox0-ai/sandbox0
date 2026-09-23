package nomadmigration

import (
	"context"
	"errors"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

type CaptureUploadGCStore interface {
	ListNomadMigrationCaptureUploadGC(context.Context, string, int) ([]string, error)
	AuthorizeNomadMigrationCaptureUploadGC(context.Context, string) (*runtimecheckpoint.CaptureScope, error)
	CompleteNomadMigrationCaptureUploadGC(context.Context, runtimecheckpoint.CaptureScope) error
}

type CheckpointCaptureUploadGCStore interface {
	ListNomadCheckpointCaptureUploadGC(context.Context, string, int) ([]string, error)
	AuthorizeNomadCheckpointCaptureUploadGC(context.Context, string) (*runtimecheckpoint.CaptureScope, error)
	CompleteNomadCheckpointCaptureUploadGC(context.Context, runtimecheckpoint.CaptureScope) error
}

type CaptureUploadCollector interface {
	CollectCapture(context.Context, runtimecheckpoint.CaptureScope) (bool, error)
}

// NewCaptureUploadGC also handles uploads whose producer failed before final
// publication. It shares the bounded coordinator and requires exact terminal
// regional authority; a missing manifest or an old timestamp is insufficient.
func NewCaptureUploadGC(store CaptureUploadGCStore, objects CaptureUploadCollector) (*Coordinator, error) {
	if store == nil || objects == nil {
		return nil, errors.New("capture upload GC authorities are required")
	}
	return newCaptureUploadGC(store.ListNomadMigrationCaptureUploadGC, store.AuthorizeNomadMigrationCaptureUploadGC,
		store.CompleteNomadMigrationCaptureUploadGC, objects), nil
}

// Checkpoint capture scopes use the same bounded collector but have their own
// lifecycle gate. Published scopes cannot be reclaimed before image GC.
func NewCheckpointCaptureUploadGC(store CheckpointCaptureUploadGCStore, objects CaptureUploadCollector) (*Coordinator, error) {
	if store == nil || objects == nil {
		return nil, errors.New("checkpoint capture upload GC authorities are required")
	}
	worker := newCaptureUploadGC(store.ListNomadCheckpointCaptureUploadGC, store.AuthorizeNomadCheckpointCaptureUploadGC,
		store.CompleteNomadCheckpointCaptureUploadGC, objects)
	worker.idleDelay = 30 * time.Second
	return worker, nil
}

func newCaptureUploadGC(list func(context.Context, string, int) ([]string, error),
	authorize func(context.Context, string) (*runtimecheckpoint.CaptureScope, error),
	complete func(context.Context, runtimecheckpoint.CaptureScope) error, objects CaptureUploadCollector) *Coordinator {
	return &Coordinator{list: list, step: func(ctx context.Context, id string) (bool, error) {
		scope, err := authorize(ctx, id)
		if err != nil || scope == nil {
			return false, err
		}
		if _, err := scope.Digest(); err != nil || scope.OperationID() != id {
			return false, errors.New("capture upload GC changed source scope")
		}
		done, err := objects.CollectCapture(ctx, *scope)
		if err != nil || !done {
			return false, err
		}
		err = complete(ctx, *scope)
		return err == nil, err
	}}
}
