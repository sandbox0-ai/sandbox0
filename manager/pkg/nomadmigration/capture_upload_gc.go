package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

type CaptureUploadGCStore interface {
	ListNomadMigrationCaptureUploadGC(context.Context, string, int) ([]string, error)
	AuthorizeNomadMigrationCaptureUploadGC(context.Context, string) (*runtimecheckpoint.CaptureScope, error)
	CompleteNomadMigrationCaptureUploadGC(context.Context, runtimecheckpoint.CaptureScope) error
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
	return &Coordinator{list: store.ListNomadMigrationCaptureUploadGC, step: func(ctx context.Context, id string) (bool, error) {
		scope, err := store.AuthorizeNomadMigrationCaptureUploadGC(ctx, id)
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
		err = store.CompleteNomadMigrationCaptureUploadGC(ctx, *scope)
		return err == nil, err
	}}, nil
}
