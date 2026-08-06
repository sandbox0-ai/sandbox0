package rootfsmaintenance

import (
	"context"
	"testing"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type typedNilStorageMeteringRecorder struct{}

func (*typedNilStorageMeteringRecorder) RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error {
	panic("typed-nil recorder should not be called")
}

func TestControllerClearsTypedNilRecorder(t *testing.T) {
	var recorder *typedNilStorageMeteringRecorder
	controller := New(nil, nil, Config{}, nil, nil)

	controller.SetStorageMeteringRecorder(recorder)

	if controller.meteringRecorder != nil {
		t.Fatal("typed-nil rootfs storage metering recorder should not be stored")
	}
}
