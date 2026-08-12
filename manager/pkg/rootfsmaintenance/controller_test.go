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

func TestControllersUseUniqueInventoryWorkerIDs(t *testing.T) {
	first := New(nil, nil, Config{}, nil, nil)
	second := New(nil, nil, Config{}, nil, nil)
	if first.workerID == "" || second.workerID == "" {
		t.Fatal("rootfs inventory worker id must not be empty")
	}
	if first.workerID == second.workerID {
		t.Fatalf("rootfs inventory worker ids must be unique: %q", first.workerID)
	}
}
