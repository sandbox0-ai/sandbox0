package service

import (
	"context"
	"testing"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type typedNilRootFSStorageMeteringRecorder struct{}

func (*typedNilRootFSStorageMeteringRecorder) RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error {
	panic("typed-nil recorder should not be called")
}

func TestConfiguredRootFSStorageMeteringRecorderRejectsTypedNil(t *testing.T) {
	var recorder *typedNilRootFSStorageMeteringRecorder
	if _, ok := configuredRootFSStorageMeteringRecorder(recorder); ok {
		t.Fatal("typed-nil rootfs storage metering recorder should be treated as disabled")
	}
}

func TestRecordRootFSStorageObservationsIgnoresTypedNilRecorder(t *testing.T) {
	var recorder *typedNilRootFSStorageMeteringRecorder
	store := &PGSandboxStore{}

	usages, err := store.RecordRootFSStorageObservations(context.Background(), recorder, "", time.Now())
	if err != nil {
		t.Fatalf("RecordRootFSStorageObservations() error = %v", err)
	}
	if usages != nil {
		t.Fatalf("RecordRootFSStorageObservations() usages = %v, want nil", usages)
	}
}

func TestRootFSMaintenanceControllerClearsTypedNilRecorder(t *testing.T) {
	var recorder *typedNilRootFSStorageMeteringRecorder
	controller := NewRootFSMaintenanceController(nil, nil, RootFSMaintenanceControllerConfig{}, nil, nil)

	controller.SetStorageMeteringRecorder(recorder)

	if controller.meteringRecorder != nil {
		t.Fatal("typed-nil rootfs storage metering recorder should not be stored")
	}
}
