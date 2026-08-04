package sandboxstore

import (
	"context"
	"testing"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type typedNilStorageMeteringRecorder struct{}

func (*typedNilStorageMeteringRecorder) RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error {
	panic("typed-nil recorder should not be called")
}

func TestConfiguredRootFSStorageMeteringRecorderRejectsTypedNil(t *testing.T) {
	var recorder *typedNilStorageMeteringRecorder
	if _, ok := ConfiguredRootFSStorageMeteringRecorder(recorder); ok {
		t.Fatal("typed-nil rootfs storage metering recorder should be treated as disabled")
	}
}

func TestRecordRootFSStorageObservationsIgnoresTypedNilRecorder(t *testing.T) {
	var recorder *typedNilStorageMeteringRecorder
	store := &PGSandboxStore{}

	usages, err := store.RecordRootFSStorageObservations(context.Background(), recorder, "", time.Now())
	if err != nil {
		t.Fatalf("RecordRootFSStorageObservations() error = %v", err)
	}
	if usages != nil {
		t.Fatalf("RecordRootFSStorageObservations() usages = %v, want nil", usages)
	}
}
