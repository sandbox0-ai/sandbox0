package snapshot

import (
	"testing"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestApplyStorageObservationMetadataMarksFunctionStorage(t *testing.T) {
	obs := &meteringpkg.StorageObservation{
		SubjectType: meteringpkg.SubjectTypeVolume,
		SubjectID:   "vol-1",
		Product:     meteringpkg.ProductSandbox,
		VolumeID:    "vol-1",
	}
	metadata := &meteringpkg.StorageObservation{
		FunctionID:         "fn-1",
		FunctionRevisionID: "rev-1",
	}

	got := applyStorageObservationMetadata(obs, metadata)

	if got.Product != meteringpkg.ProductFunction {
		t.Fatalf("product = %q, want %q", got.Product, meteringpkg.ProductFunction)
	}
	if got.FunctionID != "fn-1" || got.FunctionRevisionID != "rev-1" {
		t.Fatalf("function metadata = %q/%q, want fn-1/rev-1", got.FunctionID, got.FunctionRevisionID)
	}
	if got.SubjectID != "vol-1" || got.VolumeID != "vol-1" {
		t.Fatalf("subject changed unexpectedly: %+v", got)
	}
}
