package juicefs

import "testing"

func TestNormalizeObjectStorageTypeMapsGCS(t *testing.T) {
	if got := NormalizeObjectStorageType("gcs"); got != ObjectStorageTypeGCS {
		t.Fatalf("expected gs, got %q", got)
	}
	if got := NormalizeObjectStorageType("builtin"); got != ObjectStorageTypeS3 {
		t.Fatalf("expected builtin to map to s3, got %q", got)
	}
	if got := NormalizeObjectStorageType(""); got != ObjectStorageTypeS3 {
		t.Fatalf("expected default s3, got %q", got)
	}
}

func TestBuildObjectStorageEndpointForGCS(t *testing.T) {
	storageType, endpoint, err := BuildObjectStorageEndpoint(ObjectStorageConfig{
		Type:   "gcs",
		Bucket: "sandbox0-bucket",
	})
	if err != nil {
		t.Fatalf("BuildObjectStorageEndpoint returned error: %v", err)
	}
	if storageType != ObjectStorageTypeGCS {
		t.Fatalf("unexpected storage type: %q", storageType)
	}
	if endpoint != "gs://sandbox0-bucket" {
		t.Fatalf("unexpected endpoint: %q", endpoint)
	}
}

func TestBuildObjectStorageEndpointForS3RequiresRegionOrEndpoint(t *testing.T) {
	_, _, err := BuildObjectStorageEndpoint(ObjectStorageConfig{Type: "s3", Bucket: "sandbox0-bucket"})
	if err == nil || err.Error() != "object storage region or endpoint is required for s3" {
		t.Fatalf("unexpected error: %v", err)
	}
}
