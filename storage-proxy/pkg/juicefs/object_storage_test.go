package juicefs

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

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

func TestObservedObjectStorageRecordsRequestsAndBytes(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewStorageProxy(registry)
	base, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("CreateStorage() error = %v", err)
	}
	store := newObservedObjectStorage(base, "gcs", "sandbox0-data", metrics)
	key := "sandboxvolumes-sync/team-1/vol-1/replay/sha256/hash"

	if err := store.Put(key, bytes.NewReader([]byte("hello"))); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	reader, err := store.Get(key, 0, -1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got, err := io.ReadAll(reader); err != nil || string(got) != "hello" {
		t.Fatalf("ReadAll() = %q, %v; want hello", string(got), err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := testutil.ToFloat64(metrics.ObjectStoreRequestsTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "put", "success")); got != 1 {
		t.Fatalf("put requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ObjectStoreRequestsTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "get", "success")); got != 1 {
		t.Fatalf("get requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ObjectStoreBytesTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "put", "write")); got != 5 {
		t.Fatalf("put bytes = %v, want 5", got)
	}
	if got := testutil.ToFloat64(metrics.ObjectStoreBytesTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "get", "read")); got != 5 {
		t.Fatalf("get bytes = %v, want 5", got)
	}
}

func TestObservedObjectStorageRecordsProviderRateLimitStatus(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewStorageProxy(registry)
	base, err := object.CreateStorage("mem", "", "", "", "")
	if err != nil {
		t.Fatalf("CreateStorage() error = %v", err)
	}
	store := newObservedObjectStorage(&rateLimitedObjectStore{ObjectStorage: base}, "gs", "sandbox0-data", metrics)

	err = store.Put("sandboxvolumes-sync/team-1/vol-1/replay/sha256/e3b0", bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("Put() error = nil, want rate limit error")
	}
	if got := testutil.ToFloat64(metrics.ObjectStoreRequestsTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "put", "429")); got != 1 {
		t.Fatalf("429 put requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ObjectStoreBytesTotal.WithLabelValues("gs", "sandbox0-data", "volume_sync_replay", "put", "write")); got != 1 {
		t.Fatalf("failed put bytes = %v, want 1", got)
	}
}

func TestClassifyObjectStorePrefix(t *testing.T) {
	tests := map[string]string{
		"":                                       "none",
		".juicefs":                               "juicefs_metadata",
		"sandboxvolumes/team-1/vol-1/chunks/0/0": "volume_data",
		"sandboxvolumes-sync/team-1/vol-1/replay/sha256/hash": "volume_sync_replay",
		"sandboxvolumes-sync/team-1/vol-1/other":              "volume_sync",
		"other/key":                                           "other",
	}
	for key, want := range tests {
		if got := classifyObjectStorePrefix(key); got != want {
			t.Fatalf("classifyObjectStorePrefix(%q) = %q, want %q", key, got, want)
		}
	}
}

type rateLimitedObjectStore struct {
	object.ObjectStorage
}

func (s *rateLimitedObjectStore) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	_, _ = io.Copy(io.Discard, in)
	return errors.New("googleapi: Error 429: rateLimitExceeded")
}
