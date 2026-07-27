package requestmetering

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

type fakeTxRecorder struct {
	windows           []*meteringpkg.Window
	watermarkProducer string
	watermarkRegion   string
	watermarkTime     time.Time
	appendErr         error
}

func (f *fakeTxRecorder) AppendWindow(_ context.Context, window *meteringpkg.Window) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.windows = append(f.windows, window)
	return nil
}

func (f *fakeTxRecorder) UpsertProducerWatermark(_ context.Context, producer, regionID string, completeBefore time.Time) error {
	f.watermarkProducer = producer
	f.watermarkRegion = regionID
	f.watermarkTime = completeBefore
	return nil
}

type fakeRecorder struct {
	tx       *fakeTxRecorder
	runCalls int
}

func (f *fakeRecorder) RunInTx(ctx context.Context, fn func(txRecorder) error) error {
	f.runCalls++
	if f.tx == nil {
		f.tx = &fakeTxRecorder{}
	}
	return fn(f.tx)
}

func TestAggregatorFlushesAttributedOSSRequestWindows(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{}}
	aggregator := NewAggregator(recorder, "ali-ue1", "cluster-1", ProducerName(ProducerManager, "manager-1"), nil)
	start := time.Date(2026, 7, 27, 1, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	aggregator.windowStart = start
	aggregator.now = func() time.Time { return end }

	aggregator.ObserveRequestAttempt(objectstore.RequestAttempt{
		Provider:   objectstore.TypeOSS,
		Bucket:     "runtime-bucket",
		Operation:  "GetObject",
		Key:        "sandboxvolumes/team-1/volume-1/s0fs/segments/a",
		StatusCode: http.StatusPartialContent,
	})
	aggregator.ObserveRequestAttempt(objectstore.RequestAttempt{
		Provider:   objectstore.TypeOSS,
		Bucket:     "runtime-bucket",
		Operation:  "ListObjectsV2",
		Key:        "sandbox-rootfs/team-2/sandbox-2/1",
		StatusCode: http.StatusOK,
	})
	aggregator.ObserveRequestAttempt(objectstore.RequestAttempt{
		Provider:   objectstore.TypeOSS,
		Bucket:     "runtime-bucket",
		Operation:  "PutObject",
		Key:        "clickhouse/ali-ue1/data/a",
		StatusCode: http.StatusOK,
	})

	if err := aggregator.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if len(recorder.tx.windows) != 3 {
		t.Fatalf("window count = %d, want 3", len(recorder.tx.windows))
	}

	windows := windowsByOperation(t, recorder.tx.windows)
	volume := windows["GetObject"]
	if volume.WindowType != meteringpkg.WindowTypeSandboxObjectStoreGetRequests ||
		volume.TeamID != "team-1" ||
		volume.SubjectType != meteringpkg.SubjectTypeVolume ||
		volume.SubjectID != "volume-1" ||
		volume.VolumeID != "volume-1" ||
		volume.Value != 1 ||
		volume.Unit != meteringpkg.WindowUnitCount {
		t.Fatalf("unexpected volume window: %+v", volume)
	}
	rootfs := windows["ListObjectsV2"]
	if rootfs.WindowType != meteringpkg.WindowTypeSandboxObjectStorePutRequests ||
		rootfs.TeamID != "team-2" ||
		rootfs.SubjectType != meteringpkg.SubjectTypeRootFS ||
		rootfs.SandboxID != "sandbox-2" {
		t.Fatalf("unexpected rootfs window: %+v", rootfs)
	}
	platform := windows["PutObject"]
	if platform.TeamID != "" || platform.SubjectType != meteringpkg.SubjectTypeObjectStoreBucket {
		t.Fatalf("unexpected platform window: %+v", platform)
	}
	platformData := windowData(t, platform)
	if platformData["cost_scope"] != CostScopePlatform || platformData["prefix_class"] != "clickhouse" {
		t.Fatalf("unexpected platform data: %#v", platformData)
	}
	if recorder.tx.watermarkProducer != "manager.object_store_requests/manager-1" ||
		recorder.tx.watermarkRegion != "ali-ue1" ||
		!recorder.tx.watermarkTime.Equal(end) {
		t.Fatalf("unexpected watermark: %#v", recorder.tx)
	}
}

func TestAggregatorCountsRepeatedAttempts(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{}}
	aggregator := NewAggregator(recorder, "ali-ue1", "cluster-1", ProducerCtld, nil)
	for range 3 {
		aggregator.ObserveRequestAttempt(objectstore.RequestAttempt{
			Provider:   objectstore.TypeOSS,
			Bucket:     "runtime-bucket",
			Operation:  "PutObject",
			Key:        "sandboxvolumes/team-1/volume-1/s0fs/heads/current",
			StatusCode: http.StatusOK,
		})
	}
	if err := aggregator.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if len(recorder.tx.windows) != 1 || recorder.tx.windows[0].Value != 3 {
		t.Fatalf("windows = %#v, want one count=3 window", recorder.tx.windows)
	}
}

func TestAggregatorIgnoresNonBillableAttempts(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{}}
	aggregator := NewAggregator(recorder, "ali-ue1", "cluster-1", ProducerCtld, nil)
	for _, attempt := range []objectstore.RequestAttempt{
		{Provider: objectstore.TypeOSS, Operation: "GetObject", StatusCode: http.StatusNotFound},
		{Provider: objectstore.TypeOSS, Operation: "PutObject", StatusCode: http.StatusInternalServerError},
		{Provider: objectstore.TypeOSS, Operation: "PutObject", StatusCode: 0},
		{Provider: objectstore.TypeOSS, Operation: "UnknownOperation", StatusCode: http.StatusOK},
		{Provider: objectstore.TypeS3, Operation: "GetObject", StatusCode: http.StatusOK},
	} {
		aggregator.ObserveRequestAttempt(attempt)
	}
	if err := aggregator.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if len(recorder.tx.windows) != 0 {
		t.Fatalf("windows = %#v, want none", recorder.tx.windows)
	}
	if recorder.tx.watermarkTime.IsZero() {
		t.Fatal("watermark was not advanced")
	}
}

func TestAggregatorFlushFailureRetainsStableBatch(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{appendErr: errors.New("boom")}}
	aggregator := NewAggregator(recorder, "ali-ue1", "cluster-1", ProducerCtld, nil)
	start := time.Date(2026, 7, 27, 2, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	aggregator.windowStart = start
	aggregator.now = func() time.Time { return end }
	aggregator.ObserveRequestAttempt(objectstore.RequestAttempt{
		Provider: objectstore.TypeOSS, Bucket: "runtime-bucket", Operation: "GetObject",
		Key: "sandboxvolumes/team-1/volume-1/a", StatusCode: http.StatusOK,
	})

	if err := aggregator.Flush(context.Background()); err == nil {
		t.Fatal("Flush() error = nil, want failure")
	}
	aggregator.mu.Lock()
	failed := aggregator.pending
	aggregator.mu.Unlock()
	if failed == nil || len(failed.usage) != 1 {
		t.Fatalf("pending batch = %#v", failed)
	}

	recorder.tx.appendErr = nil
	if err := aggregator.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush() error = %v", err)
	}
	if len(recorder.tx.windows) != 1 {
		t.Fatalf("window count = %d, want 1", len(recorder.tx.windows))
	}
	if !recorder.tx.windows[0].WindowStart.Equal(start) || !recorder.tx.windows[0].WindowEnd.Equal(end) {
		t.Fatalf("window interval = [%s, %s]", recorder.tx.windows[0].WindowStart, recorder.tx.windows[0].WindowEnd)
	}
}

func TestClassifyAttribution(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		scope       string
		prefixClass string
		teamID      string
		subjectType string
		subjectID   string
	}{
		{
			name: "volume", key: "sandboxvolumes/team-a/volume-a/s0fs/head",
			scope: CostScopeCustomer, prefixClass: "volume_data", teamID: "team-a",
			subjectType: meteringpkg.SubjectTypeVolume, subjectID: "volume-a",
		},
		{
			name: "rootfs", key: "sandbox-rootfs/team-a/sandbox-a/1/sha256/a.tar",
			scope: CostScopeCustomer, prefixClass: "rootfs_data", teamID: "team-a",
			subjectType: meteringpkg.SubjectTypeRootFS, subjectID: "sandbox-a",
		},
		{
			name: "legacy", key: ".juicefs/chunks/1",
			scope: CostScopeUnattributed, prefixClass: "legacy_juicefs",
			subjectType: meteringpkg.SubjectTypeObjectStoreBucket, subjectID: "runtime-bucket",
		},
		{
			name: "bucket", key: "",
			scope: CostScopePlatform, prefixClass: "bucket",
			subjectType: meteringpkg.SubjectTypeObjectStoreBucket, subjectID: "runtime-bucket",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyAttribution("runtime-bucket", tt.key)
			if got.costScope != tt.scope || got.prefixClass != tt.prefixClass ||
				got.teamID != tt.teamID || got.subjectType != tt.subjectType || got.subjectID != tt.subjectID {
				t.Fatalf("classifyAttribution() = %+v", got)
			}
		})
	}
}

func windowsByOperation(t *testing.T, windows []*meteringpkg.Window) map[string]*meteringpkg.Window {
	t.Helper()
	result := make(map[string]*meteringpkg.Window, len(windows))
	for _, window := range windows {
		data := windowData(t, window)
		result[data["operation"]] = window
	}
	return result
}

func windowData(t *testing.T, window *meteringpkg.Window) map[string]string {
	t.Helper()
	var data map[string]string
	if err := json.Unmarshal(window.Data, &data); err != nil {
		t.Fatalf("unmarshal window data: %v", err)
	}
	return data
}
