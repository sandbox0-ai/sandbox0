package metering

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	policypkg "github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type fakeTxRecorder struct {
	windows           []*meteringpkg.Window
	watermarkProducer string
	watermarkRegion   string
	watermarkTime     time.Time
	appendErr         error
	watermarkErr      error
}

func (f *fakeTxRecorder) AppendWindow(_ context.Context, window *meteringpkg.Window) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	f.windows = append(f.windows, window)
	return nil
}

func (f *fakeTxRecorder) UpsertProducerWatermark(_ context.Context, producer string, regionID string, completeBefore time.Time) error {
	if f.watermarkErr != nil {
		return f.watermarkErr
	}
	f.watermarkProducer = producer
	f.watermarkRegion = regionID
	f.watermarkTime = completeBefore
	return nil
}

type fakeRecorder struct {
	tx       *fakeTxRecorder
	runCalls int
	runErr   error
}

func (f *fakeRecorder) RunInTx(ctx context.Context, fn func(tx txRecorder) error) error {
	f.runCalls++
	if f.runErr != nil {
		return f.runErr
	}
	if f.tx == nil {
		f.tx = &fakeTxRecorder{}
	}
	return fn(f.tx)
}

func TestAggregatorFlushesIngressAndEgressWindows(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{}}
	agg := NewAggregator(recorder, "aws-us-east-1", "cluster-a", "node-1", nil)
	start := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	agg.now = func() time.Time { return end }
	agg.windowStart = start

	compiled := &policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"}
	agg.RecordEgress(compiled, 120)
	agg.RecordIngress(compiled, 240)

	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(recorder.tx.windows) != 2 {
		t.Fatalf("window count = %d, want 2", len(recorder.tx.windows))
	}
	if recorder.tx.windows[0].WindowType != meteringpkg.WindowTypeSandboxEgressBytes || recorder.tx.windows[0].Value != 120 {
		t.Fatalf("unexpected first window: %+v", recorder.tx.windows[0])
	}
	if recorder.tx.windows[1].WindowType != meteringpkg.WindowTypeSandboxIngressBytes || recorder.tx.windows[1].Value != 240 {
		t.Fatalf("unexpected second window: %+v", recorder.tx.windows[1])
	}
	if recorder.tx.watermarkProducer != "netd.byte_windows/node-1" {
		t.Fatalf("producer = %q, want %q", recorder.tx.watermarkProducer, "netd.byte_windows/node-1")
	}
	if !recorder.tx.watermarkTime.Equal(end) {
		t.Fatalf("watermark = %v, want %v", recorder.tx.watermarkTime, end)
	}

	payload := map[string]any{}
	if err := json.Unmarshal(recorder.tx.windows[0].Data, &payload); err != nil {
		t.Fatalf("unmarshal window payload: %v", err)
	}
	if payload["node_name"] != "node-1" {
		t.Fatalf("node_name = %v, want node-1", payload["node_name"])
	}
}

func TestAggregatorFlushWithoutUsageStillAdvancesWatermark(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{}}
	agg := NewAggregator(recorder, "aws-us-east-1", "cluster-a", "node-2", nil)
	start := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	agg.now = func() time.Time { return end }
	agg.windowStart = start

	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(recorder.tx.windows) != 0 {
		t.Fatalf("window count = %d, want 0", len(recorder.tx.windows))
	}
	if !recorder.tx.watermarkTime.Equal(end) {
		t.Fatalf("watermark = %v, want %v", recorder.tx.watermarkTime, end)
	}
}

func TestAggregatorFlushFailureRetainsUsage(t *testing.T) {
	recorder := &fakeRecorder{tx: &fakeTxRecorder{appendErr: errors.New("boom")}}
	agg := NewAggregator(recorder, "aws-us-east-1", "cluster-a", "node-3", nil)
	start := time.Date(2026, 3, 12, 10, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Second)
	agg.now = func() time.Time { return end }
	agg.windowStart = start

	agg.RecordEgress(&policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"}, 50)

	if err := agg.Flush(context.Background()); err == nil {
		t.Fatal("expected flush error")
	}

	agg.mu.Lock()
	defer agg.mu.Unlock()
	if agg.usage["sb-1"] == nil || agg.usage["sb-1"].egress != 50 {
		t.Fatalf("usage not retained after failure: %#v", agg.usage["sb-1"])
	}
	if !agg.windowStart.Equal(start) {
		t.Fatalf("window start advanced on failure: %v", agg.windowStart)
	}
}
