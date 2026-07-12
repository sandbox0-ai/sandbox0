package metering

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	policypkg "github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
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

type fakeQuotaStore struct {
	limit   *quota.Limit
	current int64
}

func (f *fakeQuotaStore) GetLimit(context.Context, string, quota.Dimension) (*quota.Limit, error) {
	return f.limit, nil
}

func (f *fakeQuotaStore) CurrentUsage(context.Context, string, quota.Dimension) (int64, error) {
	return f.current, nil
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
	if agg.pending == nil || agg.pending.usage["sb-1"] == nil || agg.pending.usage["sb-1"].egress != 50 {
		t.Fatalf("pending usage not retained after failure: %#v", agg.pending)
	}
	failedStart := agg.pending.start
	failedEnd := agg.pending.end
	agg.mu.Unlock()
	if !failedStart.Equal(start) || !failedEnd.Equal(end) {
		t.Fatalf("failed window = [%v, %v], want [%v, %v]", failedStart, failedEnd, start, end)
	}

	nextEnd := end.Add(10 * time.Second)
	agg.now = func() time.Time { return nextEnd }
	agg.RecordEgress(&policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"}, 25)
	recorder.tx.appendErr = nil
	if err := agg.Flush(context.Background()); err != nil {
		t.Fatalf("retry Flush: %v", err)
	}
	if len(recorder.tx.windows) != 2 {
		t.Fatalf("window count after retry = %d, want 2", len(recorder.tx.windows))
	}
	first, second := recorder.tx.windows[0], recorder.tx.windows[1]
	if first.Value != 50 || !first.WindowStart.Equal(start) || !first.WindowEnd.Equal(end) {
		t.Fatalf("retried window = %#v, want stable failed interval", first)
	}
	if second.Value != 25 || !second.WindowStart.Equal(end) || !second.WindowEnd.Equal(nextEnd) {
		t.Fatalf("residual window = %#v, want separate next interval", second)
	}
}

func TestAggregatorAllowEgressRejectsAtQuotaLimit(t *testing.T) {
	agg := NewAggregator(&fakeRecorder{}, "aws-us-east-1", "cluster-a", "node-1", nil)
	agg.SetQuotaStore(&fakeQuotaStore{
		limit: &quota.Limit{
			TeamID:     "team-1",
			Dimension:  quota.DimensionEgress,
			LimitValue: 10,
		},
		current: 10,
	})

	err := agg.AllowEgress(&policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"})
	if !quota.IsExceeded(err) {
		t.Fatalf("AllowEgress error = %v, want quota exceeded", err)
	}
}

func TestAggregatorAllowEgressIncludesUnflushedUsage(t *testing.T) {
	agg := NewAggregator(&fakeRecorder{}, "aws-us-east-1", "cluster-a", "node-1", nil)
	agg.SetQuotaStore(&fakeQuotaStore{
		limit: &quota.Limit{
			TeamID:     "team-1",
			Dimension:  quota.DimensionEgress,
			LimitValue: 10,
		},
		current: 8,
	})
	compiled := &policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"}
	agg.RecordEgress(compiled, 2)

	err := agg.AllowEgress(compiled)
	if !quota.IsExceeded(err) {
		t.Fatalf("AllowEgress error = %v, want quota exceeded", err)
	}
}

func TestAggregatorAllowIngressRejectsAtQuotaLimit(t *testing.T) {
	agg := NewAggregator(&fakeRecorder{}, "aws-us-east-1", "cluster-a", "node-1", nil)
	agg.SetQuotaStore(&fakeQuotaStore{
		limit: &quota.Limit{
			TeamID:     "team-1",
			Dimension:  quota.DimensionIngress,
			LimitValue: 10,
		},
		current: 10,
	})

	err := agg.AllowIngress(&policypkg.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"})
	if !quota.IsExceeded(err) {
		t.Fatalf("AllowIngress error = %v, want quota exceeded", err)
	}
}
