package metering

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	policypkg "github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"go.uber.org/zap"
)

const producerPrefix = "netd.byte_windows"

type txRecorder interface {
	AppendWindow(ctx context.Context, window *meteringpkg.Window) error
	UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error
}

type Recorder interface {
	RunInTx(ctx context.Context, fn func(tx txRecorder) error) error
}

type quotaStore interface {
	GetLimit(ctx context.Context, teamID string, dimension quota.Dimension) (*quota.Limit, error)
	CurrentUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, error)
}

type repositoryAdapter struct {
	repo *meteringpkg.Repository
}

func NewRecorder(repo *meteringpkg.Repository) Recorder {
	if repo == nil {
		return nil
	}
	return &repositoryAdapter{repo: repo}
}

func (r *repositoryAdapter) RunInTx(ctx context.Context, fn func(tx txRecorder) error) error {
	return r.repo.InTx(ctx, func(tx pgx.Tx) error {
		return fn(&repositoryTxAdapter{repo: r.repo, tx: tx})
	})
}

type repositoryTxAdapter struct {
	repo *meteringpkg.Repository
	tx   pgx.Tx
}

func (r *repositoryTxAdapter) AppendWindow(ctx context.Context, window *meteringpkg.Window) error {
	return r.repo.AppendWindowTx(ctx, r.tx, window)
}

func (r *repositoryTxAdapter) UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	return r.repo.UpsertProducerWatermarkTx(ctx, r.tx, producer, regionID, completeBefore)
}

type usageTotals struct {
	sandboxID string
	teamID    string
	ownerKind string
	egress    int64
	ingress   int64
}

type Aggregator struct {
	recorder    Recorder
	regionID    string
	clusterID   string
	nodeName    string
	producer    string
	quotaStore  quotaStore
	logger      *zap.Logger
	now         func() time.Time
	mu          sync.Mutex
	windowStart time.Time
	usage       map[string]*usageTotals
}

func NewAggregator(recorder Recorder, regionID, clusterID, nodeName string, logger *zap.Logger) *Aggregator {
	if logger == nil {
		logger = zap.NewNop()
	}
	agg := &Aggregator{
		recorder:  recorder,
		regionID:  regionID,
		clusterID: clusterID,
		nodeName:  nodeName,
		producer:  producerName(nodeName),
		logger:    logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
		usage: make(map[string]*usageTotals),
	}
	agg.windowStart = agg.now()
	return agg
}

func (a *Aggregator) SetQuotaStore(store quotaStore) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.quotaStore = store
}

func (a *Aggregator) AllowEgress(compiled *policypkg.CompiledPolicy) error {
	return a.allowNetworkUsage(context.Background(), compiled, quota.DimensionEgress)
}

func (a *Aggregator) AllowIngress(compiled *policypkg.CompiledPolicy) error {
	return a.allowNetworkUsage(context.Background(), compiled, quota.DimensionIngress)
}

func (a *Aggregator) RecordEgress(compiled *policypkg.CompiledPolicy, bytes int64) {
	a.record(compiled, bytes, true)
}

func (a *Aggregator) RecordIngress(compiled *policypkg.CompiledPolicy, bytes int64) {
	a.record(compiled, bytes, false)
}

func (a *Aggregator) record(compiled *policypkg.CompiledPolicy, bytes int64, egress bool) {
	if a == nil || a.recorder == nil || compiled == nil || compiled.SandboxID == "" || bytes <= 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	entry := a.usage[compiled.SandboxID]
	if entry == nil {
		entry = &usageTotals{
			sandboxID: compiled.SandboxID,
			teamID:    compiled.TeamID,
			ownerKind: compiled.OwnerKind,
		}
		a.usage[compiled.SandboxID] = entry
	}
	if entry.ownerKind == "" {
		entry.ownerKind = compiled.OwnerKind
	}
	if egress {
		entry.egress += bytes
	} else {
		entry.ingress += bytes
	}
}

func (a *Aggregator) allowNetworkUsage(ctx context.Context, compiled *policypkg.CompiledPolicy, dimension quota.Dimension) error {
	if a == nil || compiled == nil || compiled.TeamID == "" {
		return nil
	}
	a.mu.Lock()
	store := a.quotaStore
	a.mu.Unlock()
	if store == nil {
		return nil
	}
	limit, err := store.GetLimit(ctx, compiled.TeamID, dimension)
	if err != nil {
		return err
	}
	if limit == nil {
		return nil
	}
	current, err := store.CurrentUsage(ctx, compiled.TeamID, dimension)
	if err != nil {
		return err
	}
	a.mu.Lock()
	current += a.localNetworkUsageLocked(compiled.TeamID, dimension)
	a.mu.Unlock()
	decision := quota.Check(compiled.TeamID, dimension, current, 1, limit)
	return decision.Err()
}

func (a *Aggregator) localNetworkUsageLocked(teamID string, dimension quota.Dimension) int64 {
	var total int64
	for _, usage := range a.usage {
		if usage == nil || usage.teamID != teamID {
			continue
		}
		switch dimension {
		case quota.DimensionEgress:
			total += usage.egress
		case quota.DimensionIngress:
			total += usage.ingress
		}
	}
	return total
}

func (a *Aggregator) Flush(ctx context.Context) error {
	if a == nil || a.recorder == nil {
		return nil
	}

	end := a.now()
	a.mu.Lock()
	start := a.windowStart
	if start.IsZero() {
		start = end
	}
	snapshot := cloneUsage(a.usage)
	a.mu.Unlock()

	if end.Before(start) {
		end = start
	}

	err := a.recorder.RunInTx(ctx, func(tx txRecorder) error {
		for _, usage := range snapshot {
			if usage.egress > 0 {
				if err := tx.AppendWindow(ctx, a.buildWindow(usage, meteringpkg.WindowTypeSandboxEgressBytes, start, end, usage.egress)); err != nil {
					return err
				}
			}
			if usage.ingress > 0 {
				if err := tx.AppendWindow(ctx, a.buildWindow(usage, meteringpkg.WindowTypeSandboxIngressBytes, start, end, usage.ingress)); err != nil {
					return err
				}
			}
		}
		return tx.UpsertProducerWatermark(ctx, a.producer, a.regionID, end)
	})
	if err != nil {
		a.logger.Error("Failed to flush netd metering windows",
			zap.String("producer", a.producer),
			zap.Error(err),
		)
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.windowStart = end
	for sandboxID, usage := range snapshot {
		current := a.usage[sandboxID]
		if current == nil {
			continue
		}
		current.egress -= usage.egress
		current.ingress -= usage.ingress
		if current.egress < 0 {
			current.egress = 0
		}
		if current.ingress < 0 {
			current.ingress = 0
		}
		if current.egress == 0 && current.ingress == 0 {
			delete(a.usage, sandboxID)
		}
	}
	return nil
}

func (a *Aggregator) buildWindow(usage *usageTotals, windowType string, start, end time.Time, value int64) *meteringpkg.Window {
	subjectType := meteringpkg.SubjectTypeSandbox
	subjectID := usage.sandboxID
	return &meteringpkg.Window{
		WindowID:    windowID(a.producer, usage.sandboxID, windowType, start, end),
		Producer:    a.producer,
		RegionID:    a.regionID,
		WindowType:  windowType,
		SubjectType: subjectType,
		SubjectID:   subjectID,
		TeamID:      usage.teamID,
		SandboxID:   usage.sandboxID,
		ClusterID:   a.clusterID,
		WindowStart: start,
		WindowEnd:   end,
		Value:       value,
		Unit:        meteringpkg.WindowUnitBytes,
		Data: mustJSON(map[string]any{
			"node_name":  a.nodeName,
			"product":    usageProduct(usage),
			"owner_kind": usage.ownerKind,
		}),
	}
}

func usageProduct(usage *usageTotals) string {
	return meteringpkg.ProductSandbox
}

func producerName(nodeName string) string {
	if nodeName == "" {
		return producerPrefix
	}
	return fmt.Sprintf("%s/%s", producerPrefix, nodeName)
}

func windowID(producer, sandboxID, windowType string, start, end time.Time) string {
	return fmt.Sprintf("%s/%s/%s/%d/%d", producer, sandboxID, windowType, start.UTC().UnixNano(), end.UTC().UnixNano())
}

func cloneUsage(in map[string]*usageTotals) map[string]*usageTotals {
	out := make(map[string]*usageTotals, len(in))
	for key, value := range in {
		if value == nil {
			continue
		}
		copied := *value
		out[key] = &copied
	}
	return out
}

func mustJSON(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}
