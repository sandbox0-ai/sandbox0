package requestmetering

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"go.uber.org/zap"
)

const (
	ProducerManager = "manager.object_store_requests"
	ProducerCtld    = "ctld.object_store_requests"

	DefaultFlushInterval = time.Minute

	BillingItemGetRequest = "GetRequest"
	BillingItemPutRequest = "PutRequest"

	CostScopeCustomer     = "customer"
	CostScopePlatform     = "platform"
	CostScopeUnattributed = "unattributed"
)

type txRecorder interface {
	AppendWindow(context.Context, *meteringpkg.Window) error
	UpsertProducerWatermark(context.Context, string, string, time.Time) error
}

// Recorder commits one aggregate batch and its watermark atomically.
type Recorder interface {
	RunInTx(context.Context, func(txRecorder) error) error
}

type repository interface {
	InTx(context.Context, func(pgx.Tx) error) error
	AppendWindowTx(context.Context, pgx.Tx, *meteringpkg.Window) error
	UpsertProducerWatermarkTx(context.Context, pgx.Tx, string, string, time.Time) error
}

type repositoryRecorder struct {
	repo repository
}

// NewRecorder adapts the PostgreSQL metering outbox for request aggregation.
func NewRecorder(repo repository) Recorder {
	if repo == nil {
		return nil
	}
	return &repositoryRecorder{repo: repo}
}

func (r *repositoryRecorder) RunInTx(ctx context.Context, fn func(txRecorder) error) error {
	return r.repo.InTx(ctx, func(tx pgx.Tx) error {
		return fn(&repositoryTxRecorder{repo: r.repo, tx: tx})
	})
}

type repositoryTxRecorder struct {
	repo repository
	tx   pgx.Tx
}

func (r *repositoryTxRecorder) AppendWindow(ctx context.Context, window *meteringpkg.Window) error {
	return r.repo.AppendWindowTx(ctx, r.tx, window)
}

func (r *repositoryTxRecorder) UpsertProducerWatermark(ctx context.Context, producer, regionID string, completeBefore time.Time) error {
	return r.repo.UpsertProducerWatermarkTx(ctx, r.tx, producer, regionID, completeBefore)
}

type attribution struct {
	costScope   string
	prefixClass string
	subjectType string
	subjectID   string
	teamID      string
	sandboxID   string
	volumeID    string
}

type usageKey struct {
	attribution
	provider    string
	bucket      string
	billingItem string
	windowType  string
	operation   string
}

type usageBatch struct {
	start time.Time
	end   time.Time
	usage map[usageKey]int64
}

// Aggregator converts successful OSS provider attempts into low-volume,
// attributed usage windows. It implements objectstore.RequestObserver.
type Aggregator struct {
	recorder  Recorder
	regionID  string
	clusterID string
	producer  string
	logger    *zap.Logger
	now       func() time.Time

	mu          sync.Mutex
	windowStart time.Time
	usage       map[usageKey]int64
	pending     *usageBatch
}

// NewAggregator creates a one-minute request usage aggregator backed by the
// PostgreSQL metering outbox.
func NewAggregator(recorder Recorder, regionID, clusterID, producer string, logger *zap.Logger) *Aggregator {
	if logger == nil {
		logger = zap.NewNop()
	}
	aggregator := &Aggregator{
		recorder:  recorder,
		regionID:  strings.TrimSpace(regionID),
		clusterID: strings.TrimSpace(clusterID),
		producer:  strings.TrimSpace(producer),
		logger:    logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
		usage: make(map[usageKey]int64),
	}
	if aggregator.producer == "" {
		aggregator.producer = ProducerManager
	}
	aggregator.windowStart = aggregator.now()
	return aggregator
}

// ProducerName adds a stable workload or node identity to a producer name.
func ProducerName(base, instance string) string {
	base = strings.Trim(strings.TrimSpace(base), "/")
	instance = strings.Trim(strings.TrimSpace(instance), "/")
	if instance == "" {
		return base
	}
	return base + "/" + instance
}

func (a *Aggregator) ObserveRequestAttempt(attempt objectstore.RequestAttempt) {
	if a == nil || a.recorder == nil {
		return
	}
	windowType, billingItem, ok := billableUsage(attempt)
	if !ok {
		return
	}
	attr := classifyAttribution(attempt.Bucket, attempt.Key)
	key := usageKey{
		attribution: attr,
		provider:    strings.ToLower(strings.TrimSpace(attempt.Provider)),
		bucket:      strings.TrimSpace(attempt.Bucket),
		billingItem: billingItem,
		windowType:  windowType,
		operation:   strings.TrimSpace(attempt.Operation),
	}

	a.mu.Lock()
	a.usage[key]++
	a.mu.Unlock()
}

func billableUsage(attempt objectstore.RequestAttempt) (string, string, bool) {
	if !strings.EqualFold(strings.TrimSpace(attempt.Provider), objectstore.TypeOSS) {
		return "", "", false
	}
	if attempt.StatusCode < 200 || attempt.StatusCode >= 400 {
		return "", "", false
	}
	switch strings.TrimSpace(attempt.Operation) {
	case "CreateBucket", "DeleteObject", "ListObjects", "ListObjectsV2", "PutObject":
		return meteringpkg.WindowTypeSandboxObjectStorePutRequests, BillingItemPutRequest, true
	case "GetObject", "HeadBucket", "HeadObject":
		return meteringpkg.WindowTypeSandboxObjectStoreGetRequests, BillingItemGetRequest, true
	default:
		return "", "", false
	}
}

func classifyAttribution(bucket, key string) attribution {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		bucket = "unknown"
	}
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	if cleanKey == "" {
		return bucketAttribution(bucket, CostScopePlatform, "bucket")
	}
	parts := strings.Split(cleanKey, "/")
	switch parts[0] {
	case "sandboxvolumes":
		if len(parts) >= 3 && parts[1] != "" && parts[2] != "" {
			return attribution{
				costScope:   CostScopeCustomer,
				prefixClass: "volume_data",
				subjectType: meteringpkg.SubjectTypeVolume,
				subjectID:   parts[2],
				teamID:      parts[1],
				volumeID:    parts[2],
			}
		}
		return bucketAttribution(bucket, CostScopeUnattributed, "volume_unknown")
	case "sandbox-rootfs":
		if len(parts) >= 3 && parts[1] != "" && parts[2] != "" {
			return attribution{
				costScope:   CostScopeCustomer,
				prefixClass: "rootfs_data",
				subjectType: meteringpkg.SubjectTypeRootFS,
				subjectID:   parts[2],
				teamID:      parts[1],
				sandboxID:   parts[2],
			}
		}
		return bucketAttribution(bucket, CostScopeUnattributed, "rootfs_unknown")
	case "clickhouse":
		return bucketAttribution(bucket, CostScopePlatform, "clickhouse")
	case "migration", "sandboxvolumes-sync":
		return bucketAttribution(bucket, CostScopePlatform, "migration")
	case ".juicefs", ".juicefs-init-marker":
		return bucketAttribution(bucket, CostScopeUnattributed, "legacy_juicefs")
	default:
		return bucketAttribution(bucket, CostScopeUnattributed, "other")
	}
}

func bucketAttribution(bucket, costScope, prefixClass string) attribution {
	return attribution{
		costScope:   costScope,
		prefixClass: prefixClass,
		subjectType: meteringpkg.SubjectTypeObjectStoreBucket,
		subjectID:   bucket,
	}
}

// Run periodically flushes attributed requests until ctx is canceled.
func (a *Aggregator) Run(ctx context.Context, interval time.Duration) {
	if a == nil || a.recorder == nil {
		return
	}
	if interval <= 0 {
		interval = DefaultFlushInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = a.Flush(ctx)
		}
	}
}

// Flush commits all completed request aggregates. Failed batches remain
// pending with stable IDs so a later call can retry them idempotently.
func (a *Aggregator) Flush(ctx context.Context) error {
	if a == nil || a.recorder == nil {
		return nil
	}
	for {
		batch, retrying := a.pendingBatch()
		err := a.recorder.RunInTx(ctx, func(tx txRecorder) error {
			for _, key := range sortedUsageKeys(batch.usage) {
				if err := tx.AppendWindow(ctx, a.buildWindow(key, batch.start, batch.end, batch.usage[key])); err != nil {
					return err
				}
			}
			return tx.UpsertProducerWatermark(ctx, a.producer, a.regionID, batch.end)
		})
		if err != nil {
			a.logger.Error("Failed to flush object store request metering windows",
				zap.String("producer", a.producer),
				zap.Time("window_start", batch.start),
				zap.Time("window_end", batch.end),
				zap.Error(err),
			)
			return err
		}

		a.mu.Lock()
		if a.pending == batch {
			a.pending = nil
		}
		hasResidual := len(a.usage) > 0
		a.mu.Unlock()
		if !retrying || !hasResidual {
			return nil
		}
	}
}

func (a *Aggregator) pendingBatch() (*usageBatch, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending != nil {
		return a.pending, true
	}
	end := a.now()
	start := a.windowStart
	if start.IsZero() {
		start = end
	}
	if end.Before(start) {
		end = start
	}
	a.pending = &usageBatch{
		start: start,
		end:   end,
		usage: a.usage,
	}
	a.usage = make(map[usageKey]int64)
	a.windowStart = end
	return a.pending, false
}

func (a *Aggregator) buildWindow(key usageKey, start, end time.Time, value int64) *meteringpkg.Window {
	return &meteringpkg.Window{
		WindowID:    requestWindowID(a.producer, key, start, end),
		Producer:    a.producer,
		RegionID:    a.regionID,
		WindowType:  key.windowType,
		SubjectType: key.subjectType,
		SubjectID:   key.subjectID,
		TeamID:      key.teamID,
		SandboxID:   key.sandboxID,
		VolumeID:    key.volumeID,
		ClusterID:   a.clusterID,
		WindowStart: start,
		WindowEnd:   end,
		Value:       value,
		Unit:        meteringpkg.WindowUnitCount,
		Data: mustJSON(map[string]string{
			"billing_item": key.billingItem,
			"bucket":       key.bucket,
			"cost_scope":   key.costScope,
			"operation":    key.operation,
			"prefix_class": key.prefixClass,
			"product":      meteringpkg.ProductSandbox,
			"provider":     key.provider,
		}),
	}
}

func sortedUsageKeys(usage map[usageKey]int64) []usageKey {
	keys := make([]usageKey, 0, len(usage))
	for key := range usage {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return usageKeyIdentity(keys[i]) < usageKeyIdentity(keys[j])
	})
	return keys
}

func requestWindowID(producer string, key usageKey, start, end time.Time) string {
	sum := sha256.Sum256([]byte(usageKeyIdentity(key)))
	return fmt.Sprintf(
		"%s/%s/%d/%d",
		producer,
		hex.EncodeToString(sum[:8]),
		start.UTC().UnixNano(),
		end.UTC().UnixNano(),
	)
}

func usageKeyIdentity(key usageKey) string {
	return strings.Join([]string{
		key.costScope,
		key.prefixClass,
		key.subjectType,
		key.subjectID,
		key.teamID,
		key.sandboxID,
		key.volumeID,
		key.provider,
		key.bucket,
		key.billingItem,
		key.windowType,
		key.operation,
	}, "\x00")
}

func mustJSON(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}
