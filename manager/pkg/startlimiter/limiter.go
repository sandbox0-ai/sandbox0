package startlimiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/pglock"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"go.uber.org/zap"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const (
	defaultPerSandboxNode = int32(30)
	defaultMaxLimit       = int32(80)
	defaultRetryAfter     = time.Second
	defaultReservationTTL = 5 * time.Minute
	redisScriptAttempts   = 3

	labelTemplateID = "sandbox0.ai/template-id"
	labelPoolType   = "sandbox0.ai/pool-type"

	startPressurePodIndexName  = "claimStartPressure"
	startPressurePodIndexValue = "candidate"

	// AnnotationClaimStartReservation marks a pod whose start is covered by an
	// active Redis reservation, so pressure snapshots do not double-count it.
	AnnotationClaimStartReservation = "sandbox0.ai/claim-start-reservation"

	poolTypeIdle   = "idle"
	poolTypeActive = "active"

	BackendPostgres = "postgres_advisory_lock"
	BackendRedis    = "redis"
)

var redisReservationScript = redis.NewScript(`
local reservation_key = KEYS[1]
local units_key = KEYS[2]
local metadata_key = KEYS[3]

local now_ms = tonumber(ARGV[1])
local expires_at_ms = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
local limit = tonumber(ARGV[4])
local observed_pressure = tonumber(ARGV[5])
local requested = tonumber(ARGV[6])
local reservation_token = ARGV[7]

local expired = redis.call("ZRANGEBYSCORE", reservation_key, "-inf", now_ms)
if #expired > 0 then
  redis.call("ZREM", reservation_key, unpack(expired))
  redis.call("HDEL", units_key, unpack(expired))
end

local active_count = redis.call("ZCARD", reservation_key)
local metadata = redis.call("HMGET", metadata_key, "count", "units")
local metadata_count = tonumber(metadata[1])
local reserved_units = tonumber(metadata[2])
if #expired > 0 or metadata_count == nil or reserved_units == nil or metadata_count ~= active_count then
  reserved_units = 0
  local active = redis.call("ZRANGE", reservation_key, 0, -1)
  if #active > 0 then
    local active_units = redis.call("HMGET", units_key, unpack(active))
    for _, value in ipairs(active_units) do
      local units = tonumber(value)
      if units == nil or units <= 0 then
        units = 1
      end
      reserved_units = reserved_units + units
    end
  end
  if active_count > 0 then
    redis.call("HSET", metadata_key, "count", active_count, "units", reserved_units)
    redis.call("PEXPIRE", metadata_key, ttl_ms)
  else
    redis.call("DEL", metadata_key)
  end
end

local overlap_units = 0
local seen = {}
local overlap_tokens = {}
for index = 8, #ARGV do
  local token = ARGV[index]
  if token ~= "" and seen[token] == nil then
    seen[token] = true
    table.insert(overlap_tokens, token)
  end
end
if #overlap_tokens > 0 then
  local active_tokens = redis.call("ZRANGE", reservation_key, 0, -1)
  local active = {}
  for _, token in ipairs(active_tokens) do
    active[token] = true
  end
  local units_by_token = redis.call("HMGET", units_key, unpack(overlap_tokens))
  for index, token in ipairs(overlap_tokens) do
    if active[token] then
      local units = tonumber(units_by_token[index])
      if units == nil or units <= 0 then
        units = 1
      end
      overlap_units = overlap_units + units
    end
  end
end

local in_flight = observed_pressure + reserved_units - overlap_units
if in_flight < 0 then
  in_flight = 0
end
local available = limit - in_flight
if available < 0 then
  available = 0
end

if requested <= 0 then
  return {1, in_flight, available}
end

if redis.call("ZSCORE", reservation_key, reservation_token) ~= false then
  local existing_units = tonumber(redis.call("HGET", units_key, reservation_token))
  if existing_units == nil or existing_units ~= requested then
    return redis.error_reply("claim start reservation token units mismatch")
  end
  return {1, in_flight, available}
end
if requested > available then
  return {0, in_flight, available}
end
redis.call("ZADD", reservation_key, expires_at_ms, reservation_token)
redis.call("HSET", units_key, reservation_token, requested)
redis.call("HSET", metadata_key, "count", active_count + 1, "units", reserved_units + requested)
redis.call("PEXPIRE", reservation_key, ttl_ms)
redis.call("PEXPIRE", units_key, ttl_ms)
redis.call("PEXPIRE", metadata_key, ttl_ms)
return {1, in_flight, available}
`)

var redisReservationReleaseScript = redis.NewScript(`
local reservation_key = KEYS[1]
local units_key = KEYS[2]
local metadata_key = KEYS[3]
local token = ARGV[1]

local units = tonumber(redis.call("HGET", units_key, token))
if units == nil or units <= 0 then
  units = 1
end
local removed = redis.call("ZREM", reservation_key, token)
redis.call("HDEL", units_key, token)
if removed == 0 then
  return 0
end

local active_count = redis.call("ZCARD", reservation_key)
local metadata = redis.call("HMGET", metadata_key, "count", "units")
local metadata_count = tonumber(metadata[1])
local reserved_units = tonumber(metadata[2])
if metadata_count == nil or reserved_units == nil or metadata_count ~= active_count + 1 then
  redis.call("DEL", metadata_key)
  return 1
end

if active_count == 0 then
  redis.call("DEL", metadata_key)
else
  reserved_units = reserved_units - units
  if reserved_units < 0 then
    reserved_units = 0
  end
  redis.call("HSET", metadata_key, "count", active_count, "units", reserved_units)
end
return 1
`)

// Reason describes the caller reserving cluster start budget.
type Reason string

const (
	ReasonHotClaim      Reason = "hot_claim"
	ReasonColdCreate    Reason = "cold_create"
	ReasonPoolReconcile Reason = "pool_reconcile"
)

var (
	// ErrThrottled is returned when the cluster-wide claim/start budget is full.
	ErrThrottled = errors.New("claim start admission throttled")
)

type ThrottledError struct {
	Reason     Reason
	Units      int32
	RetryAfter time.Duration
	Snapshot   Snapshot
}

func (e *ThrottledError) Error() string {
	if e == nil {
		return ErrThrottled.Error()
	}
	if e.Snapshot.Limit <= 0 {
		return fmt.Sprintf("%s: no sandbox-ready nodes", ErrThrottled)
	}
	return fmt.Sprintf("%s: in_flight=%d limit=%d requested=%d",
		ErrThrottled, e.Snapshot.InFlight, e.Snapshot.Limit, e.Units)
}

func (e *ThrottledError) Is(target error) bool {
	return target == ErrThrottled
}

// RetryAfter extracts the preferred retry delay from a throttling error.
func RetryAfter(err error) time.Duration {
	var throttled *ThrottledError
	if errors.As(err, &throttled) && throttled.RetryAfter > 0 {
		return throttled.RetryAfter
	}
	return defaultRetryAfter
}

type Snapshot struct {
	Backend               string
	WarmReadySandboxNodes int
	Limit                 int32
	InFlight              int32
	Available             int32
}

type Config struct {
	ClusterID           string
	K8sClient           kubernetes.Interface
	NodeLister          corelisters.NodeLister
	PodLister           corelisters.PodLister
	PodIndexer          cache.Indexer
	ReplicaSetLister    appslisters.ReplicaSetLister
	PGPool              *pgxpool.Pool
	Redis               rediscache.Config
	PerSandboxNode      int32
	MaxLimit            int32
	SandboxNodeSelector map[string]string
	SandboxTolerations  []corev1.Toleration
	Logger              *zap.Logger
}

type Limiter struct {
	k8sClient           kubernetes.Interface
	nodeLister          corelisters.NodeLister
	podLister           corelisters.PodLister
	podIndexer          cache.Indexer
	replicaSetLister    appslisters.ReplicaSetLister
	pgLocker            *pglock.Locker
	redisClient         *redis.Client
	redisTimeout        time.Duration
	backend             string
	lockResource        string
	reservationKey      string
	reservationUnitsKey string
	reservationMetaKey  string
	reservationTTL      time.Duration
	perSandboxNode      int32
	maxLimit            int32
	sandboxSelector     map[string]string
	sandboxTolerations  []corev1.Toleration
	podSelector         string
	replicaSetSelector  string
	logger              *zap.Logger
}

// Reservation holds cluster-wide start budget for a claim that has passed
// admission but is still waiting for the sandbox runtime to become ready.
type Reservation struct {
	limiter *Limiter
	token   string
	once    sync.Once
}

// Token returns the opaque reservation token that should be copied to created
// pods via AnnotationClaimStartReservation.
func (r *Reservation) Token() string {
	if r == nil {
		return ""
	}
	return r.token
}

// Release releases a Redis reservation. It is safe to call more than once.
func (r *Reservation) Release() {
	if r == nil || r.limiter == nil || r.token == "" {
		return
	}
	r.once.Do(func() {
		r.limiter.releaseReservation(r.token)
	})
}

// New creates a cluster-wide limiter for claim and warm-pool pod starts.
func New(ctx context.Context, cfg Config) (*Limiter, error) {
	if cfg.K8sClient == nil && !hasCachedListers(cfg) {
		return nil, fmt.Errorf("kubernetes client or cached listers are required")
	}
	if cfg.PerSandboxNode <= 0 {
		cfg.PerSandboxNode = defaultPerSandboxNode
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = defaultMaxLimit
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	podSelector, err := poolPodSelector()
	if err != nil {
		return nil, err
	}
	replicaSetSelector, err := templateReplicaSetSelector()
	if err != nil {
		return nil, err
	}
	if err := ensureStartPressurePodIndex(cfg.PodIndexer); err != nil {
		return nil, fmt.Errorf("configure claim start pressure pod index: %w", err)
	}

	limiter := &Limiter{
		k8sClient:          cfg.K8sClient,
		nodeLister:         cfg.NodeLister,
		podLister:          cfg.PodLister,
		podIndexer:         cfg.PodIndexer,
		replicaSetLister:   cfg.ReplicaSetLister,
		pgLocker:           pglock.New(cfg.PGPool),
		backend:            BackendPostgres,
		lockResource:       fmt.Sprintf("manager:claim-start:%s", cfg.ClusterID),
		perSandboxNode:     cfg.PerSandboxNode,
		maxLimit:           cfg.MaxLimit,
		sandboxSelector:    cloneStringMap(cfg.SandboxNodeSelector),
		sandboxTolerations: append([]corev1.Toleration(nil), cfg.SandboxTolerations...),
		podSelector:        podSelector,
		replicaSetSelector: replicaSetSelector,
		logger:             cfg.Logger,
	}

	if rediscache.Enabled(cfg.Redis) {
		redisCfg := cfg.Redis
		redisCfg.FailOpen = false
		redisClient, normalized, err := rediscache.NewClient(ctx, redisCfg)
		if err != nil {
			return nil, fmt.Errorf("initialize redis claim start limiter: %w", err)
		}
		limiter.redisClient = redisClient
		limiter.redisTimeout = normalized.Timeout
		limiter.backend = BackendRedis
		keyPrefix := rediscache.JoinKeyPrefix(normalized.KeyPrefix, "manager", "claim-start", cfg.ClusterID)
		limiter.reservationKey = rediscache.JoinKeyPrefix(keyPrefix, "reservations")
		limiter.reservationUnitsKey = rediscache.JoinKeyPrefix(keyPrefix, "reservation-units")
		limiter.reservationMetaKey = rediscache.JoinKeyPrefix(keyPrefix, "reservation-metadata")
		limiter.reservationTTL = defaultReservationTTL
	}

	return limiter, nil
}

func (l *Limiter) Backend() string {
	if l == nil {
		return ""
	}
	return l.backend
}

// SupportsReservations reports whether this limiter can hold admission slots
// beyond the immediate Kubernetes mutation.
func (l *Limiter) SupportsReservations() bool {
	return l != nil && l.redisClient != nil
}

// Admit runs mutate only if the cluster has enough remaining start budget.
func (l *Limiter) Admit(ctx context.Context, reason Reason, units int32, mutate func(context.Context) error) (*Snapshot, error) {
	if l == nil {
		if mutate != nil {
			return &Snapshot{}, mutate(ctx)
		}
		return &Snapshot{}, nil
	}
	if units <= 0 {
		units = 1
	}

	if l.redisClient != nil {
		reservation, snapshot, err := l.Reserve(ctx, reason, units)
		if err != nil {
			return snapshot, err
		}
		defer reservation.Release()
		if mutate == nil {
			return snapshot, nil
		}
		return snapshot, mutate(ctx)
	}

	var snapshot *Snapshot
	err := l.withPostgresLock(ctx, func(lockCtx context.Context) error {
		s, err := l.snapshotLocked(lockCtx)
		if err != nil {
			return err
		}
		snapshot = s
		if s.Available < units {
			return &ThrottledError{
				Reason:     reason,
				Units:      units,
				RetryAfter: defaultRetryAfter,
				Snapshot:   *s,
			}
		}
		if mutate == nil {
			return nil
		}
		return mutate(lockCtx)
	})
	return snapshot, err
}

// Reserve takes start budget without running a mutation. Redis-backed limiters
// hold the reservation until Release; other backends only perform the same
// guarded admission check as Admit.
func (l *Limiter) Reserve(ctx context.Context, reason Reason, units int32) (*Reservation, *Snapshot, error) {
	if l == nil {
		return nil, &Snapshot{}, nil
	}
	if units <= 0 {
		units = 1
	}
	if l.redisClient == nil {
		var snapshot *Snapshot
		err := l.withPostgresLock(ctx, func(lockCtx context.Context) error {
			s, err := l.snapshotLocked(lockCtx)
			if err != nil {
				return err
			}
			snapshot = s
			if s.Available < units {
				return &ThrottledError{
					Reason:     reason,
					Units:      units,
					RetryAfter: defaultRetryAfter,
					Snapshot:   *s,
				}
			}
			return nil
		})
		return nil, snapshot, err
	}

	observation, err := l.observeStartPressure(ctx)
	if err != nil {
		return nil, nil, err
	}
	token, err := randomToken()
	if err != nil {
		return nil, nil, err
	}
	admitted, snapshot, err := l.runRedisAdmission(ctx, observation, token, units)
	if err != nil {
		return nil, nil, err
	}
	if !admitted {
		return nil, snapshot, &ThrottledError{
			Reason:     reason,
			Units:      units,
			RetryAfter: defaultRetryAfter,
			Snapshot:   *snapshot,
		}
	}
	return &Reservation{limiter: l, token: token}, snapshot, nil
}

// Snapshot returns the current limiter pressure without taking the admission lock.
func (l *Limiter) Snapshot(ctx context.Context) (*Snapshot, error) {
	if l == nil {
		return nil, nil
	}
	if l.redisClient == nil {
		return l.snapshotLocked(ctx)
	}
	observation, err := l.observeStartPressure(ctx)
	if err != nil {
		return nil, err
	}
	_, snapshot, err := l.runRedisAdmission(ctx, observation, "", 0)
	return snapshot, err
}

func (l *Limiter) withPostgresLock(ctx context.Context, fn func(context.Context) error) error {
	return l.pgLocker.WithExclusive(ctx, l.lockResource, fn)
}

func (l *Limiter) snapshotLocked(ctx context.Context) (*Snapshot, error) {
	observation, err := l.observeStartPressure(ctx)
	if err != nil {
		return nil, err
	}
	return observation.snapshot(l.backend, observation.inFlight), nil
}

type startPressureObservation struct {
	warmReadySandboxNodes int
	limit                 int32
	inFlight              int32
	reservationTokens     []string
}

func (o *startPressureObservation) snapshot(backend string, inFlight int32) *Snapshot {
	available := o.limit - inFlight
	if available < 0 {
		available = 0
	}
	return &Snapshot{
		Backend:               backend,
		WarmReadySandboxNodes: o.warmReadySandboxNodes,
		Limit:                 o.limit,
		InFlight:              inFlight,
		Available:             available,
	}
}

func (l *Limiter) observeStartPressure(ctx context.Context) (*startPressureObservation, error) {
	nodes, err := l.listNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes for claim start limiter: %w", err)
	}
	warmReadyNodes := countWarmReadySandboxNodes(nodes, l.sandboxSelector, l.sandboxTolerations)
	limit := l.limitForNodes(warmReadyNodes)

	pods, err := l.listPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("list sandbox pods for claim start limiter: %w", err)
	}
	replicaSets, err := l.listReplicaSets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list pool replicasets for claim start limiter: %w", err)
	}

	inFlight, reservationTokens := startPressure(pods, replicaSets)
	return &startPressureObservation{
		warmReadySandboxNodes: warmReadyNodes,
		limit:                 limit,
		inFlight:              inFlight,
		reservationTokens:     reservationTokens,
	}, nil
}

func (l *Limiter) runRedisAdmission(
	ctx context.Context,
	observation *startPressureObservation,
	token string,
	units int32,
) (bool, *Snapshot, error) {
	if l == nil || l.redisClient == nil || observation == nil {
		return false, nil, fmt.Errorf("redis claim start admission is not configured")
	}
	now := time.Now()
	args := make([]interface{}, 0, 7+len(observation.reservationTokens))
	args = append(args,
		now.UnixMilli(),
		now.Add(l.reservationTTL).UnixMilli(),
		l.reservationTTL.Milliseconds(),
		observation.limit,
		observation.inFlight,
		units,
		token,
	)
	for _, reservationToken := range observation.reservationTokens {
		args = append(args, reservationToken)
	}

	result, err := l.runRedisReservationScript(ctx, redisReservationScript, args...)
	if err != nil {
		return false, nil, fmt.Errorf("run redis claim start admission: %w", err)
	}
	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		return false, nil, fmt.Errorf("unexpected redis claim start admission response: %T", result)
	}
	admitted, err := redisResultInt64(values[0])
	if err != nil {
		return false, nil, fmt.Errorf("parse redis claim start admission decision: %w", err)
	}
	inFlight, err := redisResultInt64(values[1])
	if err != nil {
		return false, nil, fmt.Errorf("parse redis claim start in-flight count: %w", err)
	}
	available, err := redisResultInt64(values[2])
	if err != nil {
		return false, nil, fmt.Errorf("parse redis claim start available count: %w", err)
	}
	if inFlight < 0 || inFlight > int64(^uint32(0)>>1) {
		return false, nil, fmt.Errorf("redis claim start in-flight count is out of range: %d", inFlight)
	}
	if available < 0 || available > int64(^uint32(0)>>1) {
		return false, nil, fmt.Errorf("redis claim start available count is out of range: %d", available)
	}
	snapshot := observation.snapshot(l.backend, int32(inFlight))
	snapshot.Available = int32(available)
	return admitted == 1, snapshot, nil
}

func redisResultInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected integer type %T", value)
	}
}

func (l *Limiter) runRedisReservationScript(
	ctx context.Context,
	script *redis.Script,
	args ...interface{},
) (interface{}, error) {
	// A timeout can happen after Redis committed the script. Admission and
	// release scripts are idempotent so the same operation can be retried safely.
	var (
		result interface{}
		err    error
	)
	for attempt := 0; attempt < redisScriptAttempts; attempt++ {
		opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
		result, err = script.Run(
			opCtx,
			l.redisClient,
			[]string{l.reservationKey, l.reservationUnitsKey, l.reservationMetaKey},
			args...,
		).Result()
		cancel()
		if err == nil {
			return result, nil
		}
		if attempt+1 >= redisScriptAttempts || ctx.Err() != nil || !redisTimeoutError(err) {
			return nil, err
		}
	}
	return nil, err
}

func redisTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var timeout interface {
		Timeout() bool
	}
	return errors.As(err, &timeout) && timeout.Timeout()
}

func (l *Limiter) listNodes(ctx context.Context) ([]*corev1.Node, error) {
	if l.nodeLister != nil {
		return l.nodeLister.List(labels.Everything())
	}
	nodes, err := l.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Node, 0, len(nodes.Items))
	for idx := range nodes.Items {
		out = append(out, &nodes.Items[idx])
	}
	return out, nil
}

func (l *Limiter) listPods(ctx context.Context) ([]*corev1.Pod, error) {
	if l.podIndexer != nil {
		items, err := l.podIndexer.ByIndex(startPressurePodIndexName, startPressurePodIndexValue)
		if err != nil {
			return nil, err
		}
		pods := make([]*corev1.Pod, 0, len(items))
		for _, item := range items {
			pod, ok := item.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("claim start pressure index contains %T, want *corev1.Pod", item)
			}
			pods = append(pods, pod)
		}
		return pods, nil
	}
	selector, err := labels.Parse(l.podSelector)
	if err != nil {
		return nil, err
	}
	if l.podLister != nil {
		return l.podLister.List(selector)
	}
	pods, err := l.k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: l.podSelector})
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, 0, len(pods.Items))
	for idx := range pods.Items {
		out = append(out, &pods.Items[idx])
	}
	return out, nil
}

func (l *Limiter) listReplicaSets(ctx context.Context) ([]*appsv1.ReplicaSet, error) {
	selector, err := labels.Parse(l.replicaSetSelector)
	if err != nil {
		return nil, err
	}
	if l.replicaSetLister != nil {
		return l.replicaSetLister.List(selector)
	}
	replicaSets, err := l.k8sClient.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: l.replicaSetSelector})
	if err != nil {
		return nil, err
	}
	out := make([]*appsv1.ReplicaSet, 0, len(replicaSets.Items))
	for idx := range replicaSets.Items {
		out = append(out, &replicaSets.Items[idx])
	}
	return out, nil
}

func (l *Limiter) releaseReservation(token string) {
	if l == nil || l.redisClient == nil || token == "" {
		return
	}
	if _, err := l.runRedisReservationScript(
		context.Background(),
		redisReservationReleaseScript,
		token,
	); err != nil {
		l.logger.Warn("Failed to release claim start reservation", zap.String("token", token), zap.Error(err))
	}
}

func (l *Limiter) limitForNodes(nodes int) int32 {
	if nodes <= 0 {
		return 0
	}
	limit := int32(nodes) * l.perSandboxNode
	if limit > l.maxLimit {
		return l.maxLimit
	}
	return limit
}

type poolKey struct {
	namespace  string
	templateID string
}

func startPressure(pods []*corev1.Pod, replicaSets []*appsv1.ReplicaSet) (int32, []string) {
	readyIdleByPool := make(map[poolKey]int32)
	startingIdleByPool := make(map[poolKey]int32)
	reservationTokens := make([]string, 0)
	var inFlight int32

	for _, pod := range pods {
		if !countsForStartPressure(pod) {
			continue
		}
		key := poolKey{namespace: pod.Namespace, templateID: pod.Labels[labelTemplateID]}
		switch pod.Labels[labelPoolType] {
		case poolTypeIdle:
			if podReady(pod) {
				readyIdleByPool[key]++
			} else {
				startingIdleByPool[key]++
				inFlight++
				reservationTokens = appendReservationToken(reservationTokens, pod)
			}
		case poolTypeActive:
			if !podReady(pod) {
				inFlight++
				reservationTokens = appendReservationToken(reservationTokens, pod)
			}
		}
	}

	for _, rs := range replicaSets {
		if rs == nil || rs.DeletionTimestamp != nil {
			continue
		}
		key := poolKey{namespace: rs.Namespace, templateID: rs.Labels[labelTemplateID]}
		if key.templateID == "" {
			continue
		}
		desired := int32(1)
		if rs.Spec.Replicas != nil {
			desired = *rs.Spec.Replicas
		}
		if desired <= 0 {
			continue
		}
		ownedIdle := readyIdleByPool[key] + startingIdleByPool[key]
		if desired > ownedIdle {
			inFlight += desired - ownedIdle
		}
	}
	return inFlight, reservationTokens
}

func appendReservationToken(tokens []string, pod *corev1.Pod) []string {
	if pod == nil {
		return tokens
	}
	token := pod.Annotations[AnnotationClaimStartReservation]
	if token == "" {
		return tokens
	}
	return append(tokens, token)
}

func countsForStartPressure(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	if !podConditionTrue(pod.Status.Conditions, corev1.PodReady) {
		return false
	}
	if !hasSandboxReadinessGate(pod) {
		return true
	}
	if !podConditionTrue(pod.Status.Conditions, v1alpha1.SandboxPodReadinessConditionType) {
		return false
	}
	live := findPodCondition(pod.Status.Conditions, v1alpha1.SandboxPodLivenessConditionType)
	return live == nil || live.Status != corev1.ConditionFalse
}

func ensureStartPressurePodIndex(indexer cache.Indexer) error {
	if indexer == nil {
		return nil
	}
	if _, exists := indexer.GetIndexers()[startPressurePodIndexName]; exists {
		return nil
	}
	// Ready active sandboxes do not contribute start pressure and can dominate
	// the pod cache on high-density nodes, so keep them out of the hot-path scan.
	return indexer.AddIndexers(cache.Indexers{
		startPressurePodIndexName: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return nil, fmt.Errorf("index claim start pressure for %T, want *corev1.Pod", obj)
			}
			if !countsForStartPressure(pod) {
				return nil, nil
			}
			switch pod.Labels[labelPoolType] {
			case poolTypeIdle:
				return []string{startPressurePodIndexValue}, nil
			case poolTypeActive:
				if !podReady(pod) {
					return []string{startPressurePodIndexValue}, nil
				}
			}
			return nil, nil
		},
	})
}

func podConditionTrue(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	condition := findPodCondition(conditions, conditionType)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

func findPodCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for idx := range conditions {
		if conditions[idx].Type == conditionType {
			return &conditions[idx]
		}
	}
	return nil
}

func hasSandboxReadinessGate(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.ReadinessGates {
		if gate.ConditionType == v1alpha1.SandboxPodReadinessConditionType {
			return true
		}
	}
	return false
}

func countWarmReadySandboxNodes(nodes []*corev1.Node, selector map[string]string, tolerations []corev1.Toleration) int {
	count := 0
	for _, node := range nodes {
		if node == nil || node.Spec.Unschedulable {
			continue
		}
		if !nodeConditionTrue(node.Status.Conditions, corev1.NodeReady) {
			continue
		}
		if !nodeMatchesSelector(node, selector) {
			continue
		}
		if !toleratesNodeTaints(node.Spec.Taints, tolerations) {
			continue
		}
		count++
	}
	return count
}

func nodeConditionTrue(conditions []corev1.NodeCondition, conditionType corev1.NodeConditionType) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func nodeMatchesSelector(node *corev1.Node, selector map[string]string) bool {
	for key, value := range selector {
		if node.Labels[key] != value {
			return false
		}
	}
	return true
}

func toleratesNodeTaints(taints []corev1.Taint, tolerations []corev1.Toleration) bool {
	for _, taint := range taints {
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		tolerated := false
		for _, toleration := range tolerations {
			if tolerationToleratesTaint(toleration, taint) {
				tolerated = true
				break
			}
		}
		if !tolerated {
			return false
		}
	}
	return true
}

func tolerationToleratesTaint(toleration corev1.Toleration, taint corev1.Taint) bool {
	if toleration.Effect != "" && toleration.Effect != taint.Effect {
		return false
	}
	operator := toleration.Operator
	if operator == "" {
		operator = corev1.TolerationOpEqual
	}
	switch operator {
	case corev1.TolerationOpExists:
		return toleration.Key == "" || toleration.Key == taint.Key
	case corev1.TolerationOpEqual:
		return toleration.Key == taint.Key && toleration.Value == taint.Value
	default:
		return false
	}
}

func poolPodSelector() (string, error) {
	req, err := labels.NewRequirement(labelPoolType, selection.In, []string{poolTypeIdle, poolTypeActive})
	if err != nil {
		return "", err
	}
	return labels.NewSelector().Add(*req).String(), nil
}

func templateReplicaSetSelector() (string, error) {
	req, err := labels.NewRequirement(labelTemplateID, selection.Exists, nil)
	if err != nil {
		return "", err
	}
	return labels.NewSelector().Add(*req).String(), nil
}

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate claim start reservation token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func hasCachedListers(cfg Config) bool {
	return cfg.NodeLister != nil && cfg.PodLister != nil && cfg.ReplicaSetLister != nil
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}
