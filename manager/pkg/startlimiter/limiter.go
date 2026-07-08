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
)

const (
	defaultPerSandboxNode = int32(30)
	defaultMaxLimit       = int32(80)
	defaultLockTTL        = 5 * time.Second
	defaultAcquireTimeout = 250 * time.Millisecond
	defaultRetryAfter     = time.Second
	defaultReservationTTL = 5 * time.Minute

	labelTemplateID = "sandbox0.ai/template-id"
	labelPoolType   = "sandbox0.ai/pool-type"

	// AnnotationClaimStartReservation marks a pod whose start is covered by an
	// active Redis reservation, so pressure snapshots do not double-count it.
	AnnotationClaimStartReservation = "sandbox0.ai/claim-start-reservation"

	poolTypeIdle   = "idle"
	poolTypeActive = "active"

	BackendPostgres = "postgres_advisory_lock"
	BackendRedis    = "redis"
)

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
	Message    string
}

func (e *ThrottledError) Error() string {
	if e == nil {
		return ErrThrottled.Error()
	}
	if e.Message != "" {
		return e.Message
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
	PGPool              *pgxpool.Pool
	Redis               rediscache.Config
	PerSandboxNode      int32
	MaxLimit            int32
	LockTTL             time.Duration
	AcquireTimeout      time.Duration
	SandboxNodeSelector map[string]string
	SandboxTolerations  []corev1.Toleration
	Logger              *zap.Logger
}

type Limiter struct {
	k8sClient           kubernetes.Interface
	pgLocker            *pglock.Locker
	redisClient         *redis.Client
	redisTimeout        time.Duration
	backend             string
	lockResource        string
	lockKey             string
	reservationKey      string
	reservationUnitsKey string
	lockTTL             time.Duration
	reservationTTL      time.Duration
	acquireTimeout      time.Duration
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
	if cfg.K8sClient == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	if cfg.PerSandboxNode <= 0 {
		cfg.PerSandboxNode = defaultPerSandboxNode
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = defaultMaxLimit
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = defaultLockTTL
	}
	if cfg.AcquireTimeout <= 0 {
		cfg.AcquireTimeout = defaultAcquireTimeout
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

	limiter := &Limiter{
		k8sClient:          cfg.K8sClient,
		pgLocker:           pglock.New(cfg.PGPool),
		backend:            BackendPostgres,
		lockResource:       fmt.Sprintf("manager:claim-start:%s", cfg.ClusterID),
		lockTTL:            cfg.LockTTL,
		acquireTimeout:     cfg.AcquireTimeout,
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
		limiter.lockKey = rediscache.JoinKeyPrefix(keyPrefix, "lock")
		limiter.reservationKey = rediscache.JoinKeyPrefix(keyPrefix, "reservations")
		limiter.reservationUnitsKey = rediscache.JoinKeyPrefix(keyPrefix, "reservation-units")
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
	err := l.withLock(ctx, func(lockCtx context.Context) error {
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
		err := l.withLock(ctx, func(lockCtx context.Context) error {
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

	var snapshot *Snapshot
	var token string
	err := l.withLock(ctx, func(lockCtx context.Context) error {
		reservations, err := l.activeReservations(lockCtx)
		if err != nil {
			return err
		}
		s, err := l.snapshotLockedWithReservations(lockCtx, reservations)
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
		token, err = randomToken()
		if err != nil {
			return err
		}
		return l.addReservation(lockCtx, token, units)
	})
	if err != nil {
		return nil, snapshot, err
	}
	return &Reservation{limiter: l, token: token}, snapshot, nil
}

// Snapshot returns the current limiter pressure without taking the admission lock.
func (l *Limiter) Snapshot(ctx context.Context) (*Snapshot, error) {
	if l == nil {
		return nil, nil
	}
	reservations, err := l.activeReservations(ctx)
	if err != nil {
		return nil, err
	}
	return l.snapshotLockedWithReservations(ctx, reservations)
}

func (l *Limiter) withLock(ctx context.Context, fn func(context.Context) error) error {
	if l.redisClient != nil {
		return l.withRedisLock(ctx, fn)
	}
	return l.pgLocker.WithExclusive(ctx, l.lockResource, fn)
}

func (l *Limiter) withRedisLock(ctx context.Context, fn func(context.Context) error) error {
	token, err := randomToken()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(l.acquireTimeout)
	for {
		opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
		ok, setErr := l.redisClient.SetNX(opCtx, l.lockKey, token, l.lockTTL).Result()
		cancel()
		if setErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(setErr, context.DeadlineExceeded) {
				return &ThrottledError{
					RetryAfter: defaultRetryAfter,
					Message:    "claim start admission lock is unavailable",
				}
			}
			return fmt.Errorf("acquire redis claim start lock: %w", setErr)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			return &ThrottledError{
				RetryAfter: defaultRetryAfter,
				Message:    "claim start admission lock is busy",
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer l.releaseRedisLock(token)
	return fn(ctx)
}

func (l *Limiter) releaseRedisLock(token string) {
	if l == nil || l.redisClient == nil {
		return
	}
	opCtx, cancel := rediscache.WithTimeout(context.Background(), l.redisTimeout)
	defer cancel()
	const releaseScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`
	if err := l.redisClient.Eval(opCtx, releaseScript, []string{l.lockKey}, token).Err(); err != nil {
		l.logger.Warn("Failed to release redis claim start lock", zap.Error(err))
	}
}

func (l *Limiter) snapshotLocked(ctx context.Context) (*Snapshot, error) {
	return l.snapshotLockedWithReservations(ctx, nil)
}

type activeReservationSnapshot struct {
	tokens map[string]struct{}
	units  int32
}

func (l *Limiter) snapshotLockedWithReservations(ctx context.Context, reservations *activeReservationSnapshot) (*Snapshot, error) {
	nodes, err := l.k8sClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes for claim start limiter: %w", err)
	}
	warmReadyNodes := countWarmReadySandboxNodes(nodes.Items, l.sandboxSelector, l.sandboxTolerations)
	limit := l.limitForNodes(warmReadyNodes)

	pods, err := l.k8sClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: l.podSelector})
	if err != nil {
		return nil, fmt.Errorf("list sandbox pods for claim start limiter: %w", err)
	}
	replicaSets, err := l.k8sClient.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{LabelSelector: l.replicaSetSelector})
	if err != nil {
		return nil, fmt.Errorf("list pool replicasets for claim start limiter: %w", err)
	}

	inFlight := startPressure(pods.Items, replicaSets.Items, reservationTokens(reservations))
	if reservations != nil {
		inFlight += reservations.units
	}
	available := limit - inFlight
	if available < 0 {
		available = 0
	}
	return &Snapshot{
		Backend:               l.backend,
		WarmReadySandboxNodes: warmReadyNodes,
		Limit:                 limit,
		InFlight:              inFlight,
		Available:             available,
	}, nil
}

func reservationTokens(reservations *activeReservationSnapshot) map[string]struct{} {
	if reservations == nil {
		return nil
	}
	return reservations.tokens
}

func (l *Limiter) activeReservations(ctx context.Context) (*activeReservationSnapshot, error) {
	if l == nil || l.redisClient == nil {
		return nil, nil
	}
	nowMs := time.Now().UnixMilli()
	expired, err := l.redisZRangeByScore(ctx, l.reservationKey, "-inf", strconv.FormatInt(nowMs, 10))
	if err != nil {
		return nil, fmt.Errorf("list expired claim start reservations: %w", err)
	}
	if len(expired) > 0 {
		if err := l.removeReservations(ctx, expired...); err != nil {
			return nil, err
		}
	}
	active, err := l.redisZRangeByScore(ctx, l.reservationKey, strconv.FormatInt(nowMs+1, 10), "+inf")
	if err != nil {
		return nil, fmt.Errorf("list active claim start reservations: %w", err)
	}
	if len(active) == 0 {
		return &activeReservationSnapshot{}, nil
	}
	values, err := l.redisHMGet(ctx, active...)
	if err != nil {
		return nil, err
	}
	snapshot := &activeReservationSnapshot{tokens: make(map[string]struct{}, len(active))}
	for idx, token := range active {
		units := int32(1)
		if idx < len(values) && values[idx] != nil {
			parsed, parseErr := strconv.ParseInt(fmt.Sprint(values[idx]), 10, 32)
			if parseErr == nil && parsed > 0 {
				units = int32(parsed)
			}
		}
		snapshot.tokens[token] = struct{}{}
		snapshot.units += units
	}
	return snapshot, nil
}

func (l *Limiter) addReservation(ctx context.Context, token string, units int32) error {
	if l == nil || l.redisClient == nil || token == "" {
		return nil
	}
	if units <= 0 {
		units = 1
	}
	expireAtMs := time.Now().Add(l.reservationTTL).UnixMilli()
	opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
	defer cancel()
	pipe := l.redisClient.TxPipeline()
	pipe.ZAdd(opCtx, l.reservationKey, redis.Z{Score: float64(expireAtMs), Member: token})
	pipe.HSet(opCtx, l.reservationUnitsKey, token, strconv.FormatInt(int64(units), 10))
	pipe.PExpire(opCtx, l.reservationKey, l.reservationTTL)
	pipe.PExpire(opCtx, l.reservationUnitsKey, l.reservationTTL)
	if _, err := pipe.Exec(opCtx); err != nil {
		return fmt.Errorf("add claim start reservation: %w", err)
	}
	return nil
}

func (l *Limiter) releaseReservation(token string) {
	if l == nil || l.redisClient == nil || token == "" {
		return
	}
	if err := l.removeReservations(context.Background(), token); err != nil {
		l.logger.Warn("Failed to release claim start reservation", zap.String("token", token), zap.Error(err))
	}
}

func (l *Limiter) removeReservations(ctx context.Context, tokens ...string) error {
	if l == nil || l.redisClient == nil || len(tokens) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(tokens))
	for _, token := range tokens {
		if token != "" {
			args = append(args, token)
		}
	}
	if len(args) == 0 {
		return nil
	}
	opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
	defer cancel()
	pipe := l.redisClient.TxPipeline()
	pipe.ZRem(opCtx, l.reservationKey, args...)
	pipe.HDel(opCtx, l.reservationUnitsKey, tokens...)
	if _, err := pipe.Exec(opCtx); err != nil {
		return fmt.Errorf("remove claim start reservation: %w", err)
	}
	return nil
}

func (l *Limiter) redisZRangeByScore(ctx context.Context, key, min, max string) ([]string, error) {
	opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
	defer cancel()
	return l.redisClient.ZRangeByScore(opCtx, key, &redis.ZRangeBy{Min: min, Max: max}).Result()
}

func (l *Limiter) redisHMGet(ctx context.Context, tokens ...string) ([]interface{}, error) {
	opCtx, cancel := rediscache.WithTimeout(ctx, l.redisTimeout)
	defer cancel()
	values, err := l.redisClient.HMGet(opCtx, l.reservationUnitsKey, tokens...).Result()
	if err != nil {
		return nil, fmt.Errorf("read claim start reservation units: %w", err)
	}
	return values, nil
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

func startPressure(pods []corev1.Pod, replicaSets []appsv1.ReplicaSet, activeReservations map[string]struct{}) int32 {
	readyIdleByPool := make(map[poolKey]int32)
	startingIdleByPool := make(map[poolKey]int32)
	var inFlight int32

	for i := range pods {
		pod := &pods[i]
		if !countsForStartPressure(pod) {
			continue
		}
		if podCoveredByActiveReservation(pod, activeReservations) {
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
			}
		case poolTypeActive:
			if !podReady(pod) {
				inFlight++
			}
		}
	}

	for i := range replicaSets {
		rs := &replicaSets[i]
		if rs.DeletionTimestamp != nil {
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
	return inFlight
}

func podCoveredByActiveReservation(pod *corev1.Pod, activeReservations map[string]struct{}) bool {
	if pod == nil || len(activeReservations) == 0 {
		return false
	}
	token := pod.Annotations[AnnotationClaimStartReservation]
	if token == "" {
		return false
	}
	_, ok := activeReservations[token]
	return ok
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

func countWarmReadySandboxNodes(nodes []corev1.Node, selector map[string]string, tolerations []corev1.Toleration) int {
	count := 0
	for i := range nodes {
		node := &nodes[i]
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
		return "", fmt.Errorf("generate claim start lock token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
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
