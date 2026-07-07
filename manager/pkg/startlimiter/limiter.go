package startlimiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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

	labelTemplateID = "sandbox0.ai/template-id"
	labelPoolType   = "sandbox0.ai/pool-type"

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
	k8sClient          kubernetes.Interface
	pgLocker           *pglock.Locker
	redisClient        *redis.Client
	redisTimeout       time.Duration
	backend            string
	lockResource       string
	lockKey            string
	lockTTL            time.Duration
	acquireTimeout     time.Duration
	perSandboxNode     int32
	maxLimit           int32
	sandboxSelector    map[string]string
	sandboxTolerations []corev1.Toleration
	podSelector        string
	replicaSetSelector string
	logger             *zap.Logger
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
		limiter.lockKey = rediscache.JoinKeyPrefix(normalized.KeyPrefix, "manager", "claim-start", cfg.ClusterID, "lock")
	}

	return limiter, nil
}

func (l *Limiter) Backend() string {
	if l == nil {
		return ""
	}
	return l.backend
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

// Snapshot returns the current limiter pressure without taking the admission lock.
func (l *Limiter) Snapshot(ctx context.Context) (*Snapshot, error) {
	if l == nil {
		return nil, nil
	}
	return l.snapshotLocked(ctx)
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

	inFlight := startPressure(pods.Items, replicaSets.Items)
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

func startPressure(pods []corev1.Pod, replicaSets []appsv1.ReplicaSet) int32 {
	readyIdleByPool := make(map[poolKey]int32)
	startingIdleByPool := make(map[poolKey]int32)
	var inFlight int32

	for i := range pods {
		pod := &pods[i]
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
