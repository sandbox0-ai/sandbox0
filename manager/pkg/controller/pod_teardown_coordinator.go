package controller

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	config "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
)

const (
	TeardownReasonStaleRollout    = "stale_rollout"
	TeardownReasonUnhealthyRepair = "unhealthy_repair"

	teardownReservationTTL    = 30 * time.Minute
	forceDeleteThrottleWindow = time.Minute
)

type teardownReservation struct {
	identity  string
	nodeName  string
	poolKey   string
	createdAt time.Time
	committed bool
}

type forceDeleteReservation struct {
	nodeName  string
	createdAt time.Time
}

// PodTeardownCoordinator serializes teardown decisions across templates and
// controllers. Node slots are released once deletion is visible in the Pod
// cache, while replacement slots are released only when pool readiness grows.
type PodTeardownCoordinator struct {
	podLister                corelisters.PodLister
	nodeLister               corelisters.NodeLister
	limits                   config.PodTeardownConfig
	metrics                  *obsmetrics.ManagerMetrics
	logger                   *zap.Logger
	now                      func() time.Time
	idlePodRepairGracePeriod time.Duration

	mu                      sync.Mutex
	nodeReservations        map[string]teardownReservation
	replacementReservations map[string]teardownReservation
	forceDeleteReservations map[string]forceDeleteReservation
	poolReadyUIDs           map[string]map[string]struct{}
}

// PodTeardownLease holds node and replacement capacity while a guarded Pod
// delete is attempted. Call Commit after a successful delete and Release when
// the candidate changed or the delete failed.
type PodTeardownLease struct {
	coordinator *PodTeardownCoordinator
	identity    string
	pod         *corev1.Pod
	once        sync.Once
}

func (l *PodTeardownLease) Pod() *corev1.Pod {
	if l == nil {
		return nil
	}
	return l.pod
}

func (l *PodTeardownLease) Commit() {
	if l == nil || l.coordinator == nil {
		return
	}
	l.once.Do(func() { l.coordinator.finish(l.identity, true) })
}

func (l *PodTeardownLease) Release() {
	if l == nil || l.coordinator == nil {
		return
	}
	l.once.Do(func() { l.coordinator.finish(l.identity, false) })
}

func NewPodTeardownCoordinator(
	podLister corelisters.PodLister,
	nodeLister corelisters.NodeLister,
	limits config.PodTeardownConfig,
	runtimeReadyTimeout time.Duration,
	metrics *obsmetrics.ManagerMetrics,
	logger *zap.Logger,
) *PodTeardownCoordinator {
	limits = normalizedPodTeardownLimits(limits)
	if logger == nil {
		logger = zap.NewNop()
	}
	return &PodTeardownCoordinator{
		podLister:                podLister,
		nodeLister:               nodeLister,
		limits:                   limits,
		metrics:                  metrics,
		logger:                   logger,
		now:                      time.Now,
		idlePodRepairGracePeriod: config.IdlePodRepairGracePeriod(runtimeReadyTimeout),
		nodeReservations:         make(map[string]teardownReservation),
		replacementReservations:  make(map[string]teardownReservation),
		forceDeleteReservations:  make(map[string]forceDeleteReservation),
		poolReadyUIDs:            make(map[string]map[string]struct{}),
	}
}

func normalizedPodTeardownLimits(limits config.PodTeardownConfig) config.PodTeardownConfig {
	if limits.MaxConcurrentPerNode <= 0 {
		limits.MaxConcurrentPerNode = 4
	}
	if limits.MaxConcurrentPerDegradedNode <= 0 {
		limits.MaxConcurrentPerDegradedNode = 1
	}
	if limits.MaxConcurrentPerDegradedNode > limits.MaxConcurrentPerNode {
		limits.MaxConcurrentPerDegradedNode = limits.MaxConcurrentPerNode
	}
	if limits.MaxConcurrentReplacements <= 0 {
		limits.MaxConcurrentReplacements = 40
	}
	return limits
}

// Acquire selects candidates fairly across nodes and reserves both node-local
// teardown capacity and cluster-wide replacement capacity.
func (c *PodTeardownCoordinator) Acquire(candidates []*corev1.Pod, reason string) ([]*PodTeardownLease, error) {
	if c == nil || c.podLister == nil {
		return nil, fmt.Errorf("pod teardown coordinator is not configured")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	c.refreshLocked(pods, now)

	terminatingByNode := make(map[string]int32)
	youngReplacementPressure := int32(0)
	terminatingTotal := int32(0)
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		if pod.DeletionTimestamp != nil && teardownUsesNode(pod) {
			terminatingByNode[pod.Spec.NodeName]++
			terminatingTotal++
		}
		if isYoungIdleReplacement(pod, now, c.idlePodRepairGracePeriod) {
			youngReplacementPressure++
		}
	}

	reservedByNode := make(map[string]int32)
	for _, reservation := range c.nodeReservations {
		reservedByNode[reservation.nodeName]++
	}
	replacementUsed := int32(len(c.replacementReservations))
	if youngReplacementPressure > replacementUsed {
		replacementUsed = youngReplacementPressure
	}
	replacementAvailable := c.limits.MaxConcurrentReplacements - replacementUsed
	if replacementAvailable < 0 {
		replacementAvailable = 0
	}

	type candidateGroup struct {
		key        string
		nodeName   string
		capacity   int32
		candidates []*corev1.Pod
		next       int
	}
	groupsByKey := make(map[string]*candidateGroup)
	seen := make(map[string]struct{})
	for _, pod := range candidates {
		if pod == nil || pod.DeletionTimestamp != nil {
			continue
		}
		identity := podTeardownIdentity(pod)
		if identity == "" {
			continue
		}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		if _, ok := c.nodeReservations[identity]; ok {
			continue
		}
		if _, ok := c.replacementReservations[identity]; ok {
			continue
		}

		nodeName := ""
		groupKey := "~unscheduled"
		capacity := c.limits.MaxConcurrentReplacements
		if teardownUsesNode(pod) {
			nodeName = pod.Spec.NodeName
			groupKey = nodeName
			capacity = c.nodeLimit(nodeName) - terminatingByNode[nodeName] - reservedByNode[nodeName]
		}
		if capacity <= 0 {
			c.observeDecision(reason, "deferred_node_budget")
			continue
		}
		group := groupsByKey[groupKey]
		if group == nil {
			group = &candidateGroup{key: groupKey, nodeName: nodeName, capacity: capacity}
			groupsByKey[groupKey] = group
		}
		group.candidates = append(group.candidates, pod)
	}

	groups := make([]*candidateGroup, 0, len(groupsByKey))
	for _, group := range groupsByKey {
		sort.SliceStable(group.candidates, func(i, j int) bool {
			iReady := IsPodReady(group.candidates[i])
			jReady := IsPodReady(group.candidates[j])
			if iReady != jReady {
				return !iReady
			}
			if !group.candidates[i].CreationTimestamp.Equal(&group.candidates[j].CreationTimestamp) {
				return group.candidates[i].CreationTimestamp.Before(&group.candidates[j].CreationTimestamp)
			}
			return group.candidates[i].Name < group.candidates[j].Name
		})
		groups = append(groups, group)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].key < groups[j].key })

	leases := make([]*PodTeardownLease, 0)
	for {
		progress := false
		for _, group := range groups {
			if group.capacity <= 0 || group.next >= len(group.candidates) {
				continue
			}
			pod := group.candidates[group.next]
			poolKey := podPoolKey(pod)
			if _, ok := c.poolReadyUIDs[poolKey]; !ok {
				c.poolReadyUIDs[poolKey] = readyIdlePodUIDsForPool(pods, poolKey)
			}

			reused := false
			if replacementAvailable <= 0 && reason == TeardownReasonUnhealthyRepair {
				reused = c.reuseCommittedReplacementLocked(poolKey)
			}
			if replacementAvailable <= 0 && !reused {
				c.observeDecision(reason, "deferred_replacement_budget")
				continue
			}

			group.next++
			group.capacity--
			if !reused {
				replacementAvailable--
			}
			identity := podTeardownIdentity(pod)
			reservation := teardownReservation{
				identity:  identity,
				nodeName:  group.nodeName,
				poolKey:   poolKey,
				createdAt: now,
			}
			if teardownUsesNode(pod) {
				c.nodeReservations[identity] = reservation
			}
			c.replacementReservations[identity] = reservation
			leases = append(leases, &PodTeardownLease{
				coordinator: c,
				identity:    identity,
				pod:         pod,
			})
			c.observeDecision(reason, "selected")
			progress = true
		}
		if !progress {
			break
		}
	}

	c.updateInFlightMetricsLocked(terminatingTotal)
	if len(leases) > 0 {
		c.logger.Info("Planned node-aware pod teardown batch",
			zap.String("reason", reason),
			zap.Int("candidates", len(candidates)),
			zap.Int("selected", len(leases)),
			zap.Int32("terminating", terminatingTotal),
			zap.Int32("youngReplacementPressure", youngReplacementPressure),
			zap.Int("replacementReservations", len(c.replacementReservations)),
		)
	} else if len(candidates) > 0 {
		c.logger.Debug("Deferred node-aware pod teardown batch",
			zap.String("reason", reason),
			zap.Int("candidates", len(candidates)),
			zap.Int32("terminating", terminatingTotal),
			zap.Int32("youngReplacementPressure", youngReplacementPressure),
			zap.Int("replacementReservations", len(c.replacementReservations)),
		)
	}
	return leases, nil
}

// SelectForceDeletes applies the same configured node and cluster limits to
// stale terminating pods. A terminating pod already owns its teardown slot, so
// force deletion does not consume another replacement slot.
func (c *PodTeardownCoordinator) SelectForceDeletes(candidates []*corev1.Pod) ([]*corev1.Pod, error) {
	if c == nil || c.podLister == nil {
		return nil, fmt.Errorf("pod teardown coordinator is not configured")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	c.refreshLocked(pods, now)

	usedByNode := make(map[string]int32)
	for _, reservation := range c.forceDeleteReservations {
		usedByNode[reservation.nodeName]++
	}
	globalAvailable := c.limits.MaxConcurrentReplacements - int32(len(c.forceDeleteReservations))
	if globalAvailable <= 0 {
		return nil, nil
	}

	groups := make(map[string][]*corev1.Pod)
	capacities := make(map[string]int32)
	for _, pod := range candidates {
		if pod == nil || pod.DeletionTimestamp == nil {
			continue
		}
		identity := podTeardownIdentity(pod)
		if _, exists := c.forceDeleteReservations[identity]; exists {
			continue
		}
		key := "~unscheduled"
		nodeName := ""
		limit := c.limits.MaxConcurrentReplacements
		if teardownUsesNode(pod) {
			nodeName = pod.Spec.NodeName
			key = nodeName
			limit = c.forceDeleteNodeLimit(nodeName)
		}
		if _, exists := capacities[key]; !exists {
			capacities[key] = limit - usedByNode[nodeName]
		}
		if capacities[key] > 0 {
			groups[key] = append(groups[key], pod)
		}
	}

	keys := make([]string, 0, len(groups))
	for key := range groups {
		sort.Slice(groups[key], func(i, j int) bool {
			return groups[key][i].DeletionTimestamp.Before(groups[key][j].DeletionTimestamp)
		})
		keys = append(keys, key)
	}
	sort.Strings(keys)
	positions := make(map[string]int)
	selected := make([]*corev1.Pod, 0)
	for globalAvailable > 0 {
		progress := false
		for _, key := range keys {
			if capacities[key] <= 0 || positions[key] >= len(groups[key]) || globalAvailable <= 0 {
				continue
			}
			pod := groups[key][positions[key]]
			positions[key]++
			capacities[key]--
			globalAvailable--
			c.forceDeleteReservations[podTeardownIdentity(pod)] = forceDeleteReservation{
				nodeName:  pod.Spec.NodeName,
				createdAt: now,
			}
			selected = append(selected, pod)
			c.observeDecision("force_delete", "selected")
			progress = true
		}
		if !progress {
			break
		}
	}
	c.updateInFlightMetricsLocked(-1)
	return selected, nil
}

func (c *PodTeardownCoordinator) finish(identity string, committed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reservation, ok := c.nodeReservations[identity]; ok {
		reservation.committed = committed
		if committed {
			c.nodeReservations[identity] = reservation
		} else {
			delete(c.nodeReservations, identity)
		}
	}
	if reservation, ok := c.replacementReservations[identity]; ok {
		reservation.committed = committed
		if committed {
			c.replacementReservations[identity] = reservation
		} else {
			delete(c.replacementReservations, identity)
		}
	}
	c.updateInFlightMetricsLocked(-1)
}

func (c *PodTeardownCoordinator) refreshLocked(pods []*corev1.Pod, now time.Time) {
	byIdentity := make(map[string]*corev1.Pod, len(pods))
	readyUIDsByPool := make(map[string]map[string]struct{})
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		byIdentity[podTeardownIdentity(pod)] = pod
		if pod.Labels[LabelPoolType] == PoolTypeIdle && !IsHotClaimReservedPod(pod) && pod.DeletionTimestamp == nil && IsPodReady(pod) {
			poolKey := podPoolKey(pod)
			if readyUIDsByPool[poolKey] == nil {
				readyUIDsByPool[poolKey] = make(map[string]struct{})
			}
			readyUIDsByPool[poolKey][podTeardownIdentity(pod)] = struct{}{}
		}
	}
	for identity, reservation := range c.nodeReservations {
		pod := byIdentity[identity]
		if now.Sub(reservation.createdAt) >= teardownReservationTTL || pod == nil || pod.DeletionTimestamp != nil {
			delete(c.nodeReservations, identity)
		}
	}
	for identity, reservation := range c.replacementReservations {
		if now.Sub(reservation.createdAt) >= teardownReservationTTL {
			delete(c.replacementReservations, identity)
		}
	}
	for identity, reservation := range c.forceDeleteReservations {
		if now.Sub(reservation.createdAt) >= forceDeleteThrottleWindow {
			delete(c.forceDeleteReservations, identity)
		}
	}

	poolKeys := make(map[string]struct{})
	for key := range readyUIDsByPool {
		poolKeys[key] = struct{}{}
	}
	for key := range c.poolReadyUIDs {
		poolKeys[key] = struct{}{}
	}
	reservationsByPool := make(map[string]int)
	for _, reservation := range c.replacementReservations {
		poolKeys[reservation.poolKey] = struct{}{}
		reservationsByPool[reservation.poolKey]++
	}
	for key := range poolKeys {
		current := readyUIDsByPool[key]
		previous, exists := c.poolReadyUIDs[key]
		if !exists {
			c.poolReadyUIDs[key] = cloneStringSet(current)
			continue
		}
		newReady := 0
		for identity := range current {
			if _, ok := previous[identity]; !ok {
				newReady++
			}
		}
		if newReady > 0 {
			c.releaseReadyReplacementsLocked(key, newReady)
		}
		if len(current) == 0 && reservationsByPool[key] == 0 {
			delete(c.poolReadyUIDs, key)
			continue
		}
		c.poolReadyUIDs[key] = cloneStringSet(current)
	}
}

func (c *PodTeardownCoordinator) releaseReadyReplacementsLocked(poolKey string, count int) {
	for count > 0 {
		identity := c.oldestCommittedReplacementLocked(poolKey)
		if identity == "" {
			return
		}
		delete(c.replacementReservations, identity)
		count--
	}
}

func (c *PodTeardownCoordinator) reuseCommittedReplacementLocked(poolKey string) bool {
	identity := c.oldestCommittedReplacementLocked(poolKey)
	if identity == "" {
		return false
	}
	delete(c.replacementReservations, identity)
	return true
}

func (c *PodTeardownCoordinator) oldestCommittedReplacementLocked(poolKey string) string {
	identity := ""
	var oldest time.Time
	for key, reservation := range c.replacementReservations {
		if reservation.poolKey != poolKey || !reservation.committed {
			continue
		}
		if identity == "" || reservation.createdAt.Before(oldest) {
			identity = key
			oldest = reservation.createdAt
		}
	}
	return identity
}

func (c *PodTeardownCoordinator) nodeLimit(nodeName string) int32 {
	if nodeName == "" || c.nodeLister == nil {
		return 0
	}
	node, err := c.nodeLister.Get(nodeName)
	if err != nil || node == nil {
		return 0
	}
	ready := corev1.ConditionUnknown
	degraded := false
	for _, condition := range node.Status.Conditions {
		switch condition.Type {
		case corev1.NodeReady:
			ready = condition.Status
		case corev1.NodeMemoryPressure, corev1.NodeDiskPressure, corev1.NodePIDPressure, corev1.NodeNetworkUnavailable:
			degraded = degraded || condition.Status == corev1.ConditionTrue
		}
	}
	if ready != corev1.ConditionTrue {
		return 0
	}
	if degraded {
		return c.limits.MaxConcurrentPerDegradedNode
	}
	return c.limits.MaxConcurrentPerNode
}

func (c *PodTeardownCoordinator) forceDeleteNodeLimit(nodeName string) int32 {
	limit := c.nodeLimit(nodeName)
	if limit > 0 {
		return limit
	}
	// A stale terminating Pod on a lost node must still make bounded progress.
	return c.limits.MaxConcurrentPerDegradedNode
}

func (c *PodTeardownCoordinator) observeDecision(reason, result string) {
	if c.metrics != nil && c.metrics.PodTeardownDecisionsTotal != nil {
		c.metrics.PodTeardownDecisionsTotal.WithLabelValues(reason, result).Inc()
	}
}

func (c *PodTeardownCoordinator) updateInFlightMetricsLocked(terminating int32) {
	if c.metrics == nil || c.metrics.PodTeardownInFlight == nil {
		return
	}
	if terminating >= 0 {
		c.metrics.PodTeardownInFlight.WithLabelValues("terminating").Set(float64(terminating))
	}
	c.metrics.PodTeardownInFlight.WithLabelValues("node_reserved").Set(float64(len(c.nodeReservations)))
	c.metrics.PodTeardownInFlight.WithLabelValues("replacement_reserved").Set(float64(len(c.replacementReservations)))
	c.metrics.PodTeardownInFlight.WithLabelValues("force_delete_throttled").Set(float64(len(c.forceDeleteReservations)))
}

func podTeardownIdentity(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	if pod.UID != "" {
		return string(pod.UID)
	}
	if pod.Namespace == "" && pod.Name == "" {
		return ""
	}
	return pod.Namespace + "/" + pod.Name
}

func podPoolKey(pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	return pod.Namespace + "/" + strings.TrimSpace(pod.Labels[LabelTemplateID])
}

func teardownUsesNode(pod *corev1.Pod) bool {
	return pod != nil && pod.Spec.NodeName != "" && !pod.Spec.HostNetwork
}

func isYoungIdleReplacement(pod *corev1.Pod, now time.Time, gracePeriod time.Duration) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Labels[LabelPoolType] != PoolTypeIdle || IsHotClaimReservedPod(pod) || IsPodReady(pod) {
		return false
	}
	if pod.CreationTimestamp.IsZero() {
		return false
	}
	return now.Sub(pod.CreationTimestamp.Time) < gracePeriod
}

func readyIdlePodUIDsForPool(pods []*corev1.Pod, poolKey string) map[string]struct{} {
	ready := make(map[string]struct{})
	for _, pod := range pods {
		if podPoolKey(pod) == poolKey && pod.Labels[LabelPoolType] == PoolTypeIdle && !IsHotClaimReservedPod(pod) && pod.DeletionTimestamp == nil && IsPodReady(pod) {
			ready[podTeardownIdentity(pod)] = struct{}{}
		}
	}
	return ready
}

func cloneStringSet(source map[string]struct{}) map[string]struct{} {
	cloned := make(map[string]struct{}, len(source))
	for value := range source {
		cloned[value] = struct{}{}
	}
	return cloned
}
