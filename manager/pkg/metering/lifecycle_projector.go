package metering

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

const sandboxLifecycleProducer = "manager.sandbox_lifecycle"

type txStore interface {
	AppendEvent(ctx context.Context, event *meteringpkg.Event) error
	AppendWindow(ctx context.Context, window *meteringpkg.Window) error
	UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error
	UpsertSandboxProjectionState(ctx context.Context, state *meteringpkg.SandboxProjectionState) error
}

type Store interface {
	GetSandboxProjectionState(ctx context.Context, sandboxID string) (*meteringpkg.SandboxProjectionState, error)
	RunInTx(ctx context.Context, fn func(tx txStore) error) error
}

type repositoryStore struct {
	repo *meteringpkg.Repository
}

type repositoryTxStore struct {
	repo *meteringpkg.Repository
	tx   pgx.Tx
}

func NewStore(repo *meteringpkg.Repository) Store {
	if repo == nil {
		return nil
	}
	return &repositoryStore{repo: repo}
}

func (s *repositoryStore) GetSandboxProjectionState(ctx context.Context, sandboxID string) (*meteringpkg.SandboxProjectionState, error) {
	return s.repo.GetSandboxProjectionState(ctx, sandboxID)
}

func (s *repositoryStore) RunInTx(ctx context.Context, fn func(tx txStore) error) error {
	return s.repo.InTx(ctx, func(tx pgx.Tx) error {
		return fn(&repositoryTxStore{repo: s.repo, tx: tx})
	})
}

func (s *repositoryTxStore) AppendEvent(ctx context.Context, event *meteringpkg.Event) error {
	return s.repo.AppendEventTx(ctx, s.tx, event)
}

func (s *repositoryTxStore) AppendWindow(ctx context.Context, window *meteringpkg.Window) error {
	return s.repo.AppendWindowTx(ctx, s.tx, window)
}

func (s *repositoryTxStore) UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	return s.repo.UpsertProducerWatermarkTx(ctx, s.tx, producer, regionID, completeBefore)
}

func (s *repositoryTxStore) UpsertSandboxProjectionState(ctx context.Context, state *meteringpkg.SandboxProjectionState) error {
	return s.repo.UpsertSandboxProjectionStateTx(ctx, s.tx, state)
}

type LifecycleProjector struct {
	store     Store
	regionID  string
	clusterID string
	logger    *zap.Logger
	metrics   *obsmetrics.ManagerMetrics
	now       func() time.Time
}

func NewLifecycleProjector(store Store, regionID string, clusterID string) *LifecycleProjector {
	return &LifecycleProjector{
		store:     store,
		regionID:  regionID,
		clusterID: clusterID,
		logger:    zap.NewNop(),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (p *LifecycleProjector) SetLogger(logger *zap.Logger) {
	if logger != nil {
		p.logger = logger
	}
}

func (p *LifecycleProjector) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	p.metrics = metrics
}

func (p *LifecycleProjector) ResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    p.handleUpsert,
		UpdateFunc: func(_, newObj any) { p.handleUpsert(newObj) },
		DeleteFunc: p.handleDelete,
	}
}

func (p *LifecycleProjector) handleUpsert(obj any) {
	pod := extractPod(obj)
	if pod == nil {
		return
	}
	if !isClaimedActiveSandbox(pod) {
		return
	}

	ctx := context.Background()
	state, err := p.store.GetSandboxProjectionState(ctx, pod.Name)
	if err != nil {
		p.recordError("load_state", pod.Name, err)
		return
	}

	claimedAt, ok := parseRFC3339(pod.Annotations[controller.AnnotationClaimedAt])
	if !ok {
		p.logger.Warn("Skipping metering projection for sandbox without valid claimed_at annotation",
			zap.String("sandboxID", pod.Name),
			zap.String("namespace", pod.Namespace),
		)
		p.incrementErrorCounter("invalid_claimed_at")
		return
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]
	templateID := pod.Labels[controller.LabelTemplateID]
	podUsage := sandboxUsageFromPod(pod)
	observedAt := p.now()
	paused := pod.Annotations[controller.AnnotationPaused] == "true"
	pausedAt, pausedAtSet := parseRFC3339(pod.Annotations[controller.AnnotationPausedAt])
	pendingEvents := make([]*meteringpkg.Event, 0, 2)
	pendingWindows := make([]*meteringpkg.Window, 0, 2)

	if state == nil {
		pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, claimedAt, meteringpkg.EventTypeSandboxClaimed, claimedEventID(pod.Name, claimedAt), claimEventData(pod)))
		state = &meteringpkg.SandboxProjectionState{
			SandboxID:         pod.Name,
			Namespace:         pod.Namespace,
			TeamID:            teamID,
			UserID:            userID,
			TemplateID:        templateID,
			ClusterID:         p.clusterID,
			OwnerKind:         podUsage.OwnerKind,
			ResourceMillicpu:  podUsage.ResourceMillicpu,
			ResourceMemoryMiB: podUsage.ResourceMemoryMiB,
			ClaimedAt:         &claimedAt,
			ActiveSince:       &claimedAt,
			LastObservedAt:    observedAt,
			LastResourceVer:   pod.ResourceVersion,
		}
	}

	if paused {
		eventTime := observedAt
		if pausedAtSet {
			eventTime = pausedAt
		}
		if !state.Paused {
			pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, eventTime, meteringpkg.EventTypeSandboxPaused, pauseEventID(pod.Name, eventTime), nil))
			pendingWindows = append(pendingWindows, p.buildSandboxResourceWindows(state, teamID, userID, templateID, state.ActiveSince, eventTime)...)
		}
		state.Paused = true
		state.PausedAt = &eventTime
		state.ActiveSince = nil
	} else if state.Paused {
		pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, observedAt, meteringpkg.EventTypeSandboxResumed, resumeEventID(pod.Name, pod.ResourceVersion), nil))
		state.Paused = false
		state.PausedAt = nil
		state.ActiveSince = ptrTime(observedAt)
		applySandboxUsage(state, podUsage)
	}

	state.Namespace = pod.Namespace
	state.TeamID = teamID
	state.UserID = userID
	state.TemplateID = templateID
	state.ClusterID = p.clusterID
	if !paused && !state.Paused {
		applySandboxUsage(state, podUsage)
	}
	state.ClaimedAt = &claimedAt
	state.TerminatedAt = nil
	state.LastObservedAt = observedAt
	state.LastResourceVer = pod.ResourceVersion
	if err := p.commitProjection(ctx, pod.Name, state, pendingEvents, pendingWindows, observedAt); err != nil {
		return
	}
}

func (p *LifecycleProjector) handleDelete(obj any) {
	pod := extractPod(obj)
	if pod == nil || !isClaimedActiveSandbox(pod) {
		return
	}

	ctx := context.Background()
	state, err := p.store.GetSandboxProjectionState(ctx, pod.Name)
	if err != nil {
		p.recordError("load_state", pod.Name, err)
		return
	}

	observedAt := p.now()
	teamID := pod.Annotations[controller.AnnotationTeamID]
	userID := pod.Annotations[controller.AnnotationUserID]
	templateID := pod.Labels[controller.LabelTemplateID]
	podUsage := sandboxUsageFromPod(pod)
	claimedAt, claimedAtSet := parseRFC3339(pod.Annotations[controller.AnnotationClaimedAt])
	pendingEvents := make([]*meteringpkg.Event, 0, 3)
	pendingWindows := make([]*meteringpkg.Window, 0, 2)
	if state == nil {
		state = &meteringpkg.SandboxProjectionState{
			SandboxID:         pod.Name,
			Namespace:         pod.Namespace,
			TeamID:            teamID,
			UserID:            userID,
			TemplateID:        templateID,
			ClusterID:         p.clusterID,
			OwnerKind:         podUsage.OwnerKind,
			ResourceMillicpu:  podUsage.ResourceMillicpu,
			ResourceMemoryMiB: podUsage.ResourceMemoryMiB,
			LastObservedAt:    observedAt,
			LastResourceVer:   pod.ResourceVersion,
		}
		if claimedAtSet {
			pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, claimedAt, meteringpkg.EventTypeSandboxClaimed, claimedEventID(pod.Name, claimedAt), claimEventData(pod)))
			state.ClaimedAt = &claimedAt
			state.ActiveSince = &claimedAt
		}
		if pod.Annotations[controller.AnnotationPaused] == "true" {
			pausedAt := observedAt
			if parsedPausedAt, ok := parseRFC3339(pod.Annotations[controller.AnnotationPausedAt]); ok {
				pausedAt = parsedPausedAt
			}
			pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, pausedAt, meteringpkg.EventTypeSandboxPaused, pauseEventID(pod.Name, pausedAt), nil))
			pendingWindows = append(pendingWindows, p.buildSandboxResourceWindows(state, teamID, userID, templateID, state.ActiveSince, pausedAt)...)
			state.Paused = true
			state.PausedAt = &pausedAt
			state.ActiveSince = nil
		}
	}
	if state.TerminatedAt == nil {
		if !state.Paused {
			pendingWindows = append(pendingWindows, p.buildSandboxResourceWindows(state, teamID, userID, templateID, state.ActiveSince, observedAt)...)
		}
		pendingEvents = append(pendingEvents, p.buildSandboxEvent(pod.Name, teamID, userID, templateID, observedAt, meteringpkg.EventTypeSandboxTerminated, terminateEventID(pod.Name, pod.ResourceVersion), nil))
	}
	state.Paused = pod.Annotations[controller.AnnotationPaused] == "true"
	state.ActiveSince = nil
	state.LastObservedAt = observedAt
	state.LastResourceVer = pod.ResourceVersion
	state.TerminatedAt = &observedAt
	if err := p.commitProjection(ctx, pod.Name, state, pendingEvents, pendingWindows, observedAt); err != nil {
		return
	}
}

func (p *LifecycleProjector) commitProjection(ctx context.Context, sandboxID string, state *meteringpkg.SandboxProjectionState, events []*meteringpkg.Event, windows []*meteringpkg.Window, observedAt time.Time) error {
	if p.store == nil {
		err := fmt.Errorf("metering store is nil")
		p.recordError("commit_projection", sandboxID, err)
		return err
	}
	if err := p.store.RunInTx(ctx, func(tx txStore) error {
		for _, event := range events {
			if event == nil {
				continue
			}
			if err := tx.AppendEvent(ctx, event); err != nil {
				return fmt.Errorf("append event %s: %w", event.EventType, err)
			}
		}
		for _, window := range windows {
			if window == nil {
				continue
			}
			if err := tx.AppendWindow(ctx, window); err != nil {
				return fmt.Errorf("append window %s: %w", window.WindowType, err)
			}
		}
		if err := tx.UpsertSandboxProjectionState(ctx, state); err != nil {
			return fmt.Errorf("upsert state: %w", err)
		}
		if err := tx.UpsertProducerWatermark(ctx, sandboxLifecycleProducer, p.regionID, observedAt); err != nil {
			return fmt.Errorf("upsert watermark: %w", err)
		}
		return nil
	}); err != nil {
		for _, event := range events {
			if event != nil {
				p.recordEventResult(event.EventType, "error")
			}
		}
		for _, window := range windows {
			if window != nil {
				p.recordWindowResult(window.WindowType, "error")
			}
		}
		p.recordError("commit_projection", sandboxID, err)
		return err
	}
	for _, event := range events {
		if event != nil {
			p.recordEventResult(event.EventType, "success")
		}
	}
	for _, window := range windows {
		if window != nil {
			p.recordWindowResult(window.WindowType, "success")
		}
	}
	return nil
}

func (p *LifecycleProjector) recordEventResult(eventType string, result string) {
	if p.metrics != nil {
		p.metrics.MeteringEventsTotal.WithLabelValues(eventType, result).Inc()
	}
}

func (p *LifecycleProjector) recordWindowResult(windowType string, result string) {
	if p.metrics != nil {
		p.metrics.MeteringWindowsTotal.WithLabelValues(windowType, result).Inc()
	}
}

func (p *LifecycleProjector) incrementErrorCounter(operation string) {
	if p.metrics != nil {
		p.metrics.MeteringErrorsTotal.WithLabelValues(operation).Inc()
	}
}

func (p *LifecycleProjector) recordError(operation string, sandboxID string, err error) {
	p.incrementErrorCounter(operation)
	p.logger.Error("Manager metering projection failed",
		zap.String("operation", operation),
		zap.String("sandboxID", sandboxID),
		zap.Error(err),
	)
}

func (p *LifecycleProjector) buildSandboxEvent(sandboxID, teamID, userID, templateID string, occurredAt time.Time, eventType, eventID string, data any) *meteringpkg.Event {
	payload := mustJSON(data)
	return &meteringpkg.Event{
		EventID:     eventID,
		Producer:    sandboxLifecycleProducer,
		RegionID:    p.regionID,
		EventType:   eventType,
		SubjectType: meteringpkg.SubjectTypeSandbox,
		SubjectID:   sandboxID,
		TeamID:      teamID,
		UserID:      userID,
		SandboxID:   sandboxID,
		TemplateID:  templateID,
		ClusterID:   p.clusterID,
		OccurredAt:  occurredAt,
		Data:        payload,
	}
}

func (p *LifecycleProjector) buildSandboxResourceWindows(state *meteringpkg.SandboxProjectionState, teamID, userID, templateID string, start *time.Time, end time.Time) []*meteringpkg.Window {
	if state == nil || start == nil || start.IsZero() || end.IsZero() || !end.After(*start) {
		return nil
	}
	windows := make([]*meteringpkg.Window, 0, 2)
	durationMS := end.Sub(*start).Milliseconds()
	if durationMS <= 0 {
		return windows
	}
	if state.ResourceMillicpu > 0 {
		windows = append(windows, &meteringpkg.Window{
			WindowID:    sandboxWindowID(state.SandboxID, meteringpkg.WindowTypeSandboxComputeMillicpuMilliseconds, *start, end),
			Producer:    sandboxLifecycleProducer,
			RegionID:    p.regionID,
			WindowType:  meteringpkg.WindowTypeSandboxComputeMillicpuMilliseconds,
			SubjectType: meteringpkg.SubjectTypeSandbox,
			SubjectID:   state.SandboxID,
			TeamID:      teamID,
			UserID:      userID,
			SandboxID:   state.SandboxID,
			TemplateID:  templateID,
			ClusterID:   p.clusterID,
			WindowStart: *start,
			WindowEnd:   end,
			Value:       state.ResourceMillicpu * durationMS,
			Unit:        meteringpkg.WindowUnitMillicpuMilliseconds,
			Data:        resourceWindowData(state),
		})
	}
	if state.ResourceMemoryMiB > 0 {
		windows = append(windows, &meteringpkg.Window{
			WindowID:    sandboxWindowID(state.SandboxID, meteringpkg.WindowTypeSandboxMemoryMiBMilliseconds, *start, end),
			Producer:    sandboxLifecycleProducer,
			RegionID:    p.regionID,
			WindowType:  meteringpkg.WindowTypeSandboxMemoryMiBMilliseconds,
			SubjectType: meteringpkg.SubjectTypeSandbox,
			SubjectID:   state.SandboxID,
			TeamID:      teamID,
			UserID:      userID,
			SandboxID:   state.SandboxID,
			TemplateID:  templateID,
			ClusterID:   p.clusterID,
			WindowStart: *start,
			WindowEnd:   end,
			Value:       state.ResourceMemoryMiB * durationMS,
			Unit:        meteringpkg.WindowUnitMiBMilliseconds,
			Data:        resourceWindowData(state),
		})
	}
	return windows
}

func isClaimedActiveSandbox(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Labels[controller.LabelPoolType] != controller.PoolTypeActive {
		return false
	}
	if pod.Labels[controller.LabelSandboxID] == "" {
		return false
	}
	return pod.Annotations[controller.AnnotationClaimedAt] != ""
}

func extractPod(obj any) *corev1.Pod {
	switch value := obj.(type) {
	case *corev1.Pod:
		return value
	case cache.DeletedFinalStateUnknown:
		if pod, ok := value.Obj.(*corev1.Pod); ok {
			return pod
		}
	case *cache.DeletedFinalStateUnknown:
		if pod, ok := value.Obj.(*corev1.Pod); ok {
			return pod
		}
	}
	return nil
}

func parseRFC3339(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
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

func claimEventData(pod *corev1.Pod) map[string]any {
	if pod == nil {
		return nil
	}
	return map[string]any{
		"claim_type":      pod.Annotations[controller.AnnotationClaimType],
		"expires_at":      pod.Annotations[controller.AnnotationExpiresAt],
		"hard_expires_at": pod.Annotations[controller.AnnotationHardExpiresAt],
	}
}

type sandboxUsageMetadata struct {
	OwnerKind         string
	ResourceMillicpu  int64
	ResourceMemoryMiB int64
}

func sandboxUsageFromPod(pod *corev1.Pod) sandboxUsageMetadata {
	if pod == nil {
		return sandboxUsageMetadata{}
	}
	cpu, memory := podResourceAllocation(pod)
	return sandboxUsageMetadata{
		OwnerKind:         pod.Annotations[controller.AnnotationOwnerKind],
		ResourceMillicpu:  cpu,
		ResourceMemoryMiB: memory,
	}
}

func applySandboxUsage(state *meteringpkg.SandboxProjectionState, usage sandboxUsageMetadata) {
	if state == nil {
		return
	}
	state.OwnerKind = usage.OwnerKind
	state.ResourceMillicpu = usage.ResourceMillicpu
	state.ResourceMemoryMiB = usage.ResourceMemoryMiB
}

func podResourceAllocation(pod *corev1.Pod) (int64, int64) {
	if pod == nil {
		return 0, 0
	}
	var cpuMillis int64
	var memoryBytes int64
	for _, container := range pod.Spec.Containers {
		cpuMillis += resourceMillicpu(container.Resources.Requests, container.Resources.Limits)
		memoryBytes += resourceBytes(container.Resources.Requests, container.Resources.Limits)
	}
	return cpuMillis, bytesToMiBRoundUp(memoryBytes)
}

func resourceMillicpu(requests, limits corev1.ResourceList) int64 {
	if quantity, ok := limits[corev1.ResourceCPU]; ok && !quantity.IsZero() {
		return quantity.MilliValue()
	}
	if quantity, ok := requests[corev1.ResourceCPU]; ok && !quantity.IsZero() {
		return quantity.MilliValue()
	}
	return 0
}

func resourceBytes(requests, limits corev1.ResourceList) int64 {
	if quantity, ok := limits[corev1.ResourceMemory]; ok && !quantity.IsZero() {
		return quantity.Value()
	}
	if quantity, ok := requests[corev1.ResourceMemory]; ok && !quantity.IsZero() {
		return quantity.Value()
	}
	return 0
}

func bytesToMiBRoundUp(value int64) int64 {
	if value <= 0 {
		return 0
	}
	const mib = int64(1024 * 1024)
	return (value + mib - 1) / mib
}

func resourceWindowData(state *meteringpkg.SandboxProjectionState) json.RawMessage {
	return mustJSON(map[string]any{
		"product":             meteringpkg.ProductSandbox,
		"resource_millicpu":   state.ResourceMillicpu,
		"resource_memory_mib": state.ResourceMemoryMiB,
	})
}

func claimedEventID(sandboxID string, claimedAt time.Time) string {
	return fmt.Sprintf("sandbox/%s/claimed/%d", sandboxID, claimedAt.UTC().UnixNano())
}

func pauseEventID(sandboxID string, pausedAt time.Time) string {
	return fmt.Sprintf("sandbox/%s/paused/%d", sandboxID, pausedAt.UTC().UnixNano())
}

func resumeEventID(sandboxID, resourceVersion string) string {
	return fmt.Sprintf("sandbox/%s/resumed/%s", sandboxID, resourceVersion)
}

func terminateEventID(sandboxID, resourceVersion string) string {
	return fmt.Sprintf("sandbox/%s/terminated/%s", sandboxID, resourceVersion)
}

func sandboxWindowID(sandboxID, windowType string, start, end time.Time) string {
	return fmt.Sprintf("sandbox/%s/windows/%s/%d/%d", sandboxID, windowType, start.UTC().UnixNano(), end.UTC().UnixNano())
}

func ptrTime(value time.Time) *time.Time {
	return &value
}
