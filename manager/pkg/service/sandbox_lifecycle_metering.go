package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	s0template "github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

const sandboxLifecycleMeteringProducer = "manager.sandbox_lifecycle"
const sandboxLifecycleMeteringUnknownNamespace = "unknown"

// SandboxLifecycleMeteringRecorder records durable sandbox lifecycle facts.
type SandboxLifecycleMeteringRecorder interface {
	RecordSandboxPaused(ctx context.Context, fact *SandboxPauseMeteringFact) error
}

// SandboxPauseMeteringFact carries the durable pause fact emitted by the
// sandbox lifecycle store.
type SandboxPauseMeteringFact struct {
	SandboxID         string
	Namespace         string
	TeamID            string
	UserID            string
	TemplateID        string
	ClusterID         string
	ResourceMillicpu  int64
	ResourceMemoryMiB int64
	ClaimedAt         time.Time
	ActiveSince       time.Time
	PausedAt          time.Time
	ExpiresAt         time.Time
	HardExpiresAt     time.Time
}

type repositorySandboxLifecycleMeteringRecorder struct {
	repo      *meteringpkg.Repository
	regionID  string
	clusterID string
}

// NewSandboxLifecycleMeteringRecorder creates a recorder backed by the metering repository.
func NewSandboxLifecycleMeteringRecorder(repo *meteringpkg.Repository, regionID, clusterID string) SandboxLifecycleMeteringRecorder {
	if repo == nil {
		return nil
	}
	return &repositorySandboxLifecycleMeteringRecorder{
		repo:      repo,
		regionID:  regionID,
		clusterID: clusterID,
	}
}

func (r *repositorySandboxLifecycleMeteringRecorder) RecordSandboxPaused(ctx context.Context, fact *SandboxPauseMeteringFact) error {
	if r == nil || r.repo == nil || fact == nil || fact.SandboxID == "" || fact.PausedAt.IsZero() {
		return nil
	}
	pausedAt := fact.PausedAt.UTC()
	state, err := r.repo.GetSandboxProjectionState(ctx, fact.SandboxID)
	if err != nil {
		return fmt.Errorf("load sandbox projection state: %w", err)
	}

	pendingEvents := make([]*meteringpkg.Event, 0, 2)
	if state == nil {
		state = &meteringpkg.SandboxProjectionState{
			SandboxID:         fact.SandboxID,
			Namespace:         sandboxPauseNamespace(fact.Namespace),
			TeamID:            fact.TeamID,
			UserID:            fact.UserID,
			TemplateID:        fact.TemplateID,
			ClusterID:         firstNonEmpty(fact.ClusterID, r.clusterID),
			ResourceMillicpu:  fact.ResourceMillicpu,
			ResourceMemoryMiB: fact.ResourceMemoryMiB,
			LastObservedAt:    pausedAt,
		}
		if !fact.ClaimedAt.IsZero() {
			claimedAt := fact.ClaimedAt.UTC()
			state.ClaimedAt = ptrMeteringTime(claimedAt)
			state.ActiveSince = ptrMeteringTime(firstNonZeroTime(fact.ActiveSince, claimedAt).UTC())
			pendingEvents = appendMeteringEvent(pendingEvents, buildSandboxLifecycleEvent(r.regionID, state, claimedAt, meteringpkg.EventTypeSandboxClaimed, sandboxClaimedEventID(fact.SandboxID, claimedAt), sandboxClaimEventData(fact)))
		}
	} else {
		mergeSandboxPauseFactIntoState(state, fact, r.clusterID)
	}

	activeSince := sandboxPauseActiveSince(state, fact)
	pauseEventID := sandboxPausedEventID(fact.SandboxID, pausedAt)

	state.Paused = true
	state.PausedAt = ptrMeteringTime(pausedAt)
	state.ActiveSince = nil
	state.TerminatedAt = nil
	state.LastObservedAt = pausedAt

	return r.repo.InTx(ctx, func(tx pgx.Tx) error {
		events := append([]*meteringpkg.Event(nil), pendingEvents...)
		windows := make([]*meteringpkg.Window, 0, 1)

		pauseEventExists, err := r.repo.EventExistsTx(ctx, tx, pauseEventID)
		if err != nil {
			return fmt.Errorf("check pause event: %w", err)
		}
		if !pauseEventExists {
			windows = appendMeteringWindow(windows, buildSandboxLifecycleRuntimeWindow(r.regionID, state, activeSince, pausedAt))
			events = appendMeteringEvent(events, buildSandboxLifecycleEvent(r.regionID, state, pausedAt, meteringpkg.EventTypeSandboxPaused, pauseEventID, nil))
		}

		if err := r.repo.AppendEventsTx(ctx, tx, events); err != nil {
			return fmt.Errorf("append lifecycle events: %w", err)
		}
		if err := r.repo.AppendWindowsTx(ctx, tx, windows); err != nil {
			return fmt.Errorf("append lifecycle windows: %w", err)
		}
		if err := r.repo.UpsertSandboxProjectionStateTx(ctx, tx, state); err != nil {
			return fmt.Errorf("upsert sandbox projection state: %w", err)
		}
		if err := r.repo.UpsertProducerWatermarkTx(ctx, tx, sandboxLifecycleMeteringProducer, r.regionID, pausedAt); err != nil {
			return fmt.Errorf("upsert lifecycle watermark: %w", err)
		}
		return nil
	})
}

func (s *SandboxService) SetLifecycleMeteringRecorder(recorder SandboxLifecycleMeteringRecorder) {
	if s == nil {
		return
	}
	s.lifecycleMetering = recorder
}

func (s *SandboxService) recordSandboxPausedMetering(ctx context.Context, record *SandboxRecord, pausedAt time.Time) {
	if s == nil || s.lifecycleMetering == nil || record == nil {
		return
	}
	fact := s.sandboxPauseMeteringFact(record, pausedAt)
	if err := s.lifecycleMetering.RecordSandboxPaused(ctx, fact); err != nil && s.logger != nil {
		s.logger.Warn("Failed to record sandbox pause metering fact",
			zap.String("sandboxID", record.ID),
			zap.Error(err),
		)
	}
}

func (s *SandboxService) sandboxPauseMeteringFact(record *SandboxRecord, pausedAt time.Time) *SandboxPauseMeteringFact {
	if record == nil {
		return nil
	}
	cpuMillis, memoryMiB := s.sandboxRecordMeteringResources(record)
	return &SandboxPauseMeteringFact{
		SandboxID:         record.ID,
		Namespace:         record.CurrentPodNamespace,
		TeamID:            record.TeamID,
		UserID:            record.UserID,
		TemplateID:        record.TemplateID,
		ClusterID:         record.ClusterID,
		ResourceMillicpu:  cpuMillis,
		ResourceMemoryMiB: memoryMiB,
		ClaimedAt:         record.ClaimedAt,
		ActiveSince:       record.ClaimedAt,
		PausedAt:          pausedAt,
		ExpiresAt:         record.ExpiresAt,
		HardExpiresAt:     record.HardExpiresAt,
	}
}

func (s *SandboxService) sandboxRecordMeteringResources(record *SandboxRecord) (int64, int64) {
	if record == nil {
		return 0, 0
	}
	quota := *record.TemplateSpec.MainContainer.Resources.DeepCopy()
	if record.Config.Resources != nil {
		if memory, err := s.validateSandboxMemory(record.Config.Resources.Memory); err == nil {
			quota.Memory = memory
			quota.CPU = s0template.CPUForMemory(memory, s.sandboxMemoryPerCPU())
		}
	}
	requirements := v1alpha1.BuildResourceRequirements(quota)
	return resourceListCPU(requirements), bytesToMiBRoundUp(resourceListMemory(requirements))
}

func mergeSandboxPauseFactIntoState(state *meteringpkg.SandboxProjectionState, fact *SandboxPauseMeteringFact, defaultClusterID string) {
	if state == nil || fact == nil {
		return
	}
	if state.Namespace == "" {
		state.Namespace = sandboxPauseNamespace(fact.Namespace)
	}
	if state.TeamID == "" {
		state.TeamID = fact.TeamID
	}
	if state.UserID == "" {
		state.UserID = fact.UserID
	}
	if state.TemplateID == "" {
		state.TemplateID = fact.TemplateID
	}
	if state.ClusterID == "" {
		state.ClusterID = firstNonEmpty(fact.ClusterID, defaultClusterID)
	}
	if state.ResourceMillicpu == 0 {
		state.ResourceMillicpu = fact.ResourceMillicpu
	}
	if state.ResourceMemoryMiB == 0 {
		state.ResourceMemoryMiB = fact.ResourceMemoryMiB
	}
	if state.ClaimedAt == nil && !fact.ClaimedAt.IsZero() {
		claimedAt := fact.ClaimedAt.UTC()
		state.ClaimedAt = &claimedAt
	}
}

func sandboxPauseNamespace(namespace string) string {
	return firstNonEmpty(namespace, sandboxLifecycleMeteringUnknownNamespace)
}

func sandboxPauseActiveSince(state *meteringpkg.SandboxProjectionState, fact *SandboxPauseMeteringFact) *time.Time {
	if state != nil && state.ActiveSince != nil {
		return state.ActiveSince
	}
	if fact != nil && !fact.ActiveSince.IsZero() {
		return ptrMeteringTime(fact.ActiveSince.UTC())
	}
	if fact != nil && !fact.ClaimedAt.IsZero() {
		return ptrMeteringTime(fact.ClaimedAt.UTC())
	}
	return nil
}

func appendMeteringEvent(events []*meteringpkg.Event, event *meteringpkg.Event) []*meteringpkg.Event {
	if event == nil {
		return events
	}
	return append(events, event)
}

func appendMeteringWindow(windows []*meteringpkg.Window, window *meteringpkg.Window) []*meteringpkg.Window {
	if window == nil {
		return windows
	}
	return append(windows, window)
}

func buildSandboxLifecycleEvent(regionID string, state *meteringpkg.SandboxProjectionState, occurredAt time.Time, eventType, eventID string, data any) *meteringpkg.Event {
	if state == nil || eventID == "" || occurredAt.IsZero() {
		return nil
	}
	return &meteringpkg.Event{
		EventID:     eventID,
		Producer:    sandboxLifecycleMeteringProducer,
		RegionID:    regionID,
		EventType:   eventType,
		SubjectType: meteringpkg.SubjectTypeSandbox,
		SubjectID:   state.SandboxID,
		TeamID:      state.TeamID,
		UserID:      state.UserID,
		SandboxID:   state.SandboxID,
		TemplateID:  state.TemplateID,
		ClusterID:   state.ClusterID,
		OccurredAt:  occurredAt.UTC(),
		Data:        mustMeteringJSON(data),
	}
}

func buildSandboxLifecycleRuntimeWindow(regionID string, state *meteringpkg.SandboxProjectionState, start *time.Time, end time.Time) *meteringpkg.Window {
	if state == nil || start == nil || start.IsZero() || end.IsZero() || !end.After(*start) {
		return nil
	}
	durationMS := end.Sub(*start).Milliseconds()
	if durationMS <= 0 || state.ResourceMemoryMiB <= 0 {
		return nil
	}
	return &meteringpkg.Window{
		WindowID:    sandboxRuntimeWindowID(state.SandboxID, meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds, *start, end),
		Producer:    sandboxLifecycleMeteringProducer,
		RegionID:    regionID,
		WindowType:  meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds,
		SubjectType: meteringpkg.SubjectTypeSandbox,
		SubjectID:   state.SandboxID,
		TeamID:      state.TeamID,
		UserID:      state.UserID,
		SandboxID:   state.SandboxID,
		TemplateID:  state.TemplateID,
		ClusterID:   state.ClusterID,
		WindowStart: start.UTC(),
		WindowEnd:   end.UTC(),
		Value:       state.ResourceMemoryMiB * durationMS,
		Unit:        meteringpkg.WindowUnitMiBMilliseconds,
		Data: mustMeteringJSON(map[string]any{
			"product":               meteringpkg.ProductSandbox,
			"resource_millicpu":     state.ResourceMillicpu,
			"resource_memory_mib":   state.ResourceMemoryMiB,
			"duration_milliseconds": durationMS,
		}),
	}
}

func sandboxClaimEventData(fact *SandboxPauseMeteringFact) map[string]any {
	if fact == nil {
		return nil
	}
	return map[string]any{
		"expires_at":      formatOptionalTime(fact.ExpiresAt),
		"hard_expires_at": formatOptionalTime(fact.HardExpiresAt),
	}
}

func sandboxClaimedEventID(sandboxID string, claimedAt time.Time) string {
	return fmt.Sprintf("sandbox/%s/claimed/%d", sandboxID, claimedAt.UTC().UnixNano())
}

func sandboxPausedEventID(sandboxID string, pausedAt time.Time) string {
	return fmt.Sprintf("sandbox/%s/paused/%d", sandboxID, pausedAt.UTC().UnixNano())
}

func sandboxRuntimeWindowID(sandboxID, windowType string, start, end time.Time) string {
	return fmt.Sprintf("sandbox/%s/windows/%s/%d/%d", sandboxID, windowType, start.UTC().UnixNano(), end.UTC().UnixNano())
}

func mustMeteringJSON(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func firstNonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func ptrMeteringTime(value time.Time) *time.Time {
	return &value
}
