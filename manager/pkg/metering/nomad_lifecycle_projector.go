package metering

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	obsmetrics "github.com/sandbox0-ai/sandbox0/manager/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

const (
	defaultNomadMeteringPollInterval       = 500 * time.Millisecond
	defaultNomadMeteringBatchSize          = 100
	defaultNomadMeteringFailureBaseBackoff = time.Second
	defaultNomadMeteringFailureMaxBackoff  = 5 * time.Minute
	defaultNomadMeteringWatermarkInterval  = time.Minute
	nomadMeteringNamespaceFallback         = "nomad"
	maxNomadMeteringErrorBytes             = 2_048
)

// NomadLifecycleProjectorConfig bounds durable lifecycle projection work and
// retry pressure. The PostgreSQL queue coalesces repeated source mutations for
// one sandbox without discarding its append-only lifecycle history.
type NomadLifecycleProjectorConfig struct {
	PollInterval       time.Duration
	BatchSize          int
	FailureBaseBackoff time.Duration
	FailureMaxBackoff  time.Duration
	WatermarkInterval  time.Duration
}

// NomadLifecycleProjector reconstructs usage from PostgreSQL-owned Nomad
// lifecycle history and commits events, windows, state, and queue completion
// in one transaction. It never treats Nomad or ClickHouse as usage truth.
type NomadLifecycleProjector struct {
	repo      repository
	regionID  string
	clusterID string
	config    NomadLifecycleProjectorConfig
	logger    *zap.Logger
	metrics   *obsmetrics.ManagerMetrics
}

type nomadMeteringQueueItem struct {
	SandboxID string
	Revision  int64
	Attempts  int
}

type nomadSandboxMeteringSource struct {
	SandboxID           string
	TeamID              string
	UserID              string
	TemplateID          string
	ClusterID           string
	DesiredState        string
	AllocationNamespace string
	OwnerKind           string
	ResourceMillicpu    int64
	ResourceMemoryMiB   int64
	ClaimedAt           *time.Time
	ExpiresAt           *time.Time
	HardExpiresAt       *time.Time
	DeletedAt           *time.Time
	InitialActiveAt     *time.Time
	ObservedAt          time.Time
}

type nomadMeteringLifecycleTransition struct {
	ID          string
	Kind        string
	Phase       string
	Source      string
	Epoch       int64
	Error       string
	CommittedAt *time.Time
	AbortedAt   *time.Time
}

type nomadProjectionMutations struct {
	state   *meteringpkg.SandboxProjectionState
	events  []*meteringpkg.Event
	windows []*meteringpkg.Window
}

// NewNomadLifecycleProjector returns a durable Nomad projector. Migrations for
// both manager and metering schemas must complete before Run starts.
func NewNomadLifecycleProjector(
	repo repository,
	regionID string,
	clusterID string,
	config NomadLifecycleProjectorConfig,
) (*NomadLifecycleProjector, error) {
	if repo == nil {
		return nil, fmt.Errorf("Nomad lifecycle metering repository is required")
	}
	config = normalizeNomadLifecycleProjectorConfig(config)
	return &NomadLifecycleProjector{
		repo: repo, regionID: strings.TrimSpace(regionID), clusterID: strings.TrimSpace(clusterID),
		config: config, logger: zap.NewNop(),
	}, nil
}

func normalizeNomadLifecycleProjectorConfig(config NomadLifecycleProjectorConfig) NomadLifecycleProjectorConfig {
	if config.PollInterval <= 0 {
		config.PollInterval = defaultNomadMeteringPollInterval
	}
	if config.BatchSize <= 0 || config.BatchSize > 1_000 {
		config.BatchSize = defaultNomadMeteringBatchSize
	}
	if config.FailureBaseBackoff <= 0 {
		config.FailureBaseBackoff = defaultNomadMeteringFailureBaseBackoff
	}
	if config.FailureMaxBackoff < config.FailureBaseBackoff {
		config.FailureMaxBackoff = defaultNomadMeteringFailureMaxBackoff
	}
	if config.WatermarkInterval <= 0 {
		config.WatermarkInterval = defaultNomadMeteringWatermarkInterval
	}
	return config
}

func (p *NomadLifecycleProjector) SetLogger(logger *zap.Logger) {
	if logger != nil {
		p.logger = logger
	}
}

func (p *NomadLifecycleProjector) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	p.metrics = metrics
}

// Run drains due rows in bounded batches and periodically publishes a
// producer watermark only while a table lock proves the durable queue empty.
func (p *NomadLifecycleProjector) Run(ctx context.Context) error {
	if p == nil || p.repo == nil {
		return fmt.Errorf("Nomad lifecycle metering projector is not configured")
	}
	poll := time.NewTicker(p.config.PollInterval)
	defer poll.Stop()
	watermark := time.NewTicker(p.config.WatermarkInterval)
	defer watermark.Stop()

	for {
		if _, err := p.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.recordError("nomad_project_batch", "", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		case <-watermark.C:
			if err := p.advanceWatermarkIfDrained(ctx); err != nil && !errors.Is(err, context.Canceled) {
				p.recordError("nomad_advance_watermark", "", err)
			}
		}
	}
}

// RunOnce projects at most one configured batch and is exposed for startup
// drains and integration tests.
func (p *NomadLifecycleProjector) RunOnce(ctx context.Context) (int, error) {
	if p == nil || p.repo == nil {
		return 0, fmt.Errorf("Nomad lifecycle metering projector is not configured")
	}
	processed := 0
	for processed < p.config.BatchSize {
		found, err := p.processOne(ctx)
		if err != nil {
			return processed, err
		}
		if !found {
			break
		}
		processed++
	}
	return processed, nil
}

func (p *NomadLifecycleProjector) processOne(ctx context.Context) (bool, error) {
	var claimed *nomadMeteringQueueItem
	var projectedEvents []*meteringpkg.Event
	var projectedWindows []*meteringpkg.Window
	err := p.repo.InTx(ctx, func(tx pgx.Tx) error {
		item, err := claimNomadMeteringQueueItem(ctx, tx)
		if err != nil || item == nil {
			return err
		}
		claimed = item
		source, err := loadNomadMeteringSource(ctx, tx, item.SandboxID)
		if err != nil {
			return err
		}
		state, err := p.repo.GetSandboxProjectionStateTx(ctx, tx, item.SandboxID)
		if err != nil {
			return err
		}
		if state != nil && state.SourceRevision >= item.Revision {
			return deleteNomadMeteringQueueItem(ctx, tx, item)
		}
		cursor := int64(0)
		if state != nil {
			cursor = state.SourceLifecycleEpoch
		}
		transitions, err := loadNomadMeteringLifecycleTransitions(ctx, tx, item.SandboxID, cursor)
		if err != nil {
			return err
		}
		mutations, err := p.project(source, state, transitions, item.Revision)
		if err != nil {
			return err
		}
		projectedEvents = mutations.events
		projectedWindows = mutations.windows
		for _, event := range mutations.events {
			if err := p.repo.AppendEventTx(ctx, tx, event); err != nil {
				return fmt.Errorf("append Nomad lifecycle event %s: %w", event.EventType, err)
			}
		}
		for _, window := range mutations.windows {
			if err := p.repo.AppendWindowTx(ctx, tx, window); err != nil {
				return fmt.Errorf("append Nomad lifecycle window %s: %w", window.WindowType, err)
			}
		}
		if err := p.repo.UpsertSandboxProjectionStateTx(ctx, tx, mutations.state); err != nil {
			return fmt.Errorf("upsert Nomad lifecycle state: %w", err)
		}
		return deleteNomadMeteringQueueItem(ctx, tx, item)
	})
	if err != nil {
		p.recordProjectionResults(projectedEvents, projectedWindows, "error")
		if claimed != nil {
			p.recordFailure(ctx, claimed, err)
			p.recordError("nomad_project_sandbox", claimed.SandboxID, err)
		}
		return false, err
	}
	if claimed == nil {
		return false, nil
	}
	p.recordProjectionResults(projectedEvents, projectedWindows, "success")
	return true, nil
}

func claimNomadMeteringQueueItem(ctx context.Context, tx pgx.Tx) (*nomadMeteringQueueItem, error) {
	item := &nomadMeteringQueueItem{}
	err := tx.QueryRow(ctx, `
		SELECT sandbox_id, revision, attempts
		FROM manager.sandbox_metering_projection_queue
		WHERE available_at <= NOW()
		ORDER BY revision, sandbox_id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&item.SandboxID, &item.Revision, &item.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim Nomad metering queue item: %w", err)
	}
	return item, nil
}

func loadNomadMeteringSource(ctx context.Context, tx pgx.Tx, sandboxID string) (*nomadSandboxMeteringSource, error) {
	source := &nomadSandboxMeteringSource{}
	err := tx.QueryRow(ctx, `
		SELECT sandbox.sandbox_id, sandbox.team_id, sandbox.user_id,
			sandbox.template_id, sandbox.cluster_id, sandbox.desired_state,
			sandbox.runtime_namespace, sandbox.owner_kind,
			sandbox.resource_millicpu, sandbox.resource_memory_mib,
			sandbox.claimed_at, sandbox.expires_at, sandbox.hard_expires_at,
			sandbox.deleted_at, claim.completed_at, NOW()
		FROM manager.sandboxes AS sandbox
		LEFT JOIN manager.sandbox_runtime_claims AS claim
			ON claim.sandbox_id = sandbox.sandbox_id
		WHERE sandbox.sandbox_id = $1
	`, sandboxID).Scan(
		&source.SandboxID, &source.TeamID, &source.UserID,
		&source.TemplateID, &source.ClusterID, &source.DesiredState,
		&source.AllocationNamespace, &source.OwnerKind,
		&source.ResourceMillicpu, &source.ResourceMemoryMiB,
		&source.ClaimedAt, &source.ExpiresAt, &source.HardExpiresAt,
		&source.DeletedAt, &source.InitialActiveAt, &source.ObservedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("Nomad metering source %s is missing", sandboxID)
	}
	if err != nil {
		return nil, fmt.Errorf("load Nomad metering source %s: %w", sandboxID, err)
	}
	return source, nil
}

func loadNomadMeteringLifecycleTransitions(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
	afterEpoch int64,
) ([]nomadMeteringLifecycleTransition, error) {
	rows, err := tx.Query(ctx, `
		SELECT txn_id, kind, phase, source, epoch, error, committed_at, aborted_at
		FROM manager.sandbox_lifecycle_txns
		WHERE sandbox_id = $1 AND epoch > $2 AND phase IN ($3, $4)
		ORDER BY epoch, txn_id
	`, sandboxID, afterEpoch, sandboxstore.SandboxLifecyclePhaseCommitted, sandboxstore.SandboxLifecyclePhaseAborted)
	if err != nil {
		return nil, fmt.Errorf("load Nomad metering lifecycle transitions: %w", err)
	}
	defer rows.Close()
	transitions := make([]nomadMeteringLifecycleTransition, 0)
	for rows.Next() {
		var transition nomadMeteringLifecycleTransition
		if err := rows.Scan(
			&transition.ID, &transition.Kind, &transition.Phase, &transition.Source,
			&transition.Epoch, &transition.Error, &transition.CommittedAt, &transition.AbortedAt,
		); err != nil {
			return nil, fmt.Errorf("scan Nomad metering lifecycle transition: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Nomad metering lifecycle transitions: %w", err)
	}
	return transitions, nil
}

func (p *NomadLifecycleProjector) project(
	source *nomadSandboxMeteringSource,
	prior *meteringpkg.SandboxProjectionState,
	transitions []nomadMeteringLifecycleTransition,
	revision int64,
) (*nomadProjectionMutations, error) {
	if err := validateNomadMeteringSource(source); err != nil {
		return nil, err
	}
	state := cloneSandboxProjectionState(prior)
	result := &nomadProjectionMutations{state: state}
	clusterID := source.ClusterID
	if clusterID == "" {
		clusterID = p.clusterID
	}
	if state == nil {
		state = &meteringpkg.SandboxProjectionState{
			SandboxID: source.SandboxID, Namespace: nomadMeteringNamespace(source.AllocationNamespace),
			TeamID: source.TeamID, UserID: source.UserID, TemplateID: source.TemplateID,
			ClusterID: clusterID, OwnerKind: source.OwnerKind,
			ResourceMillicpu: source.ResourceMillicpu, ResourceMemoryMiB: source.ResourceMemoryMiB,
			ClaimedAt: cloneTimePtr(source.ClaimedAt),
		}
		result.state = state
		result.events = append(result.events, p.nomadSandboxEvent(
			source, clusterID, *source.ClaimedAt, meteringpkg.EventTypeSandboxClaimed,
			claimedEventID(source.SandboxID, *source.ClaimedAt), nomadClaimEventData(source),
		))
		if source.InitialActiveAt != nil {
			state.ActiveSince = cloneTimePtr(source.InitialActiveAt)
		} else if source.DesiredState == sandboxstore.SandboxDesiredStatePaused {
			state.Paused = true
			state.PausedAt = cloneTimePtr(source.ClaimedAt)
		}
	} else {
		if state.SandboxID != source.SandboxID || state.TerminatedAt != nil && source.DeletedAt == nil {
			return nil, fmt.Errorf("Nomad metering projection identity changed for %s", source.SandboxID)
		}
		if state.ResourceMillicpu != 0 && state.ResourceMillicpu != source.ResourceMillicpu ||
			state.ResourceMemoryMiB != 0 && state.ResourceMemoryMiB != source.ResourceMemoryMiB {
			return nil, fmt.Errorf("Nomad metering resources changed without a durable resize transition")
		}
		if state.TerminatedAt == nil && state.ActiveSince == nil && !state.Paused && source.InitialActiveAt != nil {
			state.ActiveSince = cloneTimePtr(source.InitialActiveAt)
		}
	}

	state.Namespace = nomadMeteringNamespace(source.AllocationNamespace)
	state.TeamID = source.TeamID
	state.UserID = source.UserID
	state.TemplateID = source.TemplateID
	state.ClusterID = clusterID
	state.OwnerKind = source.OwnerKind
	state.ResourceMillicpu = source.ResourceMillicpu
	state.ResourceMemoryMiB = source.ResourceMemoryMiB
	state.ClaimedAt = cloneTimePtr(source.ClaimedAt)

	for _, transition := range transitions {
		if transition.Epoch <= state.SourceLifecycleEpoch {
			continue
		}
		if state.TerminatedAt == nil {
			switch {
			case transition.Kind == sandboxstore.SandboxLifecycleKindPause && nomadPauseTransitionSucceeded(transition):
				occurredAt, err := nomadTransitionTime(transition)
				if err != nil {
					return nil, err
				}
				if !state.Paused {
					window, err := p.closeNomadRuntimeWindow(state, source, clusterID, occurredAt)
					if err != nil {
						return nil, err
					}
					if window != nil {
						result.windows = append(result.windows, window)
					}
					result.events = append(result.events, p.nomadSandboxEvent(
						source, clusterID, occurredAt, meteringpkg.EventTypeSandboxPaused,
						nomadLifecycleEventID(source.SandboxID, "paused", transition.ID),
						nomadLifecycleEventData(transition),
					))
					state.PausedAt = &occurredAt
				}
				state.Paused = true
				state.ActiveSince = nil
			case transition.Kind == sandboxstore.SandboxLifecycleKindResume && transition.Phase == sandboxstore.SandboxLifecyclePhaseCommitted:
				occurredAt, err := nomadTransitionTime(transition)
				if err != nil {
					return nil, err
				}
				if state.Paused || state.ActiveSince == nil {
					result.events = append(result.events, p.nomadSandboxEvent(
						source, clusterID, occurredAt, meteringpkg.EventTypeSandboxResumed,
						nomadLifecycleEventID(source.SandboxID, "resumed", transition.ID),
						nomadLifecycleEventData(transition),
					))
					state.ActiveSince = &occurredAt
				}
				state.Paused = false
				state.PausedAt = nil
			}
		}
		state.SourceLifecycleEpoch = transition.Epoch
	}

	if source.DeletedAt != nil && state.TerminatedAt == nil {
		terminatedAt := source.DeletedAt.UTC()
		if !state.Paused {
			window, err := p.closeNomadRuntimeWindow(state, source, clusterID, terminatedAt)
			if err != nil {
				return nil, err
			}
			if window != nil {
				result.windows = append(result.windows, window)
			}
		}
		result.events = append(result.events, p.nomadSandboxEvent(
			source, clusterID, terminatedAt, meteringpkg.EventTypeSandboxTerminated,
			nomadLifecycleEventID(source.SandboxID, "terminated", fmt.Sprintf("%d", terminatedAt.UnixNano())), nil,
		))
		state.ActiveSince = nil
		state.TerminatedAt = &terminatedAt
	}

	observedAt := source.ObservedAt.UTC()
	if !observedAt.After(state.LastObservedAt) {
		observedAt = state.LastObservedAt.Add(time.Microsecond)
	}
	state.LastObservedAt = observedAt
	state.SourceRevision = revision
	state.LastResourceVer = fmt.Sprintf("nomad/%d/%d", revision, state.SourceLifecycleEpoch)
	return result, nil
}

func validateNomadMeteringSource(source *nomadSandboxMeteringSource) error {
	if source == nil || strings.TrimSpace(source.SandboxID) == "" {
		return fmt.Errorf("Nomad metering source is required")
	}
	if source.ClaimedAt == nil || source.ClaimedAt.IsZero() {
		return fmt.Errorf("Nomad sandbox %s has no trusted claimed_at", source.SandboxID)
	}
	if strings.TrimSpace(source.TeamID) == "" || strings.TrimSpace(source.TemplateID) == "" {
		return fmt.Errorf("Nomad sandbox %s has incomplete metering ownership", source.SandboxID)
	}
	if source.ResourceMillicpu <= 0 || source.ResourceMemoryMiB <= 0 {
		return fmt.Errorf("Nomad sandbox %s has invalid metering resources %dm/%dMiB",
			source.SandboxID, source.ResourceMillicpu, source.ResourceMemoryMiB)
	}
	if source.ObservedAt.IsZero() {
		return fmt.Errorf("Nomad sandbox %s has no PostgreSQL observation time", source.SandboxID)
	}
	return nil
}

func nomadPauseTransitionSucceeded(transition nomadMeteringLifecycleTransition) bool {
	if transition.Phase == sandboxstore.SandboxLifecyclePhaseCommitted {
		return true
	}
	if transition.Phase != sandboxstore.SandboxLifecyclePhaseAborted ||
		transition.Error != sandboxstore.RootFSWriterCrashAbandonReason {
		return false
	}
	switch transition.Source {
	case sandboxstore.SandboxLifecycleSourceCrash,
		sandboxstore.SandboxLifecycleSourceHealth,
		sandboxstore.SandboxLifecycleSourceLost:
		return true
	default:
		return false
	}
}

func nomadTransitionTime(transition nomadMeteringLifecycleTransition) (time.Time, error) {
	var value *time.Time
	switch transition.Phase {
	case sandboxstore.SandboxLifecyclePhaseCommitted:
		value = transition.CommittedAt
	case sandboxstore.SandboxLifecyclePhaseAborted:
		value = transition.AbortedAt
	}
	if value == nil || value.IsZero() {
		return time.Time{}, fmt.Errorf("Nomad lifecycle %s has no terminal timestamp", transition.ID)
	}
	return value.UTC(), nil
}

func (p *NomadLifecycleProjector) closeNomadRuntimeWindow(
	state *meteringpkg.SandboxProjectionState,
	source *nomadSandboxMeteringSource,
	clusterID string,
	endedAt time.Time,
) (*meteringpkg.Window, error) {
	if state.ActiveSince == nil {
		return nil, nil
	}
	startedAt := state.ActiveSince.UTC()
	endedAt = endedAt.UTC()
	if endedAt.Before(startedAt) {
		return nil, fmt.Errorf("Nomad runtime window for %s ends before it starts", source.SandboxID)
	}
	durationMS := endedAt.Sub(startedAt).Milliseconds()
	if durationMS == 0 {
		return nil, nil
	}
	if state.ResourceMemoryMiB > math.MaxInt64/durationMS {
		return nil, fmt.Errorf("Nomad runtime window value overflows for %s", source.SandboxID)
	}
	return &meteringpkg.Window{
		WindowID: sandboxWindowID(source.SandboxID, meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds, startedAt, endedAt),
		Producer: sandboxLifecycleProducer, RegionID: p.regionID,
		WindowType:  meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds,
		SubjectType: meteringpkg.SubjectTypeSandbox, SubjectID: source.SandboxID,
		TeamID: source.TeamID, UserID: source.UserID, SandboxID: source.SandboxID,
		TemplateID: source.TemplateID, ClusterID: clusterID,
		WindowStart: startedAt, WindowEnd: endedAt,
		Value: state.ResourceMemoryMiB * durationMS, Unit: meteringpkg.WindowUnitMiBMilliseconds,
		Data: runtimeWindowData(state, durationMS),
	}, nil
}

func (p *NomadLifecycleProjector) nomadSandboxEvent(
	source *nomadSandboxMeteringSource,
	clusterID string,
	occurredAt time.Time,
	eventType string,
	eventID string,
	data any,
) *meteringpkg.Event {
	return &meteringpkg.Event{
		EventID: eventID, Producer: sandboxLifecycleProducer, RegionID: p.regionID,
		EventType: eventType, SubjectType: meteringpkg.SubjectTypeSandbox,
		SubjectID: source.SandboxID, TeamID: source.TeamID, UserID: source.UserID,
		SandboxID: source.SandboxID, TemplateID: source.TemplateID, ClusterID: clusterID,
		OccurredAt: occurredAt.UTC(), Data: mustJSON(data),
	}
}

func nomadClaimEventData(source *nomadSandboxMeteringSource) map[string]any {
	return map[string]any{
		"owner_kind":      source.OwnerKind,
		"expires_at":      source.ExpiresAt,
		"hard_expires_at": source.HardExpiresAt,
	}
}

func nomadLifecycleEventData(transition nomadMeteringLifecycleTransition) map[string]any {
	return map[string]any{
		"lifecycle_txn_id": transition.ID,
		"lifecycle_epoch":  transition.Epoch,
		"source":           transition.Source,
	}
}

func nomadLifecycleEventID(sandboxID, event, identity string) string {
	return fmt.Sprintf("sandbox/%s/%s/nomad/%s", sandboxID, event, identity)
}

func nomadMeteringNamespace(allocationNamespace string) string {
	if value := strings.TrimSpace(allocationNamespace); value != "" {
		return value
	}
	return nomadMeteringNamespaceFallback
}

func cloneSandboxProjectionState(state *meteringpkg.SandboxProjectionState) *meteringpkg.SandboxProjectionState {
	if state == nil {
		return nil
	}
	clone := *state
	clone.ClaimedAt = cloneTimePtr(state.ClaimedAt)
	clone.ActiveSince = cloneTimePtr(state.ActiveSince)
	clone.PausedAt = cloneTimePtr(state.PausedAt)
	clone.TerminatedAt = cloneTimePtr(state.TerminatedAt)
	return &clone
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := value.UTC()
	return &clone
}

func deleteNomadMeteringQueueItem(ctx context.Context, tx pgx.Tx, item *nomadMeteringQueueItem) error {
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.sandbox_metering_projection_queue
		WHERE sandbox_id = $1 AND revision = $2
	`, item.SandboxID, item.Revision)
	if err != nil {
		return fmt.Errorf("complete Nomad metering queue item: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("Nomad metering queue item %s/%d changed", item.SandboxID, item.Revision)
	}
	return nil
}

func (p *NomadLifecycleProjector) recordFailure(ctx context.Context, item *nomadMeteringQueueItem, projectionErr error) {
	if item == nil || p.repo == nil {
		return
	}
	backoff := p.config.FailureBaseBackoff
	for attempt := 0; attempt < item.Attempts && backoff < p.config.FailureMaxBackoff; attempt++ {
		if backoff > p.config.FailureMaxBackoff/2 {
			backoff = p.config.FailureMaxBackoff
			break
		}
		backoff *= 2
	}
	if backoff > p.config.FailureMaxBackoff {
		backoff = p.config.FailureMaxBackoff
	}
	message := projectionErr.Error()
	if len(message) > maxNomadMeteringErrorBytes {
		message = message[:maxNomadMeteringErrorBytes]
	}
	if err := p.repo.InTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE manager.sandbox_metering_projection_queue
			SET attempts = attempts + 1,
				available_at = NOW() + ($3 * INTERVAL '1 millisecond'),
				last_error = $4,
				updated_at = NOW()
			WHERE sandbox_id = $1 AND revision = $2
		`, item.SandboxID, item.Revision, backoff.Milliseconds(), message)
		return err
	}); err != nil {
		p.recordError("nomad_record_failure", item.SandboxID, err)
	}
}

func (p *NomadLifecycleProjector) advanceWatermarkIfDrained(ctx context.Context) error {
	return p.repo.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE manager.sandbox_metering_projection_queue IN SHARE MODE`); err != nil {
			return fmt.Errorf("lock Nomad metering queue for watermark: %w", err)
		}
		var pending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandbox_metering_projection_queue)`).Scan(&pending); err != nil {
			return fmt.Errorf("check Nomad metering queue for watermark: %w", err)
		}
		if pending {
			return nil
		}
		var completeBefore time.Time
		if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&completeBefore); err != nil {
			return fmt.Errorf("read Nomad metering watermark time: %w", err)
		}
		return p.repo.UpsertProducerWatermarkTx(ctx, tx, sandboxLifecycleProducer, p.regionID, completeBefore)
	})
}

func (p *NomadLifecycleProjector) recordError(operation, sandboxID string, err error) {
	if p.metrics != nil {
		p.metrics.MeteringErrorsTotal.WithLabelValues(operation).Inc()
	}
	p.logger.Error("Nomad lifecycle metering projection failed",
		zap.String("operation", operation), zap.String("sandboxID", sandboxID), zap.Error(err))
}

func (p *NomadLifecycleProjector) recordProjectionResults(
	events []*meteringpkg.Event,
	windows []*meteringpkg.Window,
	result string,
) {
	if p.metrics == nil {
		return
	}
	for _, event := range events {
		if event != nil {
			p.metrics.MeteringEventsTotal.WithLabelValues(event.EventType, result).Inc()
		}
	}
	for _, window := range windows {
		if window != nil {
			p.metrics.MeteringWindowsTotal.WithLabelValues(window.WindowType, result).Inc()
		}
	}
}
