package teamquota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/pkg/pglock"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	policyWriterLockResource = "sandbox0:team-quota:policy-writer"
	policyUnlockTimeout      = 5 * time.Second
	defaultRepairInterval    = time.Second
)

// DefaultPolicyVersion fences startup reconciliation from older rollout pods.
// OwnerEpoch is the owning infra object's creation timestamp; Generation is
// its monotonically increasing metadata.generation.
type DefaultPolicyVersion struct {
	OwnerEpoch time.Time
	Generation int64
}

func (v DefaultPolicyVersion) validate() error {
	v = v.normalized()
	if v.OwnerEpoch.IsZero() {
		return fmt.Errorf("default policy owner epoch is required")
	}
	if v.Generation <= 0 {
		return fmt.Errorf("default policy generation must be positive")
	}
	return nil
}

// normalized matches PostgreSQL TIMESTAMPTZ's microsecond round-trip
// precision. Comparing a finer caller timestamp against the stored value
// would otherwise make one owner appear newer after every restart.
func (v DefaultPolicyVersion) normalized() DefaultPolicyVersion {
	v.OwnerEpoch = v.OwnerEpoch.UTC().Truncate(time.Microsecond)
	return v
}

// StaleDefaultPolicyVersionError rejects an older rollout owner.
type StaleDefaultPolicyVersionError struct {
	Stored    DefaultPolicyVersion
	Requested DefaultPolicyVersion
}

func (e *StaleDefaultPolicyVersionError) Error() string {
	return fmt.Sprintf(
		"Team Quota defaults version %s/%d is older than stored version %s/%d",
		e.Requested.OwnerEpoch.UTC().Format(time.RFC3339Nano),
		e.Requested.Generation,
		e.Stored.OwnerEpoch.UTC().Format(time.RFC3339Nano),
		e.Stored.Generation,
	)
}

// DefaultPolicyHashConflictError detects split-brain config at one version.
type DefaultPolicyHashConflictError struct {
	Version         DefaultPolicyVersion
	StoredSHA256    string
	RequestedSHA256 string
}

func (e *DefaultPolicyHashConflictError) Error() string {
	return fmt.Sprintf(
		"Team Quota defaults version %s/%d has conflicting canonical SHA256",
		e.Version.OwnerEpoch.UTC().Format(time.RFC3339Nano),
		e.Version.Generation,
	)
}

// PolicyCoordinatorConfig configures the region policy owner.
type PolicyCoordinatorConfig struct {
	RegionID        string
	ExpectedStateID string
	RedisURL        string
	RedisKeyPrefix  string
	RedisTimeout    time.Duration
	LeaseTTL        time.Duration
	RepairInterval  time.Duration
}

type policyGuardStore interface {
	guard.Reader
	ServerTime(context.Context) (time.Time, error)
	ReadRuntimeSafety(context.Context) (guard.RuntimeSafety, error)
	SetPending(context.Context, guard.Version, string, time.Time) error
	SetStable(context.Context, string, guard.State) error
	Force(context.Context, guard.State) error
	Invalidate(context.Context) error
	Close() error
}

type regionStateIdentityMaintenance interface {
	verifyAndRepairOnConn(
		context.Context,
		*pgxpool.Conn,
	) (RegionStateIdentityMaintenanceResult, error)
}

// PolicyCoordinator serializes every policy writer across PostgreSQL commit
// and Redis publication. It is used only by the region policy owner.
type PolicyCoordinator struct {
	pool                    *pgxpool.Pool
	repository              *Repository
	policyGuard             policyGuardStore
	stateIdentityMaintainer regionStateIdentityMaintenance
	leaseTTL                time.Duration
	repairInterval          time.Duration
	now                     func() time.Time
	creditDrainTTL          time.Duration
	waitUntil               func(context.Context, time.Time) error
	writerGateOnce          sync.Once
	writerGate              chan struct{}

	// commitPolicyTx is a test seam for exercising both outcomes of a
	// PostgreSQL commit whose client-visible result is ambiguous.
	commitPolicyTx func(context.Context, pgx.Tx) error
}

var (
	_ PolicyManager              = (*PolicyCoordinator)(nil)
	_ TeamAdmissionStateResolver = (*PolicyCoordinator)(nil)
)

// NewPolicyCoordinator validates Redis INFO access and creates the owner.
func NewPolicyCoordinator(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg PolicyCoordinatorConfig,
) (*PolicyCoordinator, error) {
	if pool == nil {
		return nil, fmt.Errorf("team quota policy coordinator requires PostgreSQL")
	}
	if strings.TrimSpace(cfg.RedisURL) == "" {
		return nil, fmt.Errorf("team quota policy coordinator requires Redis URL")
	}
	if cfg.LeaseTTL <= 0 || cfg.LeaseTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("team quota policy coordinator lease TTL must use positive whole milliseconds")
	}
	stateIdentityMaintainer, err := NewRegionStateIdentityMaintainer(
		pool,
		RegionStateIdentityConfig{
			RegionID:        cfg.RegionID,
			ExpectedStateID: cfg.ExpectedStateID,
			RedisURL:        cfg.RedisURL,
			RedisKeyPrefix:  cfg.RedisKeyPrefix,
			RedisTimeout:    cfg.RedisTimeout,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("initialize Team Quota region state identity maintenance: %w", err)
	}
	policyGuard, err := guard.NewRedis(ctx, guard.Config{
		URL:       cfg.RedisURL,
		KeyPrefix: NormalizeTeamQuotaRedisKeyPrefix(cfg.RedisKeyPrefix),
		Timeout:   cfg.RedisTimeout,
	})
	if err != nil {
		return nil, err
	}
	repairInterval := cfg.RepairInterval
	if repairInterval <= 0 {
		repairInterval = defaultRepairInterval
	}
	coordinator := &PolicyCoordinator{
		pool:                    pool,
		repository:              NewRepository(pool),
		policyGuard:             policyGuard,
		stateIdentityMaintainer: stateIdentityMaintainer,
		leaseTTL:                cfg.LeaseTTL,
		repairInterval:          repairInterval,
		now:                     time.Now,
		creditDrainTTL:          guard.MaxLocalCreditTTL,
		waitUntil:               waitUntil,
	}
	if err := coordinator.Repair(ctx); err != nil {
		_ = coordinator.Close()
		return nil, fmt.Errorf(
			"initialize Team Quota policy owner state and Redis 7 INFO server memory stats safety: %w",
			err,
		)
	}
	return coordinator, nil
}

// ListStatus delegates strongly consistent status reads to PostgreSQL.
func (c *PolicyCoordinator) ListStatus(ctx context.Context, teamID string) ([]Status, error) {
	return c.repository.ListStatus(ctx, teamID)
}

// EffectivePolicy delegates policy resolution to PostgreSQL.
func (c *PolicyCoordinator) EffectivePolicy(ctx context.Context, teamID string, key Key) (*Policy, error) {
	return c.repository.EffectivePolicy(ctx, teamID, key)
}

// TeamAdmissionDisabled delegates durable tombstone reads.
func (c *PolicyCoordinator) TeamAdmissionDisabled(ctx context.Context, teamID string) (bool, error) {
	return c.repository.TeamAdmissionDisabled(ctx, teamID)
}

// ListDeletedTeamTombstones delegates retention scans.
func (c *PolicyCoordinator) ListDeletedTeamTombstones(
	ctx context.Context,
	before time.Time,
	after *DeletedTeamTombstone,
	limit int,
) ([]DeletedTeamTombstone, error) {
	return c.repository.ListDeletedTeamTombstones(ctx, before, after, limit)
}

// PruneDeletedTeamTombstone delegates conditional retention cleanup.
func (c *PolicyCoordinator) PruneDeletedTeamTombstone(
	ctx context.Context,
	teamID string,
	before time.Time,
) (bool, error) {
	return c.repository.PruneDeletedTeamTombstone(ctx, teamID, before)
}

// PutTeamPolicy atomically publishes one override and its enforcement epoch.
func (c *PolicyCoordinator) PutTeamPolicy(ctx context.Context, teamID string, policy Policy) error {
	return c.withWriterLock(ctx, func(conn *pgxpool.Conn) error {
		if _, err := c.repairLocked(ctx, conn); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return unavailablePolicyWrite("begin team policy update", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		state, err := loadPolicyState(ctx, tx, true)
		if err != nil {
			return err
		}
		current, err := teamPolicyTx(ctx, tx, strings.TrimSpace(teamID), policy.Key)
		if err != nil {
			return err
		}
		changed := current == nil || !policyContentEqual(*current, Policy{
			TeamID:         strings.TrimSpace(teamID),
			Key:            policy.Key,
			Kind:           policy.Kind,
			Limit:          policy.Limit,
			Tokens:         policy.Tokens,
			IntervalMillis: policy.IntervalMillis,
			Burst:          policy.Burst,
		})
		if err := c.repository.putTeamPolicyTx(ctx, tx, teamID, policy); err != nil {
			return err
		}
		distributedChanged := changed &&
			(policy.Kind == KindRate || policy.Kind == KindConcurrency)
		token, target, err := c.prepareCommit(ctx, tx, state, distributedChanged)
		if err != nil {
			return err
		}
		if err := c.commitPolicyTransaction(ctx, tx); err != nil {
			return unavailablePolicyWrite("commit team policy update with ambiguous outcome", err)
		}
		return c.publishCommit(ctx, token, target)
	})
}

// DeleteTeamPolicy atomically reveals the current region default.
func (c *PolicyCoordinator) DeleteTeamPolicy(ctx context.Context, teamID string, key Key) error {
	return c.withWriterLock(ctx, func(conn *pgxpool.Conn) error {
		if _, err := c.repairLocked(ctx, conn); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return unavailablePolicyWrite("begin team policy deletion", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		state, err := loadPolicyState(ctx, tx, true)
		if err != nil {
			return err
		}
		current, err := teamPolicyTx(ctx, tx, strings.TrimSpace(teamID), key)
		if err != nil {
			return err
		}
		if err := c.repository.deleteTeamPolicyTx(ctx, tx, teamID, key); err != nil {
			return err
		}
		kind, _ := KindForKey(key)
		distributedChanged := current != nil &&
			(kind == KindRate || kind == KindConcurrency)
		token, target, err := c.prepareCommit(ctx, tx, state, distributedChanged)
		if err != nil {
			return err
		}
		if err := c.commitPolicyTransaction(ctx, tx); err != nil {
			return unavailablePolicyWrite("commit team policy deletion with ambiguous outcome", err)
		}
		return c.publishCommit(ctx, token, target)
	})
}

// ReplaceDefaultPoliciesVersioned applies a complete owner configuration.
func (c *PolicyCoordinator) ReplaceDefaultPoliciesVersioned(
	ctx context.Context,
	policies []Policy,
	version DefaultPolicyVersion,
) error {
	version = version.normalized()
	if err := version.validate(); err != nil {
		return err
	}
	normalized, err := normalizeDefaultPolicies(policies)
	if err != nil {
		return err
	}
	sha256, err := canonicalPolicySHA256(normalized)
	if err != nil {
		return err
	}
	return c.withWriterLock(ctx, func(conn *pgxpool.Conn) error {
		if _, err := c.repairLocked(ctx, conn); err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return unavailablePolicyWrite("begin default policy reconciliation", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		state, err := loadPolicyState(ctx, tx, true)
		if err != nil {
			return err
		}
		fence, err := compareDefaultVersion(state, version, sha256)
		if err != nil {
			return err
		}
		if fence == defaultFenceIdempotent {
			if err := c.commitPolicyTransaction(ctx, tx); err != nil {
				return unavailablePolicyWrite("commit idempotent default policy reconciliation with ambiguous outcome", err)
			}
			return nil
		}
		existing, err := listDefaultPoliciesTx(ctx, tx)
		if err != nil {
			return err
		}
		distributedChanged := distributedDefaultsChanged(existing, normalized)
		contentChanged := defaultsChanged(existing, normalized)
		if contentChanged {
			if err := c.repository.replaceDefaultPoliciesTx(ctx, tx, normalized); err != nil {
				return err
			}
		}
		state.DefaultsOwnerEpoch = version.OwnerEpoch
		state.DefaultsGeneration = version.Generation
		state.DefaultsSHA256 = sha256
		token, target, err := c.prepareCommit(ctx, tx, state, distributedChanged)
		if err != nil {
			return err
		}
		if err := c.commitPolicyTransaction(ctx, tx); err != nil {
			return unavailablePolicyWrite("commit default policy reconciliation with ambiguous outcome", err)
		}
		return c.publishCommit(ctx, token, target)
	})
}

// Repair initializes or repairs the non-expiring Redis guard.
func (c *PolicyCoordinator) Repair(ctx context.Context) error {
	return c.withWriterLock(ctx, func(conn *pgxpool.Conn) error {
		_, err := c.repairLocked(ctx, conn)
		return err
	})
}

// RunRepair periodically repairs runtime Redis loss. It returns when ctx ends.
func (c *PolicyCoordinator) RunRepair(ctx context.Context, report func(error)) {
	if c == nil {
		return
	}
	ticker := time.NewTicker(c.repairInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Repair(ctx); err != nil && report != nil {
				report(err)
			}
		}
	}
}

// Close releases the policy-owner Redis client.
func (c *PolicyCoordinator) Close() error {
	if c == nil || c.policyGuard == nil {
		return nil
	}
	return c.policyGuard.Close()
}

func (c *PolicyCoordinator) prepareCommit(
	ctx context.Context,
	tx pgx.Tx,
	state PolicyState,
	distributedChanged bool,
) (string, PolicyState, error) {
	if !distributedChanged {
		if err := storePolicyState(ctx, tx, state); err != nil {
			return "", PolicyState{}, err
		}
		return "", state, nil
	}
	token := uuid.NewString()
	if err := c.policyGuard.SetPending(ctx, state.Version(), token, time.Time{}); err != nil {
		return "", PolicyState{}, unavailablePolicyWrite("close distributed policy guard", err)
	}
	if err := c.waitForCreditDrain(ctx); err != nil {
		return "", PolicyState{}, unavailablePolicyWrite("drain local rate credits", err)
	}
	state.EnforcementEpoch++
	if err := storePolicyState(ctx, tx, state); err != nil {
		return "", PolicyState{}, err
	}
	return token, state, nil
}

func (c *PolicyCoordinator) publishCommit(
	ctx context.Context,
	token string,
	state PolicyState,
) error {
	if token == "" {
		return nil
	}
	if err := c.policyGuard.SetStable(ctx, token, state.stableGuard()); err != nil {
		return unavailablePolicyWrite("publish committed distributed policy state", err)
	}
	return nil
}

func (c *PolicyCoordinator) commitPolicyTransaction(ctx context.Context, tx pgx.Tx) error {
	if c.commitPolicyTx != nil {
		return c.commitPolicyTx(ctx, tx)
	}
	return tx.Commit(ctx)
}

func (c *PolicyCoordinator) repairLocked(
	ctx context.Context,
	conn *pgxpool.Conn,
) (PolicyState, error) {
	safety, err := c.policyGuard.ReadRuntimeSafety(ctx)
	if err != nil {
		return PolicyState{}, unavailablePolicyWrite("read Redis runtime safety", err)
	}
	state, err := c.loadStateOnConn(ctx, conn)
	if err != nil {
		return PolicyState{}, err
	}
	redisState, guardErr := c.policyGuard.ReadPolicyGuard(ctx)
	if !state.RedisInitialized {
		switch {
		case errors.Is(guardErr, guard.ErrMissing):
			return c.bootstrapRedisLocked(ctx, conn, state, safety)
		case guardErr == nil, errors.Is(guardErr, guard.ErrCorrupt):
			// PostgreSQL may have been rebuilt while Redis retained older
			// leases, local-credit grants, or replay keys. Close admission and
			// drain those consumers before publishing the replacement durable
			// generation, even though the PostgreSQL generation starts at zero.
			return c.resetRedisLocked(ctx, conn, state, safety, redisState, guardErr)
		default:
			return PolicyState{}, unavailablePolicyWrite(
				"read distributed policy guard before initial Redis bootstrap",
				guardErr,
			)
		}
	}
	if safety.RunID != state.RedisRunID ||
		safety.EvictedKeys != state.RedisEvictedKeys ||
		errors.Is(guardErr, guard.ErrMissing) ||
		errors.Is(guardErr, guard.ErrCorrupt) ||
		(guardErr == nil && redisState.RedisRunID != safety.RunID) ||
		(guardErr == nil && redisState.RedisEvictedKeys != safety.EvictedKeys) ||
		(guardErr == nil && redisState.Stable() && !redisState.Version.Equal(state.Version())) {
		return c.resetRedisLocked(ctx, conn, state, safety, redisState, guardErr)
	}
	if guardErr != nil {
		return PolicyState{}, unavailablePolicyWrite("read distributed policy guard", guardErr)
	}
	if redisState.Phase == guard.PhasePending {
		if !redisState.QuarantineUntil.IsZero() &&
			!resetPendingMatchesState(redisState, state, c.resetQuarantineTTL()) {
			// A reset-pending guard that is not backed by the exact durable
			// reset state can be left behind by an ambiguous or failed
			// PostgreSQL commit. Re-run the reset instead of publishing the
			// older generation as stable.
			return c.resetRedisLocked(ctx, conn, state, safety, redisState, nil)
		}
		if !redisState.QuarantineUntil.IsZero() {
			if err := c.waitForRedisDeadline(ctx, redisState.QuarantineUntil); err != nil {
				return PolicyState{}, err
			}
		}
		if err := c.policyGuard.Force(ctx, state.stableGuard()); err != nil {
			return PolicyState{}, unavailablePolicyWrite("recover pending distributed policy guard", err)
		}
		return state, nil
	}
	if !redisState.Version.Equal(state.Version()) {
		return c.resetRedisLocked(ctx, conn, state, safety, redisState, nil)
	}
	return state, nil
}

func resetPendingMatchesState(
	pending guard.State,
	state PolicyState,
	leaseTTL time.Duration,
) bool {
	if pending.QuarantineUntil.IsZero() ||
		state.RedisResetAt.IsZero() ||
		state.RateRefillFrom.IsZero() ||
		!pending.Version.Equal(state.Version()) {
		return false
	}
	resetAt := state.RedisResetAt.UTC().Truncate(time.Millisecond)
	rateRefillFrom := state.RateRefillFrom.UTC().Truncate(time.Millisecond)
	return pending.ResetAt.UTC().Truncate(time.Millisecond).Equal(resetAt) &&
		pending.RedisRunID == state.RedisRunID &&
		pending.RedisEvictedKeys == state.RedisEvictedKeys &&
		pending.RateRefillFrom.UTC().Truncate(time.Millisecond).Equal(rateRefillFrom) &&
		pending.QuarantineUntil.UTC().Truncate(time.Millisecond).Equal(resetAt.Add(leaseTTL))
}

func (c *PolicyCoordinator) bootstrapRedisLocked(
	ctx context.Context,
	conn *pgxpool.Conn,
	state PolicyState,
	safety guard.RuntimeSafety,
) (PolicyState, error) {
	resetAt, err := c.policyGuard.ServerTime(ctx)
	if err != nil {
		return PolicyState{}, unavailablePolicyWrite(
			"read Redis time for initial proof reset fence",
			err,
		)
	}
	target := state
	target.RedisInitialized = true
	target.RedisGeneration = 1
	target.RedisRunID = safety.RunID
	target.RedisEvictedKeys = safety.EvictedKeys
	target.RedisResetAt = resetAt
	// A zero refill origin deliberately gives a fresh token bucket its full
	// burst. The independent reset timestamp fences admission-proof replay.
	target.RateRefillFrom = time.Time{}
	token := uuid.NewString()
	pending := guard.State{
		Phase:            guard.PhasePending,
		Version:          target.Version(),
		RedisRunID:       target.RedisRunID,
		RedisEvictedKeys: target.RedisEvictedKeys,
		PendingToken:     token,
	}
	if err := c.policyGuard.Force(ctx, pending); err != nil {
		return PolicyState{}, unavailablePolicyWrite("initialize pending distributed policy guard", err)
	}
	if err := c.storeStateOnConn(ctx, conn, target); err != nil {
		return PolicyState{}, err
	}
	if err := c.policyGuard.SetStable(ctx, token, target.stableGuard()); err != nil {
		return PolicyState{}, unavailablePolicyWrite("publish initial distributed policy guard", err)
	}
	return target, nil
}

func (c *PolicyCoordinator) resetRedisLocked(
	ctx context.Context,
	conn *pgxpool.Conn,
	state PolicyState,
	safety guard.RuntimeSafety,
	redisState guard.State,
	readErr error,
) (PolicyState, error) {
	now, err := c.policyGuard.ServerTime(ctx)
	if err != nil {
		return PolicyState{}, unavailablePolicyWrite(
			"read Redis time for reset fence",
			err,
		)
	}
	target := state
	target.RedisInitialized = true
	target.RedisGeneration++
	if target.RedisGeneration <= 0 {
		return PolicyState{}, unavailablePolicyWrite("advance Redis generation", fmt.Errorf("redis generation overflow"))
	}
	target.RedisRunID = safety.RunID
	target.RedisEvictedKeys = safety.EvictedKeys
	target.RedisResetAt = now
	target.RateRefillFrom = now
	quarantineUntil := now.Add(c.resetQuarantineTTL())
	token := uuid.NewString()

	if safety.RunID == state.RedisRunID &&
		safety.EvictedKeys == state.RedisEvictedKeys &&
		readErr == nil &&
		redisState.Stable() {
		if err := c.policyGuard.SetPending(ctx, redisState.Version, token, quarantineUntil); err != nil {
			return PolicyState{}, unavailablePolicyWrite("close stale Redis policy guard", err)
		}
	} else {
		if err := c.policyGuard.Force(ctx, guard.State{
			Phase:            guard.PhasePending,
			Version:          target.Version(),
			RedisRunID:       target.RedisRunID,
			RedisEvictedKeys: target.RedisEvictedKeys,
			PendingToken:     token,
			ResetAt:          now,
			RateRefillFrom:   now,
			QuarantineUntil:  quarantineUntil,
		}); err != nil {
			return PolicyState{}, unavailablePolicyWrite("close missing Redis policy guard", err)
		}
	}
	if err := c.storeStateOnConn(ctx, conn, target); err != nil {
		return PolicyState{}, err
	}
	// Rewrite pending with the committed target generation before waiting.
	if err := c.policyGuard.Force(ctx, guard.State{
		Phase:            guard.PhasePending,
		Version:          target.Version(),
		RedisRunID:       target.RedisRunID,
		RedisEvictedKeys: target.RedisEvictedKeys,
		PendingToken:     token,
		ResetAt:          now,
		RateRefillFrom:   now,
		QuarantineUntil:  quarantineUntil,
	}); err != nil {
		return PolicyState{}, unavailablePolicyWrite("publish Redis reset quarantine", err)
	}
	if err := c.waitForRedisDeadline(ctx, quarantineUntil); err != nil {
		return PolicyState{}, err
	}
	if err := c.policyGuard.SetStable(ctx, token, target.stableGuard()); err != nil {
		return PolicyState{}, unavailablePolicyWrite("publish recovered Redis generation", err)
	}
	return target, nil
}

func (c *PolicyCoordinator) loadStateOnConn(
	ctx context.Context,
	conn *pgxpool.Conn,
) (PolicyState, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return PolicyState{}, unavailablePolicyWrite("begin policy state read", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	state, err := loadPolicyState(ctx, tx, true)
	if err != nil {
		return PolicyState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return PolicyState{}, unavailablePolicyWrite("commit policy state read", err)
	}
	return state, nil
}

func (c *PolicyCoordinator) storeStateOnConn(
	ctx context.Context,
	conn *pgxpool.Conn,
	state PolicyState,
) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return unavailablePolicyWrite("begin policy state write", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := loadPolicyState(ctx, tx, true); err != nil {
		return err
	}
	if err := storePolicyState(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return unavailablePolicyWrite("commit policy state write with ambiguous outcome", err)
	}
	return nil
}

func (c *PolicyCoordinator) withWriterLock(
	ctx context.Context,
	fn func(*pgxpool.Conn) error,
) (resultErr error) {
	if c == nil || c.pool == nil || c.policyGuard == nil {
		return unavailablePolicyWrite("use policy coordinator", fmt.Errorf("coordinator is not configured"))
	}
	if err := c.acquireWriterGate(ctx); err != nil {
		return unavailablePolicyWrite("wait for local policy writer", err)
	}
	defer c.releaseWriterGate()
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return unavailablePolicyWrite("acquire policy writer PostgreSQL connection", err)
	}
	lockKey := pglock.Key(policyWriterLockResource)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		// The server may have acquired a session lock even when its response
		// was lost. Never return an ambiguously locked connection to the pool.
		discardPolicyWriterConnection(conn)
		return unavailablePolicyWrite("acquire policy writer session lock", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), policyUnlockTimeout)
		defer cancel()
		var unlocked bool
		unlockErr := conn.QueryRow(
			unlockCtx,
			"SELECT pg_advisory_unlock($1)",
			lockKey,
		).Scan(&unlocked)
		if unlockErr == nil && unlocked {
			conn.Release()
			return
		}
		discardPolicyWriterConnection(conn)
		if resultErr == nil {
			if unlockErr != nil {
				resultErr = unavailablePolicyWrite("release policy writer session lock", unlockErr)
			} else {
				resultErr = unavailablePolicyWrite(
					"release policy writer session lock",
					fmt.Errorf("PostgreSQL reported lock was not held"),
				)
			}
		}
	}()
	identityResult, identityErr := c.verifyRegionStateIdentityLocked(ctx, conn)
	if identityErr != nil {
		resultErr = c.handleIdentityVerificationFailure(ctx, identityErr)
		return resultErr
	}
	if identityResult.RedisClaimRepaired {
		if err := c.policyGuard.Invalidate(ctx); err != nil {
			resultErr = unavailablePolicyWrite(
				"invalidate policy guard after Redis identity repair",
				err,
			)
			return resultErr
		}
	}
	resultErr = fn(conn)
	return resultErr
}

func (c *PolicyCoordinator) acquireWriterGate(ctx context.Context) error {
	c.writerGateOnce.Do(func() {
		c.writerGate = make(chan struct{}, 1)
	})
	select {
	case c.writerGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *PolicyCoordinator) releaseWriterGate() {
	if c == nil || c.writerGate == nil {
		return
	}
	<-c.writerGate
}

func (c *PolicyCoordinator) verifyRegionStateIdentityLocked(
	ctx context.Context,
	conn *pgxpool.Conn,
) (RegionStateIdentityMaintenanceResult, error) {
	if c.stateIdentityMaintainer == nil {
		return RegionStateIdentityMaintenanceResult{},
			fmt.Errorf("region state identity maintainer is not configured")
	}
	return c.stateIdentityMaintainer.verifyAndRepairOnConn(ctx, conn)
}

func (c *PolicyCoordinator) handleIdentityVerificationFailure(
	ctx context.Context,
	identityErr error,
) error {
	wrapped := unavailablePolicyWrite(
		"verify region state identity before policy coordination",
		identityErr,
	)
	if ordinaryCallerCancellation(ctx, identityErr) {
		return wrapped
	}
	invalidateCtx, cancel := context.WithTimeout(context.Background(), policyUnlockTimeout)
	defer cancel()
	if err := c.policyGuard.Invalidate(invalidateCtx); err != nil {
		return errors.Join(
			wrapped,
			unavailablePolicyWrite(
				"invalidate policy guard after identity verification failure",
				err,
			),
		)
	}
	return wrapped
}

func ordinaryCallerCancellation(ctx context.Context, err error) bool {
	if ctx == nil || ctx.Err() == nil {
		return false
	}
	if errors.Is(err, ErrRegionStateIdentityMismatch) ||
		errors.Is(err, ErrRegionStateIdentityCorrupt) ||
		errors.Is(err, ErrRegionStateIdentityUnclaimed) ||
		errors.Is(err, ErrUnsafeRedisEvictionPolicy) {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func discardPolicyWriterConnection(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}
	physical := conn.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), policyUnlockTimeout)
	defer cancel()
	_ = physical.Close(closeCtx)
}

func unavailablePolicyWrite(operation string, err error) error {
	return &UnavailableError{Operation: operation, Err: err}
}

type defaultFenceDecision int

const (
	defaultFenceAdvance defaultFenceDecision = iota
	defaultFenceIdempotent
)

func compareDefaultVersion(
	state PolicyState,
	requested DefaultPolicyVersion,
	sha256 string,
) (defaultFenceDecision, error) {
	requested = requested.normalized()
	if state.DefaultsOwnerEpoch.IsZero() {
		return defaultFenceAdvance, nil
	}
	stored := DefaultPolicyVersion{
		OwnerEpoch: state.DefaultsOwnerEpoch,
		Generation: state.DefaultsGeneration,
	}.normalized()
	switch {
	case requested.OwnerEpoch.Before(stored.OwnerEpoch):
		return 0, &StaleDefaultPolicyVersionError{Stored: stored, Requested: requested}
	case requested.OwnerEpoch.After(stored.OwnerEpoch):
		return defaultFenceAdvance, nil
	case requested.Generation < stored.Generation:
		return 0, &StaleDefaultPolicyVersionError{Stored: stored, Requested: requested}
	case requested.Generation == stored.Generation && sha256 != state.DefaultsSHA256:
		return 0, &DefaultPolicyHashConflictError{
			Version:         requested,
			StoredSHA256:    state.DefaultsSHA256,
			RequestedSHA256: sha256,
		}
	case requested.Generation == stored.Generation:
		return defaultFenceIdempotent, nil
	default:
		return defaultFenceAdvance, nil
	}
}

func defaultsChanged(existing map[Key]Policy, wanted []Policy) bool {
	if len(existing) != len(wanted) {
		return true
	}
	for _, policy := range wanted {
		current, ok := existing[policy.Key]
		if !ok || !policyContentEqual(current, policy) {
			return true
		}
	}
	return false
}

func distributedDefaultsChanged(existing map[Key]Policy, wanted []Policy) bool {
	for _, policy := range wanted {
		if policy.Kind != KindRate && policy.Kind != KindConcurrency {
			continue
		}
		current, ok := existing[policy.Key]
		if !ok || !policyContentEqual(current, policy) {
			return true
		}
	}
	for key, current := range existing {
		if current.Kind != KindRate && current.Kind != KindConcurrency {
			continue
		}
		found := false
		for _, policy := range wanted {
			if policy.Key == key {
				found = true
				break
			}
		}
		if !found {
			return true
		}
	}
	return false
}

func waitUntil(ctx context.Context, deadline time.Time) error {
	delay := time.Until(deadline)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *PolicyCoordinator) waitForCreditDrain(ctx context.Context) error {
	ttl := c.creditDrainTTL
	if ttl <= 0 {
		ttl = guard.MaxLocalCreditTTL
	}
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	return c.waitForDeadline(ctx, now().Add(ttl))
}

func (c *PolicyCoordinator) waitForDeadline(ctx context.Context, deadline time.Time) error {
	waiter := c.waitUntil
	if waiter == nil {
		waiter = waitUntil
	}
	return waiter(ctx, deadline)
}

func (c *PolicyCoordinator) waitForRedisDeadline(
	ctx context.Context,
	deadline time.Time,
) error {
	now, err := c.policyGuard.ServerTime(ctx)
	if err != nil {
		return unavailablePolicyWrite("read Redis time for quarantine deadline", err)
	}
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return nil
	}
	localNow := time.Now
	if c.now != nil {
		localNow = c.now
	}
	return c.waitForDeadline(ctx, localNow().Add(remaining))
}

func (c *PolicyCoordinator) resetQuarantineTTL() time.Duration {
	ttl := c.leaseTTL
	creditTTL := c.creditDrainTTL
	if creditTTL <= 0 {
		creditTTL = guard.MaxLocalCreditTTL
	}
	if ttl < creditTTL {
		return creditTTL
	}
	return ttl
}
