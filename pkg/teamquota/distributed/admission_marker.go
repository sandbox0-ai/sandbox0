package distributed

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"golang.org/x/sync/singleflight"
)

const (
	defaultAdmissionRecoveryFailureTTL = 100 * time.Millisecond
	defaultAdmissionRecoveryLockTTL    = time.Second
	defaultAdmissionRecoveryPoll       = 5 * time.Millisecond
	maxAdmissionRecoveryFailures       = 4096
	admissionMarkerActive              = "active"
	admissionMarkerDisabled            = "disabled"
)

// AdmissionMarker is the distributed tombstone used by lower-frequency
// concurrency admissions.
type AdmissionMarker interface {
	Disabled(ctx context.Context, teamID string) (bool, error)
	Disable(ctx context.Context, teamID string) error
	Close() error
}

// AtomicAdmissionMarker supplies the Redis key checked in the same Lua
// mutation as a rate-token grant or concurrency lease mutation. Recover is
// called only after that script reports a missing marker.
type AtomicAdmissionMarker interface {
	AdmissionMarker
	RedisKey(teamID string) (string, error)
	Recover(ctx context.Context, teamID string) error
	Forget(ctx context.Context, teamID string) error
}

// AdmissionMarkerConfig configures Redis-backed admission state.
type AdmissionMarkerConfig struct {
	RegionID           string
	RedisURL           string
	KeyPrefix          string
	Timeout            time.Duration
	RecoveryFailureTTL time.Duration
	RecoveryLockTTL    time.Duration
}

type admissionRecoveryFailure struct {
	until time.Time
	err   error
}

// RedisAdmissionMarker projects the durable PostgreSQL tombstone into a
// non-expiring Redis marker. Redis loss is recovered with both process-local
// singleflight and a Redis lock so a cold marker cannot fan out to PostgreSQL
// from every process.
type RedisAdmissionMarker struct {
	mu        sync.RWMutex
	client    *redis.Client
	resolver  teamquota.TeamAdmissionStateResolver
	regionID  string
	keyPrefix string
	timeout   time.Duration

	recoveryFailureTTL time.Duration
	recoveryLockTTL    time.Duration
	recoveryPoll       time.Duration
	now                func() time.Time
	recoveries         singleflight.Group
	failureMu          sync.Mutex
	failures           map[string]admissionRecoveryFailure
}

var _ AtomicAdmissionMarker = (*RedisAdmissionMarker)(nil)

// NewRedisAdmissionMarker creates a fail-closed distributed admission marker.
func NewRedisAdmissionMarker(
	ctx context.Context,
	resolver teamquota.TeamAdmissionStateResolver,
	cfg AdmissionMarkerConfig,
) (*RedisAdmissionMarker, error) {
	if resolver == nil {
		return nil, fmt.Errorf("team quota admission state resolver is required")
	}
	regionID := strings.TrimSpace(cfg.RegionID)
	if regionID == "" {
		return nil, fmt.Errorf("team quota region ID is required")
	}
	recoveryFailureTTL := cfg.RecoveryFailureTTL
	if recoveryFailureTTL < 0 {
		return nil, fmt.Errorf("team quota admission recovery failure TTL must be non-negative")
	}
	if recoveryFailureTTL == 0 {
		recoveryFailureTTL = defaultAdmissionRecoveryFailureTTL
	}
	recoveryLockTTL := cfg.RecoveryLockTTL
	if recoveryLockTTL < 0 {
		return nil, fmt.Errorf("team quota admission recovery lock TTL must be non-negative")
	}
	if recoveryLockTTL == 0 {
		recoveryLockTTL = defaultAdmissionRecoveryLockTTL
	}
	if recoveryLockTTL%time.Millisecond != 0 {
		return nil, fmt.Errorf("team quota admission recovery lock TTL must use whole milliseconds")
	}
	client, normalized, err := rediscache.NewClient(ctx, rediscache.Config{
		URL: cfg.RedisURL,
		KeyPrefix: rediscache.JoinKeyPrefix(
			teamquota.NormalizeTeamQuotaRedisKeyPrefix(cfg.KeyPrefix),
			"admission-state",
		),
		Timeout:  cfg.Timeout,
		FailOpen: false,
	})
	if err != nil {
		return nil, fmt.Errorf("create team quota admission marker: %w", err)
	}
	return &RedisAdmissionMarker{
		client:             client,
		resolver:           resolver,
		regionID:           regionID,
		keyPrefix:          normalized.KeyPrefix,
		timeout:            normalized.Timeout,
		recoveryFailureTTL: recoveryFailureTTL,
		recoveryLockTTL:    recoveryLockTTL,
		recoveryPoll:       defaultAdmissionRecoveryPoll,
		now:                time.Now,
		failures:           make(map[string]admissionRecoveryFailure),
	}, nil
}

// RedisKey returns the exact marker key for an atomic admission. A recent
// failed recovery is surfaced locally, before another Redis or PostgreSQL
// request can be issued.
func (m *RedisAdmissionMarker) RedisKey(teamID string) (string, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return "", fmt.Errorf("team_id is required")
	}
	if err := m.cachedRecoveryFailure(teamID); err != nil {
		return "", err
	}
	if _, err := m.redisClient(); err != nil {
		return "", err
	}
	return m.redisKey(teamID), nil
}

// Disabled checks Redis first and recovers a missing marker from PostgreSQL.
func (m *RedisAdmissionMarker) Disabled(ctx context.Context, teamID string) (bool, error) {
	key, err := m.RedisKey(teamID)
	if err != nil {
		return false, err
	}
	client, err := m.redisClient()
	if err != nil {
		return false, err
	}
	value, err := m.get(ctx, client, key)
	switch {
	case err == nil:
		return decodeAdmissionMarker(value)
	case err != redis.Nil:
		return false, fmt.Errorf("get team quota admission marker: %w", err)
	}
	if err := m.Recover(ctx, teamID); err != nil {
		return false, err
	}
	value, err = m.get(ctx, client, key)
	if err != nil {
		return false, fmt.Errorf("reload recovered team quota admission marker: %w", err)
	}
	return decodeAdmissionMarker(value)
}

// Recover reconstructs one missing marker without allowing a process or
// region-wide PostgreSQL thundering herd.
func (m *RedisAdmissionMarker) Recover(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	if err := m.cachedRecoveryFailure(teamID); err != nil {
		return err
	}
	result := m.recoveries.DoChan(teamID, func() (any, error) {
		err := m.recover(ctx, teamID)
		if err != nil {
			m.cacheRecoveryFailure(teamID, err)
			return nil, err
		}
		m.clearRecoveryFailure(teamID)
		return nil, nil
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case completed := <-result:
		return completed.Err
	}
}

// Disable publishes the durable tombstone without an expiry. The caller must
// commit PostgreSQL first. Rate-limit callers additionally wait for the local
// credit drain barrier before reporting deletion complete.
func (m *RedisAdmissionMarker) Disable(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	client, err := m.redisClient()
	if err != nil {
		return err
	}
	if err := m.set(ctx, client, m.redisKey(teamID), admissionMarkerDisabled); err != nil {
		return fmt.Errorf("disable distributed team quota admission: %w", err)
	}
	m.clearRecoveryFailure(teamID)
	return nil
}

// Forget removes a marker immediately before the durable tombstone lifecycle
// prunes PostgreSQL. If pruning fails, a later admission recovers the still
// durable disabled state instead of permitting the team.
func (m *RedisAdmissionMarker) Forget(ctx context.Context, teamID string) error {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return fmt.Errorf("team_id is required")
	}
	client, err := m.redisClient()
	if err != nil {
		return err
	}
	callCtx, cancel := rediscache.WithTimeout(ctx, m.timeout)
	defer cancel()
	if err := client.Del(callCtx, m.redisKey(teamID)).Err(); err != nil {
		return fmt.Errorf("forget distributed team quota admission marker: %w", err)
	}
	m.clearRecoveryFailure(teamID)
	return nil
}

// Close releases the marker's Redis client.
func (m *RedisAdmissionMarker) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	client := m.client
	m.client = nil
	m.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close()
}

func (m *RedisAdmissionMarker) recover(
	ctx context.Context,
	teamID string,
) error {
	client, err := m.redisClient()
	if err != nil {
		return err
	}
	key := m.redisKey(teamID)
	lockKey := key + ":recovery-lock"
	token, err := admissionRecoveryToken()
	if err != nil {
		return err
	}
	for {
		value, getErr := m.get(ctx, client, key)
		switch {
		case getErr == nil:
			_, err := decodeAdmissionMarker(value)
			return err
		case getErr != redis.Nil:
			return fmt.Errorf("get team quota admission marker during recovery: %w", getErr)
		}

		acquired, err := m.setNX(ctx, client, lockKey, token, m.recoveryLockTTL)
		if err != nil {
			return fmt.Errorf("acquire team quota admission recovery lock: %w", err)
		}
		if acquired {
			defer m.releaseRecoveryLock(client, lockKey, token)
			// A disabling writer or the previous lock owner may have
			// published between the first GET and our SET NX.
			value, getErr = m.get(ctx, client, key)
			switch {
			case getErr == nil:
				_, err := decodeAdmissionMarker(value)
				return err
			case getErr != redis.Nil:
				return fmt.Errorf("recheck team quota admission marker: %w", getErr)
			}
			return m.resolveAndPublish(ctx, client, key, teamID)
		}
		if err := waitAdmissionRecovery(ctx, m.recoveryPoll); err != nil {
			return err
		}
	}
}

func (m *RedisAdmissionMarker) resolveAndPublish(
	ctx context.Context,
	client *redis.Client,
	key string,
	teamID string,
) error {
	disabled, err := m.resolver.TeamAdmissionDisabled(ctx, teamID)
	if err != nil {
		return fmt.Errorf("resolve durable team quota admission marker: %w", err)
	}
	if disabled {
		if err := m.set(ctx, client, key, admissionMarkerDisabled); err != nil {
			return fmt.Errorf("recover disabled team quota admission marker: %w", err)
		}
		return nil
	}
	set, err := m.setNX(ctx, client, key, admissionMarkerActive, 0)
	if err != nil {
		return fmt.Errorf("recover active team quota admission marker: %w", err)
	}
	if set {
		return nil
	}
	value, err := m.get(ctx, client, key)
	if err != nil {
		return fmt.Errorf("reload raced team quota admission marker: %w", err)
	}
	_, err = decodeAdmissionMarker(value)
	return err
}

func (m *RedisAdmissionMarker) cachedRecoveryFailure(teamID string) error {
	if m == nil {
		return fmt.Errorf("team quota admission marker is not configured")
	}
	now := m.now()
	m.failureMu.Lock()
	defer m.failureMu.Unlock()
	failure, ok := m.failures[teamID]
	if !ok {
		return nil
	}
	if !now.Before(failure.until) {
		delete(m.failures, teamID)
		return nil
	}
	return fmt.Errorf("team quota admission recovery is in backoff: %w", failure.err)
}

func (m *RedisAdmissionMarker) cacheRecoveryFailure(teamID string, err error) {
	if m == nil || err == nil {
		return
	}
	now := m.now()
	m.failureMu.Lock()
	defer m.failureMu.Unlock()
	if len(m.failures) >= maxAdmissionRecoveryFailures {
		for candidate, failure := range m.failures {
			if !now.Before(failure.until) {
				delete(m.failures, candidate)
			}
		}
	}
	if len(m.failures) >= maxAdmissionRecoveryFailures {
		for candidate := range m.failures {
			delete(m.failures, candidate)
			break
		}
	}
	m.failures[teamID] = admissionRecoveryFailure{
		until: now.Add(m.recoveryFailureTTL),
		err:   err,
	}
}

func (m *RedisAdmissionMarker) clearRecoveryFailure(teamID string) {
	if m == nil {
		return
	}
	m.failureMu.Lock()
	delete(m.failures, teamID)
	m.failureMu.Unlock()
}

func (m *RedisAdmissionMarker) redisClient() (*redis.Client, error) {
	if m == nil {
		return nil, fmt.Errorf("team quota admission marker is not configured")
	}
	m.mu.RLock()
	client := m.client
	m.mu.RUnlock()
	if client == nil {
		return nil, fmt.Errorf("team quota admission marker is closed")
	}
	return client, nil
}

func (m *RedisAdmissionMarker) redisKey(teamID string) string {
	identity := fmt.Sprintf(
		"%d:%s:%d:%s",
		len(m.regionID),
		m.regionID,
		len(teamID),
		teamID,
	)
	return rediscache.HashedKey(m.keyPrefix, identity)
}

func (m *RedisAdmissionMarker) get(
	ctx context.Context,
	client *redis.Client,
	key string,
) (string, error) {
	callCtx, cancel := rediscache.WithTimeout(ctx, m.timeout)
	defer cancel()
	return client.Get(callCtx, key).Result()
}

func (m *RedisAdmissionMarker) set(
	ctx context.Context,
	client *redis.Client,
	key string,
	value string,
) error {
	callCtx, cancel := rediscache.WithTimeout(ctx, m.timeout)
	defer cancel()
	return client.Set(callCtx, key, value, 0).Err()
}

func (m *RedisAdmissionMarker) setNX(
	ctx context.Context,
	client *redis.Client,
	key string,
	value string,
	expiration time.Duration,
) (bool, error) {
	callCtx, cancel := rediscache.WithTimeout(ctx, m.timeout)
	defer cancel()
	return client.SetNX(callCtx, key, value, expiration).Result()
}

var releaseAdmissionRecoveryLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

func (m *RedisAdmissionMarker) releaseRecoveryLock(
	client *redis.Client,
	key string,
	token string,
) {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	_ = releaseAdmissionRecoveryLockScript.Run(ctx, client, []string{key}, token).Err()
}

func admissionRecoveryToken() (string, error) {
	var payload [16]byte
	if _, err := rand.Read(payload[:]); err != nil {
		return "", fmt.Errorf("generate team quota admission recovery token: %w", err)
	}
	return hex.EncodeToString(payload[:]), nil
}

func waitAdmissionRecovery(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func decodeAdmissionMarker(value string) (bool, error) {
	switch value {
	case admissionMarkerActive:
		return false, nil
	case admissionMarkerDisabled:
		return true, nil
	default:
		return false, fmt.Errorf("invalid team quota admission marker %q", value)
	}
}
