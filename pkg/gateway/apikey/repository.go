package apikey

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"golang.org/x/sync/singleflight"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrInvalidKey  = errors.New("invalid api key")
	ErrExpiredKey  = errors.New("api key expired")
	ErrInactiveKey = errors.New("api key inactive")
)

const (
	ScopeTeam     = "team"
	ScopePlatform = "platform"
)

// APIKey represents an API key stored in the database.
type APIKey struct {
	ID         string     `json:"id"`
	KeyValue   string     `json:"key_value"`
	TeamID     string     `json:"team_id"`
	UserID     *string    `json:"user_id,omitempty"`
	CreatedBy  string     `json:"created_by"`
	Name       string     `json:"name"`
	Scope      string     `json:"scope"`
	Roles      []string   `json:"roles"`
	IsActive   bool       `json:"is_active"`
	ExpiresAt  time.Time  `json:"expires_at"`
	LastUsed   *time.Time `json:"last_used_at,omitempty"`
	UsageCount int64      `json:"usage_count"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

// NormalizeScope returns the canonical API key scope. Empty scope preserves
// backward compatibility with keys created before scoped API keys existed.
func NormalizeScope(scope string) (string, bool) {
	switch strings.TrimSpace(scope) {
	case "":
		return ScopeTeam, true
	case ScopeTeam:
		return ScopeTeam, true
	case ScopePlatform:
		return ScopePlatform, true
	default:
		return "", false
	}
}

// Repository provides database access for team-scoped API keys.
type Repository struct {
	pool           *pgxpool.Pool
	teamQuotaStore teamquota.CapacityTxStore

	authenticationMu     sync.Mutex
	authenticationCache  *authenticationCache
	authenticationConfig AuthenticationCacheConfig
	authenticationLookup func(context.Context, string) (*APIKey, error)
	authenticationGroup  singleflight.Group
	authenticationEpoch  atomic.Uint64
	clock                func() time.Time

	usageRecorderConfig UsageRecorderConfig
	usageWriter         UsageBatchWriter
	usageRecorder       *usageRecorder
}

// RepositoryOption customizes an API key repository.
type RepositoryOption func(*Repository)

// WithTeamQuotaStore overrides the team quota store.
func WithTeamQuotaStore(store teamquota.CapacityTxStore) RepositoryOption {
	return func(repository *Repository) {
		repository.teamQuotaStore = store
	}
}

// WithAuthenticationCacheConfig overrides authentication cache bounds.
func WithAuthenticationCacheConfig(config AuthenticationCacheConfig) RepositoryOption {
	return func(repository *Repository) {
		repository.authenticationConfig = config
	}
}

// WithAuthenticationClock overrides the authentication clock.
func WithAuthenticationClock(clock func() time.Time) RepositoryOption {
	return func(repository *Repository) {
		if clock != nil {
			repository.clock = clock
		}
	}
}

// WithAuthenticationLookup overrides the persistent authentication lookup.
func WithAuthenticationLookup(
	lookup func(context.Context, string) (*APIKey, error),
) RepositoryOption {
	return func(repository *Repository) {
		repository.authenticationLookup = lookup
	}
}

// WithUsageRecorderConfig overrides usage recorder bounds and intervals.
func WithUsageRecorderConfig(config UsageRecorderConfig) RepositoryOption {
	return func(repository *Repository) {
		repository.usageRecorderConfig = config
	}
}

// WithUsageBatchWriter overrides the persistent usage writer.
func WithUsageBatchWriter(writer UsageBatchWriter) RepositoryOption {
	return func(repository *Repository) {
		repository.usageWriter = writer
	}
}

// NewRepository creates a new API key repository.
func NewRepository(pool *pgxpool.Pool, opts ...RepositoryOption) *Repository {
	repository := &Repository{
		pool:                 pool,
		teamQuotaStore:       teamquota.NewRepository(pool),
		authenticationConfig: DefaultAuthenticationCacheConfig(),
		clock:                time.Now,
		usageRecorderConfig:  DefaultUsageRecorderConfig(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(repository)
		}
	}
	repository.authenticationConfig = normalizeAuthenticationCacheConfig(
		repository.authenticationConfig,
	)
	repository.authenticationCache = newAuthenticationCache(
		repository.authenticationConfig.MaxEntries,
	)
	if repository.authenticationLookup == nil {
		repository.authenticationLookup = repository.lookupAPIKeyForAuthentication
	}
	if repository.usageWriter == nil && pool != nil {
		repository.usageWriter = repository
	}
	repository.usageRecorder = newUsageRecorder(
		repository.usageRecorderConfig,
		repository.usageWriter,
	)
	return repository
}

// Pool returns the underlying connection pool.
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

// CreateAPIKey creates a new API key.
func (r *Repository) CreateAPIKey(ctx context.Context, teamID, regionID, userID, name, scope string, roles []string, expiresAt time.Time) (*APIKey, string, error) {
	if err := ValidateCreateInput(name, roles); err != nil {
		return nil, "", err
	}
	normalizedScope, ok := NormalizeScope(scope)
	if !ok {
		return nil, "", ErrInvalidKey
	}

	keyValue, err := NewKeyValue(regionID)
	if err != nil {
		return nil, "", err
	}

	id := uuid.New().String()
	rolesJSON, err := json.Marshal(roles)
	if err != nil {
		return nil, "", fmt.Errorf("marshal roles: %w", err)
	}

	var key APIKey
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("begin api key transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	quotaRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindAPIKey, id),
		"create_api_key",
		1,
	)
	if err != nil {
		return nil, "", err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO api_keys (id, key_value, team_id, created_by, name, roles, scope, is_active, expires_at, user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, true, $8, $9)
		RETURNING id, key_value, team_id, created_by, name, roles, scope, is_active, expires_at, last_used_at, usage_count, created_at, updated_at
	`, id, keyValue, teamID, userID, name, rolesJSON, normalizedScope, expiresAt, userID,
	).Scan(
		&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
		&rolesJSON, &key.Scope, &key.IsActive, &key.ExpiresAt,
		&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("insert api key: %w", err)
	}
	if err := teamquota.CommitControlPlaneObjectTargetTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return nil, "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, "", fmt.Errorf("commit api key: %w", err)
	}
	r.invalidateAuthentication(key.ID, authenticationDigest(sha256.Sum256([]byte(keyValue))))

	if err := normalizeAPIKeyRecord(&key, rolesJSON, false); err != nil {
		return nil, "", err
	}
	return &key, keyValue, nil
}

// GetAPIKeysByTeamID retrieves all API keys for a team.
func (r *Repository) GetAPIKeysByTeamID(ctx context.Context, teamID string) ([]*APIKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, key_value, team_id, created_by, name, roles, scope,
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE team_id = $1
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("query api keys: %w", err)
	}
	defer rows.Close()

	keys := make([]*APIKey, 0)
	for rows.Next() {
		var key APIKey
		var rolesJSON []byte
		if err := rows.Scan(
			&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
			&rolesJSON, &key.Scope, &key.IsActive, &key.ExpiresAt,
			&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		if err := normalizeAPIKeyRecord(&key, rolesJSON, true); err != nil {
			return nil, err
		}
		keys = append(keys, &key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api keys: %w", err)
	}
	return keys, nil
}

// GetAPIKeysByUserID retrieves all API keys created by a user.
func (r *Repository) GetAPIKeysByUserID(ctx context.Context, userID string) ([]*APIKey, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, key_value, team_id, created_by, name, roles, scope,
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE user_id = $1
		ORDER BY created_at DESC
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("query api keys: %w", err)
	}
	defer rows.Close()

	keys := make([]*APIKey, 0)
	for rows.Next() {
		var key APIKey
		var rolesJSON []byte
		if err := rows.Scan(
			&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
			&rolesJSON, &key.Scope, &key.IsActive, &key.ExpiresAt,
			&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan key: %w", err)
		}
		if err := normalizeAPIKeyRecord(&key, rolesJSON, true); err != nil {
			return nil, err
		}
		keys = append(keys, &key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate api keys: %w", err)
	}
	return keys, nil
}

// DeleteAPIKey deletes an API key.
func (r *Repository) DeleteAPIKey(ctx context.Context, id string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin api key delete transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var teamID, keyValue string
	if err := tx.QueryRow(ctx, `
		SELECT team_id::text, key_value
		FROM api_keys
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&teamID, &keyValue); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("lock api key for deletion: %w", err)
	}
	quotaRef, err := teamquota.BeginControlPlaneObjectReleaseTx(
		ctx,
		r.teamQuotaStore,
		tx,
		teamquota.ControlPlaneObjectOwner(teamID, teamquota.ControlPlaneOwnerKindAPIKey, id),
		"delete_api_key",
		0,
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM api_keys WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete api key: %w", err)
	}
	if err := teamquota.ConfirmControlPlaneObjectReleaseTx(ctx, r.teamQuotaStore, tx, quotaRef); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit api key deletion: %w", err)
	}
	digest := authenticationDigest(sha256.Sum256([]byte(keyValue)))
	keyValue = ""
	r.invalidateAuthentication(id, digest)
	return nil
}

// DeactivateAPIKey deactivates an API key.
func (r *Repository) DeactivateAPIKey(ctx context.Context, id string) error {
	var keyValue string
	err := r.pool.QueryRow(ctx, `
		UPDATE api_keys
		SET is_active = false
		WHERE id = $1
		RETURNING key_value
	`, id).Scan(&keyValue)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("deactivate api key: %w", err)
	}
	digest := authenticationDigest(sha256.Sum256([]byte(keyValue)))
	keyValue = ""
	r.invalidateAuthentication(id, digest)
	return nil
}

// GetAPIKeyByID retrieves an API key by ID.
func (r *Repository) GetAPIKeyByID(ctx context.Context, id string) (*APIKey, error) {
	var key APIKey
	var rolesJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT id, key_value, team_id, created_by, name, roles, scope,
		       is_active, expires_at, last_used_at, usage_count, created_at, updated_at
		FROM api_keys
		WHERE id = $1
	`, id).Scan(
		&key.ID, &key.KeyValue, &key.TeamID, &key.CreatedBy, &key.Name,
		&rolesJSON, &key.Scope, &key.IsActive, &key.ExpiresAt,
		&key.LastUsed, &key.UsageCount, &key.CreatedAt, &key.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query api key: %w", err)
	}

	if err := normalizeAPIKeyRecord(&key, rolesJSON, true); err != nil {
		return nil, err
	}
	return &key, nil
}

// ValidateAPIKey validates an API key and returns the associated record.
func (r *Repository) ValidateAPIKey(ctx context.Context, keyValue string) (*APIKey, error) {
	if len(keyValue) > MaxAPIKeyValueBytes {
		return nil, ErrInvalidKey
	}
	if err := validateAuthenticationKeyStructure(keyValue); err != nil {
		digest := authenticationDigest(sha256.Sum256([]byte(keyValue)))
		r.putNegativeAuthentication(digest, ErrInvalidKey, "")
		return nil, ErrInvalidKey
	}
	digest := authenticationDigest(sha256.Sum256([]byte(keyValue)))
	if key, err, ok := r.getCachedAuthentication(digest); ok {
		if err == nil {
			r.recordUsage(key.ID)
		}
		return key, err
	}

	flightKey := string(digest[:])
	value, err, _ := r.authenticationGroup.Do(flightKey, func() (any, error) {
		if key, cachedErr, ok := r.getCachedAuthentication(digest); ok {
			return key, cachedErr
		}
		epoch := r.authenticationEpoch.Load()
		lookup := r.authenticationLookup
		if lookup == nil {
			return nil, fmt.Errorf("API key authentication lookup is not configured")
		}
		key, lookupErr := lookup(ctx, keyValue)
		if lookupErr != nil {
			if isCacheableAuthenticationError(lookupErr) &&
				r.authenticationEpoch.Load() == epoch {
				r.putNegativeAuthentication(digest, lookupErr, "")
			}
			return nil, lookupErr
		}
		if key == nil {
			return nil, fmt.Errorf("API key authentication lookup returned no record")
		}
		key = cloneAPIKeyWithoutSecret(key)
		now := r.now()
		switch {
		case !key.IsActive:
			if r.authenticationEpoch.Load() == epoch {
				r.putNegativeAuthentication(digest, ErrInactiveKey, key.ID)
			}
			return nil, ErrInactiveKey
		case !now.Before(key.ExpiresAt):
			if r.authenticationEpoch.Load() == epoch {
				r.putNegativeAuthentication(digest, ErrExpiredKey, key.ID)
			}
			return nil, ErrExpiredKey
		}
		if r.authenticationEpoch.Load() == epoch {
			r.putPositiveAuthentication(digest, key, now)
		}
		return key, nil
	})
	if err != nil {
		return nil, err
	}
	key, ok := value.(*APIKey)
	if !ok || key == nil {
		return nil, fmt.Errorf("invalid API key authentication result")
	}
	result := cloneAPIKeyWithoutSecret(key)
	r.recordUsage(result.ID)
	return result, nil
}

func (r *Repository) lookupAPIKeyForAuthentication(
	ctx context.Context,
	keyValue string,
) (*APIKey, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("API key repository pool is not configured")
	}
	var key APIKey
	var rolesJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT k.id, k.team_id, k.created_by, k.name, k.roles, k.scope,
		       k.is_active, k.expires_at, k.last_used_at, k.usage_count, k.created_at, k.updated_at, k.user_id
		FROM api_keys k
		INNER JOIN teams t ON t.id = k.team_id
		WHERE k.key_value = $1
	`, keyValue).Scan(
		&key.ID, &key.TeamID, &key.CreatedBy,
		&key.Name, &rolesJSON, &key.Scope, &key.IsActive,
		&key.ExpiresAt, &key.LastUsed, &key.UsageCount,
		&key.CreatedAt, &key.UpdatedAt, &key.UserID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrInvalidKey
		}
		return nil, fmt.Errorf("query api key: %w", err)
	}

	if err := normalizeAPIKeyRecord(&key, rolesJSON, false); err != nil {
		return nil, err
	}
	return &key, nil
}

func (r *Repository) getCachedAuthentication(
	digest authenticationDigest,
) (*APIKey, error, bool) {
	if r == nil {
		return nil, nil, false
	}
	r.authenticationMu.Lock()
	defer r.authenticationMu.Unlock()
	return r.authenticationCache.get(digest, r.now())
}

func (r *Repository) putPositiveAuthentication(
	digest authenticationDigest,
	key *APIKey,
	now time.Time,
) {
	expiresAt := now.Add(r.authenticationConfig.PositiveTTL)
	if key.ExpiresAt.Before(expiresAt) {
		expiresAt = key.ExpiresAt
	}
	if !now.Before(expiresAt) {
		return
	}
	r.authenticationMu.Lock()
	defer r.authenticationMu.Unlock()
	r.authenticationCache.putPositive(digest, key, expiresAt)
}

func (r *Repository) putNegativeAuthentication(
	digest authenticationDigest,
	err error,
	keyID string,
) {
	if r == nil || !isCacheableAuthenticationError(err) {
		return
	}
	now := r.now()
	r.authenticationMu.Lock()
	defer r.authenticationMu.Unlock()
	r.authenticationCache.putNegative(
		digest,
		err,
		keyID,
		now.Add(r.authenticationConfig.NegativeTTL),
	)
}

func (r *Repository) invalidateAuthentication(
	keyID string,
	knownDigests ...authenticationDigest,
) {
	if r == nil {
		return
	}
	r.authenticationEpoch.Add(1)
	digests := append([]authenticationDigest(nil), knownDigests...)
	r.authenticationMu.Lock()
	digests = append(digests, r.authenticationCache.invalidateID(keyID)...)
	for _, digest := range knownDigests {
		r.authenticationCache.invalidateDigest(digest)
	}
	r.authenticationMu.Unlock()
	for _, digest := range digests {
		r.authenticationGroup.Forget(string(digest[:]))
	}
}

func (r *Repository) now() time.Time {
	if r != nil && r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

func (r *Repository) recordUsage(keyID string) {
	if r != nil && r.usageRecorder != nil {
		r.usageRecorder.enqueue(keyID, r.now())
	}
}

// WriteAPIKeyUsageBatch persists one coalesced usage batch.
func (r *Repository) WriteAPIKeyUsageBatch(
	ctx context.Context,
	batch []APIKeyUsage,
) error {
	if len(batch) == 0 {
		return nil
	}
	if r == nil || r.pool == nil {
		return fmt.Errorf("API key repository pool is not configured")
	}
	ids := make([]string, 0, len(batch))
	counts := make([]int64, 0, len(batch))
	lastUsed := make([]time.Time, 0, len(batch))
	for _, usage := range batch {
		ids = append(ids, usage.KeyID)
		counts = append(counts, usage.Count)
		lastUsed = append(lastUsed, usage.LastUsed)
	}
	_, err := r.pool.Exec(ctx, `
		UPDATE api_keys AS keys
		SET usage_count = COALESCE(keys.usage_count, 0) + usage.count,
		    last_used_at = GREATEST(
		        COALESCE(keys.last_used_at, '-infinity'::timestamptz),
		        usage.last_used_at
		    )
		FROM unnest($1::uuid[], $2::bigint[], $3::timestamptz[])
		     AS usage(id, count, last_used_at)
		WHERE keys.id = usage.id
	`, ids, counts, lastUsed)
	if err != nil {
		return fmt.Errorf("update API key usage batch: %w", err)
	}
	return nil
}

// Close stops the usage recorder and performs one bounded final flush. It does
// not close the externally owned PostgreSQL pool.
func (r *Repository) Close() error {
	if r == nil || r.usageRecorder == nil {
		return nil
	}
	return r.usageRecorder.Close()
}

func normalizeAPIKeyRecord(key *APIKey, rolesJSON []byte, maskKey bool) error {
	if len(rolesJSON) > 0 {
		if err := json.Unmarshal(rolesJSON, &key.Roles); err != nil {
			return fmt.Errorf("parse roles: %w", err)
		}
	}
	scope, ok := NormalizeScope(key.Scope)
	if !ok {
		return fmt.Errorf("invalid api key scope: %s", key.Scope)
	}
	key.Scope = scope
	if maskKey {
		key.KeyValue = maskAPIKey(key.KeyValue)
	}
	return nil
}

func maskAPIKey(key string) string {
	if len(key) <= 12 {
		return "***"
	}
	return key[:12] + "***"
}
