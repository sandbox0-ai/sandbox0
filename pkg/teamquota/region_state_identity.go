package teamquota

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
)

const (
	// DefaultRedisKeyPrefix is the region-shared namespace used by all
	// distributed Team Quota consumers when no explicit prefix is configured.
	DefaultRedisKeyPrefix = "sandbox0:teamquota"

	regionStateIdentityFingerprintVersion = "sandbox0-team-quota-region-state-v1"
)

var (
	// ErrRegionStateIdentityMismatch means a service is connected to state
	// that was claimed by a different region Team Quota state plane.
	ErrRegionStateIdentityMismatch = errors.New("team quota region state identity mismatch")
	// ErrRegionStateIdentityCorrupt means a PostgreSQL or Redis claim no longer
	// matches its stored fingerprint and must not be trusted for admission.
	ErrRegionStateIdentityCorrupt = errors.New("team quota region state identity claim is corrupt")
	// ErrRegionStateIdentityUnclaimed means a consumer started before the
	// region policy owner established the identity in both backing stores.
	ErrRegionStateIdentityUnclaimed = errors.New("team quota region state identity is not claimed")
	// ErrUnsafeRedisEvictionPolicy means Redis may evict admission state and
	// silently allow a tenant to exceed a rate or concurrency quota.
	ErrUnsafeRedisEvictionPolicy = errors.New("team quota Redis must use maxmemory-policy noeviction")
)

var redisRuntimeCapabilityScript = redis.NewScript(`
local info = redis.call("INFO", "server", "memory", "stats")
local run_id = string.match(info, "\nrun_id:([^\r\n]+)")
if run_id == nil or run_id == "" then
  return -1
end
local maxmemory_policy = string.match(info, "\nmaxmemory_policy:([^\r\n]+)")
if maxmemory_policy == nil then
  return -2
end
if string.lower(maxmemory_policy) ~= "noeviction" then
  return -3
end
local evicted_keys = string.match(info, "\nevicted_keys:([0-9]+)")
if evicted_keys == nil then
  return -4
end
return 1
`)

var claimRedisRegionStateIdentityScript = redis.NewScript(`
local current = redis.call("GET", KEYS[1])
if current ~= false then
  return {0, current}
end
redis.call("SET", KEYS[1], ARGV[1])
redis.call("DEL", KEYS[2])
return {1, ARGV[1]}
`)

// RegionStateIdentityConfig identifies the expected region Team Quota state
// plane and its Redis namespace.
type RegionStateIdentityConfig struct {
	RegionID        string
	ExpectedStateID string
	RedisURL        string
	RedisKeyPrefix  string
	RedisTimeout    time.Duration
	// CreateIfMissing must be enabled only by the region policy owner.
	// Consumer-only services validate existing PostgreSQL and Redis claims.
	CreateIfMissing bool
}

// RegionStateIdentityMaintainer is owned by the region policy owner. A
// periodic caller uses it to restore a Redis claim after data loss and to
// detect a runtime change away from maxmemory-policy=noeviction.
type RegionStateIdentityMaintainer struct {
	pool *pgxpool.Pool
	cfg  RegionStateIdentityConfig
}

// RegionStateIdentityMaintenanceResult reports whether Redis identity loss was
// repaired. A policy owner must treat RedisClaimRepaired as a state-loss fence
// and publish a new Redis generation only after the full quarantine.
type RegionStateIdentityMaintenanceResult struct {
	RedisClaimRepaired bool
}

// NewRegionStateIdentityMaintainer prepares the policy-owner maintenance hook.
// VerifyAndRepair must be connected to the owner's fail-closed coordination
// loop so an error prevents distributed admission until a later successful
// verification.
func NewRegionStateIdentityMaintainer(
	pool *pgxpool.Pool,
	cfg RegionStateIdentityConfig,
) (*RegionStateIdentityMaintainer, error) {
	if pool == nil {
		return nil, fmt.Errorf("team quota region state identity requires a PostgreSQL pool")
	}
	cfg.CreateIfMissing = true
	normalized, err := NormalizeRegionStateIdentity(cfg)
	if err != nil {
		return nil, err
	}
	cfg.RegionID = normalized.RegionID
	cfg.ExpectedStateID = normalized.StateID
	cfg.RedisKeyPrefix = normalized.KeyPrefix
	return &RegionStateIdentityMaintainer{pool: pool, cfg: cfg}, nil
}

// VerifyAndRepair validates PostgreSQL, verifies Redis noeviction, and
// recreates a missing matching Redis identity claim. It never replaces a
// mismatched or corrupt claim.
func (m *RegionStateIdentityMaintainer) VerifyAndRepair(
	ctx context.Context,
) (RegionStateIdentityMaintenanceResult, error) {
	if m == nil {
		return RegionStateIdentityMaintenanceResult{},
			fmt.Errorf("team quota region state identity maintainer is required")
	}
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return RegionStateIdentityMaintenanceResult{},
			fmt.Errorf("acquire team quota region state identity connection: %w", err)
	}
	defer conn.Release()
	return m.verifyAndRepairOnConn(ctx, conn)
}

func (m *RegionStateIdentityMaintainer) verifyAndRepairOnConn(
	ctx context.Context,
	conn *pgxpool.Conn,
) (RegionStateIdentityMaintenanceResult, error) {
	if m == nil {
		return RegionStateIdentityMaintenanceResult{},
			fmt.Errorf("team quota region state identity maintainer is required")
	}
	if conn == nil {
		return RegionStateIdentityMaintenanceResult{},
			fmt.Errorf("team quota region state identity connection is required")
	}
	_, redisClaimRepaired, err := claimRegionStateIdentityOnConn(ctx, conn, m.cfg)
	return RegionStateIdentityMaintenanceResult{
		RedisClaimRepaired: redisClaimRepaired,
	}, err
}

// RegionStateIdentity is safe to persist and log. It intentionally contains
// no PostgreSQL or Redis credentials.
type RegionStateIdentity struct {
	RegionID    string `json:"region_id"`
	StateID     string `json:"state_id"`
	Endpoint    string `json:"redis_endpoint"`
	RedisDB     int    `json:"redis_db"`
	TLSEnabled  bool   `json:"redis_tls_enabled"`
	KeyPrefix   string `json:"redis_key_prefix"`
	Fingerprint string `json:"fingerprint"`
}

// RegionStateIdentityMismatchError reports credential-free claimed and
// requested identities.
type RegionStateIdentityMismatchError struct {
	Store     string
	Claimed   RegionStateIdentity
	Requested RegionStateIdentity
}

func (e *RegionStateIdentityMismatchError) Error() string {
	return fmt.Sprintf(
		"%v in %s for region %q: claimed state_id=%q endpoint=%q db=%d tls=%t key_prefix=%q fingerprint=%s; requested state_id=%q endpoint=%q db=%d tls=%t key_prefix=%q fingerprint=%s",
		ErrRegionStateIdentityMismatch,
		e.Store,
		e.Requested.RegionID,
		e.Claimed.StateID,
		e.Claimed.Endpoint,
		e.Claimed.RedisDB,
		e.Claimed.TLSEnabled,
		e.Claimed.KeyPrefix,
		e.Claimed.Fingerprint,
		e.Requested.StateID,
		e.Requested.Endpoint,
		e.Requested.RedisDB,
		e.Requested.TLSEnabled,
		e.Requested.KeyPrefix,
		e.Requested.Fingerprint,
	)
}

// Unwrap supports errors.Is(err, ErrRegionStateIdentityMismatch).
func (e *RegionStateIdentityMismatchError) Unwrap() error {
	return ErrRegionStateIdentityMismatch
}

// NormalizeTeamQuotaRedisKeyPrefix canonicalizes an operator or standalone
// Team Quota base prefix. Callers must use the returned prefix after the
// region state identity has been validated.
func NormalizeTeamQuotaRedisKeyPrefix(raw string) string {
	parts := strings.Split(raw, ":")
	normalized := rediscache.JoinKeyPrefix(parts...)
	if normalized == "" {
		return DefaultRedisKeyPrefix
	}
	return normalized
}

// NormalizeRegionStateIdentity derives the credential-free expected identity.
func NormalizeRegionStateIdentity(cfg RegionStateIdentityConfig) (RegionStateIdentity, error) {
	regionID, stateID, err := normalizeRegionStateCoordinates(cfg.RegionID, cfg.ExpectedStateID)
	if err != nil {
		return RegionStateIdentity{}, err
	}

	options, err := parseCredentialFreeRedisOptions(cfg.RedisURL)
	if err != nil {
		return RegionStateIdentity{}, err
	}
	endpoint, err := normalizeRedisEndpoint(options)
	if err != nil {
		return RegionStateIdentity{}, err
	}
	if options.DB < 0 {
		return RegionStateIdentity{}, fmt.Errorf("team quota Redis database must not be negative")
	}
	keyPrefix := NormalizeTeamQuotaRedisKeyPrefix(cfg.RedisKeyPrefix)
	if strings.ContainsRune(keyPrefix, '\x00') {
		return RegionStateIdentity{}, fmt.Errorf("team quota Redis key prefix contains an invalid NUL byte")
	}

	identity := RegionStateIdentity{
		RegionID:   regionID,
		StateID:    stateID,
		Endpoint:   endpoint,
		RedisDB:    options.DB,
		TLSEnabled: options.TLSConfig != nil,
		KeyPrefix:  keyPrefix,
	}
	identity.Fingerprint = regionStateIdentityFingerprint(identity)
	return identity, nil
}

// ClaimRegionStateIdentity creates or validates the immutable state-plane
// identity in both PostgreSQL and Redis. Only the policy owner may initialize
// either store. A missing, mismatched, corrupt, or unreachable claim fails
// closed for consumer-only services.
func ClaimRegionStateIdentity(
	ctx context.Context,
	pool *pgxpool.Pool,
	cfg RegionStateIdentityConfig,
) (RegionStateIdentity, error) {
	if pool == nil {
		return RegionStateIdentity{}, fmt.Errorf("team quota region state identity requires a PostgreSQL pool")
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return RegionStateIdentity{}, fmt.Errorf("acquire team quota region state identity connection: %w", err)
	}
	defer conn.Release()
	claimed, _, err := claimRegionStateIdentityOnConn(ctx, conn, cfg)
	return claimed, err
}

func claimRegionStateIdentityOnConn(
	ctx context.Context,
	conn *pgxpool.Conn,
	cfg RegionStateIdentityConfig,
) (RegionStateIdentity, bool, error) {
	if conn == nil {
		return RegionStateIdentity{}, false,
			fmt.Errorf("team quota region state identity connection is required")
	}
	requested, err := NormalizeRegionStateIdentity(cfg)
	if err != nil {
		return RegionStateIdentity{}, false, err
	}

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RegionStateIdentity{}, false,
			fmt.Errorf("begin team quota region state identity claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	claimed, found, err := loadRegionStateIdentity(ctx, tx, "FOR UPDATE")
	if err != nil {
		return RegionStateIdentity{}, false, err
	}
	if found {
		if err := validateClaimedRegionStateIdentity("PostgreSQL", claimed, requested); err != nil {
			return RegionStateIdentity{}, false, err
		}
	} else {
		if !cfg.CreateIfMissing {
			return RegionStateIdentity{}, false, fmt.Errorf(
				"%w in PostgreSQL; start the region Team Quota policy owner first",
				ErrRegionStateIdentityUnclaimed,
			)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO quota.region_state_identity_claims (
				singleton,
				region_id,
				state_id,
				endpoint,
				redis_db,
				tls_enabled,
				key_prefix,
				fingerprint
			)
			VALUES (TRUE, $1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (singleton) DO NOTHING
		`,
			requested.RegionID,
			requested.StateID,
			requested.Endpoint,
			requested.RedisDB,
			requested.TLSEnabled,
			requested.KeyPrefix,
			requested.Fingerprint,
		); err != nil {
			return RegionStateIdentity{}, false,
				fmt.Errorf("claim team quota region state identity in PostgreSQL: %w", err)
		}

		claimed, found, err = loadRegionStateIdentity(ctx, tx, "FOR UPDATE")
		if err != nil {
			return RegionStateIdentity{}, false, err
		}
		if !found {
			return RegionStateIdentity{}, false,
				fmt.Errorf("load team quota region state identity claim: %w", pgx.ErrNoRows)
		}
		if err := validateClaimedRegionStateIdentity("PostgreSQL", claimed, requested); err != nil {
			return RegionStateIdentity{}, false, err
		}
	}

	redisClaimRepaired, err := claimRegionStateIdentityInRedis(ctx, cfg, requested)
	if err != nil {
		return RegionStateIdentity{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return RegionStateIdentity{}, false,
			fmt.Errorf("commit team quota region state identity claim: %w", err)
	}
	return claimed, redisClaimRepaired, nil
}

// ValidateRegionStateIdentityInPostgreSQL validates the trusted state ID for a
// PostgreSQL-only Team Quota consumer such as scheduler. It never initializes
// the durable claim.
func ValidateRegionStateIdentityInPostgreSQL(
	ctx context.Context,
	pool *pgxpool.Pool,
	regionID string,
	expectedStateID string,
) error {
	if pool == nil {
		return fmt.Errorf("team quota region state identity requires a PostgreSQL pool")
	}
	normalizedRegionID, normalizedStateID, err := normalizeRegionStateCoordinates(regionID, expectedStateID)
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin team quota region state identity validation: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	claimed, found, err := loadRegionStateIdentity(ctx, tx, "")
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf(
			"%w in PostgreSQL; start the region Team Quota policy owner first",
			ErrRegionStateIdentityUnclaimed,
		)
	}
	if claimed.Fingerprint != regionStateIdentityFingerprint(claimed) {
		return fmt.Errorf(
			"%w in PostgreSQL for region %q",
			ErrRegionStateIdentityCorrupt,
			normalizedRegionID,
		)
	}
	if claimed.RegionID != normalizedRegionID || claimed.StateID != normalizedStateID {
		return &RegionStateIdentityMismatchError{
			Store:   "PostgreSQL",
			Claimed: claimed,
			Requested: RegionStateIdentity{
				RegionID: normalizedRegionID,
				StateID:  normalizedStateID,
			},
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit team quota region state identity validation: %w", err)
	}
	return nil
}

func normalizeRegionStateCoordinates(regionID, expectedStateID string) (string, string, error) {
	regionID = strings.TrimSpace(regionID)
	if regionID == "" {
		return "", "", fmt.Errorf("team quota region ID is required")
	}
	if strings.ContainsRune(regionID, '\x00') {
		return "", "", fmt.Errorf("team quota region ID contains an invalid NUL byte")
	}
	expectedStateID = strings.TrimSpace(expectedStateID)
	parsed, err := uuid.Parse(expectedStateID)
	if err != nil || parsed.Version() != 4 || parsed.String() != expectedStateID {
		return "", "", fmt.Errorf("team quota state ID must be a canonical random UUID v4")
	}
	return regionID, parsed.String(), nil
}

func loadRegionStateIdentity(
	ctx context.Context,
	tx pgx.Tx,
	lockClause string,
) (RegionStateIdentity, bool, error) {
	var claimed RegionStateIdentity
	query := `
		SELECT
			region_id,
			state_id::text,
			endpoint,
			redis_db,
			tls_enabled,
			key_prefix,
			fingerprint
		FROM quota.region_state_identity_claims
		WHERE singleton = TRUE
		` + lockClause
	err := tx.QueryRow(ctx, query).Scan(
		&claimed.RegionID,
		&claimed.StateID,
		&claimed.Endpoint,
		&claimed.RedisDB,
		&claimed.TLSEnabled,
		&claimed.KeyPrefix,
		&claimed.Fingerprint,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RegionStateIdentity{}, false, nil
	}
	if err != nil {
		return RegionStateIdentity{}, false, fmt.Errorf("load team quota region state identity claim: %w", err)
	}
	return claimed, true, nil
}

func validateClaimedRegionStateIdentity(
	store string,
	claimed RegionStateIdentity,
	requested RegionStateIdentity,
) error {
	if claimed.Fingerprint != regionStateIdentityFingerprint(claimed) {
		return fmt.Errorf(
			"%w in %s for region %q",
			ErrRegionStateIdentityCorrupt,
			store,
			requested.RegionID,
		)
	}
	if !sameRegionStateIdentity(claimed, requested) {
		return &RegionStateIdentityMismatchError{
			Store:     store,
			Claimed:   claimed,
			Requested: requested,
		}
	}
	return nil
}

func claimRegionStateIdentityInRedis(
	ctx context.Context,
	cfg RegionStateIdentityConfig,
	requested RegionStateIdentity,
) (bool, error) {
	options, err := regionStateIdentityRedisOptions(cfg.RedisURL)
	if err != nil {
		return false, fmt.Errorf("team quota Redis URL is invalid")
	}
	client := redis.NewClient(options)
	defer func() {
		_ = client.Close()
	}()

	encoded, err := json.Marshal(requested)
	if err != nil {
		return false, fmt.Errorf("encode team quota region state identity: %w", err)
	}
	key := regionStateIdentityRedisKey(requested)
	timeout := cfg.RedisTimeout
	if timeout <= 0 {
		timeout = rediscache.DefaultTimeout
	}
	commandCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := validateRedisRuntimeCapabilities(commandCtx, client); err != nil {
		return false, err
	}

	repaired := false
	raw, err := client.Get(commandCtx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		if !cfg.CreateIfMissing {
			return false, fmt.Errorf(
				"%w in Redis; start the region Team Quota policy owner first",
				ErrRegionStateIdentityUnclaimed,
			)
		}
		result, runErr := claimRedisRegionStateIdentityScript.Run(
			commandCtx,
			client,
			[]string{
				key,
				rediscache.JoinKeyPrefix(requested.KeyPrefix, "policy-guard"),
			},
			encoded,
		).Slice()
		if runErr != nil {
			return false, fmt.Errorf("claim team quota region state identity in Redis: %w", runErr)
		}
		if len(result) != 2 {
			return false, fmt.Errorf(
				"claim team quota region state identity in Redis returned %d values, want 2",
				len(result),
			)
		}
		created, parseErr := redisClaimCreated(result[0])
		if parseErr != nil {
			return false, parseErr
		}
		raw, parseErr = redisClaimBytes(result[1])
		if parseErr != nil {
			return false, parseErr
		}
		repaired = created
		err = nil
	}
	if err != nil {
		return false, fmt.Errorf("load team quota region state identity from Redis: %w", err)
	}

	var claimed RegionStateIdentity
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&claimed); err != nil {
		return false, fmt.Errorf(
			"%w in Redis for region %q",
			ErrRegionStateIdentityCorrupt,
			requested.RegionID,
		)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return false, fmt.Errorf(
			"%w in Redis for region %q",
			ErrRegionStateIdentityCorrupt,
			requested.RegionID,
		)
	}
	if err := validateClaimedRegionStateIdentity("Redis", claimed, requested); err != nil {
		return false, err
	}
	return repaired, nil
}

// regionStateIdentityRedisOptions disables optional maintenance notifications
// because the policy owner creates a short-lived client for each repair. A
// fresh automatic handshake on every repair would otherwise generate one
// ignored Redis error per interval on servers without that extension.
func regionStateIdentityRedisOptions(rawURL string) (*redis.Options, error) {
	options, err := redis.ParseURL(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, err
	}
	options.MaintNotificationsConfig = &maintnotifications.Config{
		Mode: maintnotifications.ModeDisabled,
	}
	return options, nil
}

func redisClaimCreated(raw any) (bool, error) {
	switch value := raw.(type) {
	case int64:
		switch value {
		case 0:
			return false, nil
		case 1:
			return true, nil
		}
	case string:
		switch value {
		case "0":
			return false, nil
		case "1":
			return true, nil
		}
	}
	return false, fmt.Errorf("claim team quota region state identity in Redis returned an invalid repair flag")
}

func redisClaimBytes(raw any) ([]byte, error) {
	switch value := raw.(type) {
	case string:
		return []byte(value), nil
	case []byte:
		return value, nil
	default:
		return nil, fmt.Errorf("claim team quota region state identity in Redis returned an invalid claim")
	}
}

func validateRedisRuntimeCapabilities(ctx context.Context, client *redis.Client) error {
	result, err := redisRuntimeCapabilityScript.Eval(ctx, client, nil).Int64()
	if err != nil {
		return fmt.Errorf(
			"verify Team Quota Redis runtime capabilities with EVAL: Redis 7 or newer with EVAL and INFO server memory stats permission is required: %w",
			err,
		)
	}
	if err := validateRedisRuntimeCapabilityResult(result); err != nil {
		return err
	}
	result, err = redisRuntimeCapabilityScript.EvalSha(ctx, client, nil).Int64()
	if err != nil {
		return fmt.Errorf(
			"verify Team Quota Redis runtime capabilities with EVALSHA: EVALSHA permission is required: %w",
			err,
		)
	}
	return validateRedisRuntimeCapabilityResult(result)
}

func validateRedisRuntimeCapabilityResult(result int64) error {
	switch result {
	case 1:
		return nil
	case -1:
		return fmt.Errorf("verify Team Quota Redis runtime capabilities: INFO did not report run_id")
	case -2:
		return fmt.Errorf("verify Team Quota Redis runtime capabilities: INFO did not report maxmemory_policy")
	case -3:
		return ErrUnsafeRedisEvictionPolicy
	case -4:
		return fmt.Errorf("verify Team Quota Redis runtime capabilities: INFO did not report a non-negative evicted_keys counter")
	default:
		return fmt.Errorf(
			"verify Team Quota Redis runtime capabilities: unexpected probe result %d",
			result,
		)
	}
}

func regionStateIdentityRedisKey(identity RegionStateIdentity) string {
	return rediscache.HashedKey(
		rediscache.JoinKeyPrefix(identity.KeyPrefix, "region-state-identity"),
		identity.RegionID,
	)
}

func parseCredentialFreeRedisOptions(rawURL string) (*redis.Options, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, fmt.Errorf("team quota Redis URL is required")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// Do not wrap url.Error because it may contain credentials from rawURL.
		return nil, fmt.Errorf("team quota Redis URL is invalid")
	}
	parsed.User = nil
	options, err := redis.ParseURL(parsed.String())
	if err != nil {
		// The sanitized URL contains no userinfo, but keep the startup error free
		// of query values as well.
		return nil, fmt.Errorf("team quota Redis URL is invalid")
	}
	return options, nil
}

func normalizeRedisEndpoint(options *redis.Options) (string, error) {
	if options == nil {
		return "", fmt.Errorf("team quota Redis options are required")
	}
	switch options.Network {
	case "tcp":
		host, port, err := net.SplitHostPort(options.Addr)
		if err != nil {
			return "", fmt.Errorf("team quota Redis TCP endpoint is invalid")
		}
		host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
		if parsedIP := net.ParseIP(host); parsedIP != nil {
			host = parsedIP.String()
		}
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return "", fmt.Errorf("team quota Redis TCP port is invalid")
		}
		return "tcp://" + net.JoinHostPort(host, strconv.Itoa(portNumber)), nil
	case "unix":
		socketPath := filepath.Clean(strings.TrimSpace(options.Addr))
		if socketPath == "" || socketPath == "." || !filepath.IsAbs(socketPath) {
			return "", fmt.Errorf("team quota Redis Unix endpoint is invalid")
		}
		return "unix://" + socketPath, nil
	default:
		return "", fmt.Errorf("team quota Redis network is unsupported")
	}
}

func regionStateIdentityFingerprint(identity RegionStateIdentity) string {
	canonical := fmt.Sprintf(
		"%s\nregion_id=%q\nstate_id=%q\nendpoint=%q\nredis_db=%d\ntls_enabled=%t\nkey_prefix=%q\n",
		regionStateIdentityFingerprintVersion,
		identity.RegionID,
		identity.StateID,
		identity.Endpoint,
		identity.RedisDB,
		identity.TLSEnabled,
		identity.KeyPrefix,
	)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func sameRegionStateBackend(left, right RegionStateIdentity) bool {
	return left.RegionID == right.RegionID &&
		left.Endpoint == right.Endpoint &&
		left.RedisDB == right.RedisDB &&
		left.TLSEnabled == right.TLSEnabled &&
		left.KeyPrefix == right.KeyPrefix
}

func sameRegionStateIdentity(left, right RegionStateIdentity) bool {
	return left.StateID == right.StateID &&
		sameRegionStateBackend(left, right) &&
		left.Fingerprint == right.Fingerprint
}
