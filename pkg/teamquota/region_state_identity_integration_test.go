package teamquota

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

func TestClaimRegionStateIdentityClaimsBothStoresAndValidatesEquivalentConsumer(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	redisURL := "redis://" + redisServer.Addr() + "/0"

	first, err := ClaimRegionStateIdentity(ctx, pool, RegionStateIdentityConfig{
		RegionID:        "region-1",
		ExpectedStateID: testRegionStateID,
		RedisURL:        redisURL,
		RedisKeyPrefix:  "sandbox0::teamquota",
		RedisTimeout:    time.Second,
		CreateIfMissing: true,
	})
	if err != nil {
		t.Fatalf("first ClaimRegionStateIdentity() error = %v", err)
	}
	if !redisServer.Exists(regionStateIdentityRedisKey(first)) {
		t.Fatal("region state identity was not claimed in Redis")
	}
	var firstRowVersion string
	if err := pool.QueryRow(ctx, `
		SELECT xmin::text
		FROM quota.region_state_identity_claims
		WHERE singleton = TRUE
	`).Scan(&firstRowVersion); err != nil {
		t.Fatalf("query first durable claim row version: %v", err)
	}

	equivalent, err := ClaimRegionStateIdentity(ctx, pool, RegionStateIdentityConfig{
		RegionID:        " region-1 ",
		ExpectedStateID: testRegionStateID,
		RedisURL:        redisURL,
		RedisKeyPrefix:  ":sandbox0:teamquota:",
		RedisTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("equivalent ClaimRegionStateIdentity() error = %v", err)
	}
	if equivalent != first {
		t.Fatalf("equivalent claim = %#v, want %#v", equivalent, first)
	}
	var validatedRowVersion string
	if err := pool.QueryRow(ctx, `
		SELECT xmin::text
		FROM quota.region_state_identity_claims
		WHERE singleton = TRUE
	`).Scan(&validatedRowVersion); err != nil {
		t.Fatalf("query validated durable claim row version: %v", err)
	}
	if validatedRowVersion != firstRowVersion {
		t.Fatalf(
			"equivalent validation rewrote durable claim: xmin %q, want %q",
			validatedRowVersion,
			firstRowVersion,
		)
	}

	mismatches := []RegionStateIdentityConfig{
		{
			RegionID:        "region-1",
			ExpectedStateID: alternateRegionStateID,
			RedisURL:        redisURL,
			RedisKeyPrefix:  "sandbox0:teamquota",
		},
		{
			RegionID:        "region-1",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "redis://user:mismatch-secret@127.0.0.1:1/0",
			RedisKeyPrefix:  "sandbox0:teamquota",
		},
		{
			RegionID:        "region-1",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "redis://" + redisServer.Addr() + "/1",
			RedisKeyPrefix:  "sandbox0:teamquota",
		},
		{
			RegionID:        "region-1",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "rediss://" + redisServer.Addr() + "/0",
			RedisKeyPrefix:  "sandbox0:teamquota",
		},
		{
			RegionID:        "region-1",
			ExpectedStateID: testRegionStateID,
			RedisURL:        redisURL,
			RedisKeyPrefix:  "sandbox0:other",
		},
		{
			RegionID:        "region-typo",
			ExpectedStateID: testRegionStateID,
			RedisURL:        redisURL,
			RedisKeyPrefix:  "sandbox0:teamquota",
		},
	}
	for _, mismatch := range mismatches {
		_, err := ClaimRegionStateIdentity(ctx, pool, mismatch)
		if !errors.Is(err, ErrRegionStateIdentityMismatch) {
			t.Fatalf("mismatched ClaimRegionStateIdentity(%#v) error = %v", mismatch, err)
		}
		if strings.Contains(err.Error(), "mismatch-secret") {
			t.Fatalf("mismatch error leaked Redis credentials: %v", err)
		}
	}

	var count int64
	var regionID, stateID, endpoint, keyPrefix, fingerprint string
	if err := pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			MIN(region_id),
			MIN(state_id::text),
			MIN(endpoint),
			MIN(key_prefix),
			MIN(fingerprint)
		FROM quota.region_state_identity_claims
	`).Scan(&count, &regionID, &stateID, &endpoint, &keyPrefix, &fingerprint); err != nil {
		t.Fatalf("query durable region state identity: %v", err)
	}
	if count != 1 ||
		regionID != first.RegionID ||
		stateID != first.StateID ||
		endpoint != first.Endpoint ||
		keyPrefix != first.KeyPrefix ||
		fingerprint != first.Fingerprint {
		t.Fatalf(
			"durable claim = count=%d region=%q state=%q endpoint=%q prefix=%q fingerprint=%q, want %#v",
			count,
			regionID,
			stateID,
			endpoint,
			keyPrefix,
			fingerprint,
			first,
		)
	}
}

func TestClaimRegionStateIdentityRejectsPartialAndFullyDisjointStatePlanes(t *testing.T) {
	firstPool := newTeamQuotaTestDatabase(t)
	secondPool := newTeamQuotaTestDatabase(t)
	firstRedis := runRegionStateRedis(t, "noeviction")
	secondRedis := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	for _, pool := range []*pgxpool.Pool{firstPool, secondPool} {
		if err := RunMigrations(ctx, pool, nil); err != nil {
			t.Fatalf("RunMigrations() error = %v", err)
		}
	}
	firstConfig := RegionStateIdentityConfig{
		RegionID:        "region-shared",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + firstRedis.Addr() + "/0",
		RedisTimeout:    time.Second,
		CreateIfMissing: true,
	}
	secondConfig := RegionStateIdentityConfig{
		RegionID:        "region-shared",
		ExpectedStateID: alternateRegionStateID,
		RedisURL:        "redis://" + secondRedis.Addr() + "/0",
		RedisTimeout:    time.Second,
		CreateIfMissing: true,
	}
	if _, err := ClaimRegionStateIdentity(ctx, firstPool, firstConfig); err != nil {
		t.Fatalf("claim first state plane: %v", err)
	}
	if _, err := ClaimRegionStateIdentity(ctx, secondPool, secondConfig); err != nil {
		t.Fatalf("claim second state plane: %v", err)
	}

	// A data-plane resource trusted by the first control plane cannot silently
	// use a different, internally consistent PostgreSQL and Redis pair.
	disjoint := firstConfig
	disjoint.RedisURL = secondConfig.RedisURL
	disjoint.CreateIfMissing = false
	if _, err := ClaimRegionStateIdentity(ctx, secondPool, disjoint); !errors.Is(err, ErrRegionStateIdentityMismatch) {
		t.Fatalf("fully disjoint state plane error = %v", err)
	}

	// A mixed pair is rejected even though its PostgreSQL side is correct.
	partial := firstConfig
	partial.RedisURL = secondConfig.RedisURL
	partial.CreateIfMissing = false
	if _, err := ClaimRegionStateIdentity(ctx, firstPool, partial); !errors.Is(err, ErrRegionStateIdentityMismatch) {
		t.Fatalf("partially mismatched state plane error = %v", err)
	}
}

func TestClaimRegionStateIdentityConsumerCannotInitializeAndOwnerRestoresRedisClaim(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	cfg := RegionStateIdentityConfig{
		RegionID:        "region-owner",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisTimeout:    time.Second,
	}
	normalized, err := NormalizeRegionStateIdentity(cfg)
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	key := regionStateIdentityRedisKey(normalized)

	if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); !errors.Is(err, ErrRegionStateIdentityUnclaimed) {
		t.Fatalf("consumer-first ClaimRegionStateIdentity() error = %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 0)
	if redisServer.Exists(key) {
		t.Fatal("consumer initialized Redis state identity")
	}

	cfg.CreateIfMissing = true
	if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); err != nil {
		t.Fatalf("owner ClaimRegionStateIdentity() error = %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 1)
	if !redisServer.Exists(key) {
		t.Fatal("owner did not initialize Redis state identity")
	}

	redisServer.FlushAll()
	cfg.CreateIfMissing = false
	if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); !errors.Is(err, ErrRegionStateIdentityUnclaimed) {
		t.Fatalf("consumer after Redis flush error = %v", err)
	}
	if redisServer.Exists(key) {
		t.Fatal("consumer restored Redis state identity")
	}

	cfg.CreateIfMissing = true
	if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); err != nil {
		t.Fatalf("owner Redis restoration error = %v", err)
	}
	if !redisServer.Exists(key) {
		t.Fatal("owner did not restore Redis state identity")
	}
}

func TestClaimRegionStateIdentityConcurrentDifferentOwnersSharingRedisHaveOneWinner(t *testing.T) {
	firstPool := newTeamQuotaTestDatabase(t)
	secondPool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	for _, pool := range []*pgxpool.Pool{firstPool, secondPool} {
		if err := RunMigrations(ctx, pool, nil); err != nil {
			t.Fatalf("RunMigrations() error = %v", err)
		}
	}

	configs := []struct {
		pool *pgxpool.Pool
		cfg  RegionStateIdentityConfig
	}{
		{
			pool: firstPool,
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-race",
				ExpectedStateID: testRegionStateID,
				RedisURL:        "redis://" + redisServer.Addr() + "/0",
				RedisTimeout:    time.Second,
				CreateIfMissing: true,
			},
		},
		{
			pool: secondPool,
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-race",
				ExpectedStateID: alternateRegionStateID,
				RedisURL:        "redis://" + redisServer.Addr() + "/0",
				RedisTimeout:    time.Second,
				CreateIfMissing: true,
			},
		},
	}
	start := make(chan struct{})
	errs := make(chan error, len(configs))
	var wg sync.WaitGroup
	for _, item := range configs {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := ClaimRegionStateIdentity(ctx, item.pool, item.cfg)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	var successes, mismatches int
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrRegionStateIdentityMismatch):
			mismatches++
		default:
			t.Fatalf("concurrent ClaimRegionStateIdentity() error = %v", err)
		}
	}
	if successes != 1 || mismatches != 1 {
		t.Fatalf("concurrent outcomes = %d successes, %d mismatches", successes, mismatches)
	}
	var total int64
	for _, pool := range []*pgxpool.Pool{firstPool, secondPool} {
		total += regionStateIdentityClaimCount(t, pool)
	}
	if total != 1 {
		t.Fatalf("committed PostgreSQL claims = %d, want 1", total)
	}
}

func TestClaimRegionStateIdentityFailsClosedOnPostgreSQLAndRedisCorruption(t *testing.T) {
	t.Run("PostgreSQL", func(t *testing.T) {
		pool := newTeamQuotaTestDatabase(t)
		redisServer := runRegionStateRedis(t, "noeviction")
		ctx := context.Background()
		if err := RunMigrations(ctx, pool, nil); err != nil {
			t.Fatalf("RunMigrations() error = %v", err)
		}
		cfg := RegionStateIdentityConfig{
			RegionID:        "region-corrupt-pg",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "redis://" + redisServer.Addr() + "/0",
			RedisTimeout:    time.Second,
			CreateIfMissing: true,
		}
		claimed, err := ClaimRegionStateIdentity(ctx, pool, cfg)
		if err != nil {
			t.Fatalf("ClaimRegionStateIdentity() error = %v", err)
		}
		corrupt := claimed.Fingerprint[:63] + "0"
		if corrupt == claimed.Fingerprint {
			corrupt = claimed.Fingerprint[:63] + "1"
		}
		if _, err := pool.Exec(ctx, `
			UPDATE quota.region_state_identity_claims
			SET fingerprint = $1
			WHERE singleton = TRUE
		`, corrupt); err != nil {
			t.Fatalf("corrupt durable fingerprint: %v", err)
		}
		if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); !errors.Is(err, ErrRegionStateIdentityCorrupt) {
			t.Fatalf("corrupt PostgreSQL claim error = %v", err)
		}
	})

	t.Run("Redis", func(t *testing.T) {
		pool := newTeamQuotaTestDatabase(t)
		redisServer := runRegionStateRedis(t, "noeviction")
		ctx := context.Background()
		if err := RunMigrations(ctx, pool, nil); err != nil {
			t.Fatalf("RunMigrations() error = %v", err)
		}
		cfg := RegionStateIdentityConfig{
			RegionID:        "region-corrupt-redis",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "redis://" + redisServer.Addr() + "/0",
			RedisTimeout:    time.Second,
			CreateIfMissing: true,
		}
		claimed, err := ClaimRegionStateIdentity(ctx, pool, cfg)
		if err != nil {
			t.Fatalf("ClaimRegionStateIdentity() error = %v", err)
		}
		redisServer.Set(regionStateIdentityRedisKey(claimed), `{"region_id":"broken"}`)
		if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); !errors.Is(err, ErrRegionStateIdentityCorrupt) {
			t.Fatalf("corrupt Redis claim error = %v", err)
		}
	})
}

func TestValidateRegionStateIdentityInPostgreSQLIsConsumerOnly(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	if err := ValidateRegionStateIdentityInPostgreSQL(
		ctx,
		pool,
		"region-scheduler",
		testRegionStateID,
	); !errors.Is(err, ErrRegionStateIdentityUnclaimed) {
		t.Fatalf("validator before owner error = %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 0)

	if _, err := ClaimRegionStateIdentity(ctx, pool, RegionStateIdentityConfig{
		RegionID:        "region-scheduler",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisTimeout:    time.Second,
		CreateIfMissing: true,
	}); err != nil {
		t.Fatalf("owner claim: %v", err)
	}
	if err := ValidateRegionStateIdentityInPostgreSQL(
		ctx,
		pool,
		"region-scheduler",
		testRegionStateID,
	); err != nil {
		t.Fatalf("equivalent PostgreSQL validation: %v", err)
	}
	if err := ValidateRegionStateIdentityInPostgreSQL(
		ctx,
		pool,
		"region-scheduler",
		thirdRegionStateID,
	); !errors.Is(err, ErrRegionStateIdentityMismatch) {
		t.Fatalf("mismatched PostgreSQL state error = %v", err)
	}
}

func TestClaimRegionStateIdentityRequiresReachableRedisBeforeFirstWrite(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}

	config := RegionStateIdentityConfig{
		RegionID:        "region-reachable",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://127.0.0.1:1/0",
		RedisTimeout:    100 * time.Millisecond,
		CreateIfMissing: true,
	}
	if _, err := ClaimRegionStateIdentity(ctx, pool, config); err == nil {
		t.Fatal("unreachable creator ClaimRegionStateIdentity() error = nil")
	}
	assertRegionStateIdentityClaimCount(t, pool, 0)

	redisServer := runRegionStateRedis(t, "noeviction")
	redisServer.RequireAuth("correct-secret")
	config.RedisURL = "redis://:wrong-secret@" + redisServer.Addr() + "/0"
	if _, err := ClaimRegionStateIdentity(ctx, pool, config); err == nil {
		t.Fatal("wrong-credential creator ClaimRegionStateIdentity() error = nil")
	} else if strings.Contains(err.Error(), "wrong-secret") {
		t.Fatalf("connectivity error leaked Redis credentials: %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 0)

	config.RedisURL = "redis://:correct-secret@" + redisServer.Addr() + "/0"
	if _, err := ClaimRegionStateIdentity(ctx, pool, config); err != nil {
		t.Fatalf("reachable creator ClaimRegionStateIdentity() error = %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 1)
}

func TestClaimRegionStateIdentityRejectsEvictingRedisPolicy(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "allkeys-lru")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	cfg := RegionStateIdentityConfig{
		RegionID:        "region-unsafe-redis",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisTimeout:    time.Second,
		CreateIfMissing: true,
	}
	if _, err := ClaimRegionStateIdentity(ctx, pool, cfg); !errors.Is(err, ErrUnsafeRedisEvictionPolicy) {
		t.Fatalf("evicting Redis policy error = %v", err)
	}
	assertRegionStateIdentityClaimCount(t, pool, 0)
}

func TestRegionStateIdentityMaintainerRepairsFlushAndDetectsUnsafePolicyChange(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer, setPolicy := runMutableRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	cfg := RegionStateIdentityConfig{
		RegionID:        "region-maintained",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisTimeout:    time.Second,
	}
	maintainer, err := NewRegionStateIdentityMaintainer(pool, cfg)
	if err != nil {
		t.Fatalf("NewRegionStateIdentityMaintainer() error = %v", err)
	}
	initialResult, err := maintainer.VerifyAndRepair(ctx)
	if err != nil {
		t.Fatalf("initial VerifyAndRepair() error = %v", err)
	}
	if !initialResult.RedisClaimRepaired {
		t.Fatal("initial VerifyAndRepair() did not report the new Redis claim")
	}
	identity, err := NormalizeRegionStateIdentity(cfg)
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	key := regionStateIdentityRedisKey(identity)

	redisServer.FlushAll()
	repairResult, err := maintainer.VerifyAndRepair(ctx)
	if err != nil {
		t.Fatalf("VerifyAndRepair() after flush error = %v", err)
	}
	if !repairResult.RedisClaimRepaired {
		t.Fatal("VerifyAndRepair() after flush did not report Redis identity repair")
	}
	if !redisServer.Exists(key) {
		t.Fatal("maintainer did not restore Redis state identity after flush")
	}

	setPolicy("allkeys-lru")
	if _, err := maintainer.VerifyAndRepair(ctx); !errors.Is(err, ErrUnsafeRedisEvictionPolicy) {
		t.Fatalf("VerifyAndRepair() after unsafe policy change error = %v", err)
	}
	setPolicy("noeviction")
	if result, err := maintainer.VerifyAndRepair(ctx); err != nil {
		t.Fatalf("VerifyAndRepair() after safe policy restoration error = %v", err)
	} else if result.RedisClaimRepaired {
		t.Fatal("VerifyAndRepair() reported a repair for an existing Redis claim")
	}
}

func TestRegionStateIdentityMaintainerClosesGuardAtomicallyOnIdentityOnlyLoss(t *testing.T) {
	pool := newTeamQuotaTestDatabase(t)
	redisServer := runRegionStateRedis(t, "noeviction")
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations() error = %v", err)
	}
	cfg := RegionStateIdentityConfig{
		RegionID:        "region-identity-only-loss",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://" + redisServer.Addr() + "/0",
		RedisTimeout:    time.Second,
	}
	maintainer, err := NewRegionStateIdentityMaintainer(pool, cfg)
	if err != nil {
		t.Fatalf("NewRegionStateIdentityMaintainer() error = %v", err)
	}
	if _, err := maintainer.VerifyAndRepair(ctx); err != nil {
		t.Fatalf("initial VerifyAndRepair() error = %v", err)
	}
	identity, err := NormalizeRegionStateIdentity(cfg)
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	identityKey := regionStateIdentityRedisKey(identity)
	guardKey := guard.Key(identity.KeyPrefix)
	if !strings.HasPrefix(identityKey, identity.KeyPrefix+":") ||
		!strings.HasPrefix(guardKey, identity.KeyPrefix+":") {
		t.Fatalf(
			"identity and guard keys escaped claimed namespace %q: identity=%q guard=%q",
			identity.KeyPrefix,
			identityKey,
			guardKey,
		)
	}
	redisServer.Set(guardKey, "stale-stable-guard")
	redisServer.Del(identityKey)

	result, err := maintainer.VerifyAndRepair(ctx)
	if err != nil {
		t.Fatalf("VerifyAndRepair() after identity-only loss error = %v", err)
	}
	if !result.RedisClaimRepaired {
		t.Fatal("VerifyAndRepair() did not report Redis identity repair")
	}
	if !redisServer.Exists(identityKey) {
		t.Fatal("VerifyAndRepair() did not restore the Redis identity")
	}
	if redisServer.Exists(guardKey) {
		t.Fatal("VerifyAndRepair() left the policy guard open after identity loss")
	}
}

func assertRegionStateIdentityClaimCount(t *testing.T, pool *pgxpool.Pool, want int64) {
	t.Helper()
	if got := regionStateIdentityClaimCount(t, pool); got != want {
		t.Fatalf("region state identity claim count = %d, want %d", got, want)
	}
}

func regionStateIdentityClaimCount(t *testing.T, pool *pgxpool.Pool) int64 {
	t.Helper()
	var got int64
	if err := pool.QueryRow(
		context.Background(),
		`SELECT COUNT(*) FROM quota.region_state_identity_claims`,
	).Scan(&got); err != nil {
		t.Fatalf("query region state identity claim count: %v", err)
	}
	return got
}

func runRegionStateRedis(t *testing.T, maxmemoryPolicy string) *miniredis.Miniredis {
	t.Helper()
	server, _ := runMutableRegionStateRedis(t, maxmemoryPolicy)
	return server
}

func runMutableRegionStateRedis(
	t *testing.T,
	maxmemoryPolicy string,
) (*miniredis.Miniredis, func(string)) {
	t.Helper()
	server := miniredis.RunT(t)
	var currentPolicy atomic.Value
	currentPolicy.Store(maxmemoryPolicy)
	server.Server().SetPreHook(func(
		peer *miniredisserver.Peer,
		command string,
		_ ...string,
	) bool {
		if command != "INFO" {
			return false
		}
		peer.WriteBulk(
			"# Server\r\nrun_id:region-state-test-run-id" +
				"\r\n# Memory\r\nmaxmemory_policy:" +
				currentPolicy.Load().(string) +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
		)
		return true
	})
	return server, func(policy string) {
		currentPolicy.Store(policy)
	}
}
