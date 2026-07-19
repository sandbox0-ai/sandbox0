package teamquota

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

const (
	testRegionStateID      = "11111111-1111-4111-8111-111111111111"
	alternateRegionStateID = "22222222-2222-4222-8222-222222222222"
	thirdRegionStateID     = "33333333-3333-4333-8333-333333333333"
)

func TestClaimRegionStateIdentityRequiresPostgreSQLPool(t *testing.T) {
	_, err := ClaimRegionStateIdentity(
		context.Background(),
		nil,
		RegionStateIdentityConfig{
			RegionID:        "region-1",
			ExpectedStateID: testRegionStateID,
			RedisURL:        "redis://redis.example/0",
		},
	)
	if err == nil || !strings.Contains(err.Error(), "PostgreSQL pool") {
		t.Fatalf("ClaimRegionStateIdentity() error = %v", err)
	}
}

func TestNormalizeRegionStateIdentityCanonicalizesAndExcludesCredentials(t *testing.T) {
	first, err := NormalizeRegionStateIdentity(RegionStateIdentityConfig{
		RegionID:        " region-1 ",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://alice:first-secret@REDIS.EXAMPLE./0",
		RedisKeyPrefix:  " :sandbox0::teamquota: ",
	})
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	second, err := NormalizeRegionStateIdentity(RegionStateIdentityConfig{
		RegionID:        "region-1",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://bob:second-secret@redis.example:6379?db=0",
		RedisKeyPrefix:  "sandbox0:teamquota",
	})
	if err != nil {
		t.Fatalf("equivalent NormalizeRegionStateIdentity() error = %v", err)
	}
	if first != second {
		t.Fatalf("equivalent identities differ:\nfirst  = %#v\nsecond = %#v", first, second)
	}
	if first.Endpoint != "tcp://redis.example:6379" {
		t.Fatalf("Endpoint = %q", first.Endpoint)
	}
	if first.StateID != testRegionStateID {
		t.Fatalf("StateID = %q", first.StateID)
	}
	if first.KeyPrefix != "sandbox0:teamquota" {
		t.Fatalf("KeyPrefix = %q", first.KeyPrefix)
	}
	printable := first.Fingerprint + first.Endpoint + first.KeyPrefix
	if strings.Contains(printable, "first-secret") || strings.Contains(printable, "second-secret") {
		t.Fatal("normalized identity contains Redis credentials")
	}
}

func TestNormalizeRegionStateIdentitySeparatesStateDBTLSAndPrefix(t *testing.T) {
	base, err := NormalizeRegionStateIdentity(RegionStateIdentityConfig{
		RegionID:        "region-1",
		ExpectedStateID: testRegionStateID,
		RedisURL:        "redis://redis.example",
	})
	if err != nil {
		t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
	}
	if base.KeyPrefix != DefaultRedisKeyPrefix {
		t.Fatalf("default KeyPrefix = %q", base.KeyPrefix)
	}

	tests := []struct {
		name string
		cfg  RegionStateIdentityConfig
	}{
		{
			name: "state ID",
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-1",
				ExpectedStateID: alternateRegionStateID,
				RedisURL:        "redis://redis.example",
			},
		},
		{
			name: "database",
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-1",
				ExpectedStateID: testRegionStateID,
				RedisURL:        "redis://redis.example/1",
			},
		},
		{
			name: "TLS",
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-1",
				ExpectedStateID: testRegionStateID,
				RedisURL:        "rediss://redis.example/0",
			},
		},
		{
			name: "prefix",
			cfg: RegionStateIdentityConfig{
				RegionID:        "region-1",
				ExpectedStateID: testRegionStateID,
				RedisURL:        "redis://redis.example/0",
				RedisKeyPrefix:  "sandbox0:other-teamquota",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity, err := NormalizeRegionStateIdentity(tt.cfg)
			if err != nil {
				t.Fatalf("NormalizeRegionStateIdentity() error = %v", err)
			}
			if identity.Fingerprint == base.Fingerprint {
				t.Fatalf("%s identity reused base fingerprint", tt.name)
			}
		})
	}
}

func TestNormalizeRegionStateIdentityRejectsInvalidInputsWithoutCredentialLeak(t *testing.T) {
	tests := []RegionStateIdentityConfig{
		{ExpectedStateID: testRegionStateID, RedisURL: "redis://redis.example"},
		{RegionID: "region-1", RedisURL: "redis://redis.example"},
		{RegionID: "region-1", ExpectedStateID: "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", RedisURL: "redis://redis.example"},
		{RegionID: "region-1", ExpectedStateID: "11111111-1111-1111-8111-111111111111", RedisURL: "redis://redis.example"},
		{RegionID: "region-1", ExpectedStateID: testRegionStateID, RedisURL: "redis://user:secret@redis.example/not-a-db"},
		{RegionID: "region-1", ExpectedStateID: testRegionStateID, RedisURL: "redis://user:%zz@redis.example/0"},
	}
	for _, cfg := range tests {
		_, err := NormalizeRegionStateIdentity(cfg)
		if err == nil {
			t.Fatalf("NormalizeRegionStateIdentity(%#v) error = nil", cfg)
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "%zz") {
			t.Fatalf("error leaked Redis URL credentials: %v", err)
		}
	}
}

func TestNormalizeRedisEndpointRequiresAbsoluteUnixSocket(t *testing.T) {
	if _, err := normalizeRedisEndpoint(&redis.Options{
		Network: "unix",
		Addr:    "relative/redis.sock",
	}); err == nil {
		t.Fatal("normalizeRedisEndpoint() accepted a relative Unix socket")
	}
	endpoint, err := normalizeRedisEndpoint(&redis.Options{
		Network: "unix",
		Addr:    "/var/run/../run/redis.sock",
	})
	if err != nil {
		t.Fatalf("normalizeRedisEndpoint() error = %v", err)
	}
	if endpoint != "unix:///var/run/redis.sock" {
		t.Fatalf("normalized Unix endpoint = %q", endpoint)
	}
}

func TestRegionStateIdentityRedisOptionsDisableMaintenanceNotifications(t *testing.T) {
	options, err := regionStateIdentityRedisOptions(
		"redis://user:secret@redis.example:6380/2",
	)
	if err != nil {
		t.Fatalf("regionStateIdentityRedisOptions() error = %v", err)
	}
	if options.MaintNotificationsConfig == nil ||
		options.MaintNotificationsConfig.Mode != maintnotifications.ModeDisabled {
		t.Fatalf(
			"maintenance notifications config = %+v, want disabled",
			options.MaintNotificationsConfig,
		)
	}
	if options.Addr != "redis.example:6380" || options.DB != 2 {
		t.Fatalf("parsed Redis endpoint = %q db=%d", options.Addr, options.DB)
	}
	if options.Username != "user" || options.Password != "secret" {
		t.Fatal("parsed Redis options did not preserve credentials")
	}
}

func TestRegionStateIdentityMismatchErrorUnwraps(t *testing.T) {
	err := &RegionStateIdentityMismatchError{Store: "PostgreSQL"}
	if !errors.Is(err, ErrRegionStateIdentityMismatch) {
		t.Fatalf("errors.Is(%v, ErrRegionStateIdentityMismatch) = false", err)
	}
}

func TestValidateRedisRuntimeCapabilities(t *testing.T) {
	tests := []struct {
		name          string
		info          string
		infoError     string
		deniedCommand string
		wantError     string
		wantUnsafe    bool
	}{
		{
			name: "supported runtime",
			info: "# Server\r\nrun_id:test-run-id" +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
		},
		{
			name:          "EVAL denied",
			deniedCommand: "EVAL",
			wantError:     "capabilities with EVAL",
		},
		{
			name:          "EVALSHA denied",
			deniedCommand: "EVALSHA",
			info: "# Server\r\nrun_id:test-run-id" +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
			wantError: "capabilities with EVALSHA",
		},
		{
			name:      "INFO denied or Redis before version 7",
			infoError: "ERR unknown command or permission denied",
			wantError: "Redis 7 or newer with EVAL and INFO",
		},
		{
			name: "missing run ID",
			info: "# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
			wantError: "did not report run_id",
		},
		{
			name: "missing eviction policy",
			info: "# Server\r\nrun_id:test-run-id" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
			wantError: "did not report maxmemory_policy",
		},
		{
			name: "unsafe eviction policy",
			info: "# Server\r\nrun_id:test-run-id" +
				"\r\n# Memory\r\nmaxmemory_policy:allkeys-lru" +
				"\r\n# Stats\r\nevicted_keys:0\r\n",
			wantUnsafe: true,
		},
		{
			name: "invalid eviction counter",
			info: "# Server\r\nrun_id:test-run-id" +
				"\r\n# Memory\r\nmaxmemory_policy:noeviction" +
				"\r\n# Stats\r\nevicted_keys:-1\r\n",
			wantError: "non-negative evicted_keys",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redisServer := miniredis.RunT(t)
			redisServer.Server().SetPreHook(func(
				peer *miniredisserver.Peer,
				command string,
				_ ...string,
			) bool {
				if command == tt.deniedCommand {
					peer.WriteError("NOPERM command is not allowed")
					return true
				}
				if command != "INFO" {
					return false
				}
				if tt.infoError != "" {
					peer.WriteError(tt.infoError)
				} else {
					peer.WriteBulk(tt.info)
				}
				return true
			})
			client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
			t.Cleanup(func() { _ = client.Close() })

			err := validateRedisRuntimeCapabilities(context.Background(), client)
			if tt.wantUnsafe {
				if !errors.Is(err, ErrUnsafeRedisEvictionPolicy) {
					t.Fatalf("validateRedisRuntimeCapabilities() error = %v", err)
				}
				return
			}
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("validateRedisRuntimeCapabilities() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf(
					"validateRedisRuntimeCapabilities() error = %v, want substring %q",
					err,
					tt.wantError,
				)
			}
		})
	}
}
