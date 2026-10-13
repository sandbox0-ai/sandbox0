package sandboxstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeNomadForkTargetUsesDatabaseTimestampPrecision(t *testing.T) {
	target := rootFSTestSandboxRecord("fork-target", "team-1")
	target.DesiredState = SandboxDesiredStatePaused
	target.RuntimeGeneration = 0
	target.RuntimeID, target.RuntimeNamespace = "", ""
	startedAt := time.Date(2026, 10, 13, 12, 0, 0, 123456789, time.FixedZone("UTC+8", 8*60*60))
	target.ClaimedAt, target.CreatedAt = startedAt, startedAt
	target.ExpiresAt, target.HardExpiresAt = startedAt.Add(5*time.Minute), startedAt.Add(30*time.Minute)
	original := *target
	digest, err := NomadSandboxForkTargetRecordDigest(target)
	require.NoError(t, err)
	normalized, err := normalizeNomadSandboxForkRequest(&NomadSandboxForkRequest{
		OperationID: "fork-operation", SourceSandboxID: "fork-source", ExpectedTeamID: target.TeamID, Target: target,
	})
	require.NoError(t, err)
	require.Equal(t, original, *target, "normalization must not mutate the caller's target")
	require.Equal(t, digest, normalized.TargetRecordDigest, "existing durable digests must remain compatible")
	for _, value := range []time.Time{
		normalized.Target.ClaimedAt, normalized.Target.CreatedAt,
		normalized.Target.ExpiresAt, normalized.Target.HardExpiresAt,
	} {
		require.Equal(t, 123456000, value.Nanosecond())
		require.Equal(t, time.UTC, value.Location())
	}
	for _, field := range []string{"expires_at", "hard_expires_at"} {
		t.Run(field, func(t *testing.T) {
			mutated := *normalized.Target
			if field == "expires_at" {
				mutated.ExpiresAt = mutated.ExpiresAt.Add(time.Microsecond)
			} else {
				mutated.HardExpiresAt = mutated.HardExpiresAt.Add(time.Microsecond)
			}
			require.False(t, nomadForkTargetMatches(&mutated, normalized.Target), "persistable expiry changes must still conflict")
		})
	}
	require.Equal(t, time.Time{}, canonicalNomadForkTime(time.Time{}))
}
