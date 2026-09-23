package nomadinventory

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWarmJobFamilyRejectsUnrelatedAndNoncanonicalNames(t *testing.T) {
	for shard := range WarmJobShardCount {
		id, err := WarmJobID("warm", shard)
		require.NoError(t, err)
		require.True(t, IsWarmJob("warm", id))
	}
	for _, candidate := range []string{"", "other", "warm-other", "warm-shard-00", "warm-shard-1", "warm-shard-001", "warm-shard-24", "warm-shard-01-extra"} {
		require.False(t, IsWarmJob("warm", candidate), candidate)
	}
	for _, base := range []string{"", " warm", "warm "} {
		require.False(t, IsWarmJob(base, base))
	}
	_, err := WarmJobID("warm", -1)
	require.Error(t, err)
	_, err = WarmJobID("warm", WarmJobShardCount)
	require.Error(t, err)
}
