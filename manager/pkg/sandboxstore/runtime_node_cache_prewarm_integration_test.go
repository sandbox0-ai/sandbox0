package sandboxstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeNodeCachePrewarmAttemptIsDurableAndSingleOwnerIntegration(t *testing.T) {
	s := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	key := RuntimeNodeCachePrewarmKey{WindowName: "benchmark", ClusterID: "ali-ue1",
		NodeUID: "prewarm-node", NodeBootID: "prewarm-boot", TemplateDigest: "sha256:" + strings.Repeat("a", 64)}
	attempt, previous, acquired, err := s.BeginRuntimeNodeCachePrewarm(t.Context(), key)
	require.NoError(t, err)
	require.True(t, acquired)
	require.Equal(t, 1, attempt)
	require.Zero(t, previous)
	_, _, acquired, err = s.BeginRuntimeNodeCachePrewarm(t.Context(), key)
	require.NoError(t, err)
	require.False(t, acquired)
	require.NoError(t, s.FinishRuntimeNodeCachePrewarm(t.Context(), key, attempt, true, ""))
	_, _, acquired, err = s.BeginRuntimeNodeCachePrewarm(t.Context(), key)
	require.NoError(t, err)
	require.False(t, acquired)
	require.Error(t, s.FinishRuntimeNodeCachePrewarm(t.Context(), key, attempt, false, "stale"))
	newBoot := key
	newBoot.NodeBootID = "prewarm-boot-2"
	_, _, acquired, err = s.BeginRuntimeNodeCachePrewarm(t.Context(), newBoot)
	require.NoError(t, err)
	require.True(t, acquired, "a replacement physical boot needs its own prewarm")
}
