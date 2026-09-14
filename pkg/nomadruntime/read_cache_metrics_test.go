package nomadruntime

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestReadCacheMetricOwnershipAcrossRuntimeReplacement(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	stats := rootfsblock.ReadCacheStats{DiskHits: 3, DiskBytes: 65536}
	unregister, err := registerReadCacheMetrics(registry, func() rootfsblock.ReadCacheStats { return stats })
	require.NoError(t, err)
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 8)
	for _, family := range families {
		if family.GetName() == "ctld_rootfs_read_cache_disk_hits_total" {
			require.Equal(t, float64(3), family.Metric[0].Counter.GetValue())
		}
		require.Empty(t, family.Metric[0].Label, "cache metrics must not expose sandbox/content identities")
	}
	unregister()
	families, err = registry.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
	replacement, err := registerReadCacheMetrics(registry, func() rootfsblock.ReadCacheStats { return rootfsblock.ReadCacheStats{} })
	require.NoError(t, err)
	defer replacement()
}
