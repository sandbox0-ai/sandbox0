package sandboxstore

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFirstFreeRuntimeNodePrefixSupports299ElasticNodes(t *testing.T) {
	supernet := netip.MustParsePrefix("172.27.0.0/17")
	allocated := make(map[netip.Prefix]struct{}, 299)
	for index := 0; index < 299; index++ {
		prefix, ok := firstFreeRuntimeNodePrefix(supernet, 26, allocated)
		require.True(t, ok)
		require.Equal(t, 26, prefix.Bits())
		require.True(t, supernet.Contains(prefix.Addr()))
		allocated[prefix] = struct{}{}
	}
	require.Len(t, allocated, 299)
}

func TestNormalizeReserveRuntimeNodeRejectsPublicAddress(t *testing.T) {
	_, _, err := normalizeReserveRuntimeNodeRequest(&ReserveRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-1", PoolKind: RuntimeNodePoolKindElastic,
		ClusterID: "nomad", NodeName: "elastic-i-1", NodeUID: "uid-1",
		PrivateIP: "203.0.113.1", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
	})
	require.ErrorContains(t, err, "private IPv4")
}
