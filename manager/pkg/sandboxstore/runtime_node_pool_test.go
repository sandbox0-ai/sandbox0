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

func TestFirstFreeRuntimeNodePrefixAvoidsOverlapsAcrossDensityProfiles(t *testing.T) {
	for _, test := range []struct {
		name, supernet string
		bits           int
		allocated      []string
		expected       string
	}{
		{"larger-new-nodes", "172.27.0.0/17", 23, []string{"172.27.0.64/26", "172.27.2.0/26"}, "172.27.4.0/23"},
		{"smaller-new-nodes", "172.27.0.0/17", 26, []string{"172.27.0.0/23"}, "172.27.2.0/26"},
		{"exhausted-by-smaller-node", "172.27.0.0/23", 23, []string{"172.27.1.192/26"}, ""},
		{"exhausted-by-larger-node", "172.27.0.0/26", 26, []string{"172.27.0.0/23"}, ""},
		{"separate-supernet", "172.28.0.0/14", 23, []string{"172.27.0.0/26"}, "172.28.0.0/23"},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocated := make(map[netip.Prefix]struct{})
			for _, value := range test.allocated {
				allocated[netip.MustParsePrefix(value)] = struct{}{}
			}
			got, ok := firstFreeRuntimeNodePrefix(netip.MustParsePrefix(test.supernet), test.bits, allocated)
			require.Equal(t, test.expected != "", ok)
			if ok {
				require.Equal(t, test.expected, got.String())
				for existing := range allocated {
					require.False(t, got.Overlaps(existing))
				}
			}
		})
	}
}
