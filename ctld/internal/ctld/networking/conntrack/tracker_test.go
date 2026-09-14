package conntrack

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPolicyCleanupPreservesAllowedFlowsForLaterRevocation(t *testing.T) {
	tracker := NewTracker()
	allowed := FlowKey{SrcIP: netip.MustParseAddr("10.0.0.2"), DstIP: netip.MustParseAddr("1.1.1.1"), DstPort: 443}
	denied := allowed
	denied.DstPort = 80
	tracker.Record(allowed)
	tracker.Record(denied)
	originalTimestamp := tracker.bySrc["10.0.0.2"][allowed]
	removed := tracker.PopBySrcMatching("10.0.0.2", func(flow FlowKey) bool { return flow.DstPort == 80 })
	require.Equal(t, []FlowKey{denied}, removed)
	require.Equal(t, 1, tracker.totalSize)
	require.Equal(t, originalTimestamp, tracker.bySrc["10.0.0.2"][allowed])
	require.Equal(t, []FlowKey{allowed}, tracker.PopBySrc("10.0.0.2"))
	require.Zero(t, tracker.totalSize)
	require.Empty(t, tracker.bySrc)
}
