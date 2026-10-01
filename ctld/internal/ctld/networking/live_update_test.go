package networking

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestLiveUpdateRejectsDuplicatingProcessLocalBandwidthBudget(t *testing.T) {
	for _, cfg := range []*config.NetworkRuntimeConfig{
		{EgressBandwidthBytesPerSecond: 1}, {IngressBandwidthBytesPerSecond: 1},
	} {
		daemon := &Daemon{cfg: cfg, proxyServer: &proxy.Server{}}
		daemon.ready.Store(true)
		files, err := daemon.PrepareLiveUpdate()
		require.ErrorContains(t, err, "shared bandwidth accounting")
		require.Empty(t, files)
		require.True(t, daemon.Ready())
	}
}
