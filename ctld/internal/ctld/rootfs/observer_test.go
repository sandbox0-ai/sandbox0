package rootfs

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSObserverRegistersReadPhaseAndCacheMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewObserver(registry, nil)
	observer.ObservePhaseDuration("read", "cache_validation", 25*time.Millisecond, nil)
	observer.ObserveCache("hit")

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"ctld_rootfs_phase_duration_seconds",
		"ctld_rootfs_object_cache_requests_total",
	} {
		_, ok := names[name]
		assert.True(t, ok, "metric family %s is registered", name)
	}
}
