package rootfs

import (
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSObserverRegistersOperationPhaseSizeAndCacheMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewObserver(registry, nil)
	observer.ObservePhaseDuration("apply", "tar_filter", 25*time.Millisecond, nil)
	observer.ObserveCache("hit")
	observer.ObserveOperation("apply", ctldapi.RootFSContainerRef{
		Namespace:     "tpl-default",
		PodName:       "sandbox-pod",
		ContainerName: "procd",
	}, 2, 2048, 1024, -1, time.Now().Add(-time.Second), http.StatusOK, "")

	families, err := registry.Gather()
	require.NoError(t, err)
	names := make(map[string]struct{}, len(families))
	for _, family := range families {
		names[family.GetName()] = struct{}{}
	}
	for _, name := range []string{
		"ctld_rootfs_operation_duration_seconds",
		"ctld_rootfs_phase_duration_seconds",
		"ctld_rootfs_checkpoint_bytes",
		"ctld_rootfs_checkpoint_chain_depth",
		"ctld_rootfs_object_cache_requests_total",
	} {
		_, ok := names[name]
		assert.True(t, ok, "metric family %s is registered", name)
	}
	for _, family := range families {
		if family.GetName() != "ctld_rootfs_checkpoint_bytes" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["operation"] == "apply" && labels["kind"] == "excluded" {
				assert.Equal(t, uint64(1), metric.GetHistogram().GetSampleCount())
				assert.Equal(t, float64(1024), metric.GetHistogram().GetSampleSum())
				return
			}
		}
	}
	t.Fatal("excluded checkpoint byte metric not found")
}
