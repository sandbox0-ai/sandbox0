package nomadruntime

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// registerReadCacheMetrics exposes the active node cache without sandbox or
// content labels. Unregister before releasing the cache during HA handoff.
func registerReadCacheMetrics(registry prometheus.Registerer, stats func() rootfsblock.ReadCacheStats) (func(), error) {
	var registered []prometheus.Collector
	unregister := func() {
		for _, collector := range registered {
			registry.Unregister(collector)
		}
	}
	for _, metric := range []struct {
		name, help string
		counter    bool
		value      func(rootfsblock.ReadCacheStats) float64
	}{
		{"disk_hits_total", "Verified node disk-cache hits", true, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskHits) }},
		{"disk_misses_total", "Node disk-cache lookups requiring source fallback", true, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskMisses) }},
		{"disk_writes_total", "Immutable ranges published into the node disk cache", true, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskWrites) }},
		{"disk_errors_total", "Node disk-cache I/O or integrity failures", true, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskErrors) }},
		{"disk_write_drops_total", "Cache fills dropped when the bounded queue is full", true, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskWriteDrops) }},
		{"disk_bytes", "Payload bytes retained in the node disk cache", false, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskBytes) }},
		{"disk_entries", "Immutable ranges indexed in the node disk cache", false, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.DiskEntries) }},
		{"queued_write_bytes", "Payload bytes retained by queued and active cache fills", false, func(s rootfsblock.ReadCacheStats) float64 { return float64(s.QueuedWriteBytes) }},
	} {
		value := func() float64 { return metric.value(stats()) }
		opts := prometheus.Opts{Name: "ctld_rootfs_read_cache_" + metric.name, Help: metric.help}
		var collector prometheus.Collector
		if metric.counter {
			collector = prometheus.NewCounterFunc(prometheus.CounterOpts(opts), value)
		} else {
			collector = prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), value)
		}
		if err := registry.Register(collector); err != nil {
			unregister()
			return nil, err
		}
		registered = append(registered, collector)
	}
	return unregister, nil
}
