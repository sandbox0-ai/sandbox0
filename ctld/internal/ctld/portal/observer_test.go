package portal

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func TestObserverDoesNotUseVolumeIDAsMetricLabel(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewObserver(registry, nil)
	observer.ObserveBind("s0fs", "hot", "vol-sensitive", time.Now(), nil)
	observer.ObserveS0FSOpen(s0fs.OpenObservation{
		VolumeID:         "vol-sensitive",
		Phase:            "complete",
		Source:           "local",
		Format:           s0fs.StateFormatV2,
		Duration:         time.Millisecond,
		Bytes:            1024,
		Nodes:            10,
		DirectoryEntries: 9,
	})
	observer.ObserveHotCacheRequest("hit")

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"ctld_volume_portal_bind_duration_seconds":  false,
		"ctld_volume_portal_phase_duration_seconds": false,
		"ctld_s0fs_open_read_bytes":                 false,
		"ctld_s0fs_state_items":                     false,
		"ctld_s0fs_hot_cache_requests_total":        false,
	}
	for _, family := range families {
		name := family.GetName()
		if _, ok := want[name]; ok {
			want[name] = true
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "volume_id" || label.GetValue() == "vol-sensitive" {
					t.Fatalf("metric %s contains a per-volume label: %s=%s", name, label.GetName(), label.GetValue())
				}
			}
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("metric %s was not gathered", name)
		}
	}
}
