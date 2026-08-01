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
	observer.ObserveHotCacheRequest("hit", "protected")
	observer.ObserveHotCacheAdmission("admitted", "protected")
	observer.ObserveHotCacheEviction("capacity", "probation")
	observer.ObserveHotCacheCandidate("probation", 1<<20, time.Minute, time.Second)
	observer.ObserveHotCacheResidence("probation", "capacity", time.Minute)
	observer.SetHotCacheBudget(1 << 30)
	observer.SetHotCacheSize(2, 2<<20, 1, 1<<20, 1, 1<<20)

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"ctld_volume_portal_bind_duration_seconds":                 false,
		"ctld_volume_portal_phase_duration_seconds":                false,
		"ctld_s0fs_open_read_bytes":                                false,
		"ctld_s0fs_state_items":                                    false,
		"ctld_s0fs_hot_cache_requests_total":                       false,
		"ctld_s0fs_hot_cache_admissions_total":                     false,
		"ctld_s0fs_hot_cache_evictions_total":                      false,
		"ctld_s0fs_hot_cache_entries":                              false,
		"ctld_s0fs_hot_cache_estimated_bytes":                      false,
		"ctld_s0fs_hot_cache_segment_entries":                      false,
		"ctld_s0fs_hot_cache_segment_estimated_bytes":              false,
		"ctld_s0fs_hot_cache_budget_bytes":                         false,
		"ctld_s0fs_hot_cache_entry_estimated_bytes":                false,
		"ctld_s0fs_hot_cache_candidate_active_duration_seconds":    false,
		"ctld_s0fs_hot_cache_candidate_cold_open_duration_seconds": false,
		"ctld_s0fs_hot_cache_residence_duration_seconds":           false,
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
