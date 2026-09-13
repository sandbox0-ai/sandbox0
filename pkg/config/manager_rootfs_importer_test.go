package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestManagerRootFSImportFormatPreservesRawPolicy(t *testing.T) {
	for _, configured := range []int{-1, 0, 1, 2, 10005} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manager.yaml")
			payload := fmt.Sprintf("rootfs_importer:\n  format_generation: %d\n", configured)
			if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CONFIG_PATH", path)
			if got := LoadManagerConfig().RootFSImporter.FormatGeneration; got != configured {
				t.Fatalf("format generation = %d, want raw policy %d", got, configured)
			}
		})
	}
}

func TestManagerMappingPolicyDefaultsOffAndPreservesExplicitInput(t *testing.T) {
	for _, policy := range []string{"", "contiguous-mapping-v1", "unknown"} {
		path := filepath.Join(t.TempDir(), "manager.yaml")
		payload := "{}\n"
		if policy != "" {
			payload = fmt.Sprintf("rootfs_importer:\n  mapping_group_policy: %q\n", policy)
		}
		if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("CONFIG_PATH", path)
		if got := LoadManagerConfig().RootFSImporter.MappingGroupPolicy; got != policy {
			t.Fatalf("mapping policy = %q, want raw policy %q", got, policy)
		}
	}
}

func TestManagerRootFSImportDataRangePreservesExplicitPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		want       int
	}{
		{name: "unset", yaml: "{}\n"},
		{name: "explicit_zero", yaml: "rootfs_importer:\n  data_range_bytes: 0\n"},
		{name: "one_mib", yaml: "rootfs_importer:\n  data_range_bytes: 1048576\n", want: 1 << 20},
		{name: "eight_mib", yaml: "rootfs_importer:\n  data_range_bytes: 8388608\n", want: 8 << 20},
		// Keep invalid input intact so importer/claimer startup validation can
		// reject it, rather than silently replacing it with legacy defaults.
		{name: "negative_not_defaulted", yaml: "rootfs_importer:\n  data_range_bytes: -1\n", want: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "manager.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("CONFIG_PATH", path)
			cfg := LoadManagerConfig()
			if cfg.RootFSImporter.DataRangeBytes != tc.want {
				t.Fatalf("import data range = %d, want raw policy %d", cfg.RootFSImporter.DataRangeBytes, tc.want)
			}
		})
	}
}
