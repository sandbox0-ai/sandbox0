package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"github.com/stretchr/testify/require"
)

func TestConfigureRootFSImportWorkerFailsClosed(t *testing.T) {
	for name, test := range map[string]struct {
		cfg     *config.ManagerConfig
		objects objectstore.Store
		want    string
	}{
		"unknown format": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{FormatGeneration: 10005}},
			want: "format_generation",
		},
		"negative format": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{FormatGeneration: -1}},
			want: "format_generation",
		},
		"compressed geometry too large": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{FormatGeneration: 2, DataRangeBytes: 1 << 20}},
			want: "data_range_bytes",
		},
		"compressed format valid before database check": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{FormatGeneration: 2}},
			want: "requires PostgreSQL",
		},
		"negative geometry": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{DataRangeBytes: -1}},
			want: "data_range_bytes",
		},
		"unaligned geometry": {
			cfg:  &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{DataRangeBytes: (1 << 20) + 1}},
			want: "data_range_bytes",
		},
		"disabled": {
			cfg: &config.ManagerConfig{
				RootFSImporter: config.RootFSImporterConfig{Disabled: true},
			},
			want: "requires the durable RootFS importer",
		},
		"database": {
			cfg:  &config.ManagerConfig{},
			want: "requires PostgreSQL",
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker, err := configureRootFSImportWorker(test.cfg, nil, test.objects)
			if err == nil || worker != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("configure importer = %v, %v", worker, err)
			}
		})
	}
}

type geometryDiscoverySources struct{ image string }

func (s geometryDiscoverySources) ListImageSourcesForRootFSImport(context.Context, templatestore.ImageSourceCursor, int) ([]templatestore.ImageSource, error) {
	return []templatestore.ImageSource{{Image: s.image, EphemeralStorage: "16Gi"}}, nil
}

type geometryDiscoveryImports struct {
	lookups []sandboxstore.ReadyRootFSArtifactRequirements
	begun   []*sandboxstore.BeginRootFSImportRequest
}

func (s *geometryDiscoveryImports) GetReadyRootFSBaseArtifact(_ context.Context, _ string, _ sandboxstore.RootFSArtifactPlatform, requirements sandboxstore.ReadyRootFSArtifactRequirements) (*sandboxstore.RootFSBaseArtifact, error) {
	s.lookups = append(s.lookups, requirements)
	return nil, sandboxstore.ErrRootFSBaseArtifactNotFound
}

func (s *geometryDiscoveryImports) BeginRootFSImport(_ context.Context, req *sandboxstore.BeginRootFSImportRequest) (*sandboxstore.RootFSImportOperation, error) {
	copy := *req
	s.begun = append(s.begun, &copy)
	return &sandboxstore.RootFSImportOperation{ID: req.OperationID, Spec: req.Spec, State: sandboxstore.RootFSImportStatePending}, nil
}

func TestConfigureRootFSImportDiscoveryWiresRawGeometry(t *testing.T) {
	for _, configured := range []int{0, 16 << 10, 64 << 10} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
			imports := &geometryDiscoveryImports{}
			cfg := &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{
				DataRangeBytes: configured, ProcdProtocol: "sandbox0.procd.v3",
				ProcdDigest: "sha256:" + strings.Repeat("b", 64),
			}}
			worker, err := configureRootFSImportDiscovery(cfg, geometryDiscoverySources{image: image}, imports,
				[]sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}})
			if err != nil {
				t.Fatal(err)
			}
			result, err := worker.RunOnce(t.Context())
			if err != nil || result.Ensured != 1 || len(imports.lookups) != 1 || len(imports.begun) != 1 {
				t.Fatalf("discovery result=%+v error=%v lookups=%d imports=%d", result, err, len(imports.lookups), len(imports.begun))
			}
			if got := imports.lookups[0]; got.ImportDataRangeBytes != configured || got.SourceOCIRef != image {
				t.Fatalf("lookup requirements=%+v, want raw geometry %d and exact image", got, configured)
			}
			want := configured
			if want == 0 {
				want = rootfsblock.DefaultDataRangeBytes
			}
			if got := imports.begun[0].Spec; got.SourceOCIRef != image || got.BlockOptions.DataRangeBytes != want || got.BlockOptions.PackBytes != rootfsblock.DefaultPackBytes {
				t.Fatalf("durable import spec = %+v", got)
			}
		})
	}
}

func TestRootFSImportWorkerIDIsCanonicalAndUnique(t *testing.T) {
	first, err := newRootFSImportWorkerID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRootFSImportWorkerID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "manager.rootfs.import.") || strings.Contains(first, "-") {
		t.Fatalf("worker IDs = %q, %q", first, second)
	}
}

func TestConfigureRootFSImportDiscoveryAndClaimerShareFormatPolicy(t *testing.T) {
	for _, policy := range []struct{ format, dataRange int }{{0, 0}, {0, 16 << 10}, {2, 0}, {2, 16 << 10}, {2, 64 << 10}} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			image := "registry.example/runtime@sha256:" + strings.Repeat("a", 64)
			cfg := &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{
				FormatGeneration: policy.format, DataRangeBytes: policy.dataRange,
				ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: "sha256:" + strings.Repeat("b", 64),
			}}
			imports := &geometryDiscoveryImports{}
			worker, err := configureRootFSImportDiscovery(cfg, geometryDiscoverySources{image: image}, imports,
				[]sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}})
			require.NoError(t, err)
			result, err := worker.RunOnce(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, result.Ensured)
			require.Len(t, imports.lookups, 1)
			require.Len(t, imports.begun, 1)
			claim := sandboxRuntimeClaimConfig(cfg, sandboxRuntimeBackendDependencies{}, nil)
			format, err := rootfsimporter.ImageImportFormat(claim.RootFSFormatGeneration)
			require.NoError(t, err)
			require.Equal(t, 2, format)
			require.Equal(t, format, imports.lookups[0].FormatGeneration)
			require.Equal(t, format, imports.begun[0].Spec.FormatGeneration)
			require.Equal(t, policy.dataRange, claim.RootFSImportDataRangeBytes)
			require.Equal(t, policy.dataRange, imports.lookups[0].ImportDataRangeBytes)
			options, err := rootfsimporter.NormalizeBlockOptions(format, rootfsblock.BuildOptions{DataRangeBytes: policy.dataRange})
			require.NoError(t, err)
			require.Equal(t, options, imports.begun[0].Spec.BlockOptions)
			if format == 2 {
				require.Equal(t, 2, options.FormatVersion)
				require.LessOrEqual(t, options.DataRangeBytes, 64<<10)
			}
		})
	}
}

func TestConfigureDiscoveryAndClaimerShareMappingPolicy(t *testing.T) {
	for _, policy := range []string{"", rootfsblock.ContiguousMappingV1, "unknown"} {
		cfg := &config.ManagerConfig{RootFSImporter: config.RootFSImporterConfig{
			FormatGeneration: 2, MappingGroupPolicy: policy,
			ProcdProtocol: "sandbox0.procd.v3", ProcdDigest: "sha256:" + strings.Repeat("b", 64),
		}}
		imports := &geometryDiscoveryImports{}
		worker, err := configureRootFSImportDiscovery(cfg,
			geometryDiscoverySources{image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)}, imports,
			[]sandboxstore.RootFSArtifactPlatform{{OS: "linux", Architecture: "amd64"}})
		if policy == "unknown" {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		_, err = worker.RunOnce(t.Context())
		require.NoError(t, err)
		require.Len(t, imports.lookups, 1)
		require.Len(t, imports.begun, 1)
		require.Equal(t, policy, imports.lookups[0].ImportMappingGroupPolicy)
		require.Equal(t, policy, imports.begun[0].Spec.BlockOptions.MappingGroupPolicy)
		claim := sandboxRuntimeClaimConfig(cfg, sandboxRuntimeBackendDependencies{}, nil)
		require.Equal(t, policy, claim.RootFSImportMappingGroupPolicy)
	}
}
