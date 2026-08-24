package legacyackmigration

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

func TestNormalizeProducesPinnedPausedNomadRecords(t *testing.T) {
	catalog := validCatalog(t)
	normalized, err := catalog.Normalize(testNormalizeOptions())
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(normalized.Sandboxes) != 1 {
		t.Fatalf("len(Sandboxes) = %d, want 1", len(normalized.Sandboxes))
	}
	got := normalized.Sandboxes[0]
	if got.Record.ClusterID != "ali-ue1-nomad" {
		t.Fatalf("ClusterID = %q", got.Record.ClusterID)
	}
	if got.Record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		t.Fatalf("DesiredState = %q", got.Record.DesiredState)
	}
	if got.Record.ResourceMillicpu != 1000 || got.Record.ResourceMemoryMiB != 2048 {
		t.Fatalf("resources = %dm/%dMiB", got.Record.ResourceMillicpu, got.Record.ResourceMemoryMiB)
	}
	if !strings.Contains(got.PinnedOCIRef, "@sha256:") || got.Record.TemplateSpec.MainContainer.Image != got.PinnedOCIRef {
		t.Fatalf("pinned image = %q", got.PinnedOCIRef)
	}
	if len(normalized.InferredLayers) != 1 || normalized.InferredLayers[0] != "layer-1" {
		t.Fatalf("InferredLayers = %#v", normalized.InferredLayers)
	}
	chain := normalized.LayerChains["layer-1"]
	if len(chain) != 1 || chain[0].PlatformOS != "linux" || chain[0].PlatformArchitecture != "amd64" {
		t.Fatalf("chain platform = %#v", chain)
	}
}

func TestNormalizeRejectsUnsafeFrozenCatalogs(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Catalog)
		wantErr string
	}{
		{
			name: "active sandbox",
			mutate: func(c *Catalog) {
				c.Sandboxes[0].DesiredState = sandboxstore.SandboxDesiredStateActive
			},
			wantErr: "every live sandbox must be paused",
		},
		{
			name: "active lifecycle transaction",
			mutate: func(c *Catalog) {
				c.ActiveLifecycleTxns = 1
			},
			wantErr: "active lifecycle transactions",
		},
		{
			name: "cross-team layer",
			mutate: func(c *Catalog) {
				c.Layers[0].TeamID = "other-team"
			},
			wantErr: "crosses team ownership",
		},
		{
			name: "compressed layer",
			mutate: func(c *Catalog) {
				c.Layers[0].DiffMediaType = ocispec.MediaTypeImageLayerGzip
			},
			wantErr: "not an uncompressed OCI tar layer",
		},
		{
			name: "filesystem cycle",
			mutate: func(c *Catalog) {
				c.Filesystems[0].SourceFilesystemID = c.Filesystems[0].ID
			},
			wantErr: "filesystem graph contains a cycle",
		},
		{
			name: "Kubernetes pod override",
			mutate: func(c *Catalog) {
				var spec map[string]any
				mustUnmarshal(t, c.Sandboxes[0].TemplateSpec, &spec)
				spec["pod"] = map[string]any{"serviceAccountName": "custom"}
				c.Sandboxes[0].TemplateSpec = mustMarshal(t, spec)
			},
			wantErr: "pod overrides cannot be migrated losslessly",
		},
		{
			name: "Kubernetes security context",
			mutate: func(c *Catalog) {
				var spec map[string]any
				mustUnmarshal(t, c.Sandboxes[0].TemplateSpec, &spec)
				main := spec["mainContainer"].(map[string]any)
				main["securityContext"] = map[string]any{"runAsUser": float64(1000)}
				c.Sandboxes[0].TemplateSpec = mustMarshal(t, spec)
			},
			wantErr: "securityContext cannot be migrated losslessly",
		},
		{
			name: "source schema drift",
			mutate: func(c *Catalog) {
				c.ManagerSchemaVersion++
			},
			wantErr: "expected 19",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := validCatalog(t)
			test.mutate(&catalog)
			_, err := catalog.Normalize(testNormalizeOptions())
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Normalize() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestNormalizeRequiresExplicitTargetAndPlatform(t *testing.T) {
	catalog := validCatalog(t)
	options := testNormalizeOptions()
	options.TargetClusterID = ""
	if _, err := catalog.Normalize(options); err == nil || !strings.Contains(err.Error(), "cluster ID is required") {
		t.Fatalf("Normalize() error = %v", err)
	}
	options = testNormalizeOptions()
	options.Platform = ocispec.Platform{Architecture: "amd64"}
	if _, err := catalog.Normalize(options); err == nil || !strings.Contains(err.Error(), "explicit canonical Linux platform") {
		t.Fatalf("Normalize() error = %v", err)
	}
}

func TestBuildInventoryClassifiesReachableAndOrphanLayers(t *testing.T) {
	catalog := validCatalog(t)
	orphan := catalog.Layers[0]
	orphan.ID = "orphan"
	orphan.ParentID = ""
	orphan.DiffDigest = digest.FromString("orphan").String()
	orphan.DiffID = orphan.DiffDigest
	orphan.DiffSize = 512
	catalog.Layers = append(catalog.Layers, orphan)
	inventory := catalog.BuildInventory()
	if inventory.LiveSandboxCount != 1 || inventory.ReachableLayerCount != 1 || inventory.OrphanLayerCount != 1 {
		t.Fatalf("inventory = %#v", inventory)
	}
	if inventory.ReachableLayerBytes != 4096 || inventory.LayerBytes != 4608 {
		t.Fatalf("inventory bytes = %d/%d", inventory.ReachableLayerBytes, inventory.LayerBytes)
	}
}

func validCatalog(t *testing.T) Catalog {
	t.Helper()
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	baseDigest := digest.FromString("base-image").String()
	diffDigest := digest.FromString("legacy-layer").String()
	templateSpec := map[string]any{
		"description": "legacy",
		"mainContainer": map[string]any{
			"image": "alpine:3.22",
			"resources": map[string]any{
				"cpu": "1", "memory": "2Gi", "ephemeralStorage": "8Gi",
			},
		},
		"pool": map[string]any{"minIdle": float64(1), "maxIdle": float64(3)},
	}
	return Catalog{
		ManagerSchemaVersion: LegacyManagerSchemaVersion,
		Sandboxes: []Sandbox{{
			ID: "sandbox-1", TeamID: "team-1", UserID: "user-1",
			TemplateID: "template-1", TemplateName: "python", TemplateNamespace: "team-1",
			ClusterID: "old-ack", DesiredState: sandboxstore.SandboxDesiredStatePaused,
			Config: mustMarshal(t, map[string]any{}), TemplateSpec: mustMarshal(t, templateSpec),
			RuntimeGeneration: 7, LifecycleEpoch: 4, OwnerKind: "claimed",
			ClaimedAt: now.Add(-time.Hour), CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now,
		}},
		Layers: []Layer{{
			ID: "layer-1", SourceSandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 7,
			BaseImageRef: "alpine:3.22", BaseImageDigest: baseDigest,
			DiffDigest: diffDigest, DiffID: diffDigest, DiffMediaType: ocispec.MediaTypeImageLayer,
			DiffSize: 4096, DiffObjectKey: "sandbox-rootfs/team-1/layer-1.tar", CreatedAt: now,
		}},
		Filesystems: []Filesystem{{
			ID: "filesystem-1", TeamID: "team-1", HeadLayerID: "layer-1",
			BaseImageRef: "alpine:3.22", BaseImageDigest: baseDigest, CreatedAt: now, UpdatedAt: now,
		}},
		Bindings: []Binding{{
			SandboxID: "sandbox-1", FilesystemID: "filesystem-1", TeamID: "team-1", CreatedAt: now, UpdatedAt: now,
		}},
		Snapshots: []Snapshot{{
			ID: "snapshot-1", TeamID: "team-1", SourceSandboxID: "sandbox-1", HeadLayerID: "layer-1",
			FilesystemID: "filesystem-1", Name: "checkpoint", CreatedAt: now,
		}},
	}
}

func testNormalizeOptions() NormalizeOptions {
	return NormalizeOptions{
		Platform:        ocispec.Platform{OS: "linux", Architecture: "amd64"},
		ResourcePolicy:  templatepkg.NewResourcePolicy("2Gi", "256Gi"),
		TargetClusterID: "ali-ue1-nomad",
	}
}

func mustMarshal(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func mustUnmarshal(t *testing.T, raw []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(raw, destination); err != nil {
		t.Fatal(err)
	}
}
