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
	if got.Record.TemplateSpec.MainContainer.SecurityClass != "privileged" {
		t.Fatalf("SecurityClass = %q", got.Record.TemplateSpec.MainContainer.SecurityClass)
	}
	if mounts := got.Record.TemplateSpec.EphemeralMounts; len(mounts) != 1 ||
		mounts[0].MountPath != "/var/lib/docker" || mounts[0].SizeLimit != "16Gi" {
		t.Fatalf("EphemeralMounts = %#v", mounts)
	}
	if len(got.CompatibilityAdjustments) != 5 {
		t.Fatalf("CompatibilityAdjustments = %#v", got.CompatibilityAdjustments)
	}
	if normalized.FilesystemLogicalSizes["filesystem-1"] != 8<<30 ||
		normalized.SourceSandboxLogicalSize["sandbox-1"] != 8<<30 {
		t.Fatalf("logical sizes = %#v / %#v", normalized.FilesystemLogicalSizes, normalized.SourceSandboxLogicalSize)
	}
	if len(normalized.MaterializedBuilds) != 1 || len(normalized.Filesystems) != 1 || len(normalized.Snapshots) != 1 {
		t.Fatalf("materialized plan = builds %#v filesystems %#v snapshots %#v",
			normalized.MaterializedBuilds, normalized.Filesystems, normalized.Snapshots)
	}
	build := normalized.MaterializedBuilds[0]
	if build.ID != normalized.Filesystems[0].HeadBuildID || build.ID != normalized.Snapshots[0].BuildID ||
		build.MutationDigest == "" || build.ObjectPrefix == "" || build.LogicalSizeBytes != 8<<30 {
		t.Fatalf("materialized build = %#v", build)
	}
	chain := normalized.LayerChains["layer-1"]
	if len(chain) != 1 || chain[0].PlatformOS != "linux" || chain[0].PlatformArchitecture != "amd64" {
		t.Fatalf("chain platform = %#v", chain)
	}
}

func TestCatalogDigestIsDeterministicAndContentAddressed(t *testing.T) {
	catalog := validCatalog(t)
	first, err := catalog.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}
	second, err := catalog.Digest()
	if err != nil {
		t.Fatalf("Digest() retry error = %v", err)
	}
	if first != second || !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("Digest() = %q then %q", first, second)
	}

	catalog.Layers[0].DiffSize++
	changed, err := catalog.Digest()
	if err != nil {
		t.Fatalf("Digest() after mutation error = %v", err)
	}
	if changed == first {
		t.Fatal("Digest() did not change with source content")
	}
}

func TestCatalogDigestIgnoresJSONObjectRepresentation(t *testing.T) {
	catalog := validCatalog(t)
	catalog.Sandboxes[0].Config = json.RawMessage(`{"alpha":1,"beta":2}`)
	reordered := catalog
	reordered.Sandboxes = append([]Sandbox(nil), catalog.Sandboxes...)
	reordered.Sandboxes[0].Config = json.RawMessage("{\n  \"beta\": 2,\n  \"alpha\": 1\n}")

	first, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second, err := reordered.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("semantically identical catalog digests differ: %s != %s", first, second)
	}
}

func TestNormalizeRecoversSnapshotOnlyFilesystemGeometryFromArchivedSource(t *testing.T) {
	catalog := validCatalog(t)
	catalog.Sandboxes = nil
	catalog.Bindings = nil
	normalized, err := catalog.Normalize(testNormalizeOptions())
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(normalized.Sandboxes) != 0 || normalized.FilesystemLogicalSizes["filesystem-1"] != 8<<30 {
		t.Fatalf("normalized catalog = %#v", normalized)
	}
}

func TestNormalizeCanonicalizesLegacySameFilesystemRestoreEdge(t *testing.T) {
	catalog := validCatalog(t)
	catalog.Filesystems[0].SourceFilesystemID = catalog.Filesystems[0].ID
	before, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}

	normalized, err := catalog.Normalize(testNormalizeOptions())
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if len(normalized.Filesystems) != 1 || normalized.Filesystems[0].Record.SourceFilesystemID != "" {
		t.Fatalf("normalized filesystems = %#v", normalized.Filesystems)
	}
	if len(normalized.NormalizedSelfSourceFilesystems) != 1 ||
		normalized.NormalizedSelfSourceFilesystems[0] != "filesystem-1" {
		t.Fatalf("normalized self sources = %#v", normalized.NormalizedSelfSourceFilesystems)
	}
	after, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if after != before || catalog.Filesystems[0].SourceFilesystemID != "filesystem-1" {
		t.Fatal("source catalog or its retirement digest was mutated during normalization")
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
				second := c.Filesystems[0]
				second.ID = "filesystem-2"
				second.SourceFilesystemID = c.Filesystems[0].ID
				c.Filesystems[0].SourceFilesystemID = second.ID
				c.Filesystems = append(c.Filesystems, second)
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
			wantErr: "pod scheduling or identity overrides cannot be migrated losslessly",
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
			wantErr: "non-root Kubernetes runAsUser cannot be migrated losslessly",
		},
		{
			name: "custom capabilities",
			mutate: func(c *Catalog) {
				var spec map[string]any
				mustUnmarshal(t, c.Sandboxes[0].TemplateSpec, &spec)
				main := spec["mainContainer"].(map[string]any)
				main["securityContext"] = map[string]any{
					"capabilities": map[string]any{"add": []any{"SYS_ADMIN"}},
				}
				c.Sandboxes[0].TemplateSpec = mustMarshal(t, spec)
			},
			wantErr: "custom Kubernetes capabilities cannot be migrated losslessly",
		},
		{
			name: "unknown retired volume mount",
			mutate: func(c *Catalog) {
				var spec map[string]any
				mustUnmarshal(t, c.Sandboxes[0].TemplateSpec, &spec)
				spec["volumeMounts"] = []any{map[string]any{"name": "other", "mountPath": "/data"}}
				c.Sandboxes[0].TemplateSpec = mustMarshal(t, spec)
			},
			wantErr: "unrecognized retired volumeMounts metadata",
		},
		{
			name: "unknown template field",
			mutate: func(c *Catalog) {
				var spec map[string]any
				mustUnmarshal(t, c.Sandboxes[0].TemplateSpec, &spec)
				spec["futureField"] = true
				c.Sandboxes[0].TemplateSpec = mustMarshal(t, spec)
			},
			wantErr: "unknown field",
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
			if len(catalog.Sandboxes) != 0 && len(catalog.SourceSandboxes) != 0 {
				catalog.SourceSandboxes[0].TemplateSpec = append(
					json.RawMessage(nil), catalog.Sandboxes[0].TemplateSpec...,
				)
			}
			_, err := catalog.Normalize(testNormalizeOptions())
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Normalize() error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestNormalizeForPreflightAllowsOnlyActiveOrPausedSandboxes(t *testing.T) {
	catalog := validCatalog(t)
	catalog.Sandboxes[0].DesiredState = sandboxstore.SandboxDesiredStateActive

	normalized, err := catalog.NormalizeForPreflight(testNormalizeOptions())
	if err != nil {
		t.Fatalf("NormalizeForPreflight() error = %v", err)
	}
	if len(normalized.Sandboxes) != 1 ||
		normalized.Sandboxes[0].Record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
		t.Fatalf("preflight normalized sandboxes = %#v", normalized.Sandboxes)
	}
	if _, err := catalog.Normalize(testNormalizeOptions()); err == nil ||
		!strings.Contains(err.Error(), "every live sandbox must be paused") {
		t.Fatalf("Normalize() after preflight error = %v", err)
	}

	catalog.Sandboxes[0].DesiredState = sandboxstore.SandboxDesiredStateTerminating
	if _, err := catalog.NormalizeForPreflight(testNormalizeOptions()); err == nil ||
		!strings.Contains(err.Error(), "unsupported preflight desired state") {
		t.Fatalf("NormalizeForPreflight() terminating error = %v", err)
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
			"image": "alpine:3.22", "imagePullPolicy": "IfNotPresent",
			"resources": map[string]any{
				"cpu": "1", "memory": "2Gi", "ephemeralStorage": "8Gi",
			},
			"securityContext": map[string]any{
				"privileged": true, "runAsUser": float64(0), "runAsGroup": float64(0),
				"runAsNonRoot": false, "readOnlyRootFilesystem": false,
				"allowPrivilegeEscalation": true,
			},
		},
		"pod": map[string]any{
			"emptyDirMounts": []any{map[string]any{"mountPath": "/var/lib/docker", "sizeLimit": "16Gi"}},
		},
		"volumeMounts": []any{map[string]any{"name": "workspace", "mountPath": "/workspace"}},
		"pool":         map[string]any{"minIdle": float64(1), "maxIdle": float64(3)},
	}
	catalog := Catalog{
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
	catalog.SourceSandboxes = []SourceSandbox{{
		ID: "sandbox-1", TeamID: "team-1", TemplateSpec: append(json.RawMessage(nil), catalog.Sandboxes[0].TemplateSpec...),
	}}
	return catalog
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
