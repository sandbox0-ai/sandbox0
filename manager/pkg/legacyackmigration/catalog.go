// Package legacyackmigration reads and validates the final Kubernetes-era
// Sandbox0 persistence graph before one-time import into the Nomad block-COW
// schema.
package legacyackmigration

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

const LegacyManagerSchemaVersion int64 = 19

type Sandbox struct {
	ID                  string
	TeamID              string
	UserID              string
	TemplateID          string
	TemplateName        string
	TemplateNamespace   string
	ClusterID           string
	DesiredState        string
	Config              json.RawMessage
	TemplateSpec        json.RawMessage
	RuntimeGeneration   int64
	LifecycleEpoch      int64
	OwnerKind           string
	HotClaimCompletedAt time.Time
	ClaimedAt           time.Time
	ExpiresAt           time.Time
	HardExpiresAt       time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type Layer struct {
	ID                   string
	ParentID             string
	SourceSandboxID      string
	TeamID               string
	RuntimeGeneration    int64
	Runtime              string
	RuntimeHandler       string
	BaseImageRef         string
	BaseImageDigest      string
	Snapshotter          string
	SnapshotParent       string
	SnapshotParentChain  json.RawMessage
	DiffDigest           string
	DiffID               string
	DiffMediaType        string
	DiffSize             int64
	DiffObjectKey        string
	PlatformOS           string
	PlatformArchitecture string
	PlatformVariant      string
	CreatedAt            time.Time
}

type Filesystem struct {
	ID                 string
	TeamID             string
	SourceFilesystemID string
	HeadLayerID        string
	BaseImageRef       string
	BaseImageDigest    string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type Binding struct {
	SandboxID    string
	FilesystemID string
	TeamID       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Snapshot struct {
	ID              string
	TeamID          string
	SourceSandboxID string
	HeadLayerID     string
	FilesystemID    string
	Name            string
	Description     string
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// SourceSandbox retains only the historical template identity needed to
// recover exact block-device geometry for layer and snapshot owners.
type SourceSandbox struct {
	ID           string
	TeamID       string
	TemplateSpec json.RawMessage
}

type Catalog struct {
	ManagerSchemaVersion int64
	ActiveLifecycleTxns  int64
	Sandboxes            []Sandbox
	Layers               []Layer
	Filesystems          []Filesystem
	Bindings             []Binding
	Snapshots            []Snapshot
	SourceSandboxes      []SourceSandbox
}

// Digest returns the immutable identity of the ordered frozen source catalog.
// Reader queries use deterministic ordering, so any source-row change produces
// a different session fence without introducing a second canonical form.
func (c Catalog) Digest() (string, error) {
	payload, err := canonicalCatalogPayload(c)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

// canonicalCatalogPayload makes JSON object ordering and insignificant
// whitespace irrelevant. PostgreSQL JSONB canonicalizes nested Config and
// TemplateSpec objects when a captured catalog is read back, so the source
// fence must not depend on their original textual representation.
func canonicalCatalogPayload(c Catalog) ([]byte, error) {
	payload, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("marshal legacy ACK catalog: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("canonicalize legacy ACK catalog: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("canonicalize legacy ACK catalog: trailing JSON value")
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode canonical legacy ACK catalog: %w", err)
	}
	return canonical, nil
}

type NormalizeOptions struct {
	Platform        ocispec.Platform
	ResourcePolicy  templatepkg.ResourcePolicy
	TargetClusterID string
}

type NormalizedSandbox struct {
	Record                   sandboxstore.SandboxRecord
	FilesystemID             string
	PinnedOCIRef             string
	CompatibilityAdjustments []string
}

type NormalizedCatalog struct {
	Sandboxes                       []NormalizedSandbox
	LayerChains                     map[string][]Layer
	PinnedImageRefs                 map[string]string
	FilesystemLogicalSizes          map[string]int64
	SourceSandboxLogicalSize        map[string]int64
	MaterializedBuilds              []MaterializedBuild
	Filesystems                     []NormalizedFilesystem
	Snapshots                       []NormalizedSnapshot
	InferredLayers                  []string
	NormalizedSelfSourceFilesystems []string
}

// MaterializedBuild is one tenant-scoped, complete generation conversion.
// Multiple filesystem generation rows may reuse its immutable descriptor and
// object inventory without sharing state across teams.
type MaterializedBuild struct {
	ID               string
	TeamID           string
	HeadLayerID      string
	PinnedOCIRef     string
	SourceOCIDigest  string
	LogicalSizeBytes int64
	Platform         ocispec.Platform
	MutationDigest   string
	ObjectPrefix     string
	Layers           []Layer
}

type NormalizedFilesystem struct {
	Record           Filesystem
	LogicalSizeBytes int64
	HeadBuildID      string
	BuildIDByLayer   map[string]string
}

type NormalizedSnapshot struct {
	Record  Snapshot
	BuildID string
}

// Normalize validates the frozen source graph and produces runtime-neutral
// sandbox records plus deterministic parent-to-child layer chains. It always
// requires every live sandbox to be paused, including when called on a catalog
// that was previously checked with NormalizeForPreflight.
func (c Catalog) Normalize(options NormalizeOptions) (*NormalizedCatalog, error) {
	return c.normalize(options, true)
}

// NormalizeForPreflight validates every migration compatibility invariant that
// is independent of the final pause barrier. It accepts active sandboxes only
// so operators can discover incompatible templates and storage graphs before
// closing ingress. It never relaxes lifecycle-transaction checks and must not
// be used by capture, retirement, or target materialization.
func (c Catalog) NormalizeForPreflight(options NormalizeOptions) (*NormalizedCatalog, error) {
	return c.normalize(options, false)
}

func (c Catalog) normalize(options NormalizeOptions, requirePaused bool) (*NormalizedCatalog, error) {
	if c.ManagerSchemaVersion != LegacyManagerSchemaVersion {
		return nil, fmt.Errorf("legacy manager schema version is %d, expected %d", c.ManagerSchemaVersion, LegacyManagerSchemaVersion)
	}
	if c.ActiveLifecycleTxns != 0 {
		return nil, fmt.Errorf("legacy manager has %d active lifecycle transactions", c.ActiveLifecycleTxns)
	}
	platform, err := normalizePlatform(options.Platform)
	if err != nil {
		return nil, err
	}
	targetClusterID := strings.TrimSpace(options.TargetClusterID)
	if targetClusterID == "" {
		return nil, fmt.Errorf("target Nomad cluster ID is required")
	}

	layers := make(map[string]Layer, len(c.Layers))
	for _, layer := range c.Layers {
		if strings.TrimSpace(layer.ID) == "" {
			return nil, fmt.Errorf("legacy layer has an empty ID")
		}
		if _, exists := layers[layer.ID]; exists {
			return nil, fmt.Errorf("duplicate legacy layer %s", layer.ID)
		}
		if strings.TrimSpace(layer.TeamID) == "" || layer.DiffSize < 0 || strings.TrimSpace(layer.DiffObjectKey) == "" {
			return nil, fmt.Errorf("legacy layer %s has incomplete team or diff metadata", layer.ID)
		}
		if parsed, parseErr := digest.Parse(strings.TrimSpace(layer.DiffDigest)); parseErr != nil || parsed.Algorithm() != digest.SHA256 {
			return nil, fmt.Errorf("legacy layer %s has invalid diff digest", layer.ID)
		}
		if layer.DiffID != "" {
			if parsed, parseErr := digest.Parse(strings.TrimSpace(layer.DiffID)); parseErr != nil || parsed.Algorithm() != digest.SHA256 {
				return nil, fmt.Errorf("legacy layer %s has invalid diff ID", layer.ID)
			}
			if strings.TrimSpace(layer.DiffID) != strings.TrimSpace(layer.DiffDigest) {
				return nil, fmt.Errorf("legacy layer %s diff digest and diff ID differ", layer.ID)
			}
		}
		if strings.TrimSpace(layer.DiffMediaType) != ocispec.MediaTypeImageLayer {
			return nil, fmt.Errorf("legacy layer %s is not an uncompressed OCI tar layer", layer.ID)
		}
		layers[layer.ID] = layer
	}
	for _, layer := range c.Layers {
		if layer.ParentID == "" {
			continue
		}
		parent, ok := layers[layer.ParentID]
		if !ok {
			return nil, fmt.Errorf("legacy layer %s has missing parent %s", layer.ID, layer.ParentID)
		}
		if parent.TeamID != layer.TeamID {
			return nil, fmt.Errorf("legacy layer %s crosses team ownership", layer.ID)
		}
	}

	filesystems := make(map[string]Filesystem, len(c.Filesystems))
	normalizedFilesystems := make([]Filesystem, 0, len(c.Filesystems))
	var normalizedSelfSources []string
	for _, filesystem := range c.Filesystems {
		if strings.TrimSpace(filesystem.ID) == "" || strings.TrimSpace(filesystem.TeamID) == "" {
			return nil, fmt.Errorf("legacy filesystem has an empty ID or team")
		}
		if _, exists := filesystems[filesystem.ID]; exists {
			return nil, fmt.Errorf("duplicate legacy filesystem %s", filesystem.ID)
		}
		// ACK managers that predate migration 00016 could reintroduce a
		// same-filesystem restore edge after the additive cleanup ran. That
		// edge carries no lineage information: the restored head already
		// describes the complete state and following the edge only loops back
		// to itself. Preserve it in the captured source catalog and digest, but
		// canonicalize exactly this historical shape in the target graph.
		if filesystem.SourceFilesystemID == filesystem.ID {
			filesystem.SourceFilesystemID = ""
			normalizedSelfSources = append(normalizedSelfSources, filesystem.ID)
		}
		filesystems[filesystem.ID] = filesystem
		normalizedFilesystems = append(normalizedFilesystems, filesystem)
	}
	for _, filesystem := range normalizedFilesystems {
		if filesystem.SourceFilesystemID != "" {
			source, ok := filesystems[filesystem.SourceFilesystemID]
			if !ok || source.TeamID != filesystem.TeamID {
				return nil, fmt.Errorf("legacy filesystem %s has an invalid source filesystem", filesystem.ID)
			}
		}
		if filesystem.HeadLayerID == "" {
			return nil, fmt.Errorf("legacy filesystem %s has no head layer", filesystem.ID)
		}
	}
	if err := validateFilesystemGraph(filesystems); err != nil {
		return nil, err
	}

	bindings := make(map[string]Binding, len(c.Bindings))
	for _, binding := range c.Bindings {
		filesystem, ok := filesystems[binding.FilesystemID]
		if !ok || filesystem.TeamID != binding.TeamID {
			return nil, fmt.Errorf("legacy binding for sandbox %s has an invalid filesystem", binding.SandboxID)
		}
		if _, exists := bindings[binding.SandboxID]; exists {
			return nil, fmt.Errorf("duplicate legacy binding for sandbox %s", binding.SandboxID)
		}
		bindings[binding.SandboxID] = binding
	}

	chainCache := make(map[string][]Layer)
	inferred := make(map[string]struct{})
	pinnedRefs := make(map[string]string)
	chainFor := func(headID string) ([]Layer, error) {
		if chain, ok := chainCache[headID]; ok {
			return append([]Layer(nil), chain...), nil
		}
		visiting := make(map[string]struct{})
		var reverse []Layer
		current := strings.TrimSpace(headID)
		for current != "" {
			if _, cycle := visiting[current]; cycle {
				return nil, fmt.Errorf("legacy layer graph contains a cycle at %s", current)
			}
			visiting[current] = struct{}{}
			layer, ok := layers[current]
			if !ok {
				return nil, fmt.Errorf("legacy layer %s is missing", current)
			}
			reverse = append(reverse, layer)
			current = strings.TrimSpace(layer.ParentID)
		}
		chain := make([]Layer, len(reverse))
		for index := range reverse {
			chain[len(reverse)-1-index] = reverse[index]
		}
		if len(chain) == 0 {
			return nil, fmt.Errorf("legacy layer chain for %s is empty", headID)
		}
		baseDigest := strings.TrimSpace(chain[0].BaseImageDigest)
		baseRef := strings.TrimSpace(chain[0].BaseImageRef)
		if _, parseErr := digest.Parse(baseDigest); parseErr != nil {
			return nil, fmt.Errorf("legacy layer chain %s has an invalid base image digest", headID)
		}
		pinned, pinErr := pinImageReference(baseRef, baseDigest)
		if pinErr != nil {
			return nil, fmt.Errorf("legacy layer chain %s: %w", headID, pinErr)
		}
		for index := range chain {
			layer := &chain[index]
			if strings.TrimSpace(layer.BaseImageDigest) != baseDigest {
				return nil, fmt.Errorf("legacy layer chain %s changes base image digest at %s", headID, layer.ID)
			}
			if layer.PlatformOS == "" && layer.PlatformArchitecture == "" && layer.PlatformVariant == "" {
				layer.PlatformOS, layer.PlatformArchitecture, layer.PlatformVariant = platform.OS, platform.Architecture, platform.Variant
				inferred[layer.ID] = struct{}{}
			}
			if layer.PlatformOS != platform.OS || layer.PlatformArchitecture != platform.Architecture || layer.PlatformVariant != platform.Variant {
				return nil, fmt.Errorf("legacy layer %s platform does not match %s/%s/%s", layer.ID, platform.OS, platform.Architecture, platform.Variant)
			}
		}
		chainCache[headID] = append([]Layer(nil), chain...)
		pinnedRefs[baseDigest] = pinned
		return chain, nil
	}

	sourceSizes, sourceTeams, err := resolveSourceSandboxLogicalSizes(c.SourceSandboxes, c.Sandboxes)
	if err != nil {
		return nil, err
	}
	normalized := &NormalizedCatalog{
		LayerChains: make(map[string][]Layer), PinnedImageRefs: pinnedRefs,
		FilesystemLogicalSizes: make(map[string]int64), SourceSandboxLogicalSize: sourceSizes,
		NormalizedSelfSourceFilesystems: normalizedSelfSources,
	}
	seenSandboxes := make(map[string]struct{}, len(c.Sandboxes))
	for _, legacy := range c.Sandboxes {
		if _, exists := seenSandboxes[legacy.ID]; exists {
			return nil, fmt.Errorf("duplicate legacy sandbox %s", legacy.ID)
		}
		seenSandboxes[legacy.ID] = struct{}{}
		if strings.TrimSpace(legacy.ID) == "" || strings.TrimSpace(legacy.TeamID) == "" {
			return nil, fmt.Errorf("legacy sandbox has an empty ID or team")
		}
		if legacy.DesiredState != sandboxstore.SandboxDesiredStatePaused {
			if requirePaused {
				return nil, fmt.Errorf("legacy sandbox %s is %s; every live sandbox must be paused", legacy.ID, legacy.DesiredState)
			}
			if legacy.DesiredState != sandboxstore.SandboxDesiredStateActive {
				return nil, fmt.Errorf("legacy sandbox %s has unsupported preflight desired state %s", legacy.ID, legacy.DesiredState)
			}
		}
		binding, ok := bindings[legacy.ID]
		if !ok || binding.TeamID != legacy.TeamID {
			return nil, fmt.Errorf("legacy sandbox %s has no team-consistent rootfs binding", legacy.ID)
		}
		filesystem := filesystems[binding.FilesystemID]
		chain, chainErr := chainFor(filesystem.HeadLayerID)
		if chainErr != nil {
			return nil, chainErr
		}
		if err := validateFilesystemChain(filesystem, chain); err != nil {
			return nil, err
		}
		normalized.LayerChains[filesystem.HeadLayerID] = chain
		pinnedRef := pinnedRefs[strings.TrimSpace(chain[0].BaseImageDigest)]

		var config sandboxstore.SandboxConfig
		if err := json.Unmarshal(legacy.Config, &config); err != nil {
			return nil, fmt.Errorf("decode legacy sandbox %s config: %w", legacy.ID, err)
		}
		spec, adjustments, err := decodeLegacyTemplateSpec(legacy.TemplateSpec)
		if err != nil {
			return nil, fmt.Errorf("decode legacy sandbox %s template spec: %w", legacy.ID, err)
		}
		spec.MainContainer.Image = pinnedRef
		var memoryOverride *string
		if config.Resources != nil && strings.TrimSpace(config.Resources.Memory) != "" {
			value := strings.TrimSpace(config.Resources.Memory)
			memoryOverride = &value
		}
		resources, resourceErr := options.ResourcePolicy.ResolveClaimResources(spec, memoryOverride)
		if resourceErr != nil {
			return nil, fmt.Errorf("resolve legacy sandbox %s resources: %w", legacy.ID, resourceErr)
		}
		spec.MainContainer.Resources = resources.Quota
		logicalSize, sizeErr := templatepkg.ResolveRootFSLogicalSize(spec)
		if sizeErr != nil {
			return nil, fmt.Errorf("resolve legacy sandbox %s RootFS logical size: %w", legacy.ID, sizeErr)
		}
		if sourceSize, ok := sourceSizes[legacy.ID]; ok && sourceSize != logicalSize {
			return nil, fmt.Errorf("legacy sandbox %s RootFS logical size changed during normalization", legacy.ID)
		}
		normalized.Sandboxes = append(normalized.Sandboxes, NormalizedSandbox{
			FilesystemID:             binding.FilesystemID,
			PinnedOCIRef:             pinnedRef,
			CompatibilityAdjustments: adjustments,
			Record: sandboxstore.SandboxRecord{
				ID: legacy.ID, TeamID: legacy.TeamID, UserID: legacy.UserID,
				TemplateID: legacy.TemplateID, TemplateName: legacy.TemplateName,
				TemplateNamespace: legacy.TemplateNamespace, ClusterID: targetClusterID,
				DesiredState: sandboxstore.SandboxDesiredStatePaused, Config: config, TemplateSpec: spec,
				RuntimeGeneration: legacy.RuntimeGeneration, LifecycleEpoch: legacy.LifecycleEpoch,
				OwnerKind: legacy.OwnerKind, ResourceMillicpu: resources.CPUMillicores,
				ResourceMemoryMiB:   (resources.MemoryBytes + (1 << 20) - 1) / (1 << 20),
				HotClaimCompletedAt: legacy.HotClaimCompletedAt, ClaimedAt: legacy.ClaimedAt,
				ExpiresAt: legacy.ExpiresAt, HardExpiresAt: legacy.HardExpiresAt,
				CreatedAt: legacy.CreatedAt, UpdatedAt: legacy.UpdatedAt,
			},
		})
	}

	for _, snapshot := range c.Snapshots {
		filesystem, ok := filesystems[snapshot.FilesystemID]
		if !ok || filesystem.TeamID != snapshot.TeamID {
			return nil, fmt.Errorf("legacy snapshot %s has an invalid filesystem", snapshot.ID)
		}
		chain, chainErr := chainFor(snapshot.HeadLayerID)
		if chainErr != nil {
			return nil, fmt.Errorf("legacy snapshot %s: %w", snapshot.ID, chainErr)
		}
		if err := validateFilesystemChain(filesystem, chain); err != nil {
			return nil, fmt.Errorf("legacy snapshot %s: %w", snapshot.ID, err)
		}
		for _, layer := range chain {
			if layer.TeamID != snapshot.TeamID {
				return nil, fmt.Errorf("legacy snapshot %s crosses team ownership", snapshot.ID)
			}
		}
		normalized.LayerChains[snapshot.HeadLayerID] = chain
	}
	for filesystemID, filesystem := range filesystems {
		candidateSources := make(map[string]struct{})
		if chain, chainErr := chainFor(filesystem.HeadLayerID); chainErr != nil {
			return nil, chainErr
		} else {
			normalized.LayerChains[filesystem.HeadLayerID] = chain
			for _, layer := range chain {
				candidateSources[layer.SourceSandboxID] = struct{}{}
			}
		}
		for sandboxID, binding := range bindings {
			if binding.FilesystemID == filesystemID {
				candidateSources[sandboxID] = struct{}{}
			}
		}
		for _, snapshot := range c.Snapshots {
			if snapshot.FilesystemID != filesystemID {
				continue
			}
			candidateSources[snapshot.SourceSandboxID] = struct{}{}
			chain := normalized.LayerChains[snapshot.HeadLayerID]
			for _, layer := range chain {
				candidateSources[layer.SourceSandboxID] = struct{}{}
			}
		}
		var logicalSize int64
		for sourceID := range candidateSources {
			size, ok := sourceSizes[sourceID]
			if !ok {
				return nil, fmt.Errorf("legacy filesystem %s source sandbox %s has no template geometry", filesystemID, sourceID)
			}
			if sourceTeams[sourceID] != filesystem.TeamID {
				return nil, fmt.Errorf("legacy filesystem %s source sandbox %s crosses team ownership", filesystemID, sourceID)
			}
			if logicalSize != 0 && logicalSize != size {
				return nil, fmt.Errorf("legacy filesystem %s has conflicting RootFS logical sizes", filesystemID)
			}
			logicalSize = size
		}
		if logicalSize == 0 {
			return nil, fmt.Errorf("legacy filesystem %s has no recoverable RootFS logical size", filesystemID)
		}
		normalized.FilesystemLogicalSizes[filesystemID] = logicalSize
	}
	if err := normalized.planMaterializedBuilds(normalizedFilesystems, c.Snapshots); err != nil {
		return nil, err
	}

	for layerID := range inferred {
		normalized.InferredLayers = append(normalized.InferredLayers, layerID)
	}
	sortStrings(normalized.InferredLayers)
	return normalized, nil
}

type layerMutationManifest struct {
	Version          int                          `json:"version"`
	TeamID           string                       `json:"team_id"`
	HeadLayerID      string                       `json:"head_layer_id"`
	SourceOCIDigest  string                       `json:"source_oci_digest"`
	LogicalSizeBytes int64                        `json:"logical_size_bytes"`
	Platform         ocispec.Platform             `json:"platform"`
	Layers           []layerMutationManifestEntry `json:"layers"`
}

type layerMutationManifestEntry struct {
	ID            string `json:"id"`
	ParentID      string `json:"parent_id,omitempty"`
	DiffDigest    string `json:"diff_digest"`
	DiffID        string `json:"diff_id"`
	DiffMediaType string `json:"diff_media_type"`
	DiffSize      int64  `json:"diff_size"`
	DiffObjectKey string `json:"diff_object_key"`
}

func (c *NormalizedCatalog) planMaterializedBuilds(filesystems []Filesystem, snapshots []Snapshot) error {
	if c == nil {
		return fmt.Errorf("normalized legacy catalog is required")
	}
	builds := make(map[string]MaterializedBuild)
	addBuild := func(filesystem Filesystem, headLayerID string) (string, error) {
		chain := c.LayerChains[headLayerID]
		if len(chain) == 0 {
			return "", fmt.Errorf("legacy filesystem %s build head %s has no normalized layer chain", filesystem.ID, headLayerID)
		}
		logicalSize := c.FilesystemLogicalSizes[filesystem.ID]
		if logicalSize <= 0 {
			return "", fmt.Errorf("legacy filesystem %s has no normalized logical size", filesystem.ID)
		}
		baseDigest := strings.TrimSpace(chain[0].BaseImageDigest)
		pinnedRef := c.PinnedImageRefs[baseDigest]
		if pinnedRef == "" {
			return "", fmt.Errorf("legacy filesystem %s has no pinned Base image", filesystem.ID)
		}
		platform := ocispec.Platform{
			OS: chain[0].PlatformOS, Architecture: chain[0].PlatformArchitecture, Variant: chain[0].PlatformVariant,
		}
		build := MaterializedBuild{
			TeamID: filesystem.TeamID, HeadLayerID: headLayerID,
			PinnedOCIRef: pinnedRef, SourceOCIDigest: baseDigest,
			LogicalSizeBytes: logicalSize, Platform: platform,
			Layers: append([]Layer(nil), chain...),
		}
		build, err := normalizeMaterializedBuildIdentity(build)
		if err != nil {
			return "", err
		}
		id := build.ID
		if existing, ok := builds[id]; ok {
			existingPayload, _ := json.Marshal(existing)
			buildPayload, _ := json.Marshal(build)
			if !bytes.Equal(existingPayload, buildPayload) {
				return "", fmt.Errorf("legacy materialized build identity collision")
			}
		} else {
			builds[id] = build
		}
		return id, nil
	}

	filesystemPlans := make(map[string]*NormalizedFilesystem, len(filesystems))
	for _, filesystem := range filesystems {
		headBuildID, err := addBuild(filesystem, filesystem.HeadLayerID)
		if err != nil {
			return err
		}
		plan := &NormalizedFilesystem{
			Record: filesystem, LogicalSizeBytes: c.FilesystemLogicalSizes[filesystem.ID],
			HeadBuildID: headBuildID, BuildIDByLayer: map[string]string{filesystem.HeadLayerID: headBuildID},
		}
		filesystemPlans[filesystem.ID] = plan
	}
	for _, snapshot := range snapshots {
		filesystem := filesystemPlans[snapshot.FilesystemID]
		if filesystem == nil {
			return fmt.Errorf("legacy snapshot %s has no normalized filesystem", snapshot.ID)
		}
		buildID, err := addBuild(filesystem.Record, snapshot.HeadLayerID)
		if err != nil {
			return fmt.Errorf("legacy snapshot %s: %w", snapshot.ID, err)
		}
		filesystem.BuildIDByLayer[snapshot.HeadLayerID] = buildID
		c.Snapshots = append(c.Snapshots, NormalizedSnapshot{Record: snapshot, BuildID: buildID})
	}
	for _, filesystem := range filesystems {
		c.Filesystems = append(c.Filesystems, *filesystemPlans[filesystem.ID])
	}
	for _, build := range builds {
		c.MaterializedBuilds = append(c.MaterializedBuilds, build)
	}
	slices.SortFunc(c.MaterializedBuilds, func(left, right MaterializedBuild) int {
		return strings.Compare(left.ID, right.ID)
	})
	return nil
}

func normalizeMaterializedBuildIdentity(build MaterializedBuild) (MaterializedBuild, error) {
	if strings.TrimSpace(build.TeamID) == "" || strings.TrimSpace(build.HeadLayerID) == "" ||
		strings.TrimSpace(build.SourceOCIDigest) == "" || build.LogicalSizeBytes <= 0 || len(build.Layers) == 0 {
		return MaterializedBuild{}, fmt.Errorf("legacy materialized build identity is incomplete")
	}
	manifest := layerMutationManifest{
		Version: 1, TeamID: build.TeamID, HeadLayerID: build.HeadLayerID,
		SourceOCIDigest: build.SourceOCIDigest, LogicalSizeBytes: build.LogicalSizeBytes,
		Platform: build.Platform, Layers: make([]layerMutationManifestEntry, 0, len(build.Layers)),
	}
	for _, layer := range build.Layers {
		manifest.Layers = append(manifest.Layers, layerMutationManifestEntry{
			ID: layer.ID, ParentID: layer.ParentID, DiffDigest: layer.DiffDigest,
			DiffID: layer.DiffID, DiffMediaType: layer.DiffMediaType,
			DiffSize: layer.DiffSize, DiffObjectKey: layer.DiffObjectKey,
		})
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return MaterializedBuild{}, fmt.Errorf("encode legacy layer mutation manifest: %w", err)
	}
	mutationDigest := digest.FromBytes(payload)
	build.ID = "legacy-ack-generation-v1-" + mutationDigest.Encoded()
	build.MutationDigest = mutationDigest.String()
	build.ObjectPrefix = "rootfs/legacy-ack-v1/" + digest.FromString(build.TeamID).Encoded() + "/" + mutationDigest.Encoded()
	return build, nil
}

func validateMaterializedBuildIdentity(build MaterializedBuild) error {
	expected, err := normalizeMaterializedBuildIdentity(build)
	if err != nil {
		return err
	}
	if build.ID != expected.ID || build.MutationDigest != expected.MutationDigest ||
		build.ObjectPrefix != expected.ObjectPrefix {
		return fmt.Errorf("legacy materialized build identity does not match its exact layer manifest")
	}
	return nil
}

func resolveSourceSandboxLogicalSizes(
	sources []SourceSandbox,
	live []Sandbox,
) (map[string]int64, map[string]string, error) {
	type sourceRecord struct {
		team string
		spec json.RawMessage
	}
	records := make(map[string]sourceRecord, len(sources)+len(live))
	add := func(id, team string, spec json.RawMessage) error {
		id, team = strings.TrimSpace(id), strings.TrimSpace(team)
		if id == "" || team == "" || len(spec) == 0 {
			return fmt.Errorf("legacy source sandbox has incomplete template geometry")
		}
		if existing, ok := records[id]; ok {
			if existing.team != team || !bytes.Equal(existing.spec, spec) {
				return fmt.Errorf("legacy source sandbox %s has conflicting template geometry", id)
			}
			return nil
		}
		records[id] = sourceRecord{team: team, spec: append(json.RawMessage(nil), spec...)}
		return nil
	}
	for _, source := range sources {
		if err := add(source.ID, source.TeamID, source.TemplateSpec); err != nil {
			return nil, nil, err
		}
	}
	for _, sandbox := range live {
		if err := add(sandbox.ID, sandbox.TeamID, sandbox.TemplateSpec); err != nil {
			return nil, nil, err
		}
	}
	sizes := make(map[string]int64, len(records))
	teams := make(map[string]string, len(records))
	for id, record := range records {
		var spec legacyTemplateSpec
		if err := decodeStrictJSON(record.spec, &spec); err != nil {
			return nil, nil, fmt.Errorf("decode legacy source sandbox %s template geometry: %w", id, err)
		}
		logicalSize, err := templatepkg.ResolveRootFSLogicalSize(sandboxspec.TemplateSpec{
			MainContainer: sandboxspec.ContainerSpec{Resources: spec.MainContainer.Resources},
		})
		if err != nil {
			return nil, nil, fmt.Errorf("resolve legacy source sandbox %s RootFS logical size: %w", id, err)
		}
		sizes[id], teams[id] = logicalSize, record.team
	}
	return sizes, teams, nil
}

func validateFilesystemGraph(filesystems map[string]Filesystem) error {
	for filesystemID := range filesystems {
		visiting := make(map[string]struct{})
		current := filesystemID
		for current != "" {
			if _, exists := visiting[current]; exists {
				return fmt.Errorf("legacy filesystem graph contains a cycle at %s", current)
			}
			visiting[current] = struct{}{}
			current = filesystems[current].SourceFilesystemID
		}
	}
	return nil
}

func validateFilesystemChain(filesystem Filesystem, chain []Layer) error {
	if len(chain) == 0 {
		return fmt.Errorf("legacy filesystem %s has an empty layer chain", filesystem.ID)
	}
	base := chain[0]
	if filesystem.BaseImageDigest != "" && strings.TrimSpace(filesystem.BaseImageDigest) != strings.TrimSpace(base.BaseImageDigest) {
		return fmt.Errorf("legacy filesystem %s base image digest does not match its layer chain", filesystem.ID)
	}
	for _, layer := range chain {
		if layer.TeamID != filesystem.TeamID {
			return fmt.Errorf("legacy filesystem %s crosses team ownership at layer %s", filesystem.ID, layer.ID)
		}
	}
	return nil
}

type legacyTemplateSpec struct {
	Description   string                            `json:"description,omitempty"`
	DisplayName   string                            `json:"displayName,omitempty"`
	Tags          []string                          `json:"tags,omitempty"`
	MainContainer legacyTemplateMainContainer       `json:"mainContainer"`
	VolumeMounts  []legacyVolumeMount               `json:"volumeMounts,omitempty"`
	Pod           *legacyPodOverride                `json:"pod,omitempty"`
	Network       *sandboxspec.SandboxNetworkPolicy `json:"network,omitempty"`
	Pool          legacyPoolStrategy                `json:"pool,omitempty"`
	EnvVars       map[string]string                 `json:"envVars,omitempty"`
	ClusterID     *string                           `json:"clusterId,omitempty"`
}

type legacyTemplateMainContainer struct {
	Image           string                    `json:"image"`
	ImagePullPolicy string                    `json:"imagePullPolicy,omitempty"`
	Env             []sandboxspec.EnvVar      `json:"env,omitempty"`
	Resources       sandboxspec.ResourceQuota `json:"resources"`
	SecurityContext *legacySecurityContext    `json:"securityContext,omitempty"`
}

type legacyVolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type legacyPoolStrategy struct {
	MinIdle int32 `json:"minIdle"`
	MaxIdle int32 `json:"maxIdle"`
}

type legacyPodOverride struct {
	NodeSelector       map[string]string     `json:"nodeSelector,omitempty"`
	Affinity           json.RawMessage       `json:"affinity,omitempty"`
	Tolerations        []json.RawMessage     `json:"tolerations,omitempty"`
	ServiceAccountName string                `json:"serviceAccountName,omitempty"`
	EmptyDirMounts     []legacyEmptyDirMount `json:"emptyDirMounts,omitempty"`
}

type legacyEmptyDirMount struct {
	MountPath string `json:"mountPath"`
	SizeLimit string `json:"sizeLimit,omitempty"`
}

type legacySecurityContext struct {
	Capabilities             *legacyCapabilities `json:"capabilities,omitempty"`
	Privileged               *bool               `json:"privileged,omitempty"`
	RunAsUser                *int64              `json:"runAsUser,omitempty"`
	RunAsGroup               *int64              `json:"runAsGroup,omitempty"`
	RunAsNonRoot             *bool               `json:"runAsNonRoot,omitempty"`
	ReadOnlyRootFilesystem   *bool               `json:"readOnlyRootFilesystem,omitempty"`
	AllowPrivilegeEscalation *bool               `json:"allowPrivilegeEscalation,omitempty"`
	SeccompProfile           json.RawMessage     `json:"seccompProfile,omitempty"`
	AppArmorProfile          json.RawMessage     `json:"appArmorProfile,omitempty"`
}

type legacyCapabilities struct {
	Add  []string `json:"add,omitempty"`
	Drop []string `json:"drop,omitempty"`
}

func decodeLegacyTemplateSpec(raw json.RawMessage) (sandboxspec.TemplateSpec, []string, error) {
	var legacy legacyTemplateSpec
	if err := decodeStrictJSON(raw, &legacy); err != nil {
		return sandboxspec.TemplateSpec{}, nil, err
	}
	if strings.TrimSpace(legacy.MainContainer.Image) == "" {
		return sandboxspec.TemplateSpec{}, nil, fmt.Errorf("main container image is required")
	}
	securityClass, securityAdjusted, err := mapLegacySecurityContext(legacy.MainContainer.SecurityContext)
	if err != nil {
		return sandboxspec.TemplateSpec{}, nil, err
	}
	ephemeralMounts, podAdjusted, err := mapLegacyPodOverride(legacy.Pod)
	if err != nil {
		return sandboxspec.TemplateSpec{}, nil, err
	}
	spec := sandboxspec.TemplateSpec{
		Description: legacy.Description,
		DisplayName: legacy.DisplayName,
		Tags:        append([]string(nil), legacy.Tags...),
		MainContainer: sandboxspec.ContainerSpec{
			Image:         legacy.MainContainer.Image,
			Env:           append([]sandboxspec.EnvVar(nil), legacy.MainContainer.Env...),
			Resources:     legacy.MainContainer.Resources,
			SecurityClass: securityClass,
		},
		EphemeralMounts: ephemeralMounts,
		Network:         legacy.Network,
		EnvVars:         cloneStrings(legacy.EnvVars),
	}
	if _, err := templatepkg.ResolveEphemeralMounts(spec); err != nil {
		return sandboxspec.TemplateSpec{}, nil, err
	}
	var adjustments []string
	if securityAdjusted {
		adjustments = append(adjustments, "mainContainer.securityContext mapped to mainContainer.securityClass")
	}
	if podAdjusted {
		adjustments = append(adjustments, "pod.emptyDirMounts mapped to ephemeralMounts")
	}
	if err := validateRetiredVolumeMounts(legacy.VolumeMounts); err != nil {
		return sandboxspec.TemplateSpec{}, nil, err
	}
	if len(legacy.VolumeMounts) != 0 {
		adjustments = append(adjustments, "retired volumeMounts metadata dropped")
	}
	if strings.TrimSpace(legacy.MainContainer.ImagePullPolicy) != "" {
		adjustments = append(adjustments, "imagePullPolicy dropped after base image digest pinning")
	}
	if legacy.Pool.MinIdle != 0 || legacy.Pool.MaxIdle != 0 {
		adjustments = append(adjustments, "per-template pool metadata dropped for the unified warm pool")
	}
	if legacy.ClusterID != nil {
		adjustments = append(adjustments, "template clusterId replaced by the target Nomad cluster")
	}
	return spec, adjustments, nil
}

func validateRetiredVolumeMounts(mounts []legacyVolumeMount) error {
	for _, mount := range mounts {
		if mount.Name != "workspace" || mount.MountPath != "/workspace" || mount.ReadOnly {
			return fmt.Errorf("unrecognized retired volumeMounts metadata cannot be migrated safely")
		}
	}
	return nil
}

func mapLegacySecurityContext(context *legacySecurityContext) (sandboxspec.SandboxSecurityClass, bool, error) {
	if context == nil {
		return sandboxspec.SandboxSecurityClassStandard, false, nil
	}
	if context.Capabilities != nil && (len(context.Capabilities.Add) != 0 || len(context.Capabilities.Drop) != 0) {
		return "", false, fmt.Errorf("custom Kubernetes capabilities cannot be migrated losslessly")
	}
	if context.RunAsUser != nil && *context.RunAsUser != 0 {
		return "", false, fmt.Errorf("non-root Kubernetes runAsUser cannot be migrated losslessly")
	}
	if context.RunAsGroup != nil && *context.RunAsGroup != 0 {
		return "", false, fmt.Errorf("non-root Kubernetes runAsGroup cannot be migrated losslessly")
	}
	if context.RunAsNonRoot != nil && *context.RunAsNonRoot {
		return "", false, fmt.Errorf("kubernetes runAsNonRoot cannot be migrated losslessly")
	}
	if context.ReadOnlyRootFilesystem != nil && *context.ReadOnlyRootFilesystem {
		return "", false, fmt.Errorf("read-only Kubernetes rootfs cannot be migrated losslessly")
	}
	if context.AllowPrivilegeEscalation != nil && !*context.AllowPrivilegeEscalation {
		return "", false, fmt.Errorf("disabled Kubernetes privilege escalation cannot be migrated losslessly")
	}
	if !emptyJSONValue(context.SeccompProfile) || !emptyJSONValue(context.AppArmorProfile) {
		return "", false, fmt.Errorf("custom Kubernetes security profiles cannot be migrated losslessly")
	}
	if context.Privileged != nil && *context.Privileged {
		return sandboxspec.SandboxSecurityClassPrivileged, true, nil
	}
	return sandboxspec.SandboxSecurityClassStandard, true, nil
}

func mapLegacyPodOverride(pod *legacyPodOverride) ([]sandboxspec.EphemeralMountSpec, bool, error) {
	if pod == nil {
		return nil, false, nil
	}
	if len(pod.NodeSelector) != 0 || !emptyJSONValue(pod.Affinity) || len(pod.Tolerations) != 0 ||
		strings.TrimSpace(pod.ServiceAccountName) != "" {
		return nil, false, fmt.Errorf("kubernetes pod scheduling or identity overrides cannot be migrated losslessly")
	}
	mounts := make([]sandboxspec.EphemeralMountSpec, 0, len(pod.EmptyDirMounts))
	for _, mount := range pod.EmptyDirMounts {
		mounts = append(mounts, sandboxspec.EphemeralMountSpec{
			MountPath: mount.MountPath, SizeLimit: mount.SizeLimit,
		})
	}
	return mounts, len(mounts) > 0, nil
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("JSON document must contain exactly one value")
	}
	return nil
}

func emptyJSONValue(raw json.RawMessage) bool {
	normalized := strings.TrimSpace(string(raw))
	return normalized == "" || normalized == "null" || normalized == "{}"
}

func cloneStrings(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func normalizePlatform(platform ocispec.Platform) (ocispec.Platform, error) {
	platform.OS = strings.TrimSpace(platform.OS)
	platform.Architecture = strings.TrimSpace(platform.Architecture)
	platform.Variant = strings.TrimSpace(platform.Variant)
	if platform.OS != "linux" || platform.Architecture == "" {
		return ocispec.Platform{}, fmt.Errorf("legacy migration requires an explicit canonical Linux platform")
	}
	return platform, nil
}

func pinImageReference(raw, rawDigest string) (string, error) {
	parsedDigest, err := digest.Parse(strings.TrimSpace(rawDigest))
	if err != nil || parsedDigest.Algorithm() != digest.SHA256 {
		return "", fmt.Errorf("base image digest must be canonical SHA-256")
	}
	named, err := distref.ParseNormalizedNamed(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse base image reference: %w", err)
	}
	if canonical, ok := named.(distref.Canonical); ok && canonical.Digest() != parsedDigest {
		return "", fmt.Errorf("base image reference digest does not match catalog digest")
	}
	pinned, err := distref.WithDigest(distref.TrimNamed(named), parsedDigest)
	if err != nil {
		return "", fmt.Errorf("pin base image reference: %w", err)
	}
	return pinned.String(), nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
