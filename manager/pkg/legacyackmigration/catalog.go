// Package legacyackmigration reads and validates the final Kubernetes-era
// Sandbox0 persistence graph before one-time import into the Nomad block-COW
// schema.
package legacyackmigration

import (
	"encoding/json"
	"fmt"
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

type Catalog struct {
	ManagerSchemaVersion int64
	ActiveLifecycleTxns  int64
	Sandboxes            []Sandbox
	Layers               []Layer
	Filesystems          []Filesystem
	Bindings             []Binding
	Snapshots            []Snapshot
}

type NormalizeOptions struct {
	Platform        ocispec.Platform
	ResourcePolicy  templatepkg.ResourcePolicy
	TargetClusterID string
}

type NormalizedSandbox struct {
	Record       sandboxstore.SandboxRecord
	FilesystemID string
	PinnedOCIRef string
}

type NormalizedCatalog struct {
	Sandboxes       []NormalizedSandbox
	LayerChains     map[string][]Layer
	PinnedImageRefs map[string]string
	InferredLayers  []string
}

// Normalize validates the frozen source graph and produces runtime-neutral
// sandbox records plus deterministic parent-to-child layer chains.
func (c Catalog) Normalize(options NormalizeOptions) (*NormalizedCatalog, error) {
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
	for _, filesystem := range c.Filesystems {
		if strings.TrimSpace(filesystem.ID) == "" || strings.TrimSpace(filesystem.TeamID) == "" {
			return nil, fmt.Errorf("legacy filesystem has an empty ID or team")
		}
		if _, exists := filesystems[filesystem.ID]; exists {
			return nil, fmt.Errorf("duplicate legacy filesystem %s", filesystem.ID)
		}
		filesystems[filesystem.ID] = filesystem
	}
	for _, filesystem := range c.Filesystems {
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

	normalized := &NormalizedCatalog{LayerChains: make(map[string][]Layer), PinnedImageRefs: pinnedRefs}
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
			return nil, fmt.Errorf("legacy sandbox %s is %s; every live sandbox must be paused", legacy.ID, legacy.DesiredState)
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
		spec, err := decodeLegacyTemplateSpec(legacy.TemplateSpec)
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
		normalized.Sandboxes = append(normalized.Sandboxes, NormalizedSandbox{
			FilesystemID: binding.FilesystemID,
			PinnedOCIRef: pinnedRef,
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

	for layerID := range inferred {
		normalized.InferredLayers = append(normalized.InferredLayers, layerID)
	}
	sortStrings(normalized.InferredLayers)
	return normalized, nil
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
	Pod           json.RawMessage                   `json:"pod,omitempty"`
	Network       *sandboxspec.SandboxNetworkPolicy `json:"network,omitempty"`
	Pool          json.RawMessage                   `json:"pool,omitempty"`
	EnvVars       map[string]string                 `json:"envVars,omitempty"`
	ClusterID     *string                           `json:"clusterId,omitempty"`
}

type legacyTemplateMainContainer struct {
	Image           string                    `json:"image"`
	ImagePullPolicy string                    `json:"imagePullPolicy,omitempty"`
	Env             []sandboxspec.EnvVar      `json:"env,omitempty"`
	Resources       sandboxspec.ResourceQuota `json:"resources"`
	SecurityContext json.RawMessage           `json:"securityContext,omitempty"`
}

func decodeLegacyTemplateSpec(raw json.RawMessage) (sandboxspec.TemplateSpec, error) {
	var legacy legacyTemplateSpec
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return sandboxspec.TemplateSpec{}, err
	}
	if !emptyJSONValue(legacy.Pod) {
		return sandboxspec.TemplateSpec{}, fmt.Errorf("Kubernetes pod overrides cannot be migrated losslessly")
	}
	if !emptyJSONValue(legacy.MainContainer.SecurityContext) {
		return sandboxspec.TemplateSpec{}, fmt.Errorf("Kubernetes securityContext cannot be migrated losslessly")
	}
	if strings.TrimSpace(legacy.MainContainer.Image) == "" {
		return sandboxspec.TemplateSpec{}, fmt.Errorf("main container image is required")
	}
	return sandboxspec.TemplateSpec{
		Description: legacy.Description,
		DisplayName: legacy.DisplayName,
		Tags:        append([]string(nil), legacy.Tags...),
		MainContainer: sandboxspec.ContainerSpec{
			Image:     legacy.MainContainer.Image,
			Env:       append([]sandboxspec.EnvVar(nil), legacy.MainContainer.Env...),
			Resources: legacy.MainContainer.Resources,
		},
		Network: legacy.Network,
		EnvVars: cloneStrings(legacy.EnvVars),
	}, nil
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
