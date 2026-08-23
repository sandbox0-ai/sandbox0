package nomadclaim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	runtimeClassCatalogVersion = 3
	maxRuntimeClassCatalogSize = 1 << 20
	maxRuntimeClassCount       = 256
)

var (
	ErrRuntimeClassUnavailable = errors.New("runtime class is unavailable")
	ErrRuntimeClassAmbiguous   = errors.New("runtime class selection is ambiguous")
)

type runtimeClassCatalogFile struct {
	Version int                         `json:"version"`
	Classes []runtimeClassCatalogRecord `json:"classes"`
}

type runtimeClassCatalogRecord struct {
	Name             string                              `json:"name"`
	ClusterID        string                              `json:"cluster_id"`
	ArtifactPlatform sandboxstore.RootFSArtifactPlatform `json:"artifact_platform"`
	Compatibility    protocol.RuntimeCompatibility       `json:"compatibility"`
}

// RuntimeClass contains only immutable carrier compatibility. CPU, memory,
// and PIDs are claim-time resource leases and must never appear here.
type RuntimeClass struct {
	Name                string
	ClusterID           string
	ArtifactPlatform    sandboxstore.RootFSArtifactPlatform
	Compatibility       protocol.RuntimeCompatibility
	CompatibilityDigest string
}

// RuntimeClassCatalog is an immutable warm-carrier scheduler input.
type RuntimeClassCatalog struct {
	classes []RuntimeClass
}

// LoadRuntimeClassCatalog loads a strict bounded catalog from a mounted
// Secret. Version 2 fixed-resource catalogs are intentionally rejected.
func LoadRuntimeClassCatalog(path string) (*RuntimeClassCatalog, error) {
	rawPath := path
	path = strings.TrimSpace(path)
	if path != rawPath || path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime class catalog must be a canonical non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime class catalog: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("runtime class catalog must resolve to a regular file: %w", errdefs.ErrInvalidArgument)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open runtime class catalog: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxRuntimeClassCatalogSize+1))
	if err != nil {
		return nil, fmt.Errorf("read runtime class catalog: %w", err)
	}
	if len(payload) > maxRuntimeClassCatalogSize {
		return nil, fmt.Errorf("runtime class catalog exceeds %d bytes: %w", maxRuntimeClassCatalogSize, errdefs.ErrResourceExhausted)
	}
	var catalog runtimeClassCatalogFile
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return nil, fmt.Errorf("decode runtime class catalog: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("runtime class catalog must contain exactly one JSON value: %w", errdefs.ErrInvalidArgument)
	}
	if catalog.Version != runtimeClassCatalogVersion || len(catalog.Classes) == 0 || len(catalog.Classes) > maxRuntimeClassCount {
		return nil, fmt.Errorf("runtime class catalog version must be %d and contain 1..%d classes: %w",
			runtimeClassCatalogVersion, maxRuntimeClassCount, errdefs.ErrInvalidArgument)
	}
	classes := make([]RuntimeClass, 0, len(catalog.Classes))
	seenNames := make(map[string]struct{}, len(catalog.Classes))
	for index, record := range catalog.Classes {
		class, err := normalizeRuntimeClass(record)
		if err != nil {
			return nil, fmt.Errorf("runtime class %d: %w", index, err)
		}
		if _, exists := seenNames[class.Name]; exists {
			return nil, fmt.Errorf("runtime class name %q is duplicated: %w", class.Name, errdefs.ErrInvalidArgument)
		}
		seenNames[class.Name] = struct{}{}
		classes = append(classes, class)
	}
	return &RuntimeClassCatalog{classes: classes}, nil
}

func normalizeRuntimeClass(record runtimeClassCatalogRecord) (RuntimeClass, error) {
	for name, value := range map[string]string{"name": record.Name, "cluster_id": record.ClusterID} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return RuntimeClass{}, fmt.Errorf("%s must be non-empty, canonical, and at most 512 bytes: %w", name, errdefs.ErrInvalidArgument)
		}
	}
	if err := naming.ValidateClusterID(record.ClusterID); err != nil {
		return RuntimeClass{}, fmt.Errorf("cluster_id: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	compatibilityDigest, err := record.Compatibility.Digest()
	if err != nil {
		return RuntimeClass{}, fmt.Errorf("compatibility: %w", err)
	}
	if err := record.ArtifactPlatform.Validate(); err != nil {
		return RuntimeClass{}, fmt.Errorf("artifact_platform: %w", err)
	}
	if record.ArtifactPlatform.Architecture != record.Compatibility.Architecture {
		return RuntimeClass{}, fmt.Errorf("artifact_platform architecture must match runtime compatibility: %w", errdefs.ErrInvalidArgument)
	}
	return RuntimeClass{
		Name: record.Name, ClusterID: record.ClusterID,
		ArtifactPlatform: record.ArtifactPlatform,
		Compatibility:    record.Compatibility, CompatibilityDigest: compatibilityDigest,
	}, nil
}

// Resolve returns the only immutable class for a requested cluster. Until the
// public API has an explicit class selector, multiple candidates fail closed.
func (c *RuntimeClassCatalog) Resolve(clusterID string) (RuntimeClass, error) {
	if c == nil {
		return RuntimeClass{}, ErrRuntimeClassUnavailable
	}
	clusterID = strings.TrimSpace(clusterID)
	var selected RuntimeClass
	found := false
	for _, class := range c.classes {
		if clusterID != "" && class.ClusterID != clusterID {
			continue
		}
		if found {
			return RuntimeClass{}, fmt.Errorf("%w for cluster %q", ErrRuntimeClassAmbiguous, clusterID)
		}
		selected = class
		found = true
	}
	if !found {
		return RuntimeClass{}, fmt.Errorf("%w for cluster %q", ErrRuntimeClassUnavailable, clusterID)
	}
	return selected, nil
}

// ResolveLegacyMeteringResources is a bounded additive-migration fallback for
// sandbox rows created before numeric metering fields existed. New usage truth
// comes from the PostgreSQL resource lease, not this template reconstruction.
func (c *RuntimeClassCatalog) ResolveLegacyMeteringResources(
	record *sandboxstore.SandboxRecord,
	resourcePolicy templatepkg.ResourcePolicy,
) (int64, int64, error) {
	if record == nil || record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
		return 0, 0, fmt.Errorf("persisted Nomad sandbox record is required")
	}
	if _, err := c.Resolve(record.ClusterID); err != nil {
		return 0, 0, fmt.Errorf("resolve persisted runtime class: %w", err)
	}
	quota, err := effectiveResources(resourcePolicy, record.TemplateSpec, &record.Config)
	if err != nil {
		return 0, 0, err
	}
	millicpu := quota.CPU.MilliValue()
	memoryMiB := bytesToMiBRoundUp(quota.Memory.Value())
	if millicpu <= 0 || memoryMiB <= 0 || resource.NewMilliQuantity(millicpu, resource.DecimalSI).Cmp(quota.CPU) != 0 {
		return 0, 0, fmt.Errorf("persisted Nomad resources are not exact metering quantities")
	}
	return millicpu, memoryMiB, nil
}
