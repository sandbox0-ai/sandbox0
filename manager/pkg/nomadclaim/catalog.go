package nomadclaim

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
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

// ArtifactPlatforms returns the unique immutable OCI platforms required by
// every configured warm-carrier class.
func (c *RuntimeClassCatalog) ArtifactPlatforms() []sandboxstore.RootFSArtifactPlatform {
	if c == nil {
		return nil
	}
	platforms := make([]sandboxstore.RootFSArtifactPlatform, 0, len(c.classes))
	seen := make(map[sandboxstore.RootFSArtifactPlatform]struct{}, len(c.classes))
	for _, class := range c.classes {
		if _, ok := seen[class.ArtifactPlatform]; ok {
			continue
		}
		seen[class.ArtifactPlatform] = struct{}{}
		platforms = append(platforms, class.ArtifactPlatform)
	}
	sort.Slice(platforms, func(i, j int) bool {
		if platforms[i].OS != platforms[j].OS {
			return platforms[i].OS < platforms[j].OS
		}
		if platforms[i].Architecture != platforms[j].Architecture {
			return platforms[i].Architecture < platforms[j].Architecture
		}
		return platforms[i].Variant < platforms[j].Variant
	})
	return platforms
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

// Resolve returns the only immutable class matching a cluster and canonical
// template security class. Other compatibility dimensions remain fail-closed
// until they gain explicit selectors.
func (c *RuntimeClassCatalog) Resolve(clusterID, requestedSecurityClass string) (RuntimeClass, error) {
	if c == nil {
		return RuntimeClass{}, ErrRuntimeClassUnavailable
	}
	clusterID = strings.TrimSpace(clusterID)
	securityClass, ok := sandboxspec.EffectiveSandboxSecurityClass(sandboxspec.SandboxSecurityClass(requestedSecurityClass))
	if !ok || string(securityClass) != requestedSecurityClass {
		return RuntimeClass{}, fmt.Errorf("%w for invalid security class %q", ErrRuntimeClassUnavailable, requestedSecurityClass)
	}
	var selected RuntimeClass
	found := false
	for _, class := range c.classes {
		if clusterID != "" && class.ClusterID != clusterID {
			continue
		}
		if class.Compatibility.SecurityClass != requestedSecurityClass {
			continue
		}
		if found {
			return RuntimeClass{}, fmt.Errorf("%w for cluster %q and security class %q",
				ErrRuntimeClassAmbiguous, clusterID, requestedSecurityClass)
		}
		selected = class
		found = true
	}
	if !found {
		return RuntimeClass{}, fmt.Errorf("%w for cluster %q and security class %q",
			ErrRuntimeClassUnavailable, clusterID, requestedSecurityClass)
	}
	return selected, nil
}
