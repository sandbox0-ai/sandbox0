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
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	profileCatalogVersion = 1
	maxProfileCatalogSize = 1 << 20
	maxProfileCount       = 256
)

type profileCatalogFile struct {
	Version  int                    `json:"version"`
	Profiles []profileCatalogRecord `json:"profiles"`
}

type profileCatalogRecord struct {
	Name           string                        `json:"name"`
	ClusterID      string                        `json:"cluster_id"`
	TemplateCPU    string                        `json:"template_cpu"`
	TemplateMemory string                        `json:"template_memory"`
	Compatibility  protocol.RuntimeCompatibility `json:"compatibility"`
}

// Profile binds one public resource shape to an exact registered Nomad slot
// compatibility digest.
type Profile struct {
	Name                string
	ClusterID           string
	TemplateCPU         resource.Quantity
	TemplateMemory      resource.Quantity
	Compatibility       protocol.RuntimeCompatibility
	CompatibilityDigest string
}

// ProfileCatalog is an immutable resource-shape scheduler input.
type ProfileCatalog struct {
	profiles []Profile
}

// LoadProfileCatalog loads a strict bounded catalog from a mounted Secret.
func LoadProfileCatalog(path string) (*ProfileCatalog, error) {
	rawPath := path
	path = strings.TrimSpace(path)
	if path != rawPath || path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime profile catalog must be a canonical non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime profile catalog: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("runtime profile catalog must resolve to a regular file: %w", errdefs.ErrInvalidArgument)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open runtime profile catalog: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxProfileCatalogSize+1))
	if err != nil {
		return nil, fmt.Errorf("read runtime profile catalog: %w", err)
	}
	if len(payload) > maxProfileCatalogSize {
		return nil, fmt.Errorf("runtime profile catalog exceeds %d bytes: %w", maxProfileCatalogSize, errdefs.ErrResourceExhausted)
	}
	var catalog profileCatalogFile
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return nil, fmt.Errorf("decode runtime profile catalog: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("runtime profile catalog must contain exactly one JSON value: %w", errdefs.ErrInvalidArgument)
	}
	if catalog.Version != profileCatalogVersion || len(catalog.Profiles) == 0 || len(catalog.Profiles) > maxProfileCount {
		return nil, fmt.Errorf("runtime profile catalog version must be %d and contain 1..%d profiles: %w",
			profileCatalogVersion, maxProfileCount, errdefs.ErrInvalidArgument)
	}
	profiles := make([]Profile, 0, len(catalog.Profiles))
	seenNames := make(map[string]struct{}, len(catalog.Profiles))
	for index, record := range catalog.Profiles {
		profile, err := normalizeProfile(record)
		if err != nil {
			return nil, fmt.Errorf("runtime profile %d: %w", index, err)
		}
		if _, exists := seenNames[profile.Name]; exists {
			return nil, fmt.Errorf("runtime profile name %q is duplicated: %w", profile.Name, errdefs.ErrInvalidArgument)
		}
		for _, existing := range profiles {
			if existing.TemplateCPU.Cmp(profile.TemplateCPU) == 0 && existing.TemplateMemory.Cmp(profile.TemplateMemory) == 0 {
				return nil, fmt.Errorf("runtime profile resource shape %s/%s is ambiguous: %w",
					profile.TemplateCPU.String(), profile.TemplateMemory.String(), errdefs.ErrInvalidArgument)
			}
		}
		seenNames[profile.Name] = struct{}{}
		profiles = append(profiles, profile)
	}
	return &ProfileCatalog{profiles: profiles}, nil
}

func normalizeProfile(record profileCatalogRecord) (Profile, error) {
	for name, value := range map[string]string{
		"name": record.Name, "cluster_id": record.ClusterID,
		"template_cpu": record.TemplateCPU, "template_memory": record.TemplateMemory,
	} {
		if value == "" || value != strings.TrimSpace(value) || len(value) > 512 {
			return Profile{}, fmt.Errorf("%s must be non-empty, canonical, and at most 512 bytes: %w", name, errdefs.ErrInvalidArgument)
		}
	}
	if err := naming.ValidateClusterID(record.ClusterID); err != nil {
		return Profile{}, fmt.Errorf("cluster_id: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	cpu, err := resource.ParseQuantity(record.TemplateCPU)
	if err != nil || cpu.Sign() <= 0 || cpu.String() != record.TemplateCPU {
		return Profile{}, fmt.Errorf("template_cpu must be a positive canonical Kubernetes quantity: %w", errdefs.ErrInvalidArgument)
	}
	memory, err := resource.ParseQuantity(record.TemplateMemory)
	if err != nil || memory.Sign() <= 0 || memory.String() != record.TemplateMemory {
		return Profile{}, fmt.Errorf("template_memory must be a positive canonical Kubernetes quantity: %w", errdefs.ErrInvalidArgument)
	}
	compatibilityDigest, err := record.Compatibility.Digest()
	if err != nil {
		return Profile{}, fmt.Errorf("compatibility: %w", err)
	}
	return Profile{
		Name: record.Name, ClusterID: record.ClusterID,
		TemplateCPU: cpu, TemplateMemory: memory,
		Compatibility: record.Compatibility, CompatibilityDigest: compatibilityDigest,
	}, nil
}

// Resolve returns the only exact resource profile or false when no warm pool
// is compatible.
func (c *ProfileCatalog) Resolve(cpu, memory resource.Quantity) (Profile, bool) {
	if c == nil {
		return Profile{}, false
	}
	for _, profile := range c.profiles {
		if profile.TemplateCPU.Cmp(cpu) == 0 && profile.TemplateMemory.Cmp(memory) == 0 {
			return profile, true
		}
	}
	return Profile{}, false
}
