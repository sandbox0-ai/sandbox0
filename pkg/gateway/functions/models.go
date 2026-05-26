package functions

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
)

const (
	RevisionSourceSandboxService = "sandbox_service"
	RevisionSourceSnapshot       = "snapshot"

	RevisionStatusCreated = "created"
	RevisionStatusActive  = "active"
	RevisionStatusFailed  = "failed"
)

var (
	ErrNotFound = errors.New("function not found")

	slugPartPattern = regexp.MustCompile(`[^a-z0-9-]+`)
)

// Function is the stable production identity and hostname for immutable revisions.
type Function struct {
	ID               string              `json:"id"`
	TeamID           string              `json:"team_id"`
	CreatedBy        string              `json:"created_by,omitempty"`
	Name             string              `json:"name"`
	Slug             string              `json:"slug"`
	DomainLabel      string              `json:"domain_label"`
	URL              string              `json:"url,omitempty"`
	ActiveRevisionID string              `json:"active_revision_id,omitempty"`
	Enabled          bool                `json:"enabled"`
	Scale            FunctionScalePolicy `json:"scale"`
	CreatedAt        time.Time           `json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
}

// FunctionScalePolicy controls scale-to-zero runtime behavior. There is no
// minimum warm count; every function can return to zero instances.
type FunctionScalePolicy struct {
	MaxInstances          int `json:"max_instances,omitempty"`
	TargetConcurrency     int `json:"target_concurrency,omitempty"`
	IdleTimeoutSeconds    int `json:"idle_timeout_seconds,omitempty"`
	StartupTimeoutSeconds int `json:"startup_timeout_seconds,omitempty"`
}

// FunctionRevision is an immutable deployable version of a function.
type FunctionRevision struct {
	ID               string               `json:"id"`
	FunctionID       string               `json:"function_id"`
	TeamID           string               `json:"team_id"`
	Number           int                  `json:"number"`
	Source           FunctionSource       `json:"source"`
	Spec             FunctionRevisionSpec `json:"spec"`
	Status           string               `json:"status"`
	RuntimeSandboxID string               `json:"runtime_sandbox_id,omitempty"`
	RuntimeClusterID string               `json:"runtime_cluster_id,omitempty"`
	RuntimeContextID string               `json:"runtime_context_id,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	ActivatedAt      *time.Time           `json:"activated_at,omitempty"`
}

type FunctionSource struct {
	Type           string                  `json:"type"`
	SandboxService *SandboxServiceSource   `json:"sandbox_service,omitempty"`
	Snapshot       *SnapshotRevisionSource `json:"snapshot,omitempty"`
}

type SandboxServiceSource struct {
	SandboxID string `json:"sandbox_id"`
	ServiceID string `json:"service_id"`
}

type SnapshotRevisionSource struct {
	SnapshotIDs []string `json:"snapshot_ids,omitempty"`
}

// FunctionRevisionSpec is the canonical runtime contract consumed by function
// execution. Both sandbox-service and snapshot deploys compile to this shape.
type FunctionRevisionSpec struct {
	Template string                  `json:"template"`
	Service  mgr.SandboxAppService   `json:"service"`
	Mounts   []FunctionRevisionMount `json:"mounts,omitempty"`
	EnvVars  map[string]string       `json:"env_vars,omitempty"`
}

type FunctionRevisionMount struct {
	SnapshotID string `json:"snapshot_id"`
	MountPath  string `json:"mount_path"`
	ReadOnly   bool   `json:"read_only,omitempty"`
}

type FunctionDeployRequest struct {
	Name     string                `json:"name,omitempty"`
	Slug     string                `json:"slug,omitempty"`
	Scale    FunctionScalePolicy   `json:"scale,omitempty"`
	Source   FunctionSource        `json:"source,omitempty"`
	Spec     *FunctionRevisionSpec `json:"spec,omitempty"`
	Activate *bool                 `json:"activate,omitempty"`
}

type FunctionDeployResult struct {
	Function Function         `json:"function"`
	Revision FunctionRevision `json:"revision"`
}

func DefaultScalePolicy() FunctionScalePolicy {
	return FunctionScalePolicy{
		MaxInstances:          1,
		TargetConcurrency:     1,
		IdleTimeoutSeconds:    300,
		StartupTimeoutSeconds: 90,
	}
}

func NormalizeScalePolicy(policy FunctionScalePolicy) FunctionScalePolicy {
	defaults := DefaultScalePolicy()
	if policy.MaxInstances <= 0 {
		policy.MaxInstances = defaults.MaxInstances
	}
	if policy.TargetConcurrency <= 0 {
		policy.TargetConcurrency = defaults.TargetConcurrency
	}
	if policy.IdleTimeoutSeconds <= 0 {
		policy.IdleTimeoutSeconds = defaults.IdleTimeoutSeconds
	}
	if policy.StartupTimeoutSeconds <= 0 {
		policy.StartupTimeoutSeconds = defaults.StartupTimeoutSeconds
	}
	return policy
}

func NormalizeSlug(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	value = slugPartPattern.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-")
	for strings.Contains(value, "--") {
		value = strings.ReplaceAll(value, "--", "-")
	}
	if value == "" {
		return "", fmt.Errorf("slug is required")
	}
	if len(value) > 48 {
		value = strings.Trim(value[:48], "-")
	}
	if value == "" {
		return "", fmt.Errorf("slug is invalid")
	}
	return value, nil
}

func NewDomainLabel(slug string) (string, error) {
	suffix, err := randomHex(4)
	if err != nil {
		return "", err
	}
	label := slug + "-" + suffix
	if len(label) <= 63 {
		return label, nil
	}
	prefixLen := 63 - len(suffix) - 1
	if prefixLen < 1 {
		return "", fmt.Errorf("slug is too long")
	}
	return strings.Trim(slug[:prefixLen], "-") + "-" + suffix, nil
}

func PublicURL(domainLabel, regionID, rootDomain string) string {
	rootDomain = strings.TrimSpace(rootDomain)
	if rootDomain == "" {
		rootDomain = "sandbox0.app"
	}
	regionID = strings.TrimSpace(regionID)
	if domainLabel == "" || regionID == "" {
		return ""
	}
	return "https://" + domainLabel + ".fn." + regionID + "." + rootDomain
}

func NormalizeRevisionSpec(spec FunctionRevisionSpec) (FunctionRevisionSpec, error) {
	spec.Template = strings.TrimSpace(spec.Template)
	if spec.Template == "" {
		return spec, fmt.Errorf("template is required")
	}
	spec.Service.Ingress.Public = true
	services, err := mgr.NormalizeSandboxAppServices([]mgr.SandboxAppService{spec.Service})
	if err != nil {
		return spec, err
	}
	service := services[0]
	for i := range service.Ingress.Routes {
		service.Ingress.Routes[i].Resume = true
	}
	blockers := mgr.SandboxAppServicePublishBlockers(service)
	if len(blockers) > 0 {
		return spec, fmt.Errorf("service is not publishable: %s", strings.Join(blockers, ", "))
	}
	spec.Service = service

	if len(spec.Mounts) > 0 {
		mounts := make([]FunctionRevisionMount, 0, len(spec.Mounts))
		seenPaths := make(map[string]struct{}, len(spec.Mounts))
		for i := range spec.Mounts {
			mount := spec.Mounts[i]
			mount.SnapshotID = strings.TrimSpace(mount.SnapshotID)
			if mount.SnapshotID == "" {
				return spec, fmt.Errorf("mounts[%d].snapshot_id is required", i)
			}
			mountPath := filepath.Clean(strings.TrimSpace(mount.MountPath))
			if mountPath == "." || mountPath == string(filepath.Separator) || !filepath.IsAbs(mountPath) || strings.Contains(mountPath, "..") {
				return spec, fmt.Errorf("mounts[%d].mount_path is invalid", i)
			}
			if _, ok := seenPaths[mountPath]; ok {
				return spec, fmt.Errorf("duplicate mount_path %q", mountPath)
			}
			mount.MountPath = mountPath
			mount.ReadOnly = true
			seenPaths[mountPath] = struct{}{}
			mounts = append(mounts, mount)
		}
		spec.Mounts = mounts
	}
	return spec, nil
}

func randomHex(bytesLen int) (string, error) {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
