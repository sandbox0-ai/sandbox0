package runs

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

	DefaultPublicRunRootDomain = "sandbox0.run"
)

var (
	ErrNotFound = errors.New("run not found")

	slugPartPattern = regexp.MustCompile(`[^a-z0-9-]+`)
)

// Run is the stable production identity and hostname for immutable revisions.
type Run struct {
	ID               string         `json:"id"`
	TeamID           string         `json:"team_id"`
	CreatedBy        string         `json:"created_by,omitempty"`
	Name             string         `json:"name"`
	Slug             string         `json:"slug"`
	DomainLabel      string         `json:"domain_label"`
	URL              string         `json:"url,omitempty"`
	ActiveRevisionID string         `json:"active_revision_id,omitempty"`
	Enabled          bool           `json:"enabled"`
	Scale            RunScalePolicy `json:"scale"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

// RunScalePolicy controls scale-to-zero runtime behavior. There is no
// minimum warm count; every run can return to zero instances.
type RunScalePolicy struct {
	MaxInstances       int `json:"max_instances,omitempty"`
	TargetConcurrency  int `json:"target_concurrency,omitempty"`
	IdleTimeoutSeconds int `json:"idle_timeout_seconds,omitempty"`
}

// RunRevision is an immutable deployable version of a run.
type RunRevision struct {
	ID               string          `json:"id"`
	RunID            string          `json:"run_id"`
	TeamID           string          `json:"team_id"`
	Number           int             `json:"number"`
	Source           RunSource       `json:"source"`
	Spec             RunRevisionSpec `json:"spec"`
	Status           string          `json:"status"`
	RuntimeSandboxID string          `json:"runtime_sandbox_id,omitempty"`
	RuntimeClusterID string          `json:"runtime_cluster_id,omitempty"`
	RuntimeContextID string          `json:"runtime_context_id,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	ActivatedAt      *time.Time      `json:"activated_at,omitempty"`
}

type RunSource struct {
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

// RunRevisionSpec is the canonical runtime contract consumed by run
// execution. Both sandbox-service and snapshot deploys compile to this shape.
type RunRevisionSpec struct {
	Template string                `json:"template"`
	Service  mgr.SandboxAppService `json:"service"`
	Mounts   []RunRevisionMount    `json:"mounts,omitempty"`
	EnvVars  map[string]string     `json:"env_vars,omitempty"`
}

type RunRevisionMount struct {
	SnapshotID string `json:"snapshot_id"`
	MountPath  string `json:"mount_path"`
	ReadOnly   bool   `json:"read_only,omitempty"`
}

type RunDeployRequest struct {
	Name     string           `json:"name,omitempty"`
	Slug     string           `json:"slug,omitempty"`
	Scale    RunScalePolicy   `json:"scale,omitempty"`
	Source   RunSource        `json:"source,omitempty"`
	Spec     *RunRevisionSpec `json:"spec,omitempty"`
	Activate *bool            `json:"activate,omitempty"`
}

type RunDeployResult struct {
	Run      Run         `json:"run"`
	Revision RunRevision `json:"revision"`
}

func DefaultScalePolicy() RunScalePolicy {
	return RunScalePolicy{
		MaxInstances:       1,
		TargetConcurrency:  1,
		IdleTimeoutSeconds: 300,
	}
}

func NormalizeScalePolicy(policy RunScalePolicy) RunScalePolicy {
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

func PublicURL(domainLabel, regionID, runRootDomain string) string {
	runRootDomain = strings.TrimSpace(runRootDomain)
	if runRootDomain == "" {
		runRootDomain = DefaultPublicRunRootDomain
	}
	regionID = strings.TrimSpace(regionID)
	if domainLabel == "" || regionID == "" {
		return ""
	}
	return "https://" + domainLabel + "." + regionID + "." + runRootDomain
}

func NormalizeRevisionSpec(spec RunRevisionSpec) (RunRevisionSpec, error) {
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
		mounts := make([]RunRevisionMount, 0, len(spec.Mounts))
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
