package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

var (
	ErrInvalidClaimRequest                        = errors.New("invalid claim request")
	ErrClaimConflict                              = errors.New("claim conflict")
	ErrDataPlaneNotReady                          = errors.New("data plane not ready")
	ErrQuotaExceeded                              = errors.New("quota exceeded")
	ErrTemplateNotFound                           = errors.New("template not found")
	ErrInvalidNetworkPolicy                       = errors.New("invalid network policy")
	ErrSandboxCheckpointRequiresCtld              = errors.New("sandbox checkpoint requires ctld")
	ErrSandboxRuntimeUpdateUnavailable            = errors.New("sandbox runtime update is unavailable")
	ErrSandboxLifecycleUnavailable                = errors.New("sandbox lifecycle operation is unavailable")
	ErrSandboxRootFSStoreUnavailable              = errors.New("sandbox rootfs store is unavailable")
	ErrSandboxRootFSRequiresPausedSandbox         = errors.New("sandbox rootfs operation requires a paused sandbox")
	ErrSandboxRootFSSourceRequiresRunningOrPaused = errors.New("sandbox rootfs source operation requires a running or paused sandbox")
	ErrRootFSSnapshotExpired                      = errors.New("rootfs snapshot expires_at must be in the future")
	ErrInvalidRootFSRebaseRequest                 = errors.New("invalid rootfs rebase request")
)

// ClaimRequest is the signed runtime-neutral sandbox creation request.
type ClaimRequest struct {
	TeamID            string                      `json:"-"`
	UserID            string                      `json:"-"`
	Template          string                      `json:"template"`
	SnapshotID        string                      `json:"snapshot_id,omitempty"`
	Config            *sandboxstore.SandboxConfig `json:"config,omitempty"`
	Metadata          *ClaimMetadata              `json:"-"`
	SandboxID         string                      `json:"-"`
	RuntimeGeneration int64                       `json:"-"`
	HardExpiresAt     time.Time                   `json:"-"`
	StartedAt         time.Time                   `json:"-"`
	OperationID       string                      `json:"-"`
}

type ClaimMetadata struct {
	OwnerKind string
}

type SandboxUpdateConfig struct {
	EnvVars    map[string]string                 `json:"env_vars,omitempty"`
	Resources  *managerapi.SandboxResourceConfig `json:"resources,omitempty"`
	TTL        *int32                            `json:"ttl,omitempty"`
	HardTTL    *int32                            `json:"hard_ttl,omitempty"`
	Network    *v1alpha1.SandboxNetworkPolicy    `json:"network,omitempty"`
	AutoResume *bool                             `json:"auto_resume,omitempty"`
	Services   []managerapi.SandboxAppService    `json:"services,omitempty"`
}

type ClaimResponse struct {
	SandboxID             string        `json:"sandbox_id"`
	Status                string        `json:"status"`
	ProcdAddress          string        `json:"procd_address"`
	RuntimeID             string        `json:"runtime_id"`
	Template              string        `json:"template"`
	ClusterId             *string       `json:"cluster_id,omitempty"`
	CommandReadyDuration  time.Duration `json:"-"`
	CommandReadyWithinSLO bool          `json:"-"`
}

type SandboxClaimer interface {
	ClaimSandbox(context.Context, *ClaimRequest) (*ClaimResponse, error)
}

type SandboxTerminator interface {
	TerminateSandbox(context.Context, string) error
}

type SandboxPauser interface {
	PauseSandboxAndWait(context.Context, string) (*PauseSandboxResponse, error)
}

type SandboxResumer interface {
	ResumeSandboxAndWait(context.Context, string) (*managerapi.ResumeSandboxResponse, error)
}

type SandboxForker interface {
	ForkSandbox(context.Context, string, string, string, *ForkSandboxRequest) (*ForkSandboxResponse, error)
}

type SandboxForkReconciler interface {
	CompleteSandboxFork(context.Context, string) error
}

type SandboxRootFSSnapshotReconciler interface {
	CompleteSandboxRootFSSnapshot(context.Context, string) error
}

type SandboxRootFSRebaser interface {
	RebaseSandboxRootFS(context.Context, string, string, *RebaseSandboxRootFSRequest) (*RebaseSandboxRootFSResponse, error)
}

type SandboxRootFSRebaseReconciler interface {
	CompleteSandboxRootFSRebase(context.Context, string) error
}

type SandboxAutoPauser interface {
	PauseSandboxByID(context.Context, string) error
}

type SandboxPauseReconciler interface {
	CompletePausingSandboxRuntime(context.Context, string) error
	ResumePausedSandboxRuntime(context.Context, string) (*managerapi.Sandbox, error)
}

type SandboxPauseEnqueuer interface {
	EnqueueSandboxPause(string)
}

// SandboxRuntime is the sole production sandbox lifecycle implementation.
type SandboxRuntime interface {
	SandboxClaimer
	SandboxTerminator
	SandboxPauser
	SandboxResumer
	SandboxForker
	SandboxAutoPauser
	SandboxPauseReconciler
	SetPauseEnqueuer(SandboxPauseEnqueuer)
}

// TokenGenerator generates manager-to-procd internal tokens.
type TokenGenerator interface {
	GenerateToken(teamID, userID, sandboxID string) (string, error)
}

func CloneSandboxConfig(cfg *sandboxstore.SandboxConfig) *sandboxstore.SandboxConfig {
	if cfg == nil {
		return nil
	}
	cloned := *cfg
	cloned.EnvVars = cloneEnvVars(cfg.EnvVars)
	cloned.Resources = cloneSandboxResourceConfig(cfg.Resources)
	cloned.TTL = cloneInt32Ptr(cfg.TTL)
	cloned.HardTTL = cloneInt32Ptr(cfg.HardTTL)
	cloned.AutoResume = cloneBoolPtr(cfg.AutoResume)
	cloned.Services = cloneSandboxAppServices(cfg.Services)
	if cloned.Network != nil {
		cloned.Network = sanitizedNetworkPolicyForPersistence(cloned.Network)
	}
	return &cloned
}

func cloneSandboxAppServices(services []managerapi.SandboxAppService) []managerapi.SandboxAppService {
	if services == nil {
		return nil
	}
	cloned := make([]managerapi.SandboxAppService, len(services))
	for i := range services {
		cloned[i] = services[i]
		cloned[i].Ingress.Routes = cloneSandboxAppServiceRoutes(services[i].Ingress.Routes)
		if services[i].Runtime != nil {
			runtime := *services[i].Runtime
			runtime.Command = append([]string(nil), services[i].Runtime.Command...)
			runtime.EnvVars = cloneEnvVars(services[i].Runtime.EnvVars)
			if services[i].Runtime.Function != nil {
				function := *services[i].Runtime.Function
				runtime.Function = &function
			}
			cloned[i].Runtime = &runtime
		}
		if services[i].HealthCheck != nil {
			health := *services[i].HealthCheck
			cloned[i].HealthCheck = &health
		}
	}
	return cloned
}

func cloneSandboxAppServiceRoutes(routes []managerapi.SandboxAppServiceRoute) []managerapi.SandboxAppServiceRoute {
	if routes == nil {
		return nil
	}
	cloned := make([]managerapi.SandboxAppServiceRoute, len(routes))
	for i := range routes {
		cloned[i] = routes[i]
		cloned[i].Methods = append([]string(nil), routes[i].Methods...)
		if routes[i].RewritePrefix != nil {
			value := *routes[i].RewritePrefix
			cloned[i].RewritePrefix = &value
		}
		if routes[i].Auth != nil {
			value := *routes[i].Auth
			cloned[i].Auth = &value
		}
		if routes[i].CORS != nil {
			value := *routes[i].CORS
			value.AllowedOrigins = append([]string(nil), routes[i].CORS.AllowedOrigins...)
			value.AllowedMethods = append([]string(nil), routes[i].CORS.AllowedMethods...)
			value.AllowedHeaders = append([]string(nil), routes[i].CORS.AllowedHeaders...)
			value.ExposeHeaders = append([]string(nil), routes[i].CORS.ExposeHeaders...)
			cloned[i].CORS = &value
		}
		if routes[i].RateLimit != nil {
			value := *routes[i].RateLimit
			cloned[i].RateLimit = &value
		}
	}
	return cloned
}

func cloneInt32Ptr(value *int32) *int32 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneSandboxResourceConfig(resources *managerapi.SandboxResourceConfig) *managerapi.SandboxResourceConfig {
	if resources == nil {
		return nil
	}
	return &managerapi.SandboxResourceConfig{Memory: strings.TrimSpace(resources.Memory)}
}

func cloneEnvVars(envVars map[string]string) map[string]string {
	if len(envVars) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(envVars))
	for key, value := range envVars {
		cloned[key] = value
	}
	return cloned
}

func NormalizeSandboxConfigForPersistence(cfg *sandboxstore.SandboxConfig) error {
	if cfg == nil {
		return nil
	}
	if err := validateSandboxConfigLifecycle(cfg.TTL, cfg.HardTTL); err != nil {
		return err
	}
	if len(cfg.Services) > 0 {
		services, err := appservice.NormalizeSandboxAppServices(cfg.Services)
		if err != nil {
			return err
		}
		cfg.Services = services
	}
	if cfg.AutoResume != nil && !*cfg.AutoResume && appservice.SandboxAppServicesHaveResumeRoute(cfg.Services) {
		return fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
	}
	return nil
}

func validateSandboxConfigLifecycle(ttl, hardTTL *int32) error {
	if ttl != nil && *ttl < 0 {
		return fmt.Errorf("%w: ttl must be >= 0", ErrInvalidClaimRequest)
	}
	if hardTTL != nil && *hardTTL < 0 {
		return fmt.Errorf("%w: hard_ttl must be >= 0", ErrInvalidClaimRequest)
	}
	if ttl != nil && hardTTL != nil && *ttl > 0 && *hardTTL > 0 && *ttl > *hardTTL {
		return fmt.Errorf("%w: ttl must be <= hard_ttl", ErrInvalidClaimRequest)
	}
	return nil
}

func cloneSandboxRecordForLifecycle(record *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	clone.Config = *CloneSandboxConfig(&record.Config)
	clone.TemplateSpec = *record.TemplateSpec.DeepCopy()
	return &clone
}

func optionalTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}
