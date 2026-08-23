package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

type sandboxRuntimeBackendDependencies struct {
	ctx             context.Context
	kubernetes      service.SandboxRuntimeBackend
	nodeAuthority   *nodeauthority.Component
	store           nomadclaim.Store
	quotaLimits     nomadclaim.QuotaLimitStore
	templates       templatestore.TemplateStore
	networkPolicies *networkpolicy.NetworkPolicyService
	resourcePolicy  template.ResourcePolicy
	prober          runtimeslotclaim.CommandProber
	tokenGenerator  runtimeslotclaim.TokenGenerator
	observer        runtimeslotclaim.Observer
	defaultTTL      time.Duration
	now             func() time.Time
	logger          *zap.Logger
}

type nomadMeteringResourceBackfiller interface {
	BackfillNomadMeteringResources(context.Context, sandboxstore.NomadMeteringResourceResolver) (int64, error)
}

func buildSandboxRuntimeBackend(cfg *config.ManagerConfig, deps sandboxRuntimeBackendDependencies) (service.SandboxRuntimeBackend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	backend := cfg.SandboxRuntimeBackend
	if backend == "" {
		backend = config.SandboxRuntimeBackendKubernetes
	}
	switch backend {
	case config.SandboxRuntimeBackendKubernetes:
		if deps.kubernetes == nil {
			return nil, fmt.Errorf("Kubernetes sandbox claimer is required")
		}
		return deps.kubernetes, nil
	case config.SandboxRuntimeBackendNomad:
		return buildNomadSandboxRuntimeBackend(cfg, deps)
	default:
		return nil, fmt.Errorf("unsupported sandbox runtime backend %q", backend)
	}
}

func buildNomadSandboxRuntimeBackend(cfg *config.ManagerConfig, deps sandboxRuntimeBackendDependencies) (service.SandboxRuntimeBackend, error) {
	if !cfg.NodeAuthority.Enabled {
		return nil, fmt.Errorf("Nomad sandbox claims require manager node authority")
	}
	if !cfg.NodeAuthority.Terminal.Enabled {
		return nil, fmt.Errorf("Nomad sandbox claims require terminal reconciliation")
	}
	claim := cfg.NodeAuthority.Claim
	if claim.SecretName == "" || claim.SecretName != strings.TrimSpace(claim.SecretName) {
		return nil, fmt.Errorf("Nomad claim Secret name must be non-empty and canonical")
	}
	if claim.ClassCatalogFile != config.NodeAuthorityRuntimeClassesPath ||
		claim.WriterTokenKeyFile != config.NodeAuthorityWriterTokenKeyPath {
		return nil, fmt.Errorf("Nomad claim assets must use operator-pinned mount paths")
	}
	runtimeClasses, err := nomadclaim.LoadRuntimeClassCatalog(claim.ClassCatalogFile)
	if err != nil {
		return nil, err
	}
	if backfiller, ok := deps.store.(nomadMeteringResourceBackfiller); ok {
		backfillContext := deps.ctx
		if backfillContext == nil {
			backfillContext = context.Background()
		}
		updated, err := backfiller.BackfillNomadMeteringResources(
			backfillContext,
			func(record *sandboxstore.SandboxRecord) (int64, int64, error) {
				return runtimeClasses.ResolveLegacyMeteringResources(record, deps.resourcePolicy)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("backfill Nomad lifecycle metering resources: %w", err)
		}
		if updated > 0 && deps.logger != nil {
			deps.logger.Info("Backfilled Nomad lifecycle metering resources", zap.Int64("sandboxes", updated))
		}
	}
	writerTokenKey, err := loadWriterTokenKey(claim.WriterTokenKeyFile)
	if err != nil {
		return nil, err
	}
	planner, err := deps.nodeAuthority.NewClaimPlanner(nodeauthority.ClaimPlannerConfig{
		Prober: deps.prober, TokenGenerator: deps.tokenGenerator, Observer: deps.observer,
		WriterTokenKey: writerTokenKey, ClaimTTL: claim.ClaimTTL.Duration,
		SLO: claim.SLO.Duration, Now: deps.now,
	})
	if err != nil {
		return nil, fmt.Errorf("create Nomad runtime slot claim planner: %w", err)
	}
	claimer, err := nomadclaim.New(nomadclaim.Config{
		Store: deps.store, Templates: deps.templates, RuntimeClasses: runtimeClasses, Planner: planner,
		Allocation:      deps.nodeAuthority.NomadAllocationController(),
		RunningFork:     deps.nodeAuthority,
		PausedRebase:    deps.nodeAuthority,
		QuotaLimits:     deps.quotaLimits,
		NetworkPolicies: deps.networkPolicies, ResourcePolicy: deps.resourcePolicy,
		ClaimTTL: claim.ClaimTTL.Duration, DefaultTTL: deps.defaultTTL,
		Now: deps.now, Logger: deps.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create Nomad sandbox claimer: %w", err)
	}
	return claimer, nil
}

func loadWriterTokenKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect writer token key: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("writer token key must resolve to a regular file")
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read writer token key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("writer token key must contain exactly 32 bytes")
	}
	return key, nil
}
