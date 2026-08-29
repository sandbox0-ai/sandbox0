package main

import (
	"fmt"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

type sandboxRuntimeBackendDependencies struct {
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
	runtimeClasses  *nomadclaim.RuntimeClassCatalog
}

func buildSandboxRuntime(cfg *config.ManagerConfig, deps sandboxRuntimeBackendDependencies) (service.SandboxRuntime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("manager config is required")
	}
	if !cfg.NodeAuthority.Enabled {
		return nil, fmt.Errorf("nomad sandbox claims require manager node authority")
	}
	if !cfg.NodeAuthority.Terminal.Enabled {
		return nil, fmt.Errorf("nomad sandbox claims require terminal reconciliation")
	}
	claim := cfg.NodeAuthority.Claim
	if claim.ClassCatalogFile != config.NodeAuthorityRuntimeClassesPath ||
		claim.WriterTokenKeyFile != config.NodeAuthorityWriterTokenKeyPath {
		return nil, fmt.Errorf("nomad claim assets must use deployment-pinned mount paths")
	}
	if deps.runtimeClasses == nil {
		return nil, fmt.Errorf("nomad sandbox claims require a runtime class catalog")
	}
	writerTokenKey, err := loadWriterTokenKey(claim.WriterTokenKeyFile)
	if err != nil {
		return nil, err
	}
	planner, err := deps.nodeAuthority.NewClaimPlanner(nodeauthority.ClaimPlannerConfig{
		Prober: deps.prober, TokenGenerator: deps.tokenGenerator, Observer: deps.observer,
		WriterTokenKey: writerTokenKey, ClaimTTL: claim.ClaimTTL.Duration,
		SLO: claim.SLO.Duration, Now: deps.now,
		DemandPoolID: demandPoolID(cfg), DemandTTL: cfg.NodePoolAutoscaler.DemandTTL.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("create Nomad runtime slot claim planner: %w", err)
	}
	claimer, err := nomadclaim.New(nomadclaim.Config{
		Store: deps.store, Templates: deps.templates, RuntimeClasses: deps.runtimeClasses, Planner: planner,
		Allocation:      deps.nodeAuthority.NomadAllocationController(),
		PlannedRetire:   deps.nodeAuthority,
		RunningFork:     deps.nodeAuthority,
		PausedRebase:    deps.nodeAuthority,
		QuotaLimits:     deps.quotaLimits,
		NetworkPolicies: deps.networkPolicies, ResourcePolicy: deps.resourcePolicy,
		RootFSFormatGeneration: rootfsblock.DescriptorVersion,
		RootFSProcdProtocol:    cfg.RootFSImporter.ProcdProtocol,
		RootFSProcdDigest:      cfg.RootFSImporter.ProcdDigest,
		ClaimTTL:               claim.ClaimTTL.Duration, DefaultTTL: deps.defaultTTL,
		Now: deps.now, Logger: deps.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("create Nomad sandbox claimer: %w", err)
	}
	return claimer, nil
}

func demandPoolID(cfg *config.ManagerConfig) string {
	if cfg == nil || !cfg.NodePoolAutoscaler.Enabled {
		return ""
	}
	return cfg.NodePoolAutoscaler.PoolID
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
