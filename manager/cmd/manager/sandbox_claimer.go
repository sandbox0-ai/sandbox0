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
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

type sandboxRuntimeBackendDependencies struct {
	nodeAuthority   *nodeauthority.Component
	capacityWake    func()
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
		CapacityWait: runtimeslotclaim.CapacityWaitConfig{Timeout: claim.CapacityWaitTimeout.Duration, MaxPending: claim.CapacityWaitMaxPending, MaxPendingPerTeam: claim.CapacityWaitMaxPendingPerTeam},
		CapacityWake: deps.capacityWake,
		Prober:       deps.prober, TokenGenerator: deps.tokenGenerator, Observer: deps.observer,
		MigrationObserver:  restorePlanningObserver(deps.logger, "Migration destination planning timing"),
		CheckpointObserver: restorePlanningObserver(deps.logger, "Memory restore planning timing"),
		WriterTokenKey:     writerTokenKey, ClaimTTL: claim.ClaimTTL.Duration,
		SLO: claim.SLO.Duration, Now: deps.now,
		DemandPoolID: demandPoolID(cfg), DemandTTL: cfg.NodePoolAutoscaler.DemandTTL.Duration,
	})
	if err != nil {
		return nil, fmt.Errorf("create Nomad runtime slot claim planner: %w", err)
	}
	claimer, err := nomadclaim.New(sandboxRuntimeClaimConfig(cfg, deps, planner))
	if err != nil {
		return nil, fmt.Errorf("create Nomad sandbox claimer: %w", err)
	}
	return claimer, nil
}

// sandboxRuntimeClaimConfig keeps image-import policy in the logical claimer,
// separate from the immutable warm-slot runtime class catalog.
func sandboxRuntimeClaimConfig(cfg *config.ManagerConfig, deps sandboxRuntimeBackendDependencies, planner *runtimeslotclaim.Planner) nomadclaim.Config {
	return nomadclaim.Config{
		RuntimeProcd: cfg.RuntimeProcd,
		Store:        deps.store, Templates: deps.templates, RuntimeClasses: deps.runtimeClasses, Planner: planner,
		Allocation:      deps.nodeAuthority.NomadAllocationController(),
		PlannedRetire:   deps.nodeAuthority,
		RunningFork:     deps.nodeAuthority,
		PausedRebase:    deps.nodeAuthority,
		QuotaLimits:     deps.quotaLimits,
		NetworkPolicies: deps.networkPolicies, ResourcePolicy: deps.resourcePolicy,
		RootFSFormatGeneration:         cfg.RootFSImporter.FormatGeneration,
		RootFSProcdProtocol:            cfg.RootFSImporter.ProcdProtocol,
		RootFSProcdDigest:              cfg.RootFSImporter.ProcdDigest,
		RootFSImportDataRangeBytes:     cfg.RootFSImporter.DataRangeBytes,
		RootFSImportDataLayoutPolicy:   cfg.RootFSImporter.DataLayoutPolicy,
		RootFSImportMappingGroupPolicy: cfg.RootFSImporter.MappingGroupPolicy,
		ClaimTTL:                       cfg.NodeAuthority.Claim.ClaimTTL.Duration, DefaultTTL: deps.defaultTTL,
		Now: deps.now, Logger: deps.logger,
	}
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

// restorePlanningObserver records only the execution-planning portion. The
// service logs the full command-ready resume duration after regional commit.
func restorePlanningObserver(logger *zap.Logger, message string) func(runtimeslotclaim.Observation) {
	return func(observation runtimeslotclaim.Observation) {
		if logger == nil {
			return
		}
		fields := []zap.Field{zap.String("operation_id", observation.OperationID),
			zap.Bool("success", observation.Succeeded), zap.Int64("duration_us", observation.Duration.Microseconds())}
		for _, phase := range observation.Phases {
			fields = append(fields, zap.Int64(phase.Phase+"_us", phase.Duration.Microseconds()))
		}
		logger.Info(message, fields...)
	}
}
