// Function autoscaling intentionally treats per-instance concurrency as a soft
// routing and scale-out signal. Sandbox0 does not currently enforce a strong
// distributed single-instance concurrency semaphore; max_active remains the hard
// pool-level capacity boundary and target_concurrency is best-effort per gateway
// replica until a future serving-path admission mechanism is added.
package http

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"go.uber.org/zap"
)

const functionAutoscalerScaleDownInterval = 30 * time.Second

type functionAutoscaler struct {
	server *Server
	logger *zap.Logger

	mu       sync.Mutex
	inflight map[string]int
}

type functionRuntimeLease struct {
	Sandbox  *mgr.Sandbox
	Revision *functions.Revision
	Instance *functions.RuntimeInstance

	release func()
}

func newFunctionAutoscaler(server *Server) *functionAutoscaler {
	logger := zap.NewNop()
	if server != nil && server.logger != nil {
		logger = server.logger
	}
	return &functionAutoscaler{
		server:   server,
		logger:   logger,
		inflight: make(map[string]int),
	}
}

func (l *functionRuntimeLease) Done() {
	if l != nil && l.release != nil {
		l.release()
		l.release = nil
	}
}

func (a *functionAutoscaler) Run(ctx context.Context) {
	if a == nil || a.server == nil || a.server.functionRepo == nil {
		return
	}
	ticker := time.NewTicker(functionAutoscalerScaleDownInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.scaleDownIdleRuntimes(ctx)
		}
	}
}

func (a *functionAutoscaler) scaleDownIdleRuntimes(ctx context.Context) {
	instances, err := a.server.functionRepo.ListRuntimeScaleDownCandidates(ctx, 50)
	if err != nil {
		a.logger.Warn("Failed to list function runtime scale-down candidates", zap.Error(err))
		return
	}
	for _, inst := range instances {
		if inst == nil || strings.TrimSpace(inst.SandboxID) == "" {
			continue
		}
		if a.hasLocalInflight(inst.SandboxID) {
			continue
		}
		if err := a.server.functionRepo.MarkRuntimeInstanceDraining(ctx, inst.TeamID, inst.FunctionID, inst.RevisionID, inst.ID); err != nil {
			if !errorsIsFunctionNotFound(err) {
				a.logger.Warn("Failed to mark function runtime instance draining",
					zap.String("function_id", inst.FunctionID),
					zap.String("revision_id", inst.RevisionID),
					zap.String("runtime_instance_id", inst.ID),
					zap.Error(err),
				)
			}
			continue
		}
		a.server.recordRuntimeLifecycleEvent(ctx, &functions.Function{ID: inst.FunctionID, TeamID: inst.TeamID}, &functions.Revision{ID: inst.RevisionID, FunctionID: inst.FunctionID, TeamID: inst.TeamID}, inst, functions.RuntimePhaseDraining, inst.ReadinessState, "scale_down", nil, 0)
		deleteCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		scaleDownStarted := time.Now()
		err := a.server.deleteSandboxViaClusterGateway(deleteCtx, inst.SandboxID, inst.TeamID, "")
		cancel()
		if err != nil {
			a.server.observeRuntimeScaleDown(inst, "error", err, time.Since(scaleDownStarted))
			a.markRuntimeInstanceFailed(context.Background(), &functions.Function{ID: inst.FunctionID, TeamID: inst.TeamID}, &functions.Revision{ID: inst.RevisionID, FunctionID: inst.FunctionID, TeamID: inst.TeamID}, inst, err, 0)
			a.logger.Warn("Failed to delete idle function runtime sandbox",
				zap.String("function_id", inst.FunctionID),
				zap.String("revision_id", inst.RevisionID),
				zap.String("runtime_instance_id", inst.ID),
				zap.String("runtime_sandbox_id", inst.SandboxID),
				zap.Error(err),
			)
			continue
		}
		a.server.observeRuntimeScaleDown(inst, "success", nil, time.Since(scaleDownStarted))
		if err := a.server.functionRepo.DeleteRuntimeInstance(ctx, inst.TeamID, inst.FunctionID, inst.RevisionID, inst.ID); err != nil && !errorsIsFunctionNotFound(err) {
			a.logger.Warn("Failed to delete scaled-down function runtime instance",
				zap.String("function_id", inst.FunctionID),
				zap.String("revision_id", inst.RevisionID),
				zap.String("runtime_instance_id", inst.ID),
				zap.Error(err),
			)
		}
	}
}

func (a *functionAutoscaler) acquire(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService) (*functionRuntimeLease, *functions.Revision, error) {
	if a == nil || a.server == nil || a.server.functionRepo == nil {
		return nil, rev, fmt.Errorf("function repository is not configured")
	}
	if fn == nil || rev == nil {
		return nil, rev, fmt.Errorf("function revision is missing")
	}
	cfg := functions.NormalizeAutoscaling(fn.Autoscaling)
	latest, err := a.server.functionRepo.GetRevision(ctx, fn.TeamID, fn.ID, rev.ID)
	if err == nil {
		rev = latest
	} else if err != nil && !errorsIsFunctionNotFound(err) {
		return nil, rev, err
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		instances, err := a.server.functionRepo.ListRuntimeInstances(ctx, fn.TeamID, fn.ID, rev.ID)
		if err != nil {
			return nil, rev, err
		}
		if inst, release := a.reserveReady(instances, cfg, false); inst != nil {
			lease, err := a.acquireExistingInstance(ctx, fn, rev, inst, service, release)
			if err == nil {
				a.server.observeRuntimeAcquire(fn, rev, "warm_reuse", "success", nil)
				return lease, rev, nil
			}
			a.server.observeRuntimeAcquire(fn, rev, "warm_reuse", "error", err)
			lastErr = err
			continue
		}
		if activeRuntimeInstanceCount(instances) < cfg.MaxActive {
			lease, updated, err := a.createRuntimeInstance(ctx, fn, rev, service, cfg)
			if err == nil {
				a.server.observeRuntimeAcquire(fn, updated, "cold_restore", "success", nil)
				return lease, updated, nil
			}
			a.server.observeRuntimeAcquire(fn, rev, "cold_restore", "error", err)
			lastErr = err
			continue
		}
		if inst, release := a.reserveReady(instances, cfg, true); inst != nil {
			lease, err := a.acquireExistingInstance(ctx, fn, rev, inst, service, release)
			if err == nil {
				a.server.observeRuntimeAcquire(fn, rev, "warm_reuse_over_target", "success", nil)
				return lease, rev, nil
			}
			a.server.observeRuntimeAcquire(fn, rev, "warm_reuse_over_target", "error", err)
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, rev, lastErr
	}
	return nil, rev, fmt.Errorf("function runtime capacity is unavailable")
}

func (a *functionAutoscaler) reserveReady(instances []*functions.RuntimeInstance, cfg functions.Autoscaling, allowOverTarget bool) (*functions.RuntimeInstance, func()) {
	cfg = functions.NormalizeAutoscaling(cfg)
	a.mu.Lock()
	defer a.mu.Unlock()

	var selected *functions.RuntimeInstance
	selectedInflight := 0
	for _, inst := range instances {
		if !runtimeInstanceReady(inst) {
			continue
		}
		current := a.inflight[inst.SandboxID]
		if !allowOverTarget && current >= cfg.TargetConcurrency {
			continue
		}
		if selected == nil || current < selectedInflight {
			selected = inst
			selectedInflight = current
		}
	}
	if selected == nil {
		return nil, nil
	}
	a.inflight[selected.SandboxID]++
	return selected, a.releaseFunc(selected.SandboxID)
}

func (a *functionAutoscaler) releaseFunc(sandboxID string) func() {
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		current := a.inflight[sandboxID]
		if current <= 1 {
			delete(a.inflight, sandboxID)
			return
		}
		a.inflight[sandboxID] = current - 1
	}
}

func (a *functionAutoscaler) hasLocalInflight(sandboxID string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.inflight[sandboxID] > 0
}

func (a *functionAutoscaler) acquireExistingInstance(ctx context.Context, fn *functions.Function, rev *functions.Revision, inst *functions.RuntimeInstance, service mgr.SandboxAppService, release func()) (*functionRuntimeLease, error) {
	if inst == nil {
		if release != nil {
			release()
		}
		return nil, fmt.Errorf("function runtime instance is missing")
	}
	lease, err := a.prepareInstance(ctx, fn, rev, inst, service, release, time.Time{})
	if err != nil {
		return nil, err
	}
	if err := a.server.functionRepo.MarkRuntimeInstanceUsed(ctx, fn.TeamID, fn.ID, rev.ID, inst.ID); err != nil {
		lease.Done()
		return nil, err
	}
	return lease, nil
}

func (a *functionAutoscaler) createRuntimeInstance(ctx context.Context, fn *functions.Function, rev *functions.Revision, service mgr.SandboxAppService, cfg functions.Autoscaling) (*functionRuntimeLease, *functions.Revision, error) {
	lock := a.server.revisionRuntimeLock(rev.ID)
	lock.Lock()
	defer lock.Unlock()

	currentRev := rev
	var lease *functionRuntimeLease
	err := a.server.withRevisionRuntimeDistributedLock(ctx, rev.ID, func(runCtx context.Context) error {
		startupStarted := time.Now()
		latest, err := a.server.functionRepo.GetRevision(runCtx, fn.TeamID, fn.ID, rev.ID)
		if err == nil {
			currentRev = latest
		} else if err != nil && !errorsIsFunctionNotFound(err) {
			return err
		}

		instances, err := a.server.functionRepo.ListRuntimeInstances(runCtx, fn.TeamID, fn.ID, currentRev.ID)
		if err != nil {
			return err
		}
		if inst, release := a.reserveReady(instances, cfg, false); inst != nil {
			selected, err := a.acquireExistingInstance(runCtx, fn, currentRev, inst, service, release)
			if err != nil {
				return err
			}
			lease = selected
			return nil
		}
		if activeRuntimeInstanceCount(instances) >= cfg.MaxActive {
			if inst, release := a.reserveReady(instances, cfg, true); inst != nil {
				selected, err := a.acquireExistingInstance(runCtx, fn, currentRev, inst, service, release)
				if err != nil {
					return err
				}
				lease = selected
				return nil
			}
			return fmt.Errorf("function runtime capacity is starting")
		}

		a.server.recordRuntimeLifecycleEvent(runCtx, fn, currentRev, nil, functions.RuntimePhaseProvisioning, functions.RuntimeReadinessStateChecking, "claim_runtime", nil, 0)
		claim, err := a.server.claimFunctionSandboxViaClusterGateway(runCtx, fn, currentRev, service)
		if err != nil {
			a.server.observeRuntimeStartup(fn, currentRev, "error", functions.RuntimeReadinessStateFailed, time.Since(startupStarted), err)
			a.server.recordRuntimeLifecycleEvent(runCtx, fn, currentRev, nil, functions.RuntimePhaseFailed, functions.RuntimeReadinessStateFailed, functionRuntimeErrorReason(err), err, time.Since(startupStarted))
			return err
		}
		inst, err := a.server.functionRepo.CreateRuntimeInstance(runCtx, &functions.RuntimeInstance{
			TeamID:     fn.TeamID,
			FunctionID: fn.ID,
			RevisionID: currentRev.ID,
			SandboxID:  claim.SandboxID,
			State:      functions.RuntimeInstanceStateStarting,
		})
		if err != nil {
			a.server.deleteFunctionRuntimeSandboxBestEffort(fn, currentRev, claim.SandboxID, "runtime instance registration failed")
			a.server.observeRuntimeStartup(fn, currentRev, "error", functions.RuntimeReadinessStateFailed, time.Since(startupStarted), err)
			a.server.recordRuntimeLifecycleEvent(runCtx, fn, currentRev, nil, functions.RuntimePhaseFailed, functions.RuntimeReadinessStateFailed, functionRuntimeErrorReason(err), err, time.Since(startupStarted))
			return err
		}
		a.server.recordRuntimeLifecycleEvent(runCtx, fn, currentRev, inst, functions.RuntimePhaseStarting, functions.RuntimeReadinessStateChecking, "start_runtime", nil, 0)
		release := a.reserveSandbox(claim.SandboxID)
		selected, err := a.prepareInstance(runCtx, fn, currentRev, inst, service, release, startupStarted)
		if err != nil {
			a.server.deleteFunctionRuntimeSandboxBestEffort(fn, currentRev, claim.SandboxID, "runtime startup failed")
			return err
		}
		a.server.observeRuntimeStartup(fn, currentRev, "success", functions.RuntimeReadinessStateReady, time.Since(startupStarted), nil)
		a.server.recordRuntimeLifecycleEvent(runCtx, fn, currentRev, selected.Instance, functions.RuntimePhaseReady, functions.RuntimeReadinessStateReady, "runtime_ready", nil, time.Since(startupStarted))
		lease = selected
		currentRev.RuntimeSandboxID = &selected.Instance.SandboxID
		currentRev.RuntimeContextID = selected.Instance.ContextID
		now := time.Now().UTC()
		currentRev.RuntimeUpdatedAt = &now
		return nil
	})
	if err != nil {
		if lease != nil {
			lease.Done()
		}
		return nil, currentRev, err
	}
	return lease, currentRev, nil
}

func (a *functionAutoscaler) reserveSandbox(sandboxID string) func() {
	a.mu.Lock()
	a.inflight[sandboxID]++
	a.mu.Unlock()
	return a.releaseFunc(sandboxID)
}

func (a *functionAutoscaler) prepareInstance(ctx context.Context, fn *functions.Function, rev *functions.Revision, inst *functions.RuntimeInstance, service mgr.SandboxAppService, release func(), startupStarted time.Time) (*functionRuntimeLease, error) {
	sandboxID := strings.TrimSpace(inst.SandboxID)
	if sandboxID == "" {
		if release != nil {
			release()
		}
		return nil, fmt.Errorf("function runtime instance is missing sandbox id")
	}
	sandbox, err := a.server.getSandboxFromClusterGateway(ctx, sandboxID)
	if err != nil {
		if release != nil {
			release()
		}
		a.markRuntimeInstanceFailed(context.Background(), fn, rev, inst, err, durationSince(startupStarted))
		return nil, err
	}
	instanceRev := *rev
	instanceRev.RuntimeSandboxID = &sandbox.ID
	instanceRev.RuntimeContextID = inst.ContextID
	contextID, err := a.server.ensureFunctionServiceRuntime(ctx, fn, &instanceRev, sandbox, service)
	if err != nil {
		if release != nil {
			release()
		}
		a.markRuntimeInstanceFailed(context.Background(), fn, rev, inst, err, durationSince(startupStarted))
		return nil, err
	}
	ready, err := a.server.functionRepo.MarkRuntimeInstanceReady(ctx, fn.TeamID, fn.ID, rev.ID, inst.ID, sandbox.ID, contextID, durationMSPtr(durationSince(startupStarted)))
	if err != nil {
		if release != nil {
			release()
		}
		return nil, err
	}
	return &functionRuntimeLease{
		Sandbox:  sandbox,
		Revision: rev,
		Instance: ready,
		release:  release,
	}, nil
}

func (a *functionAutoscaler) markRuntimeInstanceFailed(ctx context.Context, fn *functions.Function, rev *functions.Revision, inst *functions.RuntimeInstance, err error, startupDuration time.Duration) {
	if a == nil || a.server == nil || a.server.functionRepo == nil || fn == nil || rev == nil || inst == nil {
		return
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	if startupDuration > 0 {
		a.server.observeRuntimeStartup(fn, rev, "error", functions.RuntimeReadinessStateFailed, startupDuration, err)
	}
	if markErr := a.server.functionRepo.MarkRuntimeInstanceFailedWithDetails(ctx, fn.TeamID, fn.ID, rev.ID, inst.ID, message, durationMSPtr(startupDuration)); markErr != nil && !errorsIsFunctionNotFound(markErr) {
		a.logger.Warn("Failed to mark function runtime instance failed",
			zap.String("function_id", fn.ID),
			zap.String("revision_id", rev.ID),
			zap.String("runtime_instance_id", inst.ID),
			zap.Error(markErr),
		)
	}
	a.server.recordRuntimeLifecycleEvent(ctx, fn, rev, inst, functions.RuntimePhaseFailed, functions.RuntimeReadinessStateFailed, functionRuntimeErrorReason(err), err, startupDuration)
}

func runtimeInstanceReady(inst *functions.RuntimeInstance) bool {
	return inst != nil && inst.State == functions.RuntimeInstanceStateReady && strings.TrimSpace(inst.SandboxID) != ""
}

func durationSince(start time.Time) time.Duration {
	if start.IsZero() {
		return 0
	}
	return time.Since(start)
}

func activeRuntimeInstanceCount(instances []*functions.RuntimeInstance) int {
	count := 0
	for _, inst := range instances {
		if inst == nil {
			continue
		}
		switch inst.State {
		case functions.RuntimeInstanceStateStarting, functions.RuntimeInstanceStateReady, functions.RuntimeInstanceStateDraining:
			count++
		}
	}
	return count
}
