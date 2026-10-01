package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

var errNomadCheckpointFallbackPending = fmt.Errorf("%w: memory cleanup and RootFS resume are pending", service.ErrSandboxLifecycleUnavailable)

type checkpointPlanner interface {
	ClaimCheckpoint(context.Context, runtimeslotclaim.Request, protocol.CheckpointRestoreAuthority) (*runtimeslotclaim.Result, error)
}

type checkpointPauseStore interface {
	RequestNomadSandboxMemoryPause(context.Context, string) (*sandboxstore.NomadSandboxMemoryPauseCandidate, error)
}

// PauseMemorySandboxAndWait reserves the explicit memory operation. The shared
// checkpoint worker captures before source cleanup; ordinary Nomad stop and
// filesystem-pause enqueuers must not run against the still-live source.
func (s *Service) PauseMemorySandboxAndWait(ctx context.Context, sandboxID string) (*service.PauseSandboxResponse, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return nil, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	store, ok := s.store.(checkpointPauseStore)
	if !ok {
		return nil, fmt.Errorf("%w: memory pause authority is unavailable", service.ErrSandboxLifecycleUnavailable)
	}
	candidate, err := store.RequestNomadSandboxMemoryPause(ctx, sandboxID)
	if err != nil {
		return nil, mapNomadSandboxPauseError(sandboxID, err)
	}
	if candidate == nil || candidate.SandboxID != sandboxID || candidate.OperationID == "" {
		return nil, fmt.Errorf("%w: memory pause authority changed owner", service.ErrSandboxLifecycleUnavailable)
	}
	status := managerapi.SandboxStatusStarting
	if candidate.AlreadyPaused {
		status = managerapi.SandboxStatusPaused
	}
	return &service.PauseSandboxResponse{SandboxID: sandboxID, Paused: candidate.AlreadyPaused, Status: status}, nil
}

// ResumeMemorySandboxAndWait is the explicit manager boundary for restoring a
// retained process image. The existing resume entry points keep cold defaults.
func (s *Service) ResumeMemorySandboxAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	record, _, err := s.resumeNomadSandboxMode(ctx, sandboxID, true)
	if errors.Is(err, errNomadCheckpointFallbackPending) || (err == nil && record == nil) {
		return &managerapi.ResumeSandboxResponse{SandboxID: strings.TrimSpace(sandboxID), Resumed: false}, nil
	}
	if err != nil {
		return nil, err
	}
	return &managerapi.ResumeSandboxResponse{SandboxID: record.ID, Resumed: true}, nil
}

// ResumeSandboxAutomaticallyAndWait prefers retained execution state and uses
// the committed RootFS when memory cannot be recovered.
func (s *Service) ResumeSandboxAutomaticallyAndWait(ctx context.Context, sandboxID string) (*managerapi.ResumeSandboxResponse, error) {
	record, _, err := s.resumeNomadSandboxMode(ctx, sandboxID, true)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, errNomadCheckpointFallbackPending
	}
	return &managerapi.ResumeSandboxResponse{SandboxID: record.ID, Resumed: true}, nil
}

type checkpointFallbackStore interface {
	ResolveNomadCheckpointResumeFallback(context.Context, string, string, string, *int64, bool) (string, bool, error)
}

func (s *Service) resolveCheckpointFallback(ctx context.Context, sandboxID, operation, reason string) (string, bool, error) {
	store, ok := s.store.(checkpointFallbackStore)
	if !ok {
		return "", false, nil
	}
	next, handled, err := store.ResolveNomadCheckpointResumeFallback(ctx, sandboxID, operation, reason, nil, false)
	if errors.Is(err, sandboxstore.ErrNomadCheckpointFallbackQuotaRequired) {
		record, loadErr := s.store.GetSandbox(ctx, sandboxID)
		if loadErr != nil {
			return "", false, loadErr
		}
		if record == nil {
			return "", false, apierror.NewNotFound("sandbox", sandboxID)
		}
		limit, limitErr := s.activeSandboxLimit(ctx, record.TeamID)
		if limitErr != nil {
			return "", false, limitErr
		}
		next, handled, err = store.ResolveNomadCheckpointResumeFallback(ctx, sandboxID, operation, reason, limit, true)
	}
	if reason != "" && err == nil {
		s.logger.Warn("Memory resume falling back to committed RootFS", zap.String("sandboxID", sandboxID), zap.String("operationID", operation), zap.String("reason", reason))
	}
	if err != nil {
		return next, handled, mapNomadResumeError("resolve checkpoint RootFS fallback", sandboxID, err)
	}
	return next, handled, nil
}

// bindCheckpointResumePlan rejects configuration drift instead of silently
// applying only part of a mutable template to preserved processes. Session reset
// is an activation flag; it is retained from capture and never rerun on restore.
func bindCheckpointResumePlan(candidate *sandboxstore.NomadSandboxResumeCandidate, plan *nomadResumePlan) error {
	a := candidate.Checkpoint
	if a == nil || a.Assignment.Validate() != nil || a.LifecycleEpoch <= 0 ||
		a.Assignment.OperationID != candidate.OperationID || a.Assignment.Target.SandboxID != candidate.SandboxID ||
		a.Assignment.Target.TeamID != candidate.Record.TeamID || a.Assignment.Target.RuntimeGeneration != candidate.RuntimeGeneration ||
		candidate.CheckpointCompatibilityDigest != plan.runtimeClass.CompatibilityDigest {
		return fmt.Errorf("retained memory requires its exact runtime compatibility and admitted assignment")
	}
	target := a.Assignment.Target
	expected := plan.assignment
	expected.RuntimeGeneration = target.RuntimeGeneration
	expected.ResetCopiedSessionState = target.ResetCopiedSessionState
	// A newly enabled regional registry is a default for fresh processes, not
	// a mutation of a retained process. Only ignore its injection when capture
	// had no registry and the persisted sandbox/template has no explicit one.
	// Existing captured values lack provenance, so their drift still fails.
	if _, capturedRegistry := target.EnvVars["NPM_CONFIG_REGISTRY"]; !capturedRegistry && !hasExplicitNPMRegistry(candidate.Record) {
		expected.EnvVars = maps.Clone(expected.EnvVars)
		delete(expected.EnvVars, "NPM_CONFIG_REGISTRY")
	}
	// The restored guest already contains its captured daemon. A new platform
	// daemon alone must not discard recoverable process memory.
	if expected.Procd == nil || target.Procd == nil || expected.Procd.Protocol == target.Procd.Protocol {
		expected.Procd = target.Procd
	}

	expectedRevision, err := expected.Revision()
	if err != nil {
		return err
	}
	capturedRevision, err := target.Revision()
	if err != nil || expectedRevision != capturedRevision {
		return fmt.Errorf("sandbox configuration changed since memory capture")
	}
	plan.assignment = target
	return nil
}

func hasExplicitNPMRegistry(record *sandboxstore.SandboxRecord) bool {
	for _, environment := range []map[string]string{record.Config.EnvVars, record.TemplateSpec.EnvVars} {
		for key := range environment {
			if strings.EqualFold(key, "npm_config_registry") {
				return true
			}
		}
	}
	for _, variable := range record.TemplateSpec.MainContainer.Env {
		if strings.EqualFold(variable.Name, "npm_config_registry") {
			return true
		}
	}
	return false
}
