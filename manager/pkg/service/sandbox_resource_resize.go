package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/apierror"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

type SandboxResourceResizeStore interface {
	NomadSandboxProjectionStore
	BeginSandboxResourceResize(context.Context, sandboxstore.RequestSandboxResourceResize) (*sandboxstore.SandboxResourceResize, error)
	PrepareSandboxResourceResize(context.Context, string) (*sandboxstore.SandboxResourceResize, error)
	ListPendingSandboxResourceResizes(context.Context, string, int) ([]*sandboxstore.SandboxResourceResize, error)
}

type SandboxResourceResizeRuntime interface {
	PauseSandboxForResourceResize(context.Context, string, string) error
	ResumeSandboxAndWait(context.Context, string) (*managerapi.ResumeSandboxResponse, error)
}

// SandboxResourceResizeService replaces compute while preserving sandbox ID and
// durable files. The desired operation, pause, cleanup and resume are durable;
// request deadlines and manager restarts cannot turn it into a config-only edit.
type SandboxResourceResizeService struct {
	store   SandboxResourceResizeStore
	runtime SandboxResourceResizeRuntime
	policy  template.ResourcePolicy
	reader  *NomadSandboxReader
	enqueue func(string)
}

func NewSandboxResourceResizeService(store SandboxResourceResizeStore, runtime SandboxResourceResizeRuntime, policy template.ResourcePolicy) (*SandboxResourceResizeService, error) {
	if store == nil || runtime == nil {
		return nil, fmt.Errorf("resource resize store and runtime are required")
	}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		return nil, err
	}
	return &SandboxResourceResizeService{store: store, runtime: runtime, policy: policy, reader: reader}, nil
}

func (s *SandboxResourceResizeService) UpdateResources(ctx context.Context, sandboxID string, resources *managerapi.SandboxResourceConfig) (*managerapi.Sandbox, error) {
	if resources == nil {
		return nil, fmt.Errorf("%w: resources are required", ErrInvalidClaimRequest)
	}
	memory, err := s.policy.ParseMemory(resources.Memory, "config.resources.memory")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidClaimRequest, err)
	}
	record, err := s.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if record == nil || !record.DeletedAt.IsZero() || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted {
		return nil, apierror.NewNotFound("sandbox", sandboxID)
	}
	memoryText := memory.String()
	desired, err := s.policy.ResolveClaimResources(record.TemplateSpec, &memoryText)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidClaimRequest, err)
	}
	override := ""
	if record.Config.Resources != nil {
		override = strings.TrimSpace(record.Config.Resources.Memory)
	}
	var currentOverride *string
	if override != "" {
		currentOverride = &override
	}
	current, err := s.policy.ResolveClaimResources(record.TemplateSpec, currentOverride)
	if err != nil {
		return nil, fmt.Errorf("%w: stored resources are invalid: %v", ErrInvalidClaimRequest, err)
	}
	noChange := desired.MemoryBytes == current.MemoryBytes && desired.CPUMillicores == current.CPUMillicores
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
		noChange = noChange && record.ResourceMillicpu == desired.CPUMillicores && record.ResourceMemoryMiB == (desired.MemoryBytes+(1<<20)-1)/(1<<20)
	}
	resize, err := s.store.BeginSandboxResourceResize(ctx, sandboxstore.RequestSandboxResourceResize{
		SandboxID: sandboxID, ExpectedTeamID: record.TeamID, ExpectedGeneration: record.RuntimeGeneration,
		ExpectedMemoryOverride: override, Memory: memory.String(), CPUMillicores: desired.CPUMillicores, MemoryBytes: desired.MemoryBytes, NoChange: noChange,
	})
	if err != nil {
		return nil, mapSandboxResourceResizeError(sandboxID, err)
	}
	if resize == nil {
		return s.reader.GetSandbox(ctx, sandboxID)
	}
	if s.enqueue != nil {
		s.enqueue(sandboxID)
	}
	// Give normal pause/cleanup a bounded opportunity to finish synchronously.
	// PostgreSQL scanning, not this wait or this replica, owns further progress.
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		err = s.CompleteSandboxResourceResize(waitCtx, sandboxID)
		if err == nil {
			return s.reader.GetSandbox(ctx, sandboxID)
		}
		if !errors.Is(err, sandboxstore.ErrSandboxResourceResizePending) {
			return nil, err
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: resource resize is durably pending; retry the same memory limit", ErrSandboxRuntimeUpdateUnavailable)
		case <-timer.C:
		}
	}
}

func (s *SandboxResourceResizeService) CompleteSandboxResourceResize(ctx context.Context, sandboxID string) error {
	r, err := s.store.PrepareSandboxResourceResize(ctx, sandboxID)
	if err != nil {
		return mapSandboxResourceResizeError(sandboxID, err)
	}
	if r == nil || r.Phase == sandboxstore.SandboxResourceResizeApplied {
		return nil
	}
	if r.Phase == sandboxstore.SandboxResourceResizeCanceled {
		return apierror.NewConflict("sandbox", sandboxID, fmt.Errorf("resource resize canceled: %s", r.Error))
	}
	switch r.Phase {
	case sandboxstore.SandboxResourceResizePausing:
		if r.WasActive {
			if err := s.runtime.PauseSandboxForResourceResize(ctx, sandboxID, r.OperationID); err != nil {
				return fmt.Errorf("%w: pause for resource resize: %v", sandboxstore.ErrSandboxResourceResizePending, err)
			}
		}
	case sandboxstore.SandboxResourceResizeResuming:
		if _, err := s.runtime.ResumeSandboxAndWait(ctx, sandboxID); err != nil {
			return fmt.Errorf("%w: resume with resized resource lease: %v", sandboxstore.ErrSandboxResourceResizePending, err)
		}
	default:
		return fmt.Errorf("unknown resource resize phase %q", r.Phase)
	}
	r, err = s.store.PrepareSandboxResourceResize(ctx, sandboxID)
	if err != nil {
		return mapSandboxResourceResizeError(sandboxID, err)
	}
	if r != nil && r.Phase == sandboxstore.SandboxResourceResizeApplied {
		return nil
	}
	return sandboxstore.ErrSandboxResourceResizePending
}

func mapSandboxResourceResizeError(sandboxID string, err error) error {
	if errors.Is(err, sandboxstore.ErrSandboxResourceResizeConflict) {
		return apierror.NewConflict("sandbox", sandboxID, err)
	}
	if errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) {
		return apierror.NewNotFound("sandbox", sandboxID)
	}
	if errors.Is(err, sandboxstore.ErrNomadSandboxResumeNotReady) {
		return fmt.Errorf("%w: %v", sandboxstore.ErrSandboxResourceResizePending, err)
	}
	return err
}

// SetEnqueuer requests immediate reconciliation; durable scanning also recovers
// operations admitted by a manager that exits before enqueueing.
func (s *SandboxResourceResizeService) SetEnqueuer(enqueue func(string)) { s.enqueue = enqueue }
