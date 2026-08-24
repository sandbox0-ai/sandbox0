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

// NomadSandboxTemplateSourceResolver validates source sandboxes from the
// regional PostgreSQL projection and exact runtime-slot readiness.
type NomadSandboxTemplateSourceResolver struct {
	store            NomadSandboxProjectionStore
	reader           *NomadSandboxReader
	captureAvailable bool
	now              func() time.Time
}

// NewNomadSandboxTemplateSourceResolver creates a resolver backed by durable
// sandbox projection and runtime-slot state.
func NewNomadSandboxTemplateSourceResolver(
	store NomadSandboxProjectionStore,
	captureAvailable bool,
	now func() time.Time,
) (*NomadSandboxTemplateSourceResolver, error) {
	if store == nil {
		return nil, fmt.Errorf("nomad sandbox template source store is required")
	}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &NomadSandboxTemplateSourceResolver{
		store: store, reader: reader, captureAvailable: captureAvailable, now: now,
	}, nil
}

// ResolveSandboxTemplateSource returns the immutable claim-time template
// context after proving that an active source remains command-ready.
func (r *NomadSandboxTemplateSourceResolver) ResolveSandboxTemplateSource(
	ctx context.Context,
	sandboxID, teamID string,
) (*template.SandboxTemplateSource, error) {
	if !r.captureAvailable {
		return nil, fmt.Errorf("%w: template RootFS capture is not configured in the source cluster", template.ErrTemplateSourceUnavailable)
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" || teamID == "" {
		return nil, fmt.Errorf("%w: sandbox_id and team_id are required", template.ErrTemplateSourceNotFound)
	}
	record, err := r.store.GetSandbox(ctx, sandboxID)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrSandboxRecordNotFound) || apierror.IsNotFound(err) {
			return nil, template.ErrTemplateSourceNotFound
		}
		return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceUnavailable, err)
	}
	if err := validateRootFSSourceSandboxRecord(record, sandboxID, teamID, r.now()); err != nil {
		return nil, mapSandboxTemplateSourceValidationError(err)
	}
	activeTxn, err := r.store.GetActiveLifecycleTxn(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("%w: load active source lifecycle: %v", template.ErrTemplateSourceNotReady, err)
	}
	if activeTxn != nil {
		return nil, fmt.Errorf("%w: source lifecycle %s is %s", template.ErrTemplateSourceNotReady, activeTxn.Kind, activeTxn.Phase)
	}
	if record.DesiredState == sandboxstore.SandboxDesiredStateActive {
		projected, err := r.reader.GetSandbox(ctx, sandboxID)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", template.ErrTemplateSourceNotReady, err)
		}
		if projected.Status != managerapi.SandboxStatusRunning {
			return nil, fmt.Errorf("%w: current status is %s", template.ErrTemplateSourceNotReady, projected.Status)
		}
	}
	return sandboxTemplateSourceFromRecord(record), nil
}

func mapSandboxTemplateSourceValidationError(err error) error {
	switch {
	case apierror.IsNotFound(err):
		return template.ErrTemplateSourceNotFound
	case apierror.IsForbidden(err):
		return template.ErrTemplateSourceForbidden
	default:
		return fmt.Errorf("%w: %v", template.ErrTemplateSourceNotReady, err)
	}
}

func sandboxTemplateSourceFromRecord(record *sandboxstore.SandboxRecord) *template.SandboxTemplateSource {
	if record == nil {
		return nil
	}
	return &template.SandboxTemplateSource{
		SandboxID:  record.ID,
		TeamID:     record.TeamID,
		UserID:     record.UserID,
		ClusterID:  record.ClusterID,
		TemplateID: record.TemplateID,
		Spec:       *record.TemplateSpec.DeepCopy(),
	}
}
