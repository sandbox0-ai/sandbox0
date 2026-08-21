package service

import (
	"context"
	"fmt"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/appservice"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// NomadSandboxMutationStore is the durable transaction boundary used by
// public Nomad sandbox mutations.
type NomadSandboxMutationStore interface {
	NomadSandboxProjectionStore
	WithSandboxLock(context.Context, string, func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error
}

// NomadSandboxUpdater persists runtime-neutral Nomad sandbox mutations. It
// deliberately rejects updates that still lack explicit runtime orchestration.
type NomadSandboxUpdater struct {
	store      NomadSandboxMutationStore
	reader     *NomadSandboxReader
	defaultTTL time.Duration
	now        func() time.Time
}

// NewNomadSandboxUpdater creates a public mutation service backed only by the
// regional PostgreSQL sandbox store and runtime-slot projection.
func NewNomadSandboxUpdater(
	store NomadSandboxMutationStore,
	defaultTTL time.Duration,
	now func() time.Time,
) (*NomadSandboxUpdater, error) {
	if store == nil {
		return nil, fmt.Errorf("Nomad sandbox mutation store is required")
	}
	if defaultTTL < 0 {
		return nil, fmt.Errorf("Nomad sandbox default TTL must not be negative")
	}
	reader, err := NewNomadSandboxReader(store)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = time.Now
	}
	return &NomadSandboxUpdater{store: store, reader: reader, defaultTTL: defaultTTL, now: now}, nil
}

// UpdateSandbox updates mutable durable fields that do not require replacing
// or mutating a running allocation.
func (u *NomadSandboxUpdater) UpdateSandbox(
	ctx context.Context,
	sandboxID string,
	config *SandboxUpdateConfig,
) (*managerapi.Sandbox, error) {
	if config == nil {
		return nil, fmt.Errorf("sandbox config is required")
	}
	if config.EnvVars != nil || config.Resources != nil || config.Network != nil || config.Services != nil {
		return nil, fmt.Errorf("%w: Nomad env, resource, network, and service updates require runtime orchestration",
			ErrSandboxRuntimeUpdateUnavailable)
	}
	err := u.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
			return k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
		}
		if record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
			return fmt.Errorf("sandbox runtime backend changed during update")
		}
		if record.DesiredState == sandboxstore.SandboxDesiredStateTerminating {
			return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox termination is in progress"))
		}
		if record.DesiredState != sandboxstore.SandboxDesiredStateActive && record.DesiredState != sandboxstore.SandboxDesiredStatePaused {
			return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID,
				fmt.Errorf("sandbox state %s does not accept updates", record.DesiredState))
		}
		updated := cloneSandboxRecordForLifecycle(record)
		merged := CloneSandboxConfig(&record.Config)
		if merged == nil {
			merged = &sandboxstore.SandboxConfig{}
		}
		now := u.now()
		if config.TTL != nil {
			merged.TTL = cloneInt32Ptr(config.TTL)
			updated.ExpiresAt = refreshDeadline(now, *config.TTL)
			if *config.TTL <= 0 {
				updated.ExpiresAt = time.Time{}
			}
		}
		if config.HardTTL != nil {
			merged.HardTTL = cloneInt32Ptr(config.HardTTL)
			updated.HardExpiresAt = refreshDeadline(now, *config.HardTTL)
			if *config.HardTTL <= 0 {
				updated.HardExpiresAt = time.Time{}
			}
		}
		if err := validateSandboxConfigLifecycle(merged.TTL, merged.HardTTL); err != nil {
			return err
		}
		if config.AutoResume != nil {
			merged.AutoResume = cloneBoolPtr(config.AutoResume)
		}
		if merged.AutoResume != nil && !*merged.AutoResume && appservice.SandboxAppServicesHaveResumeRoute(merged.Services) {
			return fmt.Errorf("cannot set resume=true on public routes when sandbox auto_resume is disabled")
		}
		updated.Config = *merged
		return tx.SaveSandbox(lockCtx, updated)
	})
	if err != nil {
		return nil, fmt.Errorf("update Nomad sandbox record: %w", err)
	}
	return u.reader.GetSandbox(ctx, sandboxID)
}

// RefreshSandbox refreshes soft and hard expiration from one locked durable
// record without consulting a runtime Pod.
func (u *NomadSandboxUpdater) RefreshSandbox(
	ctx context.Context,
	sandboxID string,
	request *RefreshRequest,
) (*RefreshResponse, error) {
	var plan sandboxRefreshPlan
	err := u.store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
			return k8serrors.NewNotFound(schema.GroupResource{Resource: "sandbox"}, sandboxID)
		}
		if record.RuntimeBackend != sandboxstore.SandboxRuntimeBackendNomad {
			return fmt.Errorf("sandbox runtime backend changed during refresh")
		}
		if record.DesiredState == sandboxstore.SandboxDesiredStateTerminating {
			return k8serrors.NewConflict(schema.GroupResource{Resource: "sandbox"}, sandboxID, fmt.Errorf("sandbox termination is in progress"))
		}
		var err error
		plan, err = buildSandboxRefreshPlan(record.Config, u.defaultTTL, u.now(), request)
		if err != nil {
			return err
		}
		updated := cloneSandboxRecordForLifecycle(record)
		updated.ExpiresAt = plan.expiresAt
		updated.HardExpiresAt = plan.hardExpiresAt
		return tx.SaveSandbox(lockCtx, updated)
	})
	if err != nil {
		return nil, fmt.Errorf("refresh Nomad sandbox expiration: %w", err)
	}
	return &RefreshResponse{
		SandboxID: sandboxID, ExpiresAt: optionalTime(plan.expiresAt),
		HardExpiresAt: optionalTime(plan.hardExpiresAt),
	}, nil
}
