package nomadruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/containerd/errdefs"
)

type migrationStagingGuard interface {
	Verify(string) error
	Admit(string) error
	AdmitBudget(string, int64, uint64) error
}

func (d *nodeRuntime) checkMigrationStaging(admit bool) error {
	if d.migrationStaging == nil {
		return fmt.Errorf("kernel-enforced migration staging quota unavailable: %w", errdefs.ErrUnavailable)
	}
	if admit {
		err := d.migrationStaging.Admit(d.journal.migrationRoot)
		if errors.Is(err, errdefs.ErrResourceExhausted) && d.evictRetainedImages() {
			return d.migrationStaging.Admit(d.journal.migrationRoot)
		}
		return err
	}
	return d.migrationStaging.Verify(d.journal.migrationRoot)
}

func (d *nodeRuntime) checkMigrationStagingBudget(bytes int64, inodes uint64) error {
	if d.migrationStaging == nil {
		return errdefs.ErrUnavailable
	}
	err := d.migrationStaging.AdmitBudget(d.journal.migrationRoot, bytes, inodes)
	if errors.Is(err, errdefs.ErrResourceExhausted) && d.evictRetainedImages() {
		return d.migrationStaging.AdmitBudget(d.journal.migrationRoot, bytes, inodes)
	}
	return err
}

func (d *nodeRuntime) evictRetainedImages() bool {
	runtime, ok := d.runtime.(interface{ EvictRetainedImages(context.Context) error })
	if !ok {
		return false
	}
	ctx := d.migrationContext
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.EvictRetainedImages(ctx) == nil
}

func (r *rootfsRuntime) EvictRetainedImages(ctx context.Context) error {
	if r == nil || r.checkpoints == nil {
		return nil
	}
	return r.checkpoints.EvictRetainedImages(ctx)
}
