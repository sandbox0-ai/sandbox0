package nomadruntime

import (
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
		return d.migrationStaging.Admit(d.journal.migrationRoot)
	}
	return d.migrationStaging.Verify(d.journal.migrationRoot)
}

func (d *nodeRuntime) checkMigrationStagingBudget(bytes int64, inodes uint64) error {
	if d.migrationStaging == nil {
		return errdefs.ErrUnavailable
	}
	return d.migrationStaging.AdmitBudget(d.journal.migrationRoot, bytes, inodes)
}
