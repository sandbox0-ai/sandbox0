package gvisorcli

import (
	"context"
	"fmt"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CPUPreflightRunsc binds each current capability observation to the exact
// runsc artifact; a matching version string alone is insufficient.
type CPUPreflightRunsc interface {
	CPUCoverageRunsc
	ExecutableDigest(context.Context) (string, error)
}

func checkMigrationExecutable(ctx context.Context, observer CPUPreflightRunsc, launch *protocol.MigrationCPULaunch) error {
	if launch.Observation.Profile.RunscVersion != cpuLaunchSupportedVersion || launch.GuestCPUProfile().RunscVersion != cpuLaunchSupportedVersion {
		return fmt.Errorf("migration requires the qualified stock runsc release with epoll restore repair")
	}
	digest, err := observer.ExecutableDigest(ctx)
	if err != nil {
		return err
	}
	if digest != launch.ExecutableDigest {
		return fmt.Errorf("migration runsc artifact differs from source launch")
	}
	return ctx.Err()
}

// CheckMigrationSourceCPU measures the current source only after validating
// trusted historical launch evidence and exact source custody. Missing history
// cannot be repaired by a fresh observation. The caller obtains launchAttempt
// and resources from durable node authority, not from a migration requester.
// This check does not freeze execution, authorize capture or extend a lease.
func CheckMigrationSourceCPU(ctx context.Context, observer CPUPreflightRunsc, launch *protocol.MigrationCPULaunch, capture protocol.MigrationCaptureRequest, launchAttempt string, resources protocol.RuntimeResourceLease) (*protocol.MigrationCPUObservation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("CPU preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if observer == nil || launch == nil {
		return nil, fmt.Errorf("CPU preflight requires launch history and a current observer")
	}
	if err := launch.ValidateCapture(capture, launchAttempt, resources); err != nil {
		return nil, err
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return nil, err
	}
	current, err := observer.CPUCoverage(ctx, resources.CPUSetCPUs)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := launch.CheckCurrentSource(current, resources.CPUSetCPUs); err != nil {
		return nil, err
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return nil, err
	}
	return &current, nil
}

// CheckMigrationTargetCPU checks every eligible target CPU against the source
// launch profile. CPU numbers and quota values are local resource constraints;
// target CPU IDs need not resemble source IDs. The caller must separately bind
// the target reservation, node boot and freshness over an authenticated path.
func CheckMigrationTargetCPU(ctx context.Context, observer CPUPreflightRunsc, launch *protocol.MigrationCPULaunch, resources protocol.RuntimeResourceLease) (*protocol.MigrationCPUObservation, error) {
	if ctx == nil {
		return nil, fmt.Errorf("CPU preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if observer == nil || launch == nil {
		return nil, fmt.Errorf("CPU preflight requires source launch history and a target observer")
	}
	if err := launch.Validate(); err != nil {
		return nil, err
	}
	if err := resources.Validate(); err != nil {
		return nil, err
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return nil, err
	}
	current, err := observer.CPUCoverage(ctx, resources.CPUSetCPUs)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := current.Covers(resources.CPUSetCPUs); err != nil {
		return nil, err
	}
	if err := protocol.CheckMigrationCPUProfiles(launch.GuestCPUProfile(), current.Profile); err != nil {
		return nil, err
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return nil, err
	}
	return &current, nil
}
