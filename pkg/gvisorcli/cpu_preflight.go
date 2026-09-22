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

// observeMigrationCPU reuses the qualified warm observation only while its
// executable monitors, boot identity and freshly sampled native capability on
// every eligible CPU still match. Invalid warm evidence fails closed; it is
// never silently replaced by a new profile during migration. Other observers
// retain the full stock-runsc sampling and before/after bundle hashing path.
func observeMigrationCPU(ctx context.Context, observer CPUPreflightRunsc, launch *protocol.MigrationCPULaunch, cpus string) (protocol.MigrationCPUObservation, error) {
	var empty protocol.MigrationCPUObservation
	if launch.Observation.Profile.RunscVersion != cpuLaunchSupportedVersion || launch.GuestCPUProfile().RunscVersion != cpuLaunchSupportedVersion {
		return empty, fmt.Errorf("migration requires the qualified stock runsc release with epoll restore repair")
	}
	if runner, ok := observer.(*Command); ok && runner.hasPreparedCPULaunch() {
		witness, err := runner.BeginCPULaunch(ctx, cpus)
		if err != nil {
			return empty, err
		}
		current, digest, err := witness.Complete(ctx)
		if err != nil {
			return empty, err
		}
		if current == nil || digest != launch.ExecutableDigest {
			return empty, fmt.Errorf("migration runsc artifact differs from source launch")
		}
		return *current, ctx.Err()
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return empty, err
	}
	current, err := observer.CPUCoverage(ctx, cpus)
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := checkMigrationExecutable(ctx, observer, launch); err != nil {
		return empty, err
	}
	return current, nil
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
	current, err := observeMigrationCPU(ctx, observer, launch, resources.CPUSetCPUs)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := launch.CheckCurrentSource(current, resources.CPUSetCPUs); err != nil {
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
	current, err := observeMigrationCPU(ctx, observer, launch, resources.CPUSetCPUs)
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
	return &current, nil
}

// An unprepared adapter still supports full observation. Once it has retained
// a snapshot, invalidation or closure must not silently bypass its monitors.
func (r *Command) hasPreparedCPULaunch() bool {
	cache := r.config.CPULaunchCache
	if cache == nil {
		return false
	}
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	return cache.snapshot != nil || cache.closed
}
