package gvisorcli

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

const cpuCoverageTimeout = 2 * time.Minute
const cpuAffinityMaskBits = int(unsafe.Sizeof(unix.CPUSet{})) * 8

// CPUCoverage binds each observation and its runsc subprocess to one CPU on a
// dedicated locked thread. The explicit CPU set must come from the intended
// runtime resource boundary, not runtime.NumCPU or a sampled core count.
func (r *Command) CPUCoverage(ctx context.Context, cpuSet string) (protocol.MigrationCPUObservation, error) {
	return observeCPUCoverage(ctx, cpuSet, cpuAffinityOps{
		get: func() (unix.CPUSet, error) {
			var set unix.CPUSet
			err := unix.SchedGetaffinity(0, &set)
			return set, err
		},
		set: func(set unix.CPUSet) error { return unix.SchedSetaffinity(0, &set) },
	}, r.CPUProfile)
}

type cpuAffinityOps struct {
	get func() (unix.CPUSet, error)
	set func(unix.CPUSet) error
}

type cpuCoverageResult struct {
	observation protocol.MigrationCPUObservation
	err         error
}

func observeCPUCoverage(ctx context.Context, cpuSet string, affinity cpuAffinityOps, sample func(context.Context) (protocol.MigrationCPUProfile, error)) (protocol.MigrationCPUObservation, error) {
	var empty protocol.MigrationCPUObservation
	if ctx == nil {
		return empty, fmt.Errorf("CPU coverage context is required")
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	requested, err := cpuCoverageMask(cpuSet)
	if err != nil {
		return empty, err
	}
	ctx, cancel := context.WithTimeout(ctx, cpuCoverageTimeout)
	defer cancel()
	result := make(chan cpuCoverageResult, 1)
	go func() {
		runtime.LockOSThread()
		var outcome cpuCoverageResult
		// If restoring affinity fails, exit with this thread still locked so
		// Go destroys it rather than giving a narrowed thread to other work.
		unlock := true
		defer func() {
			if unlock {
				runtime.UnlockOSThread()
			}
			result <- outcome
		}()
		original, err := affinity.get()
		if err != nil {
			outcome.err = fmt.Errorf("read CPU observation affinity: %w", err)
			return
		}
		for cpu := 0; cpu < cpuAffinityMaskBits; cpu++ {
			if requested.IsSet(cpu) && !original.IsSet(cpu) {
				outcome.err = fmt.Errorf("CPU %d is outside observer affinity", cpu)
				return
			}
		}
		defer func() {
			if err := affinity.set(original); err != nil {
				unlock = false
				outcome.err = errors.Join(outcome.err, fmt.Errorf("restore CPU observation affinity: %w", err))
			} else if restored, err := affinity.get(); err != nil || restored != original {
				unlock = false
				outcome.err = errors.Join(outcome.err, fmt.Errorf("CPU observation affinity changed during measurement"), err)
			}
			if outcome.err != nil {
				outcome.observation = protocol.MigrationCPUObservation{}
			}
		}()
		var firstDigest string
		for cpu := 0; cpu < cpuAffinityMaskBits; cpu++ {
			if !requested.IsSet(cpu) {
				continue
			}
			if err := ctx.Err(); err != nil {
				outcome.err = err
				return
			}
			var single unix.CPUSet
			single.Set(cpu)
			if err := affinity.set(single); err != nil {
				outcome.err = fmt.Errorf("pin CPU %d: %w", cpu, err)
				return
			}
			if pinned, err := affinity.get(); err != nil || pinned != single {
				outcome.err = errors.Join(fmt.Errorf("CPU %d affinity was not applied", cpu), err)
				return
			}
			profile, err := sample(ctx)
			if err != nil {
				outcome.err = fmt.Errorf("measure CPU %d: %w", cpu, err)
				return
			}
			if err := ctx.Err(); err != nil {
				outcome.err = err
				return
			}
			if pinned, err := affinity.get(); err != nil || pinned != single {
				outcome.err = errors.Join(fmt.Errorf("CPU %d affinity changed during measurement", cpu), err)
				return
			}
			digest, err := profile.Digest()
			if err != nil {
				outcome.err = err
				return
			}
			if firstDigest == "" {
				firstDigest = digest
				outcome.observation = protocol.MigrationCPUObservation{Profile: profile, CPUSet: cpuSet}
			} else if firstDigest != digest {
				outcome.err = fmt.Errorf("CPU %d differs from the observed runtime CPU profile", cpu)
				return
			}
		}
	}()
	// Await affinity restoration even on cancellation. Each subprocess receives
	// the bounded context, and the caller never receives partial evidence.
	outcome := <-result
	return outcome.observation, outcome.err
}

func cpuCoverageMask(cpuSet string) (unix.CPUSet, error) {
	var mask unix.CPUSet
	if _, err := protocol.ValidateCPUSet(cpuSet); err != nil {
		return mask, err
	}
	for _, part := range strings.Split(cpuSet, ",") {
		bounds := strings.Split(part, "-")
		first, _ := strconv.Atoi(bounds[0])
		last := first
		if len(bounds) == 2 {
			last, _ = strconv.Atoi(bounds[1])
		}
		// x/sys's fixed mask must never silently truncate a requested CPU.
		if last >= cpuAffinityMaskBits {
			return unix.CPUSet{}, fmt.Errorf("CPU coverage exceeds supported affinity mask")
		}
		for cpu := first; cpu <= last; cpu++ {
			mask.Set(cpu)
		}
	}
	return mask, nil
}
