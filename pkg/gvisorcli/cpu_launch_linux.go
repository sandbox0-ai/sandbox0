//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

func systemCPUAffinity() cpuAffinityOps {
	return cpuAffinityOps{get: func() (unix.CPUSet, error) {
		var set unix.CPUSet
		err := unix.SchedGetaffinity(0, &set)
		return set, err
	}, set: func(set unix.CPUSet) error { return unix.SchedSetaffinity(0, &set) }}
}

func currentCPUSet() (string, error) {
	set, err := systemCPUAffinity().get()
	if err != nil {
		return "", err
	}
	var cpus []string
	for cpu := 0; cpu < cpuAffinityMaskBits; cpu++ {
		if set.IsSet(cpu) {
			cpus = append(cpus, strconv.Itoa(cpu))
		}
	}
	value := strings.Join(cpus, ",")
	if _, err := protocol.ValidateCPUSet(value); err != nil {
		return "", err
	}
	return value, nil
}

func cpuBootID() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(data))
	if len(value) != 36 {
		return "", fmt.Errorf("CPU observation lacks kernel boot identity")
	}
	return value, nil
}

func (r *Command) cpuExecutable() (string, os.FileInfo, error) {
	path, err := exec.LookPath(r.config.Path)
	if err != nil {
		return "", nil, err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", nil, err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Size() <= 0 || info.Size() > maxCPUObservedExecutableBytes {
		return "", nil, fmt.Errorf("CPU observation requires a bounded executable")
	}
	return path, info, nil
}

func sameCPUExecutable(a, b os.FileInfo) bool {
	if a == nil || b == nil || !os.SameFile(a, b) || a.Size() != b.Size() || a.Mode() != b.Mode() || !a.ModTime().Equal(b.ModTime()) {
		return false
	}
	first, ok := a.Sys().(*syscall.Stat_t)
	second, other := b.Sys().(*syscall.Stat_t)
	// ctime is an additional guard, not sufficient by itself: multiple writes
	// can share a filesystem clock tick. The inotify guard covers that case.
	return ok && other && first.Ctim == second.Ctim
}

func (r *Command) PrepareCPULaunch(ctx context.Context) error {
	if ctx == nil || r.config.CPULaunchCache == nil || r.config.CPULaunchCache.gate == nil {
		return fmt.Errorf("CPU warm-up requires context and cache")
	}
	ctx, cancel := context.WithTimeout(ctx, cpuCoverageTimeout)
	defer cancel()
	cache := r.config.CPULaunchCache
	select {
	case <-ctx.Done():
		return ctx.Err()
	case cache.gate <- struct{}{}:
	}
	defer func() { <-cache.gate }()
	cache.mu.RLock()
	prior := cache.snapshot
	closed := cache.closed
	cache.mu.RUnlock()
	if closed {
		return fmt.Errorf("CPU launch cache is closed")
	}
	if prior != nil && prior.validate(ctx, r) == nil {
		return nil
	}
	path, file, err := r.cpuExecutable()
	if err != nil {
		return err
	}
	guard, err := watchCPUBundle(r)
	if err != nil {
		return err
	}
	retained := false
	defer func() {
		if !retained {
			_ = guard.Close()
		}
	}()
	boot, err := cpuBootID()
	if err != nil {
		return err
	}
	cpus, err := currentCPUSet()
	if err != nil {
		return err
	}
	snapshot := &cpuLaunchSnapshot{path: path, file: file, boot: boot, hardware: make(map[string]string), guard: guard}
	// Resolve the inspected file once for all slow stock-runsc commands.
	observer := *r
	observer.config.Path = path
	observation, err := observeCPUCoverage(ctx, cpus, systemCPUAffinity(), func(ctx context.Context) (protocol.MigrationCPUProfile, error) {
		var empty protocol.MigrationCPUProfile
		cpu, err := currentCPUSet()
		if err != nil {
			return empty, err
		}
		before, err := nativeCPUCapabilityFingerprint()
		if err != nil {
			return empty, err
		}
		profile, err := observer.CPUProfile(ctx)
		if err != nil {
			return empty, err
		}
		if profile.RunscVersion != cpuLaunchSupportedVersion {
			return empty, fmt.Errorf("CPU launch fingerprint is not qualified for this runsc version")
		}
		after, err := nativeCPUCapabilityFingerprint()
		if err != nil || before != after {
			return empty, errors.Join(fmt.Errorf("CPU changed during warm observation"), err)
		}
		snapshot.hardware[cpu] = before
		return profile, nil
	})
	if err != nil {
		return err
	}
	snapshot.observation = observation
	snapshot.executableDigest, err = r.ExecutableDigest(ctx)
	if err != nil {
		return err
	}
	if err := snapshot.validate(ctx, r); err != nil {
		return err
	}
	cache.mu.Lock()
	if cache.closed {
		cache.mu.Unlock()
		return fmt.Errorf("CPU launch cache closed during warm-up")
	}
	cache.snapshot = snapshot
	retained = true
	cache.mu.Unlock()
	if prior != nil && prior.guard != nil {
		_ = prior.guard.Close()
	}
	return nil
}

func (s *cpuLaunchSnapshot) validate(ctx context.Context, r *Command) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.guard == nil {
		return fmt.Errorf("CPU launch lacks executable monitor")
	}
	if err := s.guard.Unchanged(); err != nil {
		return err
	}
	path, file, err := r.cpuExecutable()
	if err != nil {
		return err
	}
	boot, err := cpuBootID()
	if err != nil {
		return err
	}
	cpus, err := currentCPUSet()
	if err != nil {
		return err
	}
	if path != s.path || !sameCPUExecutable(s.file, file) || boot != s.boot || cpus != s.observation.CPUSet {
		return fmt.Errorf("CPU launch executable, boot or eligible CPU set changed")
	}
	_, err = observeCPUCoverage(ctx, cpus, systemCPUAffinity(), func(context.Context) (protocol.MigrationCPUProfile, error) {
		cpu, err := currentCPUSet()
		if err != nil {
			return protocol.MigrationCPUProfile{}, err
		}
		actual, err := nativeCPUCapabilityFingerprint()
		if err != nil || actual != s.hardware[cpu] {
			return protocol.MigrationCPUProfile{}, errors.Join(fmt.Errorf("CPU launch capability changed"), err)
		}
		return s.observation.Profile, nil
	})
	if err != nil {
		return err
	}
	// Recheck the executable after scanning every CPU. Observation never pins
	// the workload or changes the calling thread's execution affinity.
	path, file, err = r.cpuExecutable()
	if err != nil || path != s.path || !sameCPUExecutable(s.file, file) {
		return errors.Join(fmt.Errorf("runsc changed during CPU launch verification"), err)
	}
	finalBoot, bootErr := cpuBootID()
	finalCPUs, cpuErr := currentCPUSet()
	if bootErr != nil || cpuErr != nil || finalBoot != s.boot || finalCPUs != s.observation.CPUSet {
		return errors.Join(fmt.Errorf("CPU launch coverage changed during verification"), bootErr, cpuErr)
	}
	if err := s.guard.Unchanged(); err != nil {
		return err
	}
	return ctx.Err()
}

func (r *Command) BeginCPULaunch(ctx context.Context, cpuSet string) (CPULaunchVerifier, error) {
	if ctx == nil || r.config.CPULaunchCache == nil || r.config.CPULaunchCache.gate == nil {
		return nil, fmt.Errorf("CPU launch requires context and warm cache")
	}
	cache := r.config.CPULaunchCache
	cache.mu.RLock()
	snapshot := cache.snapshot
	closed := cache.closed
	cache.mu.RUnlock()
	if snapshot == nil || closed {
		return nil, fmt.Errorf("CPU launch lacks warm observation")
	}
	if err := snapshot.observation.Covers(cpuSet); err != nil {
		return nil, err
	}
	if err := snapshot.validate(ctx, r); err != nil {
		return nil, err
	}
	return &cpuLaunchVerifier{expires: time.Now().Add(cpuLaunchWitnessMaxAge), command: r, snapshot: snapshot}, nil
}

func (v *cpuLaunchVerifier) Complete(ctx context.Context) (*protocol.MigrationCPUObservation, string, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.used {
		return nil, "", fmt.Errorf("CPU launch witness was already consumed")
	}
	v.used = true
	if ctx == nil || !time.Now().Before(v.expires) {
		return nil, "", fmt.Errorf("CPU launch witness expired")
	}
	if err := v.snapshot.validate(ctx, v.command); err != nil {
		return nil, "", err
	}
	observation := v.snapshot.observation
	observation.Profile.Features = append([]string(nil), observation.Profile.Features...)
	return &observation, v.snapshot.executableDigest, nil
}

// executableFileDigest hashes one stable bundle member. The caller monitors
// the full bundle across all member reads before publishing a combined digest.
func (r *Command) executableFileDigest(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("executable observation requires context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	path, file, err := r.cpuExecutable()
	if err != nil {
		return "", err
	}
	executable, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	n, hashErr := io.Copy(hash, io.LimitReader(cpuContextReader{ctx: ctx, reader: executable}, maxCPUObservedExecutableBytes+1))
	opened, statErr := executable.Stat()
	closeErr := executable.Close()
	currentPath, current, currentErr := r.cpuExecutable()
	if hashErr != nil || statErr != nil || closeErr != nil || currentErr != nil || n != file.Size() || currentPath != path ||
		!sameCPUExecutable(file, opened) || !sameCPUExecutable(file, current) {
		return "", errors.Join(fmt.Errorf("runsc changed while hashing CPU launch identity"), hashErr, statErr, closeErr, currentErr)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

type cpuContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r cpuContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
