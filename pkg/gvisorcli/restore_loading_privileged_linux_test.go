//go:build linux

package gvisorcli

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestPrivilegedRestoreLoadingLatency compares stock loading modes under the
// same real guest limit. It owns its cgroup and runtime roots and never changes
// ctld policy. Restore-command latency excludes transport and command readiness;
// the subsequent full-memory check is reported separately, not hidden.
func TestPrivilegedRestoreLoadingLatency(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_RESTORE_LOADING_PROBE") != "1" {
		t.Skip("explicit isolated Linux restore-loading experiment only")
	}
	require.Zero(t, os.Geteuid())
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	cpuMillicores := int64(150)
	if value := os.Getenv("SANDBOX0_RESTORE_PROBE_CPU_MILLICORES"); value != "" {
		cpuMillicores, err = strconv.ParseInt(value, 10, 64)
		require.NoError(t, err)
		require.Contains(t, []int64{150, 1000}, cpuMillicores, "explicit isolated resource comparison only")
	}
	period := uint64(100000)
	if value := os.Getenv("SANDBOX0_RESTORE_PROBE_CPU_PERIOD_US"); value != "" {
		period, err = strconv.ParseUint(value, 10, 64)
		require.NoError(t, err)
		require.Contains(t, []uint64{100000, 20000, 10000}, period, "explicit isolated scheduling-period comparison only")
	}
	// Keep the quota ratio fixed. This probe does not change the canonical
	// resource lease period or the product's minimum supported CPU allocation.
	quota := cpuMillicores * int64(period) / 1000
	direct := false
	if value := os.Getenv("SANDBOX0_RESTORE_PROBE_DIRECT"); value != "" {
		require.Contains(t, []string{"0", "1"}, value)
		direct = value == "1"
	}
	writeback := os.Getenv("SANDBOX0_RESTORE_PROBE_WRITEBACK")
	if writeback == "" {
		writeback = "0"
	}
	require.Contains(t, []string{"0", "1", "2"}, writeback)
	t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_MIB", "128")
	t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_PATTERN", "random")
	light := os.Getenv("SANDBOX0_RESTORE_PROBE_LIGHT_WORKLOAD") == "1"
	if light {
		t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_VERIFY_ON_REQUEST", "1")
	} else {
		t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_VERIFY_ON_REQUEST", "0")
	}
	for _, background := range []bool{false, true} {
		t.Run(fmt.Sprintf("background_%t", background), func(t *testing.T) {
			root, err := os.MkdirTemp("", "restore-load-")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
			prepareCrossHostBundle(t, root)
			bundle := filepath.Join(root, "bundle")
			payload, err := os.ReadFile(filepath.Join(bundle, "config.json"))
			require.NoError(t, err)
			var spec specs.Spec
			require.NoError(t, json.Unmarshal(payload, &spec))
			cgroup := "sandbox0-" + filepath.Base(root)
			spec.Linux.CgroupsPath = "/" + cgroup
			memory := int64(512 << 20)
			spec.Linux.Resources = &specs.LinuxResources{
				CPU:    &specs.LinuxCPU{Period: &period, Quota: &quota},
				Memory: &specs.LinuxMemory{Limit: &memory},
			}
			payload, err = json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0o600))
			makeRunner := func(id string) *Command {
				config := Config{Path: runsc, Root: filepath.Join(root, id), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}
				runner := New(config).(*Command)
				// This comparative probe owns its experimental writeback mode.
				// Full migrations exercise the production descriptor-custody path.
				runner.checkpointWriteback = func(context.Context, string) (func() error, error) {
					return func() error { return nil }, nil
				}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					require.NoError(t, runner.Delete(ctx, id, true))
					if err := unix.Unmount(filepath.Join(config.Root, "null-netns"), 0); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
						t.Error(err)
					}
				})
				return runner
			}
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			source, target := makeRunner("source"), makeRunner("target")
			require.NoError(t, source.Create(ctx, bundle, "source"))
			require.NoError(t, source.Start(ctx, "source"))
			evidence := filepath.Join(root, "evidence")
			before := waitCheckpointEvidence(t, ctx, evidence, 2)
			image := filepath.Join(root, "image")
			cgroupPath := filepath.Join("/sys/fs/cgroup", cgroup)
			cpuMax, err := os.ReadFile(filepath.Join(cgroupPath, "cpu.max"))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d %d", quota, period), strings.TrimSpace(string(cpuMax)))
			stopWriteback := startCheckpointWritebackProbe(ctx, image, writeback)
			finishUpload := startCheckpointUploadProbe(t, ctx, image)
			checkpointStarted := time.Now()
			checkpointErr := source.Checkpoint(ctx, "source", image)
			writebackErr := stopWriteback()
			checkpointDuration := time.Since(checkpointStarted)
			require.NoError(t, checkpointErr)
			require.NoError(t, writebackErr)
			syncStarted := time.Now()
			require.NoError(t, syncCheckpointProbeImage(image))
			syncDuration := time.Since(syncStarted)
			t.Logf("checkpoint_writeback mode=%s checkpoint_us=%d sync_us=%d total_us=%d", writeback, checkpointDuration.Microseconds(), syncDuration.Microseconds(), time.Since(checkpointStarted).Microseconds())
			image = finishUpload(checkpointStarted)
			require.NoError(t, source.Delete(ctx, "source", true))
			payload, err = os.ReadFile(filepath.Join(evidence, "state.json"))
			require.NoError(t, err)
			var cut checkpointPayloadEvidence
			require.NoError(t, json.Unmarshal(payload, &cut))
			require.Equal(t, before.Token, cut.Token)
			require.NoError(t, target.Create(ctx, bundle, "target"))
			cpuMax, err = os.ReadFile(filepath.Join(cgroupPath, "cpu.max"))
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%d %d", quota, period), strings.TrimSpace(string(cpuMax)))
			memoryMax, err := os.ReadFile(filepath.Join(cgroupPath, "memory.max"))
			require.NoError(t, err)
			require.Equal(t, "536870912", strings.TrimSpace(string(memoryMax)))
			statsBefore := readRestoreProbeCounters(t, cgroupPath, "cpu.stat")
			stopProfile := startRestoreProbeProfile(t, ctx, cgroupPath)
			started := time.Now()
			if background || direct {
				state, err := target.State(ctx, "target")
				require.NoError(t, err)
				require.Equal(t, "created", state.Status)
				args := []string{"restore", "--image-path=" + image, "--detach"}
				if background {
					args = append(args, "--background")
				}
				if direct {
					args = append(args, "--direct")
				}
				require.NoError(t, target.run(ctx, append(args, "target")...))
			} else {
				require.NoError(t, target.Restore(ctx, "target", image))
			}
			restoreDuration := time.Since(started)
			statsAfter := readRestoreProbeCounters(t, cgroupPath, "cpu.stat")
			stopProfile()
			memoryStats := readRestoreProbeCounters(t, cgroupPath, "memory.stat")
			after := waitCheckpointEvidence(t, ctx, evidence, cut.Counter+1)
			readyDuration := time.Since(started)
			if light {
				var nonce [16]byte
				_, err := rand.Read(nonce[:])
				require.NoError(t, err)
				challenge := fmt.Sprintf("%x", nonce)
				request := filepath.Join(evidence, "verify-memory")
				require.NoError(t, os.WriteFile(request+".new", []byte(challenge), 0o600))
				require.NoError(t, os.Rename(request+".new", request))
				for after.MemoryVerification != challenge {
					after = waitCheckpointEvidence(t, ctx, evidence, after.Counter+1)
				}
			}
			require.Equal(t, cut.Token, after.Token)
			require.Equal(t, cut.PID, after.PID)
			require.Equal(t, cut.Offset, after.Offset)
			require.Equal(t, cut.Temp, after.Temp)
			require.Equal(t, cut.CPUFlagsDigest, after.CPUFlagsDigest)
			require.Equal(t, cut.MemorySHA256, after.MemorySHA256)
			t.Logf("restore_loading background=%t direct=%t light=%t memory_mib=128 cpu_millicores=%d cpu_period_us=%d checkpoint_us=%d restore_us=%d ready_us=%d verified_us=%d cpu_usage_us=%d throttled_us=%d throttled_periods=%d shmem_thp_bytes=%d anon_thp_bytes=%d",
				background, direct, light, cpuMillicores, period, checkpointDuration.Microseconds(), restoreDuration.Microseconds(), readyDuration.Microseconds(), time.Since(started).Microseconds(),
				statsAfter["usage_usec"]-statsBefore["usage_usec"], statsAfter["throttled_usec"]-statsBefore["throttled_usec"], statsAfter["nr_throttled"]-statsBefore["nr_throttled"], memoryStats["shmem_thp"], memoryStats["anon_thp"])
		})
	}
}

// External perf sampling is opt-in and restricted to this isolated target
// cgroup, including all existing and newly created sentry/gofer threads. It
// does not enable runsc's profiling mode or relax its seccomp policy.
// Profiled timings include observer overhead and are not migration SLO samples.
func startRestoreProbeProfile(t *testing.T, ctx context.Context, cgroupPath string) func() {
	t.Helper()
	directory := os.Getenv("SANDBOX0_RESTORE_PROBE_PROFILE_DIR")
	if directory == "" {
		return func() {}
	}
	require.True(t, filepath.IsAbs(directory))
	info, err := os.Lstat(directory)
	require.NoError(t, err)
	require.True(t, info.IsDir() && info.Mode().Perm()&0o077 == 0, "profile directory must be private")
	require.Equal(t, "/sys/fs/cgroup", filepath.Dir(cgroupPath))
	require.True(t, strings.HasPrefix(filepath.Base(cgroupPath), "sandbox0-restore-load-"))
	output, err := os.CreateTemp(directory, "restore-*.perf")
	require.NoError(t, err)
	require.NoError(t, output.Close())
	log, err := os.OpenFile(output.Name()+".log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	// perf's build-ID cache may hard-link a watched runtime executable. That
	// changes its inode metadata and correctly invalidates CPU launch custody.
	command := exec.CommandContext(ctx, "perf", "record", "--no-buildid-cache", "-a", "-e", "cpu-clock", "-G", filepath.Base(cgroupPath), "-F", "499", "-g", "-o", output.Name())
	command.Stderr = log
	require.NoError(t, command.Start())
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		_ = command.Process.Signal(unix.SIGINT)
		waitErr := command.Wait()
		require.NoError(t, log.Close())
		if waitErr != nil {
			var exit *exec.ExitError
			require.ErrorAs(t, waitErr, &exit)
			status, ok := exit.Sys().(syscall.WaitStatus)
			require.True(t, ok && status.Signaled() && status.Signal() == syscall.SIGINT, "perf must exit normally or from the requested interrupt: %v", waitErr)
		}
		info, err := os.Stat(output.Name())
		require.NoError(t, err)
		require.Positive(t, info.Size(), "perf must retain its capture")
		samples, err := exec.CommandContext(ctx, "perf", "script", "-i", output.Name()).Output()
		require.NoError(t, err)
		require.NotEmpty(t, samples, "a metadata-only perf file does not prove CPU coverage")
		t.Logf("restore_profile cgroup=%s path=%s", cgroupPath, output.Name())
	}
	t.Cleanup(stop)
	// Allow the sampling process to attach before the measured restore call.
	time.Sleep(100 * time.Millisecond)
	return stop
}

func readRestoreProbeCounters(t *testing.T, cgroup, name string) map[string]int64 {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(cgroup, name))
	require.NoError(t, err)
	result := make(map[string]int64)
	for _, line := range strings.Split(strings.TrimSpace(string(payload)), "\n") {
		fields := strings.Fields(line)
		require.Len(t, fields, 2)
		n, err := strconv.ParseInt(fields[1], 10, 64)
		require.NoError(t, err)
		result[fields[0]] = n
	}
	return result
}
