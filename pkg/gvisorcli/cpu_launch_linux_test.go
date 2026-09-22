//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// The helper is native ELF so CPUProfile exercises its real inspection and
// affinity path. A marker makes any execution after warm-up fail the test.
func TestMain(m *testing.M) {
	if os.Getenv("SANDBOX0_CPU_LAUNCH_HELPER") == "1" {
		if _, err := os.Stat(os.Getenv("SANDBOX0_CPU_LAUNCH_DENY")); !os.IsNotExist(err) {
			os.Exit(73)
		}
		switch os.Args[len(os.Args)-1] {
		case "--version":
			fmt.Println(cpuLaunchSupportedVersion)
		case "cpu-features":
			fmt.Println("aes")
		default:
			os.Exit(74)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestCPULaunchWarmCacheAndOneShotWitness(t *testing.T) {
	originalPath, err := os.Executable()
	require.NoError(t, err)
	data, err := os.ReadFile(originalPath)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "runsc")
	require.NoError(t, os.WriteFile(path, data, 0700))
	writeCPUBundleCompanions(t, path)
	deny := filepath.Join(t.TempDir(), "deny-exec")
	t.Setenv("SANDBOX0_CPU_LAUNCH_HELPER", "1")
	t.Setenv("SANDBOX0_CPU_LAUNCH_DENY", deny)
	// Avoid the race runtime's one-second exit sleep in each helper process.
	t.Setenv("GORACE", os.Getenv("GORACE")+" atexit_sleep_ms=0")
	cache := NewCPULaunchCache()
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	runner := New(Config{Path: path, CPULaunchCache: cache}).(*Command)
	_, err = runner.BeginCPULaunch(t.Context(), "0")
	require.ErrorContains(t, err, "lacks warm observation")
	require.NoError(t, runner.PrepareCPULaunch(t.Context()))
	before := cache.snapshot
	require.NoError(t, os.WriteFile(deny, []byte("deny"), 0600))
	launch := &protocol.MigrationCPULaunch{Observation: before.observation, ExecutableDigest: before.executableDigest}
	current, err := observeMigrationCPU(t.Context(), runner, launch, before.observation.CPUSet)
	require.NoError(t, err, "migration must revalidate native state without launching stock probes")
	require.Equal(t, before.observation, current)
	current.Profile.Features[0] = "changed"
	require.Equal(t, "aes", before.observation.Profile.Features[0])
	changedLaunch := *launch
	changedLaunch.ExecutableDigest = "sha256:" + strings.Repeat("f", 64)
	_, err = observeMigrationCPU(t.Context(), runner, &changedLaunch, before.observation.CPUSet)
	require.ErrorContains(t, err, "artifact differs")
	_, err = observeMigrationCPU(t.Context(), runner, launch, "1048575")
	require.Error(t, err, "warm evidence cannot cover an unobserved target CPU")
	// A different carrier shares the same bounded cache and cannot shell out
	// during reuse, begin or completion, even when several claims overlap.
	other := New(Config{Path: path, CPULaunchCache: cache}).(*Command)
	require.NoError(t, other.PrepareCPULaunch(t.Context()))
	require.Same(t, before, cache.snapshot)
	var wg sync.WaitGroup
	errorsCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			witness, err := other.BeginCPULaunch(t.Context(), before.observation.CPUSet)
			if err == nil {
				observation, executable, completeErr := witness.Complete(t.Context())
				err = completeErr
				if err == nil && (observation == nil || executable != before.executableDigest) {
					err = errors.New("launch lost original executable or observation")
				}
			}
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	witness, err := runner.BeginCPULaunch(t.Context(), before.observation.CPUSet)
	require.NoError(t, err)
	observation, executable, err := witness.Complete(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.executableDigest, executable)
	observation.Profile.Features[0] = "changed"
	require.Equal(t, "aes", before.observation.Profile.Features[0])
	_, _, err = witness.Complete(t.Context())
	require.ErrorContains(t, err, "already consumed")
	_, err = runner.BeginCPULaunch(t.Context(), "1048575")
	require.Error(t, err)

	for _, name := range []string{"boot", "hardware", "coverage", "executable"} {
		t.Run(name, func(t *testing.T) {
			changed := *before
			switch name {
			case "boot":
				changed.boot = "another-boot"
			case "hardware":
				changed.hardware = map[string]string{}
			case "coverage":
				changed.observation.CPUSet = "1048575"
			case "executable":
				changed.path += "-replacement"
			}
			cache.mu.Lock()
			cache.snapshot = &changed
			cache.mu.Unlock()
			_, err := runner.BeginCPULaunch(t.Context(), changed.observation.CPUSet)
			require.Error(t, err)
			_, err = observeMigrationCPU(t.Context(), runner, launch, before.observation.CPUSet)
			require.Error(t, err, "migration must fail closed when warm evidence changes")
			// Completing an old witness verifies its own immutable history,
			// rather than adopting evidence from a later cache replacement.
			old := &cpuLaunchVerifier{expires: time.Now().Add(time.Minute), command: runner, snapshot: before}
			_, _, err = old.Complete(t.Context())
			require.NoError(t, err)
		})
	}
	for _, mode := range []string{"expired", "canceled", "nil"} {
		t.Run(mode, func(t *testing.T) {
			v := &cpuLaunchVerifier{expires: time.Now().Add(time.Minute), command: runner, snapshot: before}
			ctx := t.Context()
			switch mode {
			case "expired":
				v.expires = time.Now().Add(-time.Second)
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil":
				ctx = nil
			}
			observation, executable, err := v.Complete(ctx)
			require.Error(t, err)
			require.Nil(t, observation)
			require.Empty(t, executable)
			_, _, err = v.Complete(t.Context())
			require.ErrorContains(t, err, "already consumed")
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	cache.gate <- struct{}{}
	require.ErrorIs(t, runner.PrepareCPULaunch(ctx), context.Canceled)
	<-cache.gate
	require.NoError(t, cache.Close())
	_, err = runner.BeginCPULaunch(t.Context(), before.observation.CPUSet)
	require.Error(t, err)
	require.Error(t, runner.PrepareCPULaunch(t.Context()))
	_, err = observeMigrationCPU(t.Context(), runner, launch, before.observation.CPUSet)
	require.Error(t, err, "closed monitors cannot certify migration")
}

func TestCPUExecutableDigestAndInPlaceReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executable")
	data := []byte("original executable bytes")
	require.NoError(t, os.WriteFile(path, data, 0700))
	runner := New(Config{Path: path}).(*Command)
	_, original, err := runner.cpuExecutable()
	require.NoError(t, err)
	monitor, err := watchCPUExecutable(path, original)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, monitor.Close()) })
	require.NoError(t, monitor.Unchanged())
	actual, err := runner.executableFileDigest(t.Context())
	require.NoError(t, err)
	require.Equal(t, digest.FromBytes(data).String(), actual)
	// Same inode, size, mode and restored mtime must not certify a new binary.
	require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", len(data))), 0700))
	require.NoError(t, os.Chtimes(path, original.ModTime(), original.ModTime()))
	require.Error(t, monitor.Unchanged(), "metadata may share a clock tick, but write events must invalidate")
	require.Error(t, monitor.Unchanged(), "draining cannot recertify changed evidence")
	actual, err = runner.executableFileDigest(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, digest.FromBytes(data).String(), actual)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = runner.executableFileDigest(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCPUHWCapRejectsIncompleteOrDuplicateEvidence(t *testing.T) {
	encode := func(pairs ...uint64) []byte {
		var data []byte
		for _, value := range pairs {
			data = binary.LittleEndian.AppendUint64(data, value)
		}
		return data
	}
	for _, payload := range [][]byte{nil, {1}, make([]byte, 8208), encode(26, 1, 0, 0), encode(16, 1), encode(16, 1, 16, 2, 0, 0), encode(16, 1, 26, 2, 26, 3, 0, 0)} {
		_, err := parseCPUHWCap(payload)
		require.Error(t, err)
	}
	actual, err := parseCPUHWCap(encode(99, 123, 16, 3, 26, 7, 0, 0))
	require.NoError(t, err)
	require.Equal(t, [2]uint64{3, 7}, actual)
	actual, err = parseCPUHWCap(encode(16, 0, 0, 0))
	require.NoError(t, err)
	require.Equal(t, [2]uint64{}, actual)
}

func TestCPUExecutableMonitorInvalidatesReplacementAndClosure(t *testing.T) {
	for _, change := range []string{"rename", "replace", "unlink", "metadata", "closed"} {
		t.Run(change, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "runsc")
			require.NoError(t, os.WriteFile(path, []byte("binary"), 0700))
			before, err := os.Stat(path)
			require.NoError(t, err)
			monitor, err := watchCPUExecutable(path, before)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, monitor.Close()) })
			require.NoError(t, monitor.Unchanged())
			switch change {
			case "rename":
				require.NoError(t, os.Rename(path, path+".old"))
			case "replace":
				require.NoError(t, os.WriteFile(path+".new", []byte("binary"), 0700))
				require.NoError(t, os.Rename(path+".new", path))
			case "unlink":
				require.NoError(t, os.Remove(path))
			case "metadata":
				require.NoError(t, os.Chmod(path, 0500))
			case "closed":
				require.NoError(t, monitor.Close())
			}
			require.Error(t, monitor.Unchanged())
			require.Error(t, monitor.Unchanged(), "invalid monitor must remain invalid")
		})
	}
}

func TestStockRunscCPULaunchWarmCache(t *testing.T) {
	path := os.Getenv("SANDBOX0_CPU_PROFILE_RUNSC")
	if path == "" {
		t.Skip("set SANDBOX0_CPU_PROFILE_RUNSC to a native stock runsc executable")
	}
	runner := New(Config{Path: path, Root: t.TempDir(), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}).(*Command)
	t.Cleanup(func() { require.NoError(t, runner.config.CPULaunchCache.Close()) })
	started := time.Now()
	require.NoError(t, runner.PrepareCPULaunch(t.Context()))
	warm := time.Since(started)
	cpus := runner.config.CPULaunchCache.snapshot.observation.CPUSet
	started = time.Now()
	witness, err := runner.BeginCPULaunch(t.Context(), cpus)
	require.NoError(t, err)
	observation, executable, err := witness.Complete(t.Context())
	require.NoError(t, err)
	require.NoError(t, observation.Validate())
	require.NoError(t, digest.Digest(executable).Validate())
	t.Logf("stock warm CPU observation %s, begin+complete %s, CPUs %s, executable %s; no workload launched", warm, time.Since(started), cpus, executable)
}
