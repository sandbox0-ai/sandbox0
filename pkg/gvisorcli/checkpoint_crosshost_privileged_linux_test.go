//go:build linux

package gvisorcli

import (
	"context"
	"crypto/sha256"
	"debug/elf"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type crossHostCheckpointCut struct {
	BootID          string                    `json:"boot_id"`
	Evidence        checkpointPayloadEvidence `json:"evidence"`
	CheckpointNanos int64                     `json:"checkpoint_nanos"`
	RestoreNanos    int64                     `json:"restore_nanos"`
}

type crossHostCheckpointRecord struct {
	RunscSHA256 string                   `json:"runsc_sha256"`
	DirectFS    bool                     `json:"directfs"`
	Cuts        []crossHostCheckpointCut `json:"cuts"`
}

// TestPrivilegedCrossHostCheckpoint is a three-phase runtime primitive probe.
// The operator transfers the retained directory A -> B -> A between invocations.
// Distinct boot identities are mandatory: three roots on one host cannot pass.
// It does not exercise Nomad, node authority, mutable RootFS or network handover.
func TestPrivilegedCrossHostCheckpoint(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_CROSS_HOST_CHECKPOINT") != "1" {
		t.Skip("explicit isolated two-host checkpoint probe only")
	}
	require.Zero(t, os.Geteuid())
	phase := os.Getenv("SANDBOX0_CHECKPOINT_PHASE")
	require.Contains(t, []string{"source", "target", "return"}, phase)
	root := os.Getenv("SANDBOX0_CHECKPOINT_DIRECTORY")
	require.True(t, filepath.IsAbs(root) && filepath.Clean(root) == root && root != "/")
	directFS, err := strconv.ParseBool(os.Getenv("SANDBOX0_CHECKPOINT_DIRECTFS"))
	require.NoError(t, err)
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	bootID := strings.TrimSpace(string(boot))
	require.NotEmpty(t, bootID)
	record := crossHostCheckpointRecord{RunscSHA256: crossHostFileSHA(t, runsc), DirectFS: directFS}
	if phase == "source" {
		// Never reuse a partial source attempt or an operator's existing tree.
		require.NoError(t, os.Mkdir(root, 0o700))
		prepareCrossHostBundle(t, root)
	} else {
		info, err := os.Lstat(root)
		require.NoError(t, err)
		require.True(t, info.IsDir() && info.Mode().Perm()&0o077 == 0)
		payload, err := os.ReadFile(filepath.Join(root, "record.json"))
		require.NoError(t, err)
		var previous crossHostCheckpointRecord
		require.NoError(t, json.Unmarshal(payload, &previous))
		require.Equal(t, record.RunscSHA256, previous.RunscSHA256)
		require.Equal(t, directFS, previous.DirectFS)
		record = previous
		if phase == "target" {
			require.Len(t, record.Cuts, 1)
			require.NotEqual(t, record.Cuts[0].BootID, bootID, "destination must be another Linux host")
		} else {
			require.Len(t, record.Cuts, 2)
			require.Equal(t, record.Cuts[0].BootID, bootID, "return to the original host")
			require.NotEqual(t, record.Cuts[1].BootID, bootID)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	config := Config{Path: runsc, Root: filepath.Join(root, "runtime-"+phase), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: directFS}
	runner := New(config).(CheckpointRunsc)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if err := runner.Delete(cleanup, phase, true); err != nil {
			t.Logf("cleanup %s: %v", phase, err)
		}
		if err := unix.Unmount(filepath.Join(config.Root, "null-netns"), 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("release probe netns: %v", err)
		}
	})
	require.NoError(t, runner.Create(ctx, filepath.Join(root, "bundle"), phase))
	cut := crossHostCheckpointCut{BootID: bootID}
	evidence := filepath.Join(root, "evidence")
	minimum := int64(2)
	if phase == "source" {
		require.NoError(t, runner.Start(ctx, phase))
	} else {
		previous := record.Cuts[len(record.Cuts)-1].Evidence
		minimum = previous.Counter + 2
		started := time.Now()
		require.NoError(t, runner.Restore(ctx, phase, filepath.Join(root, "image-"+strconv.Itoa(len(record.Cuts)))))
		cut.RestoreNanos = time.Since(started).Nanoseconds()
	}
	after := waitCheckpointEvidence(t, ctx, evidence, minimum)
	if phase != "source" {
		before := record.Cuts[0].Evidence
		require.Equal(t, before.Token, after.Token, "entrypoint must not execute again")
		require.Equal(t, before.PID, after.PID)
		require.Equal(t, before.Offset, after.Offset)
		require.Equal(t, before.Temp, after.Temp)
		require.Equal(t, before.CPUFlagsDigest, after.CPUFlagsDigest)
	}
	if phase != "return" {
		started := time.Now()
		require.NoError(t, runner.Checkpoint(ctx, phase, filepath.Join(root, "image-"+strconv.Itoa(len(record.Cuts)+1))))
		cut.CheckpointNanos = time.Since(started).Nanoseconds()
	}
	// No source process remains while the directory is transported. Comparing
	// evidence after deletion also prevents a still-running source from passing.
	require.NoError(t, runner.Delete(ctx, phase, true))
	payload, err := os.ReadFile(filepath.Join(evidence, "state.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(payload, &cut.Evidence))
	require.Equal(t, after.Token, cut.Evidence.Token)
	require.GreaterOrEqual(t, cut.Evidence.Counter, after.Counter)
	record.Cuts = append(record.Cuts, cut)
	payload, err = json.MarshalIndent(record, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "record.json"), payload, 0o600))
	t.Logf("cross-host checkpoint phase %s: %s", phase, payload)
}

func crossHostFileSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	return hex.EncodeToString(h.Sum(nil))
}

func prepareCrossHostBundle(t *testing.T, root string) {
	t.Helper()
	binary, err := os.Executable()
	require.NoError(t, err)
	e, err := elf.Open(binary)
	require.NoError(t, err)
	defer e.Close()
	for _, p := range e.Progs {
		require.NotEqual(t, elf.PT_INTERP, p.Type, "compile payload with CGO_ENABLED=0")
	}
	for _, dir := range []string{"rootfs", "rootfs/proc", "rootfs/dev", "rootfs/tmp", "rootfs/evidence", "evidence", "bundle"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0o700))
	}
	src, err := os.Open(binary)
	require.NoError(t, err)
	defer src.Close()
	dst, err := os.OpenFile(filepath.Join(root, "rootfs/payload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	require.NoError(t, err)
	_, err = io.Copy(dst, src)
	require.NoError(t, err)
	require.NoError(t, dst.Close())
	spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: filepath.Join(root, "rootfs"), Readonly: true},
		Process: &specs.Process{Cwd: "/", Args: []string{"/payload", "-test.run=^TestCheckpointPayload$"}, Env: []string{checkpointPayloadEnv + "=1", "GOMAXPROCS=2"}},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
			{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=1777", "size=32m"}},
			{Destination: "/evidence", Type: "bind", Source: filepath.Join(root, "evidence"), Options: []string{"rbind", "rw"}},
		}, Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{
			{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace},
		}}}
	payload, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "bundle/config.json"), payload, 0o600))
}
