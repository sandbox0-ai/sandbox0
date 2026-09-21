//go:build linux

package gvisorcli

import (
	"context"
	"debug/elf"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const checkpointPayloadEnv = "SANDBOX0_CHECKPOINT_TEST_PAYLOAD"

type checkpointPayloadEvidence struct {
	Token          int64  `json:"token"`
	PID            int    `json:"pid"`
	Counter        int64  `json:"counter"`
	Offset         int64  `json:"offset"`
	Temp           string `json:"temp"`
	CPUFlagsDigest string `json:"cpu_flags_digest"`
}

// TestPrivilegedExecutionCheckpoint is a runtime primitive check, not the
// multi-node migration acceptance test. It uses three isolated runsc roots on
// one Linux host and verifies that restored Go memory, PID, tmpfs and an open
// unlinked file and guest CPU flags survive two successive moves. No production
// ctld, device or allocation is touched.
func TestPrivilegedExecutionCheckpoint(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_CHECKPOINT") != "1" {
		t.Skip("set SANDBOX0_RUN_PRIVILEGED_CHECKPOINT=1 on an isolated Linux test host")
	}
	require.Zero(t, os.Geteuid(), "checkpoint runtime probe requires root")
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	binary, err := os.Executable()
	require.NoError(t, err)
	elfFile, err := elf.Open(binary)
	require.NoError(t, err)
	for _, program := range elfFile.Progs {
		if program.Type == elf.PT_INTERP {
			_ = elfFile.Close()
			t.Fatal("checkpoint payload must be compiled with CGO_ENABLED=0")
		}
	}
	require.NoError(t, elfFile.Close())
	for _, directFS := range []bool{true, false} {
		t.Run(fmt.Sprintf("directfs_%t", directFS), func(t *testing.T) {
			root := t.TempDir()
			rootfs := filepath.Join(root, "rootfs")
			evidence := filepath.Join(root, "evidence")
			for _, directory := range []string{rootfs, evidence, filepath.Join(rootfs, "proc"),
				filepath.Join(rootfs, "dev"), filepath.Join(rootfs, "tmp"), filepath.Join(rootfs, "evidence")} {
				require.NoError(t, os.MkdirAll(directory, 0o700))
			}
			input, err := os.Open(binary)
			require.NoError(t, err)
			output, err := os.OpenFile(filepath.Join(rootfs, "payload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
			require.NoError(t, err)
			_, copyErr := io.Copy(output, input)
			require.NoError(t, input.Close())
			require.NoError(t, output.Close())
			require.NoError(t, copyErr)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			makeRunner := func(name string) CheckpointRunsc {
				config := Config{Path: runsc, Root: filepath.Join(root, name), Platform: "systrap",
					Overlay2: "none", FileAccess: "shared", DirectFS: directFS}
				runner := New(config).(CheckpointRunsc)
				t.Cleanup(func() {
					cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
					defer done()
					if err := runner.Delete(cleanup, name, true); err != nil {
						t.Logf("cleanup %s: %v", name, err)
					}
					// Stock runsc retains its root-scoped null network namespace
					// after the last container exits. This root belongs only to
					// the probe; release its mount before TempDir cleanup.
					if err := unix.Unmount(filepath.Join(config.Root, "null-netns"), 0); err != nil &&
						!errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
						t.Errorf("release isolated runsc namespace: %v", err)
					}
				})
				return runner
			}
			source, target, third := makeRunner("source"), makeRunner("target"), makeRunner("third")
			bundle := filepath.Join(root, "bundle")
			require.NoError(t, os.Mkdir(bundle, 0o700))
			spec := specs.Spec{
				Version: specs.Version, Root: &specs.Root{Path: rootfs, Readonly: true},
				Process: &specs.Process{Cwd: "/", Args: []string{"/payload", "-test.run=^TestCheckpointPayload$"},
					Env: []string{checkpointPayloadEnv + "=1", "GOMAXPROCS=2"}},
				Mounts: []specs.Mount{
					{Destination: "/proc", Type: "proc", Source: "proc"},
					{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
					{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=1777", "size=32m"}},
					{Destination: "/evidence", Type: "bind", Source: evidence, Options: []string{"rbind", "rw"}},
				},
				Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{
					{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace},
					{Type: specs.IPCNamespace}, {Type: specs.UTSNamespace},
				}},
			}
			payload, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0o600))
			require.NoError(t, source.Create(ctx, bundle, "source"))
			require.NoError(t, source.Start(ctx, "source"))
			before := waitCheckpointEvidence(t, ctx, evidence, 2)
			image := filepath.Join(root, "checkpoint")
			require.NoError(t, source.Checkpoint(ctx, "source", image))
			// Removing the source Sentry before restore proves that the observed
			// destination progress cannot come from a still-running source.
			require.NoError(t, source.Delete(ctx, "source", true))
			state, err := os.ReadFile(filepath.Join(evidence, "state.json"))
			require.NoError(t, err)
			var cut checkpointPayloadEvidence
			require.NoError(t, json.Unmarshal(state, &cut))
			require.Equal(t, before.Token, cut.Token)
			store, err := runtimecheckpoint.New(objectstore.NewMemoryStore(""), 256<<20)
			require.NoError(t, err)
			d := digest.FromString("isolated-runtime-probe").String()
			binding := runtimecheckpoint.Binding{OperationID: "probe", SandboxID: "probe", TeamID: "probe",
				SourceBindingDigest: d, RuntimeCompatibilityDigest: d, AssignmentRevision: d,
				CPUFeaturesDigest: d, RootFSGenerationID: "probe-readonly-rootfs", RootFSDescriptorDigest: d}
			ref, err := store.Publish(ctx, binding, image)
			require.NoError(t, err)
			copied := filepath.Join(root, "restored-image")
			_, err = store.Download(ctx, binding, ref, copied)
			require.NoError(t, err)
			require.NoError(t, target.Create(ctx, bundle, "target"))
			require.NoError(t, target.Restore(ctx, "target", copied))
			after := waitCheckpointEvidence(t, ctx, evidence, cut.Counter+2)
			require.Equal(t, cut.Token, after.Token, "entrypoint must not execute again")
			require.Equal(t, cut.PID, after.PID)
			require.Equal(t, cut.Offset, after.Offset, "open unlinked file offset must survive")
			require.Equal(t, cut.Temp, after.Temp, "tmpfs contents must survive")
			require.Equal(t, cut.CPUFlagsDigest, after.CPUFlagsDigest, "guest CPU flags must preserve source exposure")
			// A second checkpoint must save the already-restored guest state,
			// without rerunning its entrypoint or replacing inherited CPU flags.
			secondImage := filepath.Join(root, "checkpoint-second")
			require.NoError(t, target.Checkpoint(ctx, "target", secondImage))
			require.NoError(t, target.Delete(ctx, "target", true))
			secondState, err := os.ReadFile(filepath.Join(evidence, "state.json"))
			require.NoError(t, err)
			var secondCut checkpointPayloadEvidence
			require.NoError(t, json.Unmarshal(secondState, &secondCut))
			binding.OperationID = "probe-second"
			secondRef, err := store.Publish(ctx, binding, secondImage)
			require.NoError(t, err)
			secondCopy := filepath.Join(root, "restored-second-image")
			_, err = store.Download(ctx, binding, secondRef, secondCopy)
			require.NoError(t, err)
			require.NoError(t, third.Create(ctx, bundle, "third"))
			require.NoError(t, third.Restore(ctx, "third", secondCopy))
			last := waitCheckpointEvidence(t, ctx, evidence, secondCut.Counter+2)
			require.Equal(t, cut.Token, last.Token)
			require.Equal(t, cut.PID, last.PID)
			require.Equal(t, cut.Offset, last.Offset)
			require.Equal(t, cut.Temp, last.Temp)
			require.Equal(t, cut.CPUFlagsDigest, last.CPUFlagsDigest)
		})
	}
}

func waitCheckpointEvidence(t *testing.T, ctx context.Context, directory string, counter int64) checkpointPayloadEvidence {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		payload, err := os.ReadFile(filepath.Join(directory, "state.json"))
		var evidence checkpointPayloadEvidence
		if err == nil && json.Unmarshal(payload, &evidence) == nil && evidence.Counter >= counter {
			return evidence
		}
		select {
		case <-ctx.Done():
			t.Fatalf("checkpoint payload did not reach counter %d: %v", counter, ctx.Err())
		case <-ticker.C:
		}
	}
}

// TestCheckpointPayload runs only inside the explicitly constructed guest.
// Ordinary test runs skip it without creating files or starting a workload.
func TestCheckpointPayload(t *testing.T) {
	if os.Getenv(checkpointPayloadEnv) != "1" {
		t.Skip("guest-only checkpoint payload")
	}
	token := time.Now().UnixNano()
	sentinel := strconv.FormatInt(token, 10)
	require.NoError(t, os.WriteFile("/tmp/sentinel", []byte(sentinel), 0o600))
	file, err := os.OpenFile("/tmp/unlinked", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString("unlinked-execution-state")
	require.NoError(t, err)
	_, err = file.Seek(7, io.SeekStart)
	require.NoError(t, err)
	require.NoError(t, os.Remove("/tmp/unlinked"))
	for counter := int64(1); ; counter++ {
		data, err := os.ReadFile("/tmp/sentinel")
		require.NoError(t, err)
		require.Equal(t, sentinel, string(data))
		offset, err := file.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		cpuInfo, err := os.ReadFile("/proc/cpuinfo")
		require.NoError(t, err)
		flags := ""
		for _, line := range strings.Split(string(cpuInfo), "\n") {
			key, value, found := strings.Cut(line, ":")
			if found && (strings.TrimSpace(key) == "flags" || strings.TrimSpace(key) == "Features") {
				value = strings.Join(strings.Fields(value), " ")
				if flags != "" {
					require.Equal(t, flags, value, "guest CPUs must expose homogeneous feature flags")
				}
				flags = value
			}
		}
		require.NotEmpty(t, flags, "read CPU flags afresh after every restore")
		payload, err := json.Marshal(checkpointPayloadEvidence{
			Token: token, PID: os.Getpid(), Counter: counter, Offset: offset, Temp: string(data),
			CPUFlagsDigest: digest.FromString(flags).String(),
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile("/evidence/state.new", payload, 0o600))
		require.NoError(t, os.Rename("/evidence/state.new", "/evidence/state.json"))
		time.Sleep(50 * time.Millisecond)
	}
}
