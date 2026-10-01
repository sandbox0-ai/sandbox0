//go:build linux

package rootfsblock

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"debug/elf"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type liveGuestEvidence struct {
	Token      string `json:"token"`
	PID        int    `json:"pid"`
	Counter    uint64 `json:"counter"`
	Offset     int64  `json:"offset"`
	HeapDigest string `json:"heap_digest"`
	Mmap       uint64 `json:"mmap"`
}

func startLiveNBDGuest(t *testing.T, root, mountRoot string) (func(), func()) {
	t.Helper()
	path := os.Getenv("S0_NBD_LIVE_RUNSC_PATH")
	if path == "" {
		return func() {}, func() {}
	}
	binaryPath, err := os.Executable()
	require.NoError(t, err)
	executable, err := elf.Open(binaryPath)
	require.NoError(t, err)
	for _, program := range executable.Progs {
		if program.Type == elf.PT_INTERP {
			t.Fatal("live guest fixture must be compiled with CGO_ENABLED=0")
		}
	}
	require.NoError(t, executable.Close())
	input, err := os.Open(binaryPath)
	require.NoError(t, err)
	output, err := os.OpenFile(filepath.Join(mountRoot, "payload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	require.NoError(t, err)
	_, err = io.Copy(output, input)
	require.NoError(t, err)
	require.NoError(t, input.Close())
	require.NoError(t, output.Close())
	for _, directory := range []string{"proc", "dev", "tmp"} {
		require.NoError(t, os.MkdirAll(filepath.Join(mountRoot, directory), 0755))
	}
	bundle := filepath.Join(root, "guest-bundle")
	require.NoError(t, os.Mkdir(bundle, 0700))
	spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: mountRoot},
		Process: &specs.Process{Cwd: "/", Args: []string{"/payload", "-test.run=^TestKernelNBDLiveGuestHelper$", "-test.timeout=10m"}, Env: []string{"S0_NBD_GUEST_PAYLOAD=1"}},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
			{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=1777", "size=16m"}},
		}, Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}}},
	}
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), data, 0600))
	isolationRoot := filepath.Join(root, "unrelated-storage")
	require.NoError(t, os.Mkdir(isolationRoot, 0700))
	runscRoot := filepath.Join(root, "runsc")
	arguments := []string{"--root=" + runscRoot, "--platform=systrap", "--overlay2=none", "--file-access=shared", "--directfs=false"}
	run := func(ctx context.Context, args ...string) {
		command := exec.CommandContext(ctx, path, append(append([]string{}, arguments...), args...)...)
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		require.NoError(t, command.Run())
	}
	type runscState struct {
		PID    int    `json:"pid"`
		Status string `json:"status"`
	}
	stateOf := func(ctx context.Context) runscState {
		command := exec.CommandContext(ctx, path, append(append([]string{}, arguments...), "state", "live-guest")...)
		output, err := command.Output()
		require.NoError(t, err)
		var state runscState
		require.NoError(t, json.Unmarshal(output, &state))
		return state
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	run(ctx, "create", "--bundle="+bundle, "live-guest")
	run(ctx, "start", "live-guest")
	state := stateOf(ctx)
	read := func() liveGuestEvidence {
		var evidence liveGuestEvidence
		data, err := os.ReadFile(filepath.Join(mountRoot, "guest-state.json"))
		if err == nil {
			_ = json.Unmarshal(data, &evidence)
		}
		return evidence
	}
	require.Eventually(t, func() bool { return read().Counter >= 2 }, 20*time.Second, 10*time.Millisecond)
	before := read()
	var closeOnce sync.Once
	stop := func() {
		closeOnce.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			run(ctx, "delete", "--force", "live-guest")
			_ = unix.Unmount(filepath.Join(runscRoot, "null-netns"), 0)
		})
	}
	t.Cleanup(stop)
	return func() {
		require.Eventually(t, func() bool { return read().Counter > before.Counter+2 }, 10*time.Second, 10*time.Millisecond)
		after := read()
		require.Equal(t, before.Token, after.Token, "guest entrypoint was recreated")
		require.Equal(t, before.PID, after.PID)
		require.Equal(t, before.HeapDigest, after.HeapDigest)
		require.Greater(t, after.Offset, before.Offset, "unlinked descriptor position must progress")
		require.Equal(t, after.Counter, after.Mmap)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		current := stateOf(ctx)
		require.Equal(t, state.PID, current.PID, "original Sentry must survive")
		require.Equal(t, "running", current.Status)
		before = after
	}, stop
}

func TestKernelNBDLiveGuestHelper(t *testing.T) {
	if os.Getenv("S0_NBD_GUEST_PAYLOAD") != "1" {
		t.Skip("stock-runsc guest-only payload")
	}
	var token [32]byte
	_, err := rand.Read(token[:])
	require.NoError(t, err)
	tokenText := hex.EncodeToString(token[:])
	memory := make([]byte, 4<<20)
	_, err = rand.Read(memory)
	require.NoError(t, err)
	digest := sha256.Sum256(memory)
	heapDigest := hex.EncodeToString(digest[:])
	require.NoError(t, os.WriteFile("/tmp/token", []byte(tokenText), 0600))
	unlinked, err := os.OpenFile("/open-unlinked", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, os.Remove("/open-unlinked"))
	mappedFile, err := os.OpenFile("/mapped", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, mappedFile.Truncate(4096))
	mapped, err := unix.Mmap(int(mappedFile.Fd()), 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	for counter := uint64(1); ; counter++ {
		require.Equal(t, digest, sha256.Sum256(memory))
		actualToken, err := os.ReadFile("/tmp/token")
		require.NoError(t, err)
		require.Equal(t, tokenText, string(actualToken))
		_, err = fmt.Fprintf(unlinked, "%d:%s\n", counter, tokenText)
		require.NoError(t, err)
		require.NoError(t, unlinked.Sync())
		offset, err := unlinked.Seek(0, io.SeekCurrent)
		require.NoError(t, err)
		binary.LittleEndian.PutUint64(mapped, counter)
		require.NoError(t, unix.Msync(mapped, unix.MS_SYNC))
		data, err := json.Marshal(liveGuestEvidence{Token: tokenText, PID: os.Getpid(), Counter: counter, Offset: offset, HeapDigest: heapDigest, Mmap: binary.LittleEndian.Uint64(mapped)})
		require.NoError(t, err)
		file, err := os.OpenFile("/guest-state.tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		require.NoError(t, err)
		_, err = file.Write(data)
		require.NoError(t, err)
		require.NoError(t, file.Sync())
		require.NoError(t, file.Close())
		require.NoError(t, os.Rename("/guest-state.tmp", "/guest-state.json"))
		time.Sleep(10 * time.Millisecond)
	}
}
