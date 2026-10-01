//go:build linux

package session

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/livehandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

type liveFixtureObjects string

func (s liveFixtureObjects) PutImmutable(_ context.Context, key string, data []byte) error {
	path := filepath.Join(string(s), key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

type liveFixtureRange struct {
	io.Reader
	closer *os.File
}

func (r liveFixtureRange) Close() error { return r.closer.Close() }

func (s liveFixtureObjects) Get(key string, offset, length int64) (io.ReadCloser, error) {
	file, err := os.Open(filepath.Join(string(s), key))
	if err != nil {
		return nil, err
	}
	return liveFixtureRange{io.NewSectionReader(file, offset, length), file}, nil
}

type liveManagerMessage struct {
	Action   string
	Manifest LiveHandoff
	Mount    string
}

// Destructive only to the explicitly named unused test device on an isolated
// host. Unlike the protocol fixture, this runs the complete durable session
// manager, XFS and Overlay adoption path in different owner processes.
func TestLiveManagerKernelCrossProcessHandoff(t *testing.T) {
	device := os.Getenv("S0_NBD_LIVE_TEST_DEVICE")
	if device == "" {
		t.Skip("requires isolated Linux NBD fixture")
	}
	require.Zero(t, os.Geteuid())
	root, err := os.MkdirTemp("/tmp", "s0-live-manager-")
	require.NoError(t, err)
	defer os.RemoveAll(root)
	image, err := os.Create(filepath.Join(root, "base.xfs"))
	require.NoError(t, err)
	require.NoError(t, image.Truncate(512<<20))
	output, err := exec.Command("mkfs.xfs", "-f", image.Name()).CombinedOutput()
	require.NoError(t, err, "%s", output)
	baseMount := filepath.Join(root, "base-mount")
	require.NoError(t, os.Mkdir(baseMount, 0700))
	output, err = exec.Command("mount", "-o", "loop,nouuid", image.Name(), baseMount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	for _, name := range []string{"lower", "upper", "work"} {
		require.NoError(t, os.Mkdir(filepath.Join(baseMount, name), 0755))
	}
	output, err = exec.Command("umount", baseMount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	objects := liveFixtureObjects(filepath.Join(root, "objects"))
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), image, 512<<20, objects, rootfsblock.BuildOptions{})
	require.NoError(t, err)
	require.NoError(t, image.Close())
	stage := testStageRequest(t, newSessionObjectStore(), "real")
	stage.Generation.BaseBlockRoot, stage.Generation.CurrentBlockHead = built.Descriptor.MappingRoot.RootDigest, built.Descriptor.MappingRoot.RootDigest
	stage.Generation.Descriptor = built.Payload
	data, err := json.Marshal(stage)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "stage.json"), data, 0600))
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(root, "owner.sock"), Net: "unix"})
	require.NoError(t, err)
	defer listener.Close()
	start := func(files []*os.File, manifest LiveHandoff) (*exec.Cmd, *net.UnixConn, string) {
		t.Helper()
		data, err := json.Marshal(manifest)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "manifest.json"), data, 0600))
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLiveManagerOwnerHelper$", "-test.v")
		command.Env = append(os.Environ(), "S0_LIVE_MANAGER_HELPER=1", "S0_LIVE_MANAGER_ROOT="+root, "S0_LIVE_MANAGER_DEVICE="+device)
		if files != nil {
			command.ExtraFiles = files
			command.Env = append(command.Env, "S0_LIVE_MANAGER_ADOPT=1")
		}
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		require.NoError(t, command.Start())
		require.NoError(t, listener.SetDeadline(time.Now().Add(30*time.Second)))
		connection, err := listener.AcceptUnix()
		require.NoError(t, err)
		require.NoError(t, connection.SetDeadline(time.Now().Add(30*time.Second)))
		var ready liveManagerMessage
		received, err := livehandoff.Receive(connection, &ready)
		require.NoError(t, err)
		require.Empty(t, received)
		require.Equal(t, "ready", ready.Action)
		return command, connection, ready.Mount
	}
	owner, control, mount := start(nil, LiveHandoff{})
	writer, err := os.OpenFile(filepath.Join(mount, "continuous"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	defer writer.Close()
	stop, result := make(chan struct{}), make(chan error, 1)
	var writes atomic.Uint64
	go func() {
		for {
			select {
			case <-stop:
				result <- nil
				return
			default:
			}
			if _, err := fmt.Fprintln(writer, writes.Load()); err != nil {
				result <- err
				return
			}
			if err := writer.Sync(); err != nil {
				result <- err
				return
			}
			writes.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	for cycle := range 3 {
		before := writes.Load()
		require.Eventually(t, func() bool { return writes.Load() > before+3 }, 10*time.Second, time.Millisecond)
		require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "prepare"}, nil))
		var prepared liveManagerMessage
		files, err := livehandoff.Receive(control, &prepared)
		require.NoError(t, err)
		require.Len(t, files, 2)
		if cycle == 0 {
			require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "abort"}, nil))
			_, err = livehandoff.Receive(control, &prepared)
			require.NoError(t, err)
			require.NoError(t, livehandoff.CloseFiles(files))
			require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "prepare"}, nil))
			files, err = livehandoff.Receive(control, &prepared)
			require.NoError(t, err)
		}
		candidateRuntime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{device}, TransferableNBD: true})
		require.NoError(t, err)
		require.NoError(t, PreflightLiveHandoff(t.Context(), Config{
			BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"),
			Source: objects, Runtime: candidateRuntime,
		}, prepared.Manifest, files))
		require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "commit"}, nil))
		require.NoError(t, owner.Wait())
		require.NoError(t, control.Close())
		owner, control, _ = start(files, prepared.Manifest)
		require.NoError(t, livehandoff.CloseFiles(files))
		require.Eventually(t, func() bool { return writes.Load() > before+8 }, 10*time.Second, time.Millisecond)
	}
	close(stop)
	require.NoError(t, <-result)
	require.NoError(t, writer.Close())
	require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "close"}, nil))
	require.NoError(t, owner.Wait())
	require.NoError(t, control.Close())
	t.Logf("complete session manager kept original NBD/XFS/Overlay through three old-owner exits; durable writes=%d", writes.Load())
}

func TestLiveManagerOwnerHelper(t *testing.T) {
	if os.Getenv("S0_LIVE_MANAGER_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	root := os.Getenv("S0_LIVE_MANAGER_ROOT")
	require.True(t, strings.HasPrefix(root, "/tmp/s0-live-manager-"))
	data, err := os.ReadFile(filepath.Join(root, "stage.json"))
	require.NoError(t, err)
	var stage rootfshandoff.StageRequest
	require.NoError(t, json.Unmarshal(data, &stage))
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{os.Getenv("S0_LIVE_MANAGER_DEVICE")}, TransferableNBD: true})
	require.NoError(t, err)
	objects := liveFixtureObjects(filepath.Join(root, "objects"))
	manager, err := New(Config{StatePath: filepath.Join(root, "sessions.db"), BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"), Source: objects, Publisher: objects, Runtime: runtime})
	require.NoError(t, err)
	defer manager.Close()
	if os.Getenv("S0_LIVE_MANAGER_ADOPT") == "1" {
		data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
		require.NoError(t, err)
		var manifest LiveHandoff
		require.NoError(t, json.Unmarshal(data, &manifest))
		files := []*os.File{os.NewFile(3, "socket"), os.NewFile(4, "index")}
		require.NoError(t, manager.AdoptLiveHandoff(t.Context(), manifest, files))
		require.NoError(t, livehandoff.CloseFiles(files))
	} else {
		_, err := manager.Ensure(t.Context(), stage)
		require.NoError(t, err)
		namespace, err := os.Readlink("/proc/self/ns/mnt")
		require.NoError(t, err)
		require.NoError(t, manager.RegisterConsumer(stage.Parent, stage.Identity, ConsumerRegistration{LeaseID: "driver", ActiveKey: "task", ContainerID: "guest", StableMount: "/task/root", HostMountNamespace: namespace, LeaseExpiresAt: time.Now().Add(2 * time.Minute).Format(time.RFC3339Nano), RenewalProtocol: 1, OwnerProcess: "fixture-driver"}))
	}
	current, err := manager.load(stage.Parent)
	require.NoError(t, err)
	control, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(root, "owner.sock"), Net: "unix"})
	require.NoError(t, err)
	defer control.Close()
	require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "ready", Mount: current.MergedRoot}, nil))
	var prepared *PreparedLiveHandoff
	for {
		var request liveManagerMessage
		files, err := livehandoff.Receive(control, &request)
		require.NoError(t, err)
		require.Empty(t, files)
		switch request.Action {
		case "prepare":
			prepared, err = manager.PrepareLiveHandoff(t.Context())
			require.NoError(t, err)
			require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "prepared", Manifest: prepared.Manifest}, prepared.Files))
		case "abort":
			prepared.Abort()
			require.NoError(t, livehandoff.Send(control, liveManagerMessage{Action: "ready"}, nil))
		case "commit":
			require.NoError(t, prepared.Commit())
			require.NoError(t, livehandoff.CloseFiles(prepared.Files))
			return
		case "close":
			require.NoError(t, manager.Release(t.Context(), stage.Identity))
			return
		default:
			t.Fatalf("unknown action %s", request.Action)
		}
	}
}
