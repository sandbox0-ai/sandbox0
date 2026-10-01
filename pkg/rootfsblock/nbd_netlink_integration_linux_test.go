//go:build linux

package rootfsblock

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/livehandoff"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type zeroBranchBase struct{}

func (zeroBranchBase) ReadAt(data []byte, _ int64) (int, error) { clear(data); return len(data), nil }

type nbdOwnerMessage struct {
	Action   string `json:"action"`
	Sequence uint64 `json:"sequence"`
}

// This test is explicitly opt-in and destructive to the named unused test
// device. Run only on an isolated test host, never against a production pool.
func TestKernelNBDLiveCrossProcessHandoff(t *testing.T) {
	devicePath := os.Getenv("S0_NBD_LIVE_TEST_DEVICE")
	if devicePath == "" {
		t.Skip("requires an isolated Linux NBD fixture")
	}
	require.Zero(t, os.Geteuid())
	root, err := os.MkdirTemp("/tmp", "s0-live-nbd-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(root, "owner.sock"), Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	start := func(sockets []*os.File, sequence uint64) (*exec.Cmd, *net.UnixConn) {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestKernelNBDLiveOwnerHelper$", "-test.v")
		command.Env = append(os.Environ(), "S0_NBD_OWNER_HELPER=1", "S0_NBD_OWNER_ROOT="+root,
			"S0_NBD_OWNER_DEVICE="+devicePath, "S0_NBD_OWNER_SEQUENCE="+strconv.FormatUint(sequence, 10))
		if sockets != nil {
			command.ExtraFiles = sockets
			command.Env = append(command.Env, "S0_NBD_OWNER_ADOPT=1")
		}
		command.Stdout, command.Stderr = os.Stdout, os.Stderr
		require.NoError(t, command.Start())
		require.NoError(t, listener.SetDeadline(time.Now().Add(20*time.Second)))
		connection, err := listener.AcceptUnix()
		require.NoError(t, err)
		require.NoError(t, livehandoff.RequireRoot(connection))
		require.NoError(t, connection.SetDeadline(time.Now().Add(20*time.Second)))
		var ready nbdOwnerMessage
		files, err := livehandoff.Receive(connection, &ready)
		require.NoError(t, err)
		require.Empty(t, files)
		require.Equal(t, "ready", ready.Action)
		return command, connection
	}
	owner, control := start(nil, 0)
	output, err := exec.Command("mkfs.xfs", "-f", devicePath).CombinedOutput()
	require.NoError(t, err, string(output))
	mountRoot := filepath.Join(root, "mount")
	require.NoError(t, os.Mkdir(mountRoot, 0700))
	require.NoError(t, unix.Mount(devicePath, mountRoot, "xfs", unix.MS_NOSUID|unix.MS_NODEV, "nouuid"))
	t.Cleanup(func() { _ = unix.Unmount(mountRoot, 0) })
	checkGuest, stopGuest := startLiveNBDGuest(t, root, mountRoot)
	writer, err := os.OpenFile(filepath.Join(mountRoot, "continuous"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })
	unlinked, err := os.OpenFile(filepath.Join(mountRoot, "unlinked"), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unlinked.Close() })
	_, err = unlinked.WriteString("retained-open-description")
	require.NoError(t, err)
	require.NoError(t, os.Remove(unlinked.Name()))
	stop := make(chan struct{})
	writeResult := make(chan error, 1)
	var writes atomic.Uint64
	go func() {
		for {
			select {
			case <-stop:
				writeResult <- nil
				return
			default:
			}
			if _, err := fmt.Fprintf(writer, "%016d\n", writes.Load()); err != nil {
				writeResult <- err
				return
			}
			if err := writer.Sync(); err != nil {
				writeResult <- err
				return
			}
			writes.Add(1)
			time.Sleep(time.Millisecond)
		}
	}()
	for cycle := 0; cycle < 5; cycle++ {
		before := writes.Load()
		require.Eventually(t, func() bool { return writes.Load() > before+3 }, 10*time.Second, 5*time.Millisecond)
		// First exercise a rejected candidate: restore source admission.
		require.NoError(t, livehandoff.Send(control, nbdOwnerMessage{Action: "prepare"}, nil))
		var cut nbdOwnerMessage
		files, err := livehandoff.Receive(control, &cut)
		require.NoError(t, err)
		require.Len(t, files, 2)
		require.NoError(t, livehandoff.Send(control, nbdOwnerMessage{Action: "abort"}, nil))
		_, err = livehandoff.Receive(control, &cut)
		require.NoError(t, err)
		require.NoError(t, livehandoff.CloseFiles(files))
		require.NoError(t, livehandoff.Send(control, nbdOwnerMessage{Action: "prepare"}, nil))
		files, err = livehandoff.Receive(control, &cut)
		require.NoError(t, err)
		require.Len(t, files, 2)
		require.NoError(t, livehandoff.Send(control, nbdOwnerMessage{Action: "commit"}, nil))
		require.NoError(t, owner.Wait()) // No userspace storage owner remains.
		require.NoError(t, control.Close())
		time.Sleep(50 * time.Millisecond) // The mounted filesystem queues I/O.
		owner, control = start(files, cut.Sequence)
		require.NoError(t, livehandoff.CloseFiles(files))
		require.Eventually(t, func() bool { return writes.Load() > before+8 }, 10*time.Second, 5*time.Millisecond)
		payload := make([]byte, len("retained-open-description"))
		_, err = unlinked.ReadAt(payload, 0)
		require.NoError(t, err)
		require.Equal(t, "retained-open-description", string(payload))
		checkGuest()
	}
	close(stop)
	require.NoError(t, <-writeResult)
	require.NoError(t, writer.Close())
	require.NoError(t, unlinked.Close())
	stopGuest()
	require.NoError(t, unix.Unmount(mountRoot, 0))
	require.NoError(t, livehandoff.Send(control, nbdOwnerMessage{Action: "close"}, nil))
	require.NoError(t, owner.Wait())
	require.NoError(t, control.Close())
	t.Logf("same device and mounted XFS survived five source-process exits; durable writes=%d", writes.Load())
}

func TestKernelNBDLiveOwnerHelper(t *testing.T) {
	if os.Getenv("S0_NBD_OWNER_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	root, path := os.Getenv("S0_NBD_OWNER_ROOT"), os.Getenv("S0_NBD_OWNER_DEVICE")
	require.True(t, strings.HasPrefix(root, "/tmp/s0-live-nbd-"))
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: filepath.Join(root, "owner.sock"), Net: "unix"})
	require.NoError(t, err)
	defer connection.Close()
	require.NoError(t, livehandoff.RequireRoot(connection))
	options := BranchOptions{}
	if os.Getenv("S0_NBD_OWNER_ADOPT") == "1" {
		options.LiveIndex = os.NewFile(4, "inherited-live-index")
		defer options.LiveIndex.Close()
	}
	branch, err := OpenBranchWithOptions(filepath.Join(root, "branch.log"), testBranchIdentity(512<<20), zeroBranchBase{}, options)
	require.NoError(t, err)
	defer branch.Close()
	var device *KernelNBDDevice
	if os.Getenv("S0_NBD_OWNER_ADOPT") == "1" {
		sequence, err := branch.DurableSequence()
		require.NoError(t, err)
		require.Equal(t, os.Getenv("S0_NBD_OWNER_SEQUENCE"), strconv.FormatUint(sequence, 10))
		socket := os.NewFile(3, "inherited-nbd")
		device, err = AdoptKernelNBD(context.Background(), path, socket, branch)
		require.NoError(t, socket.Close())
		require.NoError(t, err)
	} else {
		device, err = StartKernelNBD(context.Background(), t.Context(), branch, KernelNBDOptions{DevicePath: path, Transferable: true})
		require.NoError(t, err)
	}
	require.NoError(t, livehandoff.Send(connection, nbdOwnerMessage{Action: "ready"}, nil))
	for {
		var request nbdOwnerMessage
		files, err := livehandoff.Receive(connection, &request)
		require.NoError(t, err)
		require.Empty(t, files)
		switch request.Action {
		case "prepare":
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			socket, err := device.PrepareHandoff(ctx)
			cancel()
			require.NoError(t, err)
			require.NoError(t, branch.Flush())
			sequence, err := branch.DurableSequence()
			require.NoError(t, err)
			index, err := branch.ExportLiveIndex()
			require.NoError(t, err)
			require.NoError(t, livehandoff.Send(connection, nbdOwnerMessage{Action: "prepared", Sequence: sequence}, []*os.File{socket, index}))
			require.NoError(t, index.Close())
			require.NoError(t, socket.Close())
		case "abort":
			device.AbortHandoff()
			require.NoError(t, livehandoff.Send(connection, nbdOwnerMessage{Action: "ready"}, nil))
		case "commit":
			require.NoError(t, device.CommitHandoff())
			require.NoError(t, device.Close())
			return
		case "close":
			require.NoError(t, device.Close())
			return
		default:
			t.Fatalf("unknown command %s", request.Action)
		}
	}
}
