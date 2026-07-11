//go:build linux

package ha

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"github.com/sandbox0-ai/sandbox0/pkg/fuseportal"
	"golang.org/x/sys/unix"
)

const processFailoverChildEnv = "CTLD_HA_FAILOVER_CHILD"

type processFailoverFS struct {
	fuse.RawFileSystem
	mu   sync.Mutex
	data []byte
}

func newProcessFailoverFS(data string) *processFailoverFS {
	return &processFailoverFS{RawFileSystem: fuse.NewDefaultRawFileSystem(), data: []byte(data)}
}

func (fs *processFailoverFS) String() string { return "ctld-ha-process-failover" }

func (fs *processFailoverFS) Lookup(_ <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if header.NodeId != 1 || name != "value" {
		return fuse.ENOENT
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out.NodeId = 2
	out.Generation = 1
	out.Ino = 2
	out.Mode = syscall.S_IFREG | 0o644
	out.Size = uint64(len(fs.data))
	return fuse.OK
}

func (fs *processFailoverFS) GetAttr(_ <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out.Ino = input.NodeId
	if input.NodeId == 1 {
		out.Mode = syscall.S_IFDIR | 0o755
		out.Nlink = 2
		return fuse.OK
	}
	if input.NodeId != 2 {
		return fuse.ENOENT
	}
	out.Mode = syscall.S_IFREG | 0o644
	out.Nlink = 1
	out.Size = uint64(len(fs.data))
	return fuse.OK
}

func (fs *processFailoverFS) Open(_ <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if input.NodeId != 2 {
		return fuse.ENOENT
	}
	out.Fh = 1
	out.OpenFlags = fuse.FOPEN_DIRECT_IO
	return fuse.OK
}

func (fs *processFailoverFS) Read(_ <-chan struct{}, input *fuse.ReadIn, _ []byte) (fuse.ReadResult, fuse.Status) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if input.NodeId != 2 {
		return nil, fuse.ENOENT
	}
	if input.Offset >= uint64(len(fs.data)) {
		return fuse.ReadResultData(nil), fuse.OK
	}
	end := input.Offset + uint64(input.Size)
	if end > uint64(len(fs.data)) {
		end = uint64(len(fs.data))
	}
	return fuse.ReadResultData(append([]byte(nil), fs.data[input.Offset:end]...)), fuse.OK
}

func TestCtldHAProcessFailoverKeepsFuseMount(t *testing.T) {
	if os.Getenv(processFailoverChildEnv) == "1" {
		runProcessFailoverChild(t)
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root")
	}
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("FUSE device unavailable: %v", err)
	}
	preflightMount := t.TempDir()
	preflight, err := fuseportal.Mount(newProcessFailoverFS("preflight"), preflightMount, processFailoverMountOptions())
	if err != nil {
		t.Skipf("FUSE mount capability unavailable: %v", err)
	}
	if err := preflight.Unmount(); err != nil {
		t.Fatalf("unmount FUSE preflight: %v", err)
	}

	root := t.TempDir()
	mountPoint := t.TempDir()
	t.Cleanup(func() { _ = unix.Unmount(mountPoint, unix.MNT_DETACH) })
	readyPath := filepath.Join(t.TempDir(), "ready")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCtldHAProcessFailoverKeepsFuseMount$")
	cmd.Env = append(os.Environ(),
		processFailoverChildEnv+"=1",
		"CTLD_HA_TEST_ROOT="+root,
		"CTLD_HA_TEST_MOUNT="+mountPoint,
		"CTLD_HA_TEST_READY="+readyPath,
	)
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe() error = %v", err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatalf("start primary child: %v", err)
	}
	childDone := false
	t.Cleanup(func() {
		if !childDone && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	waitForPath(t, readyPath)
	assertProcessFailoverContents(t, filepath.Join(mountPoint, "value"), "primary")

	standbyCoordinator := newTestCoordinator(t, root, "b")
	standbyResult := waitForPrimaryAsync(context.Background(), standbyCoordinator)
	waitForCoordinatorSync(t, standbyCoordinator)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill primary child: %v", err)
	}
	childOutput, _ := io.ReadAll(output)
	if err := cmd.Wait(); err == nil {
		t.Fatal("primary child exited cleanly after SIGKILL")
	}
	childDone = true
	promoted := receivePrimary(t, standbyResult)
	defer promoted.Close()
	if len(promoted.Recovery) != 1 {
		t.Fatalf("recovered portals = %d, want 1; child output: %s", len(promoted.Recovery), childOutput)
	}
	recovered := &promoted.Recovery[0]
	standbyFS := newProcessFailoverFS("standby")
	server, err := fuseportal.Attach(standbyFS, recovered.Channel, mountPoint, recovered.Manifest.InitRequest, processFailoverMountOptions())
	if err != nil {
		t.Fatalf("Attach(promoted) error = %v; child output: %s", err, childOutput)
	}
	_ = recovered.Channel.Close()
	recovered.Channel = nil
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve() }()
	assertProcessFailoverContents(t, filepath.Join(mountPoint, "value"), "standby")
	if err := server.Unmount(); err != nil {
		t.Fatalf("Unmount(promoted) error = %v", err)
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("promoted Serve() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("promoted FUSE server did not stop")
	}
}

func runProcessFailoverChild(t *testing.T) {
	root := os.Getenv("CTLD_HA_TEST_ROOT")
	mountPoint := os.Getenv("CTLD_HA_TEST_MOUNT")
	readyPath := os.Getenv("CTLD_HA_TEST_READY")
	coordinator := newTestCoordinator(t, root, "a")
	lease, err := coordinator.WaitForPrimary(context.Background())
	if err != nil {
		t.Fatalf("child WaitForPrimary() error = %v", err)
	}
	defer lease.Close()
	fs := newProcessFailoverFS("primary")
	server, err := fuseportal.Mount(fs, mountPoint, processFailoverMountOptions())
	if err != nil {
		t.Fatalf("child Mount() error = %v", err)
	}
	go func() { _ = server.Serve() }()
	manifest := testManifest("pod-process\x00workspace")
	manifest.TargetPath = mountPoint
	manifest.RootFSBackingPath = filepath.Join(root, "rootfs")
	manifest.InitRequest = server.InitRequest()
	lease.Replicator.SetSnapshotProvider(func(ctx context.Context, target ctldportal.PortalReplicator) error {
		channel, err := server.CloneChannel()
		if err != nil {
			return err
		}
		defer channel.Close()
		return target.Publish(ctx, manifest, channel)
	})
	if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
		t.Fatalf("write child readiness: %v", err)
	}
	select {}
}

func processFailoverMountOptions() *fuse.MountOptions {
	return &fuse.MountOptions{
		Name: "ctld-ha-process-failover", FsName: "ctld-ha-process-failover",
		MaxWrite: 128 * 1024, MaxBackground: 16,
	}
}

func waitForPath(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("path %s was not created", path)
}

func waitForCoordinatorSync(t *testing.T, coordinator *Coordinator) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if state := coordinator.State(); state.Role == RoleStandby && state.Synchronized {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("coordinator state = %#v, want synchronized standby", coordinator.State())
}

func assertProcessFailoverContents(t *testing.T, path, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		payload, err := os.ReadFile(path)
		if err == nil && string(payload) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	payload, err := os.ReadFile(path)
	t.Fatalf("ReadFile(%s) = %q, %v; want %q", path, payload, err, want)
}
