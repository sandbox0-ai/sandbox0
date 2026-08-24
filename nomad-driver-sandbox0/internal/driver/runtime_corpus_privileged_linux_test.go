//go:build linux

package driver

import (
	"bytes"
	"context"
	"debug/elf"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

const (
	runtimeCorpusEnabledEnv = "SANDBOX0_RUN_PRIVILEGED_RUNTIME_CORPUS"
	runtimeCorpusRootFSEnv  = "SANDBOX0_RUNTIME_CORPUS_ROOTFS"
	runtimeCorpusPayloadEnv = "SANDBOX0_RUNTIME_CORPUS_PAYLOAD"
	runtimeCorpusTimeoutEnv = "SANDBOX0_RUNTIME_CORPUS_LANE_TIMEOUT"
	runtimeCorpusTimeout    = 45 * time.Second
	maxRuntimeCorpusTimeout = 10 * time.Minute
)

type runtimeCorpusReport struct {
	ReadWrite        bool     `json:"read_write"`
	MmapSync         bool     `json:"mmap_sync"`
	Xattr            bool     `json:"xattr"`
	Hardlink         bool     `json:"hardlink"`
	Symlink          bool     `json:"symlink"`
	RenameDirFD      bool     `json:"rename_dirfd"`
	SparseHole       bool     `json:"sparse_hole"`
	Truncate         bool     `json:"truncate"`
	Inotify          bool     `json:"inotify"`
	Chown            bool     `json:"chown"`
	UnixRights       bool     `json:"unix_rights"`
	Whiteout         bool     `json:"whiteout"`
	RootIsolated     bool     `json:"root_isolated"`
	HostUDSIsolated  bool     `json:"host_uds_isolated"`
	HostFIFOIsolated bool     `json:"host_fifo_isolated"`
	MountDenied      bool     `json:"mount_denied"`
	MknodDenied      bool     `json:"mknod_denied"`
	NetworkIsolated  bool     `json:"network_isolated"`
	Errors           []string `json:"errors,omitempty"`
}

// TestPrivilegedRuntimeGoldenCorpus runs the same filesystem and isolation
// contract through stock runsc DirectFS, stock runsc Gofer, and runc. The test
// is opt-in because it requires root, mount namespaces, and external runtime
// binaries.
func TestPrivilegedRuntimeGoldenCorpus(t *testing.T) {
	if os.Getenv(runtimeCorpusEnabledEnv) != "1" {
		t.Skipf("set %s=1 to run the privileged runtime corpus", runtimeCorpusEnabledEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatalf("%s requires root", t.Name())
	}
	laneTimeout, err := parseRuntimeCorpusLaneTimeout(os.Getenv(runtimeCorpusTimeoutEnv))
	if err != nil {
		t.Fatalf("%s: %v", runtimeCorpusTimeoutEnv, err)
	}
	baseRoot := strings.TrimSpace(os.Getenv(runtimeCorpusRootFSEnv))
	if !filepath.IsAbs(baseRoot) || filepath.Clean(baseRoot) == "/" {
		t.Fatalf("%s must name a non-root absolute extracted OCI rootfs", runtimeCorpusRootFSEnv)
	}
	for _, path := range []string{"bin/sh", "proc", "dev", "sys", "tmp"} {
		if _, err := os.Stat(filepath.Join(baseRoot, path)); err != nil {
			t.Fatalf("runtime corpus rootfs lacks %s: %v", path, err)
		}
	}
	runscPath, err := exec.LookPath("runsc")
	if err != nil {
		t.Fatalf("stock runsc is required: %v", err)
	}
	runcPath, err := exec.LookPath("runc")
	if err != nil {
		t.Fatalf("runc is required: %v", err)
	}
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve runtime corpus test binary: %v", err)
	}
	if static, err := staticELFBinary(testBinary); err != nil {
		t.Fatalf("inspect runtime corpus test binary: %v", err)
	} else if !static {
		t.Fatal("runtime corpus requires a static payload; rerun with CGO_ENABLED=0")
	}
	hostSecret := filepath.Join(t.TempDir(), "host-secret")
	if err := os.WriteFile(hostSecret, []byte("host-only"), 0o600); err != nil {
		t.Fatalf("write host isolation sentinel: %v", err)
	}
	hostSocket := hostSecret + ".sock"
	listener, err := net.Listen("unix", hostSocket)
	if err != nil {
		t.Fatalf("create host Unix socket sentinel: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	hostFIFO := hostSecret + ".fifo"
	if err := unix.Mkfifo(hostFIFO, 0o600); err != nil {
		t.Fatalf("create host FIFO sentinel: %v", err)
	}

	type lane struct {
		name     string
		runtime  string
		directFS bool
	}
	lanes := []lane{
		{name: "runsc-directfs", runtime: runscPath, directFS: true},
		{name: "runsc-gofer", runtime: runscPath, directFS: false},
		{name: "runc", runtime: runcPath},
	}
	reports := make(map[string]runtimeCorpusReport, len(lanes))
	allPassed := true
	for _, lane := range lanes {
		if !t.Run(lane.name, func(t *testing.T) {
			reports[lane.name] = runPrivilegedRuntimeCorpusLane(
				t, lane.runtime, lane.directFS, baseRoot, testBinary, hostSecret, laneTimeout,
			)
		}) {
			allPassed = false
		}
	}
	if !allPassed {
		return
	}
	want := reports[lanes[0].name]
	for _, lane := range lanes[1:] {
		if !reflect.DeepEqual(reports[lane.name], want) {
			t.Fatalf("runtime corpus differs: %s=%+v %s=%+v", lanes[0].name, want, lane.name, reports[lane.name])
		}
	}
}

func runPrivilegedRuntimeCorpusLane(
	t *testing.T,
	runtimePath string,
	directFS bool,
	baseRoot string,
	testBinary string,
	hostSecret string,
	laneTimeout time.Duration,
) runtimeCorpusReport {
	t.Helper()
	root := t.TempDir()
	upper := filepath.Join(root, "upper")
	work := filepath.Join(root, "work")
	merged := filepath.Join(root, "merged")
	bundle := filepath.Join(root, "bundle")
	runtimeRoot := filepath.Join(root, "runtime")
	for _, path := range []string{upper, work, merged, bundle, runtimeRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("create runtime lane path: %v", err)
		}
	}
	t.Cleanup(func() {
		_ = unix.Unmount(filepath.Join(runtimeRoot, "null-netns"), unix.MNT_DETACH)
	})
	if err := copyCorpusBinary(testBinary, filepath.Join(upper, "runtime-corpus")); err != nil {
		t.Fatalf("copy runtime corpus payload: %v", err)
	}
	if err := os.WriteFile(filepath.Join(baseRoot, "whiteout-victim"), []byte("lower"), 0o600); err != nil {
		t.Fatalf("create lower whiteout fixture: %v", err)
	}
	if err := unix.Mount("overlay", merged, "overlay", 0,
		"lowerdir="+baseRoot+",upperdir="+upper+",workdir="+work); err != nil {
		t.Fatalf("mount runtime corpus overlay: %v", err)
	}
	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = unix.Unmount(merged, unix.MNT_DETACH)
		}
	})

	spec := runtimeCorpusSpec(merged, hostSecret)
	payload, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatalf("encode runtime corpus OCI spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0o600); err != nil {
		t.Fatalf("write runtime corpus OCI spec: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), laneTimeout)
	defer cancel()
	containerID := "s0-corpus-" + strings.ReplaceAll(t.Name(), "/", "-")
	var runtimeErr error
	if filepath.Base(runtimePath) == "runsc" {
		runner := NewCommandRunsc(PluginConfig{
			RunscPath: runtimePath, RunscRoot: runtimeRoot,
			Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: directFS,
		})
		defer runner.Delete(context.Background(), containerID, true)
		if err := runner.Create(ctx, bundle, containerID); err != nil {
			t.Fatalf("create runsc corpus container: %v", err)
		}
		if err := runner.Start(ctx, containerID); err != nil {
			t.Fatalf("start runsc corpus container: %v", err)
		}
		result, err := runner.Wait(ctx, containerID)
		if err != nil {
			t.Fatalf("wait runsc corpus container: %v", err)
		}
		if result.ExitStatus != 0 {
			runtimeErr = fmt.Errorf("runsc corpus exit status = %d", result.ExitStatus)
		}
		if err := runner.Delete(ctx, containerID, false); err != nil {
			t.Fatalf("delete runsc corpus container: %v", err)
		}
	} else {
		defer exec.Command(runtimePath, "--root", runtimeRoot, "delete", "--force", containerID).Run()
		command := exec.CommandContext(ctx, runtimePath, "--root", runtimeRoot, "run", "--bundle", bundle, containerID)
		output, err := command.CombinedOutput()
		if err != nil {
			runtimeErr = fmt.Errorf("run runc corpus container: %v: %s", err, output)
		}
	}

	reportPayload, err := os.ReadFile(filepath.Join(merged, "runtime-corpus-report.json"))
	if err != nil {
		t.Fatalf("read runtime corpus report: %v", err)
	}
	var report runtimeCorpusReport
	if err := json.Unmarshal(reportPayload, &report); err != nil {
		t.Fatalf("decode runtime corpus report: %v", err)
	}
	if runtimeErr != nil || len(report.Errors) != 0 {
		t.Fatalf("runtime corpus failed: runtime=%v payload=%s", runtimeErr, strings.Join(report.Errors, "; "))
	}
	whiteoutPath := filepath.Join(upper, "whiteout-victim")
	if !overlayWhiteout(whiteoutPath) {
		t.Fatalf("runtime did not preserve OverlayFS whiteout in %s", whiteoutPath)
	}
	var sparseStat unix.Stat_t
	if err := unix.Stat(filepath.Join(upper, "corpus", "sparse"), &sparseStat); err != nil {
		t.Fatalf("inspect runtime sparse file: %v", err)
	}
	if sparseStat.Size != 1<<20 || sparseStat.Blocks*512 >= sparseStat.Size {
		t.Fatalf("runtime expanded sparse file: size=%d blocks=%d", sparseStat.Size, sparseStat.Blocks)
	}

	if err := unix.Unmount(merged, 0); err != nil {
		t.Fatalf("unmount runtime corpus overlay: %v", err)
	}
	mounted = false
	return report
}

func parseRuntimeCorpusLaneTimeout(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return runtimeCorpusTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", value, err)
	}
	if timeout <= 0 || timeout > maxRuntimeCorpusTimeout {
		return 0, fmt.Errorf("must be within (0, %s]", maxRuntimeCorpusTimeout)
	}
	return timeout, nil
}

func TestRuntimeCorpusLaneTimeout(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{name: "default", want: runtimeCorpusTimeout},
		{name: "software emulation", value: "8m", want: 8 * time.Minute},
		{name: "invalid", value: "not-a-duration", wantErr: true},
		{name: "zero", value: "0s", wantErr: true},
		{name: "over maximum", value: "11m", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parseRuntimeCorpusLaneTimeout(test.value)
			if test.wantErr {
				if err == nil {
					t.Fatalf("parseRuntimeCorpusLaneTimeout(%q) unexpectedly succeeded", test.value)
				}
				return
			}
			if err != nil || actual != test.want {
				t.Fatalf("parseRuntimeCorpusLaneTimeout(%q) = %s, %v; want %s", test.value, actual, err, test.want)
			}
		})
	}
}

func runtimeCorpusSpec(rootfs, hostSecret string) specs.Spec {
	capabilities := []string{
		"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID", "CAP_KILL",
		"CAP_NET_BIND_SERVICE", "CAP_SETFCAP", "CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_CHROOT",
	}
	return specs.Spec{
		Version: specs.Version,
		Process: &specs.Process{
			Terminal: false, User: specs.User{UID: 0, GID: 0},
			Args: []string{"/runtime-corpus", "-test.run=^TestRuntimeGoldenCorpusPayload$", "-test.v"},
			Env: []string{
				"PATH=/bin:/usr/bin:/sbin:/usr/sbin",
				runtimeCorpusPayloadEnv + "=1",
				"SANDBOX0_RUNTIME_CORPUS_REPORT=/runtime-corpus-report.json",
				"SANDBOX0_RUNTIME_CORPUS_HOST_SECRET=" + hostSecret,
				"SANDBOX0_RUNTIME_CORPUS_HOST_SOCKET=" + hostSecret + ".sock",
				"SANDBOX0_RUNTIME_CORPUS_HOST_FIFO=" + hostSecret + ".fifo",
			},
			Cwd: "/", NoNewPrivileges: true,
			Capabilities: &specs.LinuxCapabilities{
				Bounding: capabilities, Effective: capabilities, Permitted: capabilities,
			},
		},
		Root:     &specs.Root{Path: rootfs},
		Hostname: "sandbox0-corpus",
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
			{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
			{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace}, {Type: specs.MountNamespace},
			},
			MaskedPaths: []string{
				"/proc/acpi", "/proc/asound", "/proc/kcore", "/proc/keys", "/proc/latency_stats",
				"/proc/timer_list", "/proc/timer_stats", "/proc/sched_debug", "/sys/firmware", "/sys/devices/virtual/powercap",
			},
			ReadonlyPaths: []string{"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"},
		},
	}
}

func copyCorpusBinary(source, target string) error {
	payload, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	return os.WriteFile(target, payload, 0o755)
}

func staticELFBinary(path string) (bool, error) {
	file, err := elf.Open(path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	for _, program := range file.Progs {
		if program.Type == elf.PT_INTERP {
			return false, nil
		}
	}
	return true, nil
}

func overlayWhiteout(path string) bool {
	info, err := os.Lstat(path)
	if err == nil && info.Mode()&os.ModeCharDevice != 0 {
		return true
	}
	for _, attribute := range []string{"trusted.overlay.whiteout", "user.overlay.whiteout"} {
		if _, err := unix.Getxattr(path, attribute, nil); err == nil {
			return true
		}
	}
	return false
}

// TestRuntimeGoldenCorpusPayload is executed as PID 1 inside each OCI runtime
// lane by TestPrivilegedRuntimeGoldenCorpus.
func TestRuntimeGoldenCorpusPayload(t *testing.T) {
	if os.Getenv(runtimeCorpusPayloadEnv) != "1" {
		t.Skip("runtime corpus payload runs only inside an OCI lane")
	}
	report := runtimeCorpusReport{}
	check := func(name string, err error) bool {
		if err == nil {
			return true
		}
		report.Errors = append(report.Errors, name+": "+err.Error())
		return false
	}
	defer func() {
		payload, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(os.Getenv("SANDBOX0_RUNTIME_CORPUS_REPORT"), payload, 0o600)
		}
		if err != nil {
			t.Errorf("write runtime corpus report: %v", err)
		}
	}()

	root := "/corpus"
	check("mkdir corpus", os.MkdirAll(root, 0o755))
	report.ReadWrite = check("read/write", corpusReadWrite(root))
	report.MmapSync = check("mmap/msync", corpusMmap(root))
	report.Xattr = check("xattr", corpusXattr(root))
	report.Hardlink = check("hardlink", corpusHardlink(root))
	report.Symlink = check("symlink", corpusSymlink(root))
	report.RenameDirFD = check("rename dirfd", corpusRenameDirFD(root))
	report.SparseHole = check("sparse hole", corpusSparseHole(root))
	report.Truncate = check("truncate", corpusTruncate(root))
	report.Inotify = check("inotify", corpusInotify(root))
	report.Chown = check("chown", corpusChown(root))
	report.UnixRights = check("SCM_RIGHTS", corpusUnixRights(root))
	report.Whiteout = check("whiteout", os.Remove("/whiteout-victim"))
	report.RootIsolated = check("root isolation", corpusRootIsolation(os.Getenv("SANDBOX0_RUNTIME_CORPUS_HOST_SECRET")))
	report.HostUDSIsolated = check("host Unix socket isolation", corpusHostUDSIsolation(os.Getenv("SANDBOX0_RUNTIME_CORPUS_HOST_SOCKET")))
	report.HostFIFOIsolated = check("host FIFO isolation", corpusHostFIFOIsolation(os.Getenv("SANDBOX0_RUNTIME_CORPUS_HOST_FIFO")))
	mountTarget := filepath.Join(root, "mount-denied")
	check("mkdir mount target", os.Mkdir(mountTarget, 0o700))
	mountErr := unix.Mount("tmpfs", mountTarget, "tmpfs", 0, "size=4096")
	report.MountDenied = errors.Is(mountErr, unix.EPERM)
	if mountErr == nil {
		_ = unix.Unmount(mountTarget, 0)
	}
	report.MknodDenied = errors.Is(
		unix.Mknod(filepath.Join(root, "mknod-denied"), unix.S_IFCHR|0o600, int(unix.Mkdev(1, 3))), unix.EPERM,
	)
	report.NetworkIsolated = check("network isolation", corpusNetworkIsolation())
	if !report.MountDenied {
		report.Errors = append(report.Errors, "mount without CAP_SYS_ADMIN was not denied")
	}
	if !report.MknodDenied {
		report.Errors = append(report.Errors, "mknod without CAP_MKNOD was not denied")
	}
	if len(report.Errors) != 0 {
		t.Fatalf("runtime corpus failed: %s", strings.Join(report.Errors, "; "))
	}
}

func corpusReadWrite(root string) error {
	path := filepath.Join(root, "read-write")
	want := []byte("sandbox0-runtime-corpus")
	if err := os.WriteFile(path, want, 0o640); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return err
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("content mismatch")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) == 0 {
		return fmt.Errorf("readdir: entries=%d error=%w", len(entries), err)
	}
	return nil
}

func corpusMmap(root string) error {
	path := filepath.Join(root, "mmap")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Truncate(4096); err != nil {
		return err
	}
	payload, err := unix.Mmap(int(file.Fd()), 0, 4096, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return err
	}
	copy(payload, "mmap-durable")
	err = unix.Msync(payload, unix.MS_SYNC)
	unmapErr := unix.Munmap(payload)
	if err != nil {
		return err
	}
	if unmapErr != nil {
		return unmapErr
	}
	got := make([]byte, len("mmap-durable"))
	if _, err := file.ReadAt(got, 0); err != nil {
		return err
	}
	if string(got) != "mmap-durable" {
		return fmt.Errorf("mmap content mismatch")
	}
	return nil
}

func corpusXattr(root string) error {
	path := filepath.Join(root, "xattr")
	if err := os.WriteFile(path, []byte("xattr"), 0o600); err != nil {
		return err
	}
	if err := unix.Setxattr(path, "user.sandbox0", []byte("value"), 0); err != nil {
		return err
	}
	payload := make([]byte, 32)
	length, err := unix.Getxattr(path, "user.sandbox0", payload)
	if err != nil {
		return err
	}
	if string(payload[:length]) != "value" {
		return fmt.Errorf("xattr content mismatch")
	}
	return unix.Removexattr(path, "user.sandbox0")
}

func corpusHardlink(root string) error {
	source := filepath.Join(root, "hardlink-source")
	target := filepath.Join(root, "hardlink-target")
	if err := os.WriteFile(source, []byte("linked"), 0o600); err != nil {
		return err
	}
	if err := os.Link(source, target); err != nil {
		return err
	}
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return err
	}
	targetInfo, err := os.Stat(target)
	if err != nil {
		return err
	}
	if !os.SameFile(sourceInfo, targetInfo) {
		return fmt.Errorf("hardlink inode mismatch")
	}
	return nil
}

func corpusSymlink(root string) error {
	target := filepath.Join(root, "symlink-target")
	link := filepath.Join(root, "symlink")
	if err := os.WriteFile(target, []byte("symlink"), 0o600); err != nil {
		return err
	}
	if err := os.Symlink("symlink-target", link); err != nil {
		return err
	}
	payload, err := os.ReadFile(link)
	if err != nil {
		return err
	}
	if string(payload) != "symlink" {
		return fmt.Errorf("symlink content mismatch")
	}
	return nil
}

func corpusRenameDirFD(root string) error {
	original := filepath.Join(root, "dirfd-old")
	renamed := filepath.Join(root, "dirfd-new")
	if err := os.Mkdir(original, 0o700); err != nil {
		return err
	}
	directory, err := os.Open(original)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err := os.Rename(original, renamed); err != nil {
		return err
	}
	fd, err := unix.Openat(int(directory.Fd()), "after-rename", unix.O_CREAT|unix.O_WRONLY|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	if err := unix.Close(fd); err != nil {
		return err
	}
	_, err = os.Stat(filepath.Join(renamed, "after-rename"))
	return err
}

func corpusSparseHole(root string) error {
	path := filepath.Join(root, "sparse")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteAt(bytes.Repeat([]byte{0x7f}, 4096), (1<<20)-4096); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	payload := make([]byte, 4096)
	if _, err := file.ReadAt(payload, 0); err != nil {
		return err
	}
	if !bytes.Equal(payload, make([]byte, len(payload))) {
		return fmt.Errorf("sparse leading range is not a hole")
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Size != 1<<20 {
		return fmt.Errorf("sparse size=%d", stat.Size)
	}
	return nil
}

func corpusTruncate(root string) error {
	path := filepath.Join(root, "truncate")
	if err := os.WriteFile(path, bytes.Repeat([]byte{0x41}, 8192), 0o600); err != nil {
		return err
	}
	if err := os.Truncate(path, 1024); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() != 1024 {
		return fmt.Errorf("truncate size=%d", info.Size())
	}
	return nil
}

func corpusInotify(root string) error {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if _, err := unix.InotifyAddWatch(fd, root, unix.IN_CREATE); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(root, "inotify-created"), []byte("event"), 0o600); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Second)
	payload := make([]byte, 4096)
	for time.Now().Before(deadline) {
		if count, readErr := unix.Read(fd, payload); count > 0 {
			return nil
		} else if readErr != nil && !errors.Is(readErr, unix.EAGAIN) {
			return readErr
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("inotify event timed out")
}

func corpusChown(root string) error {
	path := filepath.Join(root, "chown")
	if err := os.WriteFile(path, []byte("owner"), 0o600); err != nil {
		return err
	}
	if err := os.Chown(path, 12345, 23456); err != nil {
		return err
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return err
	}
	if stat.Uid != 12345 || stat.Gid != 23456 {
		return fmt.Errorf("owner=%d:%d", stat.Uid, stat.Gid)
	}
	return nil
}

func corpusUnixRights(root string) error {
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(sockets[0])
	defer unix.Close(sockets[1])
	file, err := os.Open(filepath.Join(root, "read-write"))
	if err != nil {
		return err
	}
	defer file.Close()
	if err := unix.Sendmsg(sockets[0], []byte("f"), unix.UnixRights(int(file.Fd())), nil, 0); err != nil {
		return err
	}
	payload := make([]byte, 1)
	control := make([]byte, unix.CmsgSpace(4))
	_, controlLength, _, _, err := unix.Recvmsg(sockets[1], payload, control, 0)
	if err != nil {
		return err
	}
	messages, err := unix.ParseSocketControlMessage(control[:controlLength])
	if err != nil || len(messages) != 1 {
		return fmt.Errorf("parse SCM_RIGHTS: messages=%d error=%w", len(messages), err)
	}
	descriptors, err := unix.ParseUnixRights(&messages[0])
	if err != nil || len(descriptors) != 1 {
		return fmt.Errorf("parse received descriptor: descriptors=%d error=%w", len(descriptors), err)
	}
	defer unix.Close(descriptors[0])
	return nil
}

func corpusRootIsolation(hostSecret string) error {
	root, err := os.Readlink("/proc/self/root")
	if err != nil || root != "/" {
		return fmt.Errorf("proc root=%q error=%w", root, err)
	}
	if _, err := os.ReadFile(hostSecret); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("host sentinel was reachable: %v", err)
	}
	if _, err := os.ReadFile(filepath.Join("/proc/1/root", hostSecret)); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("host sentinel was reachable through /proc/1/root: %v", err)
	}
	mountInfo, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	if bytes.Contains(mountInfo, []byte(filepath.Dir(hostSecret))) {
		return fmt.Errorf("host sentinel path leaked through mountinfo")
	}
	return nil
}

func corpusHostUDSIsolation(path string) error {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("host Unix socket path was visible: %v", err)
	}
	connection, err := net.DialTimeout("unix", path, 100*time.Millisecond)
	if err == nil {
		_ = connection.Close()
		return fmt.Errorf("host Unix socket accepted a connection")
	}
	return nil
}

func corpusHostFIFOIsolation(path string) error {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("host FIFO path was visible: %v", err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("host FIFO opened")
	}
	if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("host FIFO isolation error: %w", err)
	}
	return nil
}

func corpusNetworkIsolation() error {
	interfaces, err := net.Interfaces()
	if err != nil {
		return err
	}
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagLoopback == 0 {
			return fmt.Errorf("non-loopback interface %q is visible", networkInterface.Name)
		}
	}
	return nil
}
