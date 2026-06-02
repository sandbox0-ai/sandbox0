package snapshotter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfs"
	"golang.org/x/sys/unix"
)

const (
	defaultFuseOverlayBinary = "fuse-overlayfs"
	fuseOverlayMountTimeout  = 5 * time.Second
)

// OverlayMounter materializes an overlay mount and returns the containerd mount
// that should be used as the runtime rootfs.
type OverlayMounter interface {
	Mount(ctx context.Context, key string, overlay rootfs.Mount, prepared *ctldapi.PrepareRootFSResponse) (rootfs.Mount, error)
	Unmount(ctx context.Context, key string) error
	Close() error
}

type FuseOverlayMounter struct {
	binary string

	mu     sync.Mutex
	active map[string]*fuseOverlayMount
}

type fuseOverlayMount struct {
	key      string
	merged   string
	cmd      *exec.Cmd
	done     chan error
	log      *limitedBuffer
	unmount  func(string) error
	mounted  func(string) bool
	stopOnce sync.Once
}

func NewFuseOverlayMounter(binary string) *FuseOverlayMounter {
	binary = strings.TrimSpace(binary)
	if binary == "" {
		binary = defaultFuseOverlayBinary
	}
	return &FuseOverlayMounter{binary: binary}
}

func (m *FuseOverlayMounter) Mount(ctx context.Context, key string, overlay rootfs.Mount, prepared *ctldapi.PrepareRootFSResponse) (rootfs.Mount, error) {
	if m == nil {
		return rootfs.Mount{}, fmt.Errorf("rootfs overlay mounter is required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return rootfs.Mount{}, fmt.Errorf("snapshot key is required")
	}
	lowerDir := mountOptionValue(overlay.Options, "lowerdir=")
	if lowerDir == "" {
		return rootfs.Mount{}, fmt.Errorf("overlay lowerdir is required")
	}
	if prepared == nil {
		return rootfs.Mount{}, fmt.Errorf("prepared rootfs response is required")
	}
	upperDir := strings.TrimSpace(prepared.UpperDir)
	workDir := strings.TrimSpace(prepared.WorkDir)
	mergedDir, err := mergedDirForRootFS(prepared)
	if err != nil {
		return rootfs.Mount{}, err
	}
	if upperDir == "" || workDir == "" {
		return rootfs.Mount{}, fmt.Errorf("prepared rootfs upperdir and workdir are required")
	}

	m.mu.Lock()
	if m.active == nil {
		m.active = make(map[string]*fuseOverlayMount)
	}
	if active := m.active[key]; active != nil {
		mergedDir = active.merged
		m.mu.Unlock()
		return bindRootFSMount(overlay, mergedDir), nil
	}
	mount := &fuseOverlayMount{
		key:      key,
		merged:   mergedDir,
		log:      newLimitedBuffer(8192),
		unmount:  unixUnmount,
		mounted:  isMounted,
		stopOnce: sync.Once{},
	}
	m.active[key] = mount
	m.mu.Unlock()

	if err := mount.start(ctx, m.binary, lowerDir, upperDir, workDir); err != nil {
		_ = m.Unmount(context.Background(), key)
		return rootfs.Mount{}, err
	}
	return bindRootFSMount(overlay, mergedDir), nil
}

func (m *FuseOverlayMounter) Unmount(_ context.Context, key string) error {
	if m == nil {
		return nil
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	m.mu.Lock()
	mount := m.active[key]
	delete(m.active, key)
	m.mu.Unlock()
	if mount == nil {
		return nil
	}
	return mount.stop()
}

func (m *FuseOverlayMounter) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	active := make([]*fuseOverlayMount, 0, len(m.active))
	for key, mount := range m.active {
		active = append(active, mount)
		delete(m.active, key)
	}
	m.mu.Unlock()
	var errs []error
	for _, mount := range active {
		if err := mount.stop(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *fuseOverlayMount) start(ctx context.Context, binary, lowerDir, upperDir, workDir string) error {
	if err := os.MkdirAll(m.merged, 0o755); err != nil {
		return fmt.Errorf("create fuse-overlayfs merged dir: %w", err)
	}
	options := "lowerdir=" + lowerDir + ",upperdir=" + upperDir + ",workdir=" + workDir
	cmd := exec.Command(binary, "-f", "-o", options, m.merged)
	cmd.Stdout = m.log
	cmd.Stderr = m.log
	m.cmd = cmd
	m.done = make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start fuse-overlayfs: %w", err)
	}
	go func() {
		m.done <- cmd.Wait()
		close(m.done)
	}()

	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	timeoutCtx, cancel := context.WithTimeout(waitCtx, fuseOverlayMountTimeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.mounted(m.merged) {
			return nil
		}
		select {
		case err := <-m.done:
			return fmt.Errorf("fuse-overlayfs exited before mount was ready: %w: %s", err, strings.TrimSpace(m.log.String()))
		case <-timeoutCtx.Done():
			return fmt.Errorf("fuse-overlayfs mount did not become ready: %w: %s", timeoutCtx.Err(), strings.TrimSpace(m.log.String()))
		case <-ticker.C:
		}
	}
}

func (m *fuseOverlayMount) stop() error {
	var err error
	m.stopOnce.Do(func() {
		if m.mounted != nil && m.mounted(m.merged) {
			if m.unmount != nil {
				err = m.unmount(m.merged)
			}
		}
		if m.cmd != nil && m.cmd.Process != nil {
			_ = m.cmd.Process.Signal(syscall.SIGTERM)
		}
		if m.done != nil {
			select {
			case waitErr := <-m.done:
				if err == nil && waitErr != nil && !expectedProcessExit(waitErr) {
					err = waitErr
				}
			case <-time.After(2 * time.Second):
				if m.cmd != nil && m.cmd.Process != nil {
					_ = m.cmd.Process.Kill()
				}
				select {
				case waitErr := <-m.done:
					if err == nil && waitErr != nil && !expectedProcessExit(waitErr) {
						err = waitErr
					}
				case <-time.After(2 * time.Second):
					if err == nil {
						err = fmt.Errorf("fuse-overlayfs process did not exit after kill")
					}
				}
			}
		}
	})
	if err != nil {
		return fmt.Errorf("unmount fuse-overlayfs rootfs %s: %w", m.merged, err)
	}
	return nil
}

func expectedProcessExit(err error) bool {
	if err == nil {
		return true
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL)
}

func mergedDirForRootFS(prepared *ctldapi.PrepareRootFSResponse) (string, error) {
	mountPoint := strings.TrimSpace(prepared.MountPoint)
	if mountPoint != "" {
		return filepath.Join(filepath.Dir(mountPoint), "merged"), nil
	}
	upperDir := strings.TrimSpace(prepared.UpperDir)
	if upperDir == "" {
		return "", fmt.Errorf("prepared rootfs mount point or upperdir is required")
	}
	return filepath.Join(filepath.Dir(filepath.Dir(upperDir)), "merged"), nil
}

func bindRootFSMount(_ rootfs.Mount, mergedDir string) rootfs.Mount {
	return rootfs.Mount{
		Type:    "bind",
		Source:  mergedDir,
		Options: []string{"rbind", "rw"},
	}
}

func mountOptionValue(options []string, prefix string) string {
	for _, option := range options {
		if strings.HasPrefix(option, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(option, prefix))
		}
	}
	return ""
}

func unixUnmount(target string) error {
	if err := unix.Unmount(target, 0); err == nil || errors.Is(err, unix.EINVAL) {
		return nil
	}
	if err := unix.Unmount(target, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) {
		return err
	}
	return nil
}

func isMounted(target string) bool {
	target = filepath.Clean(target)
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		if unescapeMountInfoPath(fields[4]) == target {
			return true
		}
	}
	return false
}

func unescapeMountInfoPath(path string) string {
	replacer := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	return replacer.Replace(path)
}

type limitedBuffer struct {
	mu    sync.Mutex
	limit int
	buf   bytes.Buffer
}

func newLimitedBuffer(limit int) *limitedBuffer {
	return &limitedBuffer{limit: limit}
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= b.limit {
		b.buf.Reset()
		_, _ = b.buf.Write(p[len(p)-b.limit:])
		return len(p), nil
	}
	overflow := b.buf.Len() + len(p) - b.limit
	if overflow > 0 {
		current := b.buf.Bytes()
		b.buf.Reset()
		_, _ = b.buf.Write(current[overflow:])
	}
	_, _ = b.buf.Write(p)
	return len(p), nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
