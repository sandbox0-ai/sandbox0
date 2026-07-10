package portal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const procSelfMountInfo = "/proc/self/mountinfo"

type nodeFSMounter interface {
	EnsureBind(sourcePath, targetPath string) error
	Unmount(targetPath string) error
}

type systemNodeFSMounter struct {
	kubeletPodsRoot string
}

// EnsureBind publishes one route from a nodefs shard at the kubelet CSI target.
// An existing mount is accepted only when it names the same inode as the
// desired source; an unrelated mount is never hidden by stacking another bind.
func (m systemNodeFSMounter) EnsureBind(sourcePath, targetPath string) error {
	sourcePath = filepath.Clean(strings.TrimSpace(sourcePath))
	targetPath = filepath.Clean(strings.TrimSpace(targetPath))
	if sourcePath == "." || targetPath == "." {
		return fmt.Errorf("nodefs bind source and target are required")
	}
	if !isSandboxCSIMountPath(m.kubeletPodsRoot, targetPath) {
		return fmt.Errorf("refusing to bind unsafe nodefs target %q", targetPath)
	}
	if err := rejectSymlinkPath(filepath.Clean(m.kubeletPodsRoot), targetPath); err != nil {
		return err
	}
	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("stat nodefs bind source %s: %w", sourcePath, err)
	}

	mounted, err := isMountPoint(procSelfMountInfo, targetPath)
	if err != nil {
		return err
	}
	if mounted {
		same, err := sameFile(sourcePath, targetPath)
		if err != nil {
			return fmt.Errorf("verify existing nodefs bind %s: %w", targetPath, err)
		}
		if !same {
			return fmt.Errorf("nodefs target %s is mounted from an unexpected source", targetPath)
		}
		return nil
	}
	if err := os.MkdirAll(targetPath, 0o755); err != nil {
		return fmt.Errorf("create nodefs bind target %s: %w", targetPath, err)
	}
	entries, err := os.ReadDir(targetPath)
	if err != nil {
		return fmt.Errorf("read nodefs bind target %s: %w", targetPath, err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("refusing to hide non-empty nodefs target %s", targetPath)
	}
	if err := unix.Mount(sourcePath, targetPath, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind nodefs route %s at %s: %w", sourcePath, targetPath, err)
	}
	same, err := sameFile(sourcePath, targetPath)
	if err != nil || !same {
		_ = unix.Unmount(targetPath, unix.MNT_DETACH)
		if err != nil {
			return fmt.Errorf("verify nodefs bind %s: %w", targetPath, err)
		}
		return fmt.Errorf("verify nodefs bind %s: source inode changed", targetPath)
	}
	return nil
}

func (m systemNodeFSMounter) Unmount(targetPath string) error {
	targetPath = filepath.Clean(strings.TrimSpace(targetPath))
	if targetPath == "." {
		return nil
	}
	if !isSandboxCSIMountPath(m.kubeletPodsRoot, targetPath) {
		return fmt.Errorf("refusing to unmount unsafe nodefs target %q", targetPath)
	}
	if err := rejectSymlinkPath(filepath.Clean(m.kubeletPodsRoot), targetPath); err != nil {
		return err
	}
	err := unix.Unmount(targetPath, unix.MNT_DETACH)
	if err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("unmount nodefs target %s: %w", targetPath, err)
	}
	if err := os.RemoveAll(targetPath); err != nil {
		return fmt.Errorf("remove nodefs target %s: %w", targetPath, err)
	}
	return nil
}

func sameFile(firstPath, secondPath string) (bool, error) {
	first, err := os.Stat(firstPath)
	if err != nil {
		return false, err
	}
	second, err := os.Stat(secondPath)
	if err != nil {
		return false, err
	}
	return os.SameFile(first, second), nil
}

func isMountPoint(mountInfoPath, targetPath string) (bool, error) {
	file, err := os.Open(mountInfoPath)
	if err != nil {
		return false, fmt.Errorf("open mountinfo: %w", err)
	}
	defer file.Close()
	return mountInfoContains(file, targetPath)
}

func mountInfoContains(reader io.Reader, targetPath string) (bool, error) {
	targetPath = filepath.Clean(targetPath)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			continue
		}
		mountPath, err := decodeMountInfoField(fields[4])
		if err != nil {
			return false, fmt.Errorf("decode mountinfo mount point: %w", err)
		}
		if filepath.Clean(mountPath) == targetPath {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("read mountinfo: %w", err)
	}
	return false, nil
}

func decodeMountInfoField(value string) (string, error) {
	var decoded strings.Builder
	decoded.Grow(len(value))
	for i := 0; i < len(value); {
		if value[i] != '\\' {
			decoded.WriteByte(value[i])
			i++
			continue
		}
		if i+3 >= len(value) {
			return "", fmt.Errorf("invalid escape in %q", value)
		}
		n, err := strconv.ParseUint(value[i+1:i+4], 8, 8)
		if err != nil {
			return "", fmt.Errorf("invalid escape in %q", value)
		}
		decoded.WriteByte(byte(n))
		i += 4
	}
	return decoded.String(), nil
}
