// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux

package rootfsartifact

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
	"time"

	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	maxCommandOutputBytes = 1 << 20
	xfsCleanupTimeout     = 30 * time.Second
)

// ErrXFSImageStillMounted means cleanup could not detach a build mount. The
// image path is intentionally retained and must not be deleted by the caller.
var ErrXFSImageStillMounted = errors.New("XFS build image is still mounted")

// CommandRunner externalizes privileged filesystem commands for tests.
type CommandRunner interface {
	Run(context.Context, string, ...string) error
}

// XFSBuilder creates the on-disk lower/upper/work XFS layout used by a RootFS branch.
type XFSBuilder struct {
	Runner CommandRunner
}

// Build writes a sparse, reflink-capable XFS image containing sourceRoot under lower/.
func (b XFSBuilder) Build(ctx context.Context, sourceRoot, destination string, logicalSize int64) (resultErr error) {
	if err := validateXFSBuildPaths(sourceRoot, destination); err != nil {
		return err
	}
	if filepath.Clean(sourceRoot) == "/" {
		return fmt.Errorf("source root and destination must be safe absolute paths")
	}
	if logicalSize < MinimumLogicalSizeBytes || logicalSize > MaximumLogicalSizeBytes ||
		logicalSize%rootfsblock.LogicalBlockSize != 0 {
		return fmt.Errorf(
			"logical size must be between %d and %d and aligned to %d bytes",
			MinimumLogicalSizeBytes, MaximumLogicalSizeBytes, rootfsblock.LogicalBlockSize,
		)
	}
	runner := b.Runner
	if runner == nil {
		runner = execRunner{}
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create XFS image: %w", err)
	}
	mountRoot := ""
	mounted := false
	defer func() {
		var cleanupErr error
		if mounted {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), xfsCleanupTimeout)
			unmountErr := runner.Run(cleanupCtx, "umount", mountRoot)
			if unmountErr != nil {
				unmountErr = errors.Join(unmountErr, runner.Run(cleanupCtx, "umount", "-l", mountRoot))
			}
			cancel()
			if unmountErr != nil {
				cleanupErr = errors.Join(cleanupErr, ErrXFSImageStillMounted, fmt.Errorf("detach XFS build mount %s: %w", mountRoot, unmountErr))
			} else {
				mounted = false
			}
		}
		if !mounted && mountRoot != "" {
			cleanupErr = errors.Join(cleanupErr, os.RemoveAll(mountRoot))
		}
		resultErr = errors.Join(resultErr, cleanupErr)
		if resultErr != nil && !mounted {
			removeErr := os.Remove(destination)
			if errors.Is(removeErr, os.ErrNotExist) {
				removeErr = nil
			}
			resultErr = errors.Join(resultErr, removeErr)
		}
	}()
	if err := file.Truncate(logicalSize); err != nil {
		return fmt.Errorf("size XFS image: %w", errors.Join(err, file.Close()))
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close sized XFS image: %w", err)
	}
	if err := runner.Run(ctx, "mkfs.xfs", "-f", "-m", "crc=1,reflink=1", "-n", "ftype=1", "-L", "s0-base-v1", destination); err != nil {
		return fmt.Errorf("format XFS image: %w", err)
	}
	mountRoot, err = os.MkdirTemp(filepath.Dir(destination), "xfs-mount-*")
	if err != nil {
		return fmt.Errorf("create XFS mount directory: %w", err)
	}
	if err := runner.Run(ctx, "mount", "-t", "xfs", "-o", "loop,nouuid,noatime", destination, mountRoot); err != nil {
		return fmt.Errorf("mount XFS image: %w", err)
	}
	mounted = true
	lower := filepath.Join(mountRoot, "lower")
	for _, path := range []string{lower, filepath.Join(mountRoot, "upper"), filepath.Join(mountRoot, "work")} {
		if err := os.Mkdir(path, 0o755); err != nil {
			return fmt.Errorf("create RootFS layout: %w", err)
		}
	}
	// filepath.Join cleans a trailing "." away, which would copy sourceRoot as a
	// nested child instead of copying its entries into lower/.
	sourceEntries := sourceRoot + string(filepath.Separator) + "."
	if err := runner.Run(ctx, "cp", "-a", "--reflink=auto", "--sparse=always", sourceEntries, lower); err != nil {
		return fmt.Errorf("copy filesystem into XFS lower: %w", err)
	}
	rootFD, err := unix.Open(mountRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open XFS root for sync: %w", err)
	}
	syncErr := unix.Syncfs(rootFD)
	closeErr := unix.Close(rootFD)
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync XFS image: %w", errors.Join(syncErr, closeErr))
	}
	if err := runner.Run(ctx, "umount", mountRoot); err != nil {
		return fmt.Errorf("cleanly unmount XFS image: %w", err)
	}
	mounted = false
	if err := runner.Run(ctx, "xfs_repair", "-n", destination); err != nil {
		return fmt.Errorf("verify XFS image: %w", err)
	}
	return nil
}

func validateXFSBuildPaths(sourceRoot, destination string) error {
	if err := validateCanonicalXFSPath("source root", sourceRoot); err != nil {
		return err
	}
	if err := validateCanonicalXFSPath("destination", destination); err != nil {
		return err
	}
	resolvedSource, err := filepath.EvalSymlinks(sourceRoot)
	if err != nil || resolvedSource != sourceRoot {
		return fmt.Errorf("source root must not traverse symlinks")
	}
	sourceInfo, err := os.Lstat(sourceRoot)
	if err != nil || !sourceInfo.IsDir() || sourceInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("source root must be a directory without symlinks")
	}
	destinationParent := filepath.Dir(destination)
	resolvedParent, err := filepath.EvalSymlinks(destinationParent)
	if err != nil || resolvedParent != destinationParent {
		return fmt.Errorf("destination parent must not traverse symlinks")
	}
	parentInfo, err := os.Lstat(destinationParent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("destination parent must be a directory without symlinks")
	}
	return nil
}

func validateCanonicalXFSPath(name, value string) error {
	if value == "" || value != strings.TrimSpace(value) || value != filepath.Clean(value) ||
		!filepath.IsAbs(value) || value == string(filepath.Separator) {
		return fmt.Errorf("%s must be a canonical non-root absolute path", name)
	}
	return nil
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	output := &boundedCommandOutput{limit: maxCommandOutputBytes}
	command.Stdout = output
	command.Stderr = output
	err := command.Run()
	if err != nil {
		message := strings.TrimSpace(output.String())
		if message == "" {
			return fmt.Errorf("%s: %w", name, err)
		}
		return fmt.Errorf("%s: %s: %w", name, message, err)
	}
	return nil
}

type boundedCommandOutput struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (o *boundedCommandOutput) Write(payload []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	remaining := o.limit - o.buffer.Len()
	if remaining > 0 {
		_, _ = o.buffer.Write(payload[:min(len(payload), remaining)])
	}
	if len(payload) > remaining {
		o.truncated = true
	}
	return len(payload), nil
}

func (o *boundedCommandOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	value := o.buffer.String()
	if o.truncated {
		value += "\n[output truncated]"
	}
	return value
}
