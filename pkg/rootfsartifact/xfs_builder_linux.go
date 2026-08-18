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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const MinimumLogicalSizeBytes = 300 << 20

// CommandRunner externalizes privileged filesystem commands for tests.
type CommandRunner interface {
	Run(context.Context, string, ...string) error
}

// XFSBuilder creates the on-disk lower/upper/work XFS layout used by a RootFS branch.
type XFSBuilder struct {
	Runner CommandRunner
}

// Build writes a sparse, reflink-capable XFS image containing sourceRoot under lower/.
func (b XFSBuilder) Build(ctx context.Context, sourceRoot, destination string, logicalSize int64) error {
	if !filepath.IsAbs(sourceRoot) || !filepath.IsAbs(destination) || filepath.Clean(sourceRoot) == "/" {
		return fmt.Errorf("source root and destination must be safe absolute paths")
	}
	if logicalSize < MinimumLogicalSizeBytes || logicalSize%rootfsblock.LogicalBlockSize != 0 {
		return fmt.Errorf("logical size must be at least %d and aligned to %d bytes", MinimumLogicalSizeBytes, rootfsblock.LogicalBlockSize)
	}
	runner := b.Runner
	if runner == nil {
		runner = execRunner{}
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create XFS image: %w", err)
	}
	if err := file.Truncate(logicalSize); err != nil {
		_ = file.Close()
		return fmt.Errorf("size XFS image: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close sized XFS image: %w", err)
	}
	if err := runner.Run(ctx, "mkfs.xfs", "-f", "-m", "crc=1,reflink=1", "-n", "ftype=1", "-L", "s0-base-v1", destination); err != nil {
		return fmt.Errorf("format XFS image: %w", err)
	}
	mountRoot, err := os.MkdirTemp(filepath.Dir(destination), "xfs-mount-*")
	if err != nil {
		return fmt.Errorf("create XFS mount directory: %w", err)
	}
	defer os.RemoveAll(mountRoot)
	if err := runner.Run(ctx, "mount", "-t", "xfs", "-o", "loop,nouuid,noatime", destination, mountRoot); err != nil {
		return fmt.Errorf("mount XFS image: %w", err)
	}
	mounted := true
	defer func() {
		if mounted {
			_ = runner.Run(context.Background(), "umount", mountRoot)
		}
	}()
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

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return fmt.Errorf("%s: %w", name, err)
		}
		return fmt.Errorf("%s: %s: %w", name, message, err)
	}
	return nil
}
