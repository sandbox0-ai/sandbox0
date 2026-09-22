//go:build linux

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

package nomadruntime

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

func networkNamespaceIdentity(path string) (string, error) {
	return runtimePathIdentity(path, "netns-v1", "Nomad network namespace")
}

func stableMountIdentity(path string) (string, error) {
	return runtimePathIdentity(path, "mount-v1", "runtime slot stable mount")
}

func stableMountCanonicalPath(path, root string) (string, error) {
	if !filepath.IsAbs(path) || !filepath.IsAbs(root) || filepath.Clean(path) != path {
		return "", errdefs.ErrFailedPrecondition
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	allowed, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	resolved := filepath.Join(parent, filepath.Base(path))
	relative, err := filepath.Rel(allowed, resolved)
	if err != nil || resolved != path || relative == "." || filepath.IsAbs(relative) || startsWithDotDot(relative) {
		return "", errdefs.ErrFailedPrecondition
	}
	return resolved, nil
}

// A bind mount hides the directory whose identity was registered by the warm
// carrier. Clone only its parent (not child mounts) into a detached mount FD,
// so validation can inspect that original directory without unmounting RootFS
// or adding a mount to the host namespace.
func stableMountUnderlyingIdentity(path string) (string, error) {
	parent, err := unix.OpenTree(unix.AT_FDCWD, filepath.Dir(path), unix.OPEN_TREE_CLONE|unix.OPEN_TREE_CLOEXEC)
	if err != nil {
		return "", fmt.Errorf("open stable mount parent view: %w", err)
	}
	defer unix.Close(parent)
	fd, err := unix.Openat(parent, filepath.Base(path), unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return "", fmt.Errorf("open underlying stable mount directory: %w", err)
	}
	defer unix.Close(fd)
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return "", fmt.Errorf("stat underlying stable mount directory: %w", err)
	}
	return fmt.Sprintf("mount-v1:%x:%x", uint64(stat.Dev), stat.Ino), nil
}

func runtimePathIdentity(path, version, description string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return "", fmt.Errorf("%s path must be a non-root absolute path", description)
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("stat %s: %w", description, err)
	}
	return fmt.Sprintf("%s:%x:%x", version, uint64(stat.Dev), stat.Ino), nil
}

func networkChainName(containerID string) string {
	return protocol.NomadNetworkChainName(containerID)
}
