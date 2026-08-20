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

package driver

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mounter isolates bind-mount operations for unit tests and future handoff hooks.
type Mounter interface {
	Bind(source, target string) error
	Unmount(target string) error
}

type systemMounter struct{}

func (systemMounter) Bind(source, target string) error {
	if err := unix.Mount(source, target, "bind", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s to %s: %w", source, target, err)
	}
	if err := unix.Mount("", target, "none", unix.MS_PRIVATE, ""); err != nil {
		_ = unix.Unmount(target, unix.MNT_DETACH)
		return fmt.Errorf("make %s private: %w", target, err)
	}
	return nil
}

func (systemMounter) Unmount(target string) error {
	if err := normalizeUnmountError(target, unix.Unmount(target, unix.MNT_DETACH)); err != nil {
		return fmt.Errorf("unmount %s: %w", target, err)
	}
	return nil
}

func normalizeUnmountError(_ string, err error) error {
	// A node reboot removes every task mount before the Nomad driver can recover
	// its persisted RootMounted bit. Treat an already absent mount as successful
	// so recovery can continue to the regional writer fence instead of leaving a
	// consumed grant orphaned.
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func validateRootfsPath(source, allowedRoot string) (string, error) {
	resolvedSource, err := validateExistingPath(source, allowedRoot)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolvedSource)
	if err != nil {
		return "", fmt.Errorf("stat rootfs source: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("rootfs source %s is not a directory", source)
	}
	return resolvedSource, nil
}

// validateExistingPath resolves both sides before checking containment so a
// root-owned daemon never persists a caller-controlled symlink escape.
func validateExistingPath(source, allowedRoot string) (string, error) {
	if source == "" || allowedRoot == "" {
		return "", fmt.Errorf("source and allowed root must be non-empty")
	}
	if !filepath.IsAbs(source) || !filepath.IsAbs(allowedRoot) {
		return "", fmt.Errorf("source and allowed root must be absolute")
	}
	cleanAllowed := filepath.Clean(allowedRoot)
	cleanSource := filepath.Clean(source)
	resolvedAllowed := cleanAllowed
	if _, err := os.Stat(cleanAllowed); err == nil {
		resolvedAllowed, err = filepath.EvalSymlinks(cleanAllowed)
		if err != nil {
			return "", fmt.Errorf("resolve allowed rootfs root: %w", err)
		}
	}
	resolvedSource, err := filepath.EvalSymlinks(cleanSource)
	if err != nil {
		return "", fmt.Errorf("resolve source: %w", err)
	}
	relative, err := filepath.Rel(resolvedAllowed, resolvedSource)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || startsWithDotDot(relative) {
		return "", fmt.Errorf("source %s is outside allowed root %s", source, allowedRoot)
	}
	return resolvedSource, nil
}

func startsWithDotDot(path string) bool {
	return path == ".." || len(path) >= 3 && path[:3] == ".."+string(filepath.Separator)
}
