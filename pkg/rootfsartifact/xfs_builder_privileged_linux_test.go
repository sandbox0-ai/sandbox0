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

package rootfsartifact

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPrivilegedXFSBuilderCreatesMountableReflinkLayout(t *testing.T) {
	if os.Getenv("SANDBOX0_PRIVILEGED_XFS_BUILDER") != "1" {
		t.Skip("set SANDBOX0_PRIVILEGED_XFS_BUILDER=1 on an isolated Linux host")
	}
	if os.Geteuid() != 0 {
		t.Fatal("privileged XFS builder test requires root")
	}
	for _, command := range []string{"mkfs.xfs", "mount", "cp", "umount", "xfs_repair"} {
		if _, err := exec.LookPath(command); err != nil {
			t.Fatalf("required command %s: %v", command, err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	workRoot := t.TempDir()
	source := filepath.Join(workRoot, "source")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "etc"), 0o755); err != nil {
		t.Fatal(err)
	}
	const marker = "sandbox0-oci-xfs-import\n"
	if err := os.WriteFile(filepath.Join(source, "etc", "marker"), []byte(marker), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("etc/marker", filepath.Join(source, "marker-link")); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(workRoot, "base.xfs")
	if err := (XFSBuilder{}).Build(ctx, source, destination, MinimumLogicalSizeBytes); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	mountRoot := filepath.Join(workRoot, "verify")
	if err := os.Mkdir(mountRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runPrivilegedXFSCommand(ctx, "mount", "-t", "xfs", "-o", "loop,ro,nouuid,noatime", destination, mountRoot); err != nil {
		t.Fatal(err)
	}
	mounted := true
	defer func() {
		if mounted {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			if err := runPrivilegedXFSCommand(cleanupCtx, "umount", mountRoot); err != nil {
				t.Errorf("unmount verification image: %v", err)
			}
		}
	}()
	payload, err := os.ReadFile(filepath.Join(mountRoot, "lower", "etc", "marker"))
	if err != nil || string(payload) != marker {
		t.Fatalf("mounted marker = %q, %v", payload, err)
	}
	link, err := os.Readlink(filepath.Join(mountRoot, "lower", "marker-link"))
	if err != nil || link != "etc/marker" {
		t.Fatalf("mounted symlink = %q, %v", link, err)
	}
	for _, directory := range []string{"lower", "upper", "work"} {
		info, err := os.Stat(filepath.Join(mountRoot, directory))
		if err != nil || !info.IsDir() {
			t.Fatalf("RootFS directory %s = %+v, %v", directory, info, err)
		}
	}
	if err := runPrivilegedXFSCommand(ctx, "umount", mountRoot); err != nil {
		t.Fatal(err)
	}
	mounted = false
	leftovers, err := filepath.Glob(filepath.Join(workRoot, "xfs-mount-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("XFS builder mount leftovers = %v, %v", leftovers, err)
	}
}

func runPrivilegedXFSCommand(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %w", name, strings.TrimSpace(string(output)), err)
	}
	return nil
}
