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

package gvisorcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

const isolatedRunscMarker = "__sandbox0_isolated_runsc_v1"

// Reexec only the launcher: the daemon's mount namespace and the qualified
// stock runsc executable/companions remain unchanged. gVisor's executable-file
// references can retain its detached host mount tree after pivot_root; prune
// unrelated RootFS mounts before that tree is copied into a Sentry or Gofer.
func init() {
	if len(os.Args) < 2 || os.Args[1] != isolatedRunscMarker {
		return
	}
	if err := isolatedRunscExec(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(125)
	}
	os.Exit(125)
}

func (r *Command) isolatedCommand(ctx context.Context, bundle string, args ...string) *exec.Cmd {
	if r.config.RootFSMountRoot == "" {
		return r.command(ctx, args...)
	}
	argv := []string{isolatedRunscMarker, r.config.RootFSMountRoot, bundle, r.config.Path}
	argv = append(argv, r.globalArgs()...)
	argv = append(argv, args...)
	return exec.CommandContext(ctx, "/proc/self/exe", argv...)
}

func isolatedRunscExec(args []string) error {
	if len(args) < 4 {
		return fmt.Errorf("isolated runsc: incomplete launch arguments")
	}
	root, bundle, binary := args[0], args[1], args[2]
	if !filepath.IsAbs(root) || filepath.Clean(root) == "/" || !filepath.IsAbs(bundle) || !filepath.IsAbs(binary) {
		return fmt.Errorf("isolated runsc requires absolute trusted paths")
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve attested RootFS mount root: %w", err)
	}
	if root == "/" {
		return fmt.Errorf("RootFS mount root cannot be the host root")
	}
	keep, err := bundleMountPaths(bundle)
	if err != nil {
		return err
	}
	if err := isolateAndPruneRootFSMounts(filepath.Clean(root), keep, readMountTable); err != nil {
		return err
	}
	// Resolve and exec the same canonical runsc, including its unchanged argv.
	return unix.Exec(binary, append([]string{binary}, args[3:]...), os.Environ())
}

// A host cleanup can remove a mountpoint's directory after this launcher has
// copied the mount namespace. ENOENT does not prove that the copied mount was
// detached: executing runsc from that namespace could retain its superblock.
// Abandon the entire copy and retry from the current host namespace instead.
// No runsc command has executed, and each attempt still prunes every unrelated
// mount. Real unmount errors and repeated churn retain the fail-closed behavior.
func isolateAndPruneRootFSMounts(root string, keep []string, readTable func() ([]byte, error)) error {
	runtime.LockOSThread()
	// Success intentionally retains this locked thread until exec. On error,
	// the reexec launcher exits; it must not return this private namespace to a
	// reusable Go runtime thread.
	host, err := unix.Open("/proc/self/ns/mnt", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("open host mount namespace: %w", err)
	}
	defer unix.Close(host)
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt != 0 {
			if err := unix.Setns(host, unix.CLONE_NEWNS); err != nil {
				return fmt.Errorf("restore host namespace before fresh isolation: %w", err)
			}
		}
		if err := isolateAndPruneMountAttempt(root, keep, readTable); err != nil {
			if errors.Is(err, unix.ENOENT) && attempt+1 < maxAttempts {
				continue
			}
			return err
		}
		return nil
	}
	panic("unreachable mount isolation attempt")
}

func isolateAndPruneMountAttempt(root string, keep []string, readTable func() ([]byte, error)) error {
	// Go may share fs_struct across threads; unshare it before changing this
	// locked thread's namespace. No work is performed on the parent namespace.
	if err := unix.Unshare(unix.CLONE_FS | unix.CLONE_NEWNS); err != nil {
		return fmt.Errorf("isolate runsc mounts: %w", err)
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make runsc mounts private: %w", err)
	}
	data, err := readTable()
	if err != nil {
		return err
	}
	paths, err := rootFSPrunePaths(data, root, keep)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := unix.Unmount(path, 0); err != nil {
			return fmt.Errorf("prune unrelated RootFS mount %s: %w", path, err)
		}
	}
	return nil
}

func bundleMountPaths(bundle string) ([]string, error) {
	file, err := os.Open(filepath.Join(bundle, "config.json"))
	if err != nil {
		return nil, fmt.Errorf("read isolated runsc bundle: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 8<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 8<<20 {
		return nil, fmt.Errorf("isolated runsc bundle exceeds limit")
	}
	var spec struct {
		Root *struct {
			Path string `json:"path"`
		} `json:"root"`
		Mounts []struct {
			Source string `json:"source"`
		} `json:"mounts"`
	}
	if err = json.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	if spec.Root == nil || spec.Root.Path == "" {
		return nil, fmt.Errorf("isolated runsc bundle has no RootFS")
	}
	root := spec.Root.Path
	if !filepath.IsAbs(root) {
		root = filepath.Join(bundle, root)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, err
	}
	if len(spec.Mounts) > 1024 {
		return nil, fmt.Errorf("isolated runsc bundle mount count exceeds limit")
	}
	paths := []string{filepath.Clean(root)}
	for _, mount := range spec.Mounts {
		if filepath.IsAbs(mount.Source) {
			p, e := filepath.EvalSymlinks(mount.Source)
			if e != nil {
				return nil, e
			}
			paths = append(paths, filepath.Clean(p))
		}
	}
	return paths, nil
}

func readMountTable() ([]byte, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 4<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) > 4<<20 {
		return nil, fmt.Errorf("runsc mount table exceeds limit")
	}
	return data, nil
}

type isolatedMount struct{ path, device, fs string }

func mountPathWithin(path, root string) bool {
	return root == "/" || path == root || strings.HasPrefix(path, root+"/")
}
func decodeMountPath(s string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(s)
}

// Device identity finds bind aliases outside ctld's mount root. Preserve only
// mounts required by this bundle, not every alias of its backing filesystem.
func rootFSPrunePaths(data []byte, root string, keep []string) ([]string, error) {
	var mounts []isolatedMount
	devices := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return nil, fmt.Errorf("invalid runsc mount table")
		}
		separator := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				separator = i
				break
			}
		}
		if separator < 0 || separator+3 >= len(fields) {
			return nil, fmt.Errorf("invalid runsc mount fields")
		}
		m := isolatedMount{path: decodeMountPath(fields[4]), device: fields[2], fs: fields[separator+1]}
		mounts = append(mounts, m)
		if len(mounts) > 8192 {
			return nil, fmt.Errorf("runsc mount count exceeds limit")
		}
		if mountPathWithin(m.path, root) && (m.fs == "xfs" || m.fs == "overlay") {
			devices[m.device+"/"+m.fs] = true
		}
	}
	needed := func(path string) bool {
		for _, p := range keep {
			if mountPathWithin(path, p) || mountPathWithin(p, path) {
				return true
			}
		}
		return false
	}
	var candidates []string
	for _, m := range mounts {
		if (mountPathWithin(m.path, root) || devices[m.device+"/"+m.fs]) && !needed(m.path) {
			candidates = append(candidates, m.path)
		}
	}
	var paths []string
	for _, m := range mounts {
		if needed(m.path) {
			continue
		}
		for _, p := range candidates {
			if mountPathWithin(m.path, p) {
				paths = append(paths, m.path)
				break
			}
		}
	}
	sort.SliceStable(paths, func(i, j int) bool { return strings.Count(paths[i], "/") > strings.Count(paths[j], "/") })
	return paths, nil
}
