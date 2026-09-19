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
	"os"
	"path/filepath"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/require"
)

func TestDiskBackedTmpPreservesLimitAndPersistentRoot(t *testing.T) {
	bundle := t.TempDir()
	spec := specs.Spec{Root: &specs.Root{Path: "rootfs"}, Mounts: []specs.Mount{
		{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "nodev", "mode=1777", "size=268435456"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm"},
	}}
	require.NoError(t, PrepareEphemeralTmp(bundle, &spec))
	require.Equal(t, "rootfs", spec.Root.Path)
	require.False(t, spec.Root.Readonly)
	require.Equal(t, "bind", spec.Mounts[0].Type)
	require.Equal(t, filepath.Join(bundle, EphemeralTmpDirectory), spec.Mounts[0].Source)
	require.Equal(t, "tmpfs", spec.Mounts[1].Type)
	require.Equal(t, "container", spec.Annotations["dev.gvisor.spec.mount.sandbox0-tmp.share"])
	require.Equal(t, "tmpfs", spec.Annotations["dev.gvisor.spec.mount.sandbox0-tmp.type"])
	require.Equal(t, "nosuid,nodev,mode=1777,size=268435456", spec.Annotations["dev.gvisor.spec.mount.sandbox0-tmp.options"])
	require.Empty(t, spec.Annotations["dev.gvisor.spec.rootfs.overlay"])
}

func TestRuntimeTmpCleanupFencesExactContainer(t *testing.T) {
	bundle := t.TempDir()
	dir := filepath.Join(bundle, EphemeralTmpDirectory)
	require.NoError(t, os.Mkdir(dir, 0o700))
	file := filepath.Join(dir, ".gvisor.filestore.container-a")
	require.NoError(t, os.WriteFile(file, []byte("uncommitted temporary data"), 0o600))
	require.Error(t, CleanupEphemeralTmp(bundle, "container-b"))
	require.FileExists(t, file)
	require.NoError(t, CleanupEphemeralTmp(bundle, "container-a"))
	require.NoDirExists(t, dir)
	require.NoError(t, CleanupEphemeralTmp(bundle, "container-a"))
}

func TestRuntimeTmpRefusesSymlinkAndMemoryBacking(t *testing.T) {
	bundle := t.TempDir()
	dir := filepath.Join(bundle, EphemeralTmpDirectory)
	target := t.TempDir()
	require.NoError(t, os.Symlink(target, dir))
	spec := specs.Spec{Mounts: []specs.Mount{{Destination: "/tmp", Type: "tmpfs"}}}
	require.Error(t, PrepareEphemeralTmp(bundle, &spec))
	require.Error(t, CleanupEphemeralTmp(bundle, "container-a"))
	require.DirExists(t, target)
	if _, err := os.Stat("/dev/shm"); err == nil {
		require.Error(t, requireDiskFilesystem("/dev/shm"))
	}
}
