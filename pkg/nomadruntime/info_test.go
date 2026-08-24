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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeInfoDigestBindsRootOwnedLimits(t *testing.T) {
	info := runtimeInfoFromConfig(Config{
		RootFSMountRoot:                       "/run/sandbox0/rootfs",
		RootFSMaxDirtyTailBytes:               1 << 30,
		RootFSMaxNodeDirtyTailBytes:           8 << 30,
		RootFSDirtyTailRetirementReserveBytes: 64 << 20,
	})
	digest, err := info.Digest()
	require.NoError(t, err)
	require.Len(t, digest, 64)

	changed := info
	changed.MaxNodeDirtyTailBytes++
	changedDigest, err := changed.Digest()
	require.NoError(t, err)
	require.NotEqual(t, digest, changedDigest)
}

func TestRuntimeInfoRejectsUnboundedOrNoncanonicalMetadata(t *testing.T) {
	valid := runtimeInfoFromConfig(Config{RootFSMountRoot: "/run/sandbox0/rootfs"})
	for name, mutate := range map[string]func(*RuntimeInfo){
		"version":       func(info *RuntimeInfo) { info.Version++ },
		"relative root": func(info *RuntimeInfo) { info.MountRoot = "rootfs" },
		"zero session":  func(info *RuntimeInfo) { info.MaxDirtyTailBytes = 0 },
		"zero node":     func(info *RuntimeInfo) { info.MaxNodeDirtyTailBytes = 0 },
		"zero reserve":  func(info *RuntimeInfo) { info.DirtyTailRetirementReserveBytes = 0 },
		"large node":    func(info *RuntimeInfo) { info.MaxNodeDirtyTailBytes = maxDirtyTailLimitBytes + 1 },
		"large reserve": func(info *RuntimeInfo) {
			info.DirtyTailRetirementReserveBytes = info.MaxNodeDirtyTailBytes + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			require.Error(t, candidate.Validate())
		})
	}
}
