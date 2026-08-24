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

package rootfsimporter

import (
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestNormalizeOperationSpecAppliesCanonicalBuildDefaults(t *testing.T) {
	source := digest.FromString("operation-source")
	spec, err := NormalizeOperationSpec(OperationSpec{
		SourceOCIRef:     "registry.example/sandbox@" + source.String(),
		Platform:         ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 3, ProcdProtocol: "sandbox0.procd.v3",
		ProcdDigest:      digest.FromString("operation-procd").String(),
		LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
	})
	require.NoError(t, err)
	require.Equal(t, rootfsblock.DefaultDataRangeBytes, spec.BlockOptions.DataRangeBytes)
	require.Equal(t, rootfsblock.DefaultPackBytes, spec.BlockOptions.PackBytes)
	require.Equal(t, rootfsblock.DefaultPageEntries, spec.BlockOptions.PageEntries)
	require.Equal(t, "rootfs/v1", spec.BlockOptions.ObjectPrefix)
}

func TestNormalizeOperationSpecRejectsMutableOrUnboundedInputs(t *testing.T) {
	valid := OperationSpec{
		SourceOCIRef:     "registry.example/sandbox@" + digest.FromString("operation-source").String(),
		Platform:         ReadyArtifactPlatform{OS: "linux", Architecture: "amd64"},
		FormatGeneration: 3, ProcdProtocol: "sandbox0.procd.v3",
		ProcdDigest:      digest.FromString("operation-procd").String(),
		LogicalSizeBytes: rootfsartifact.MinimumLogicalSizeBytes,
		BlockOptions:     rootfsblock.BuildOptions{ObjectPrefix: "rootfs/import/operation"},
	}
	for name, mutate := range map[string]func(*OperationSpec){
		"mutable source": func(value *OperationSpec) { value.SourceOCIRef = "registry.example/sandbox:latest" },
		"platform":       func(value *OperationSpec) { value.Platform.Architecture = "AMD64" },
		"format":         func(value *OperationSpec) { value.FormatGeneration = 0 },
		"protocol":       func(value *OperationSpec) { value.ProcdProtocol = "sandbox0 procd" },
		"procd":          func(value *OperationSpec) { value.ProcdDigest = "sha256:ABC" },
		"logical size":   func(value *OperationSpec) { value.LogicalSizeBytes-- },
		"unbounded size": func(value *OperationSpec) {
			value.LogicalSizeBytes = rootfsartifact.MaximumLogicalSizeBytes + rootfsblock.LogicalBlockSize
		},
		"object prefix":    func(value *OperationSpec) { value.BlockOptions.ObjectPrefix = "../tenant" },
		"unbounded pack":   func(value *OperationSpec) { value.BlockOptions.PackBytes = rootfsblock.DefaultPackBytes * 2 },
		"unbounded prefix": func(value *OperationSpec) { value.BlockOptions.ObjectPrefix = strings.Repeat("a", 513) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			_, err := NormalizeOperationSpec(candidate)
			require.Error(t, err)
		})
	}
}
