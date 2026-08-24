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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// OperationSpec is the immutable, non-secret input persisted before an OCI
// import worker may perform local or remote side effects.
type OperationSpec struct {
	SourceOCIRef     string
	Platform         ReadyArtifactPlatform
	FormatGeneration int
	ProcdProtocol    string
	ProcdDigest      string
	LogicalSizeBytes int64
	BlockOptions     rootfsblock.BuildOptions
}

type operationIdentity struct {
	SourceOCIRef     string                   `json:"source_oci_ref"`
	Platform         ReadyArtifactPlatform    `json:"platform"`
	FormatGeneration int                      `json:"format_generation"`
	ProcdProtocol    string                   `json:"procd_protocol"`
	ProcdDigest      string                   `json:"procd_digest"`
	LogicalSizeBytes int64                    `json:"logical_size_bytes"`
	BlockOptions     rootfsblock.BuildOptions `json:"block_options"`
}

// DeterministicOperation returns the canonical durable identity shared by all
// producers of the same OCI-to-block import requirement.
func DeterministicOperation(input OperationSpec) (string, OperationSpec, error) {
	normalized, err := NormalizeOperationSpec(input)
	if err != nil {
		return "", OperationSpec{}, err
	}
	payload, err := json.Marshal(operationIdentity{
		SourceOCIRef: normalized.SourceOCIRef, Platform: normalized.Platform,
		FormatGeneration: normalized.FormatGeneration,
		ProcdProtocol:    normalized.ProcdProtocol, ProcdDigest: normalized.ProcdDigest,
		LogicalSizeBytes: normalized.LogicalSizeBytes, BlockOptions: normalized.BlockOptions,
	})
	if err != nil {
		return "", OperationSpec{}, fmt.Errorf("encode immutable import identity: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "template-import:" + hex.EncodeToString(sum[:]), normalized, nil
}

// NormalizeOperationSpec validates a durable import contract and applies the
// same block-build defaults used by BlockBuilder.
func NormalizeOperationSpec(input OperationSpec) (OperationSpec, error) {
	normalized := input
	sourceDigest, err := pinnedSourceDigest(input.SourceOCIRef)
	if err != nil {
		return OperationSpec{}, err
	}
	if sourceDigest.String() == "" {
		return OperationSpec{}, fmt.Errorf("OCI image source digest is required")
	}
	if err := validateReadyArtifactPlatform(input.Platform); err != nil {
		return OperationSpec{}, err
	}
	if input.FormatGeneration <= 0 {
		return OperationSpec{}, fmt.Errorf("RootFS format generation must be positive")
	}
	if err := validateProcdProtocol(input.ProcdProtocol); err != nil {
		return OperationSpec{}, err
	}
	procdDigest, err := digest.Parse(input.ProcdDigest)
	if err != nil || validateArtifactSHA256Digest(procdDigest) != nil {
		return OperationSpec{}, fmt.Errorf("procd digest must be canonical SHA-256")
	}
	if input.LogicalSizeBytes < rootfsartifact.MinimumLogicalSizeBytes ||
		input.LogicalSizeBytes > rootfsartifact.MaximumLogicalSizeBytes ||
		input.LogicalSizeBytes%rootfsblock.LogicalBlockSize != 0 {
		return OperationSpec{}, fmt.Errorf(
			"logical size must be between %d and %d and aligned to %d bytes",
			rootfsartifact.MinimumLogicalSizeBytes, rootfsartifact.MaximumLogicalSizeBytes,
			rootfsblock.LogicalBlockSize,
		)
	}
	normalized.BlockOptions, err = rootfsblock.NormalizeBuildOptions(input.BlockOptions)
	if err != nil {
		return OperationSpec{}, fmt.Errorf("RootFS block build options: %w", err)
	}
	return normalized, nil
}
