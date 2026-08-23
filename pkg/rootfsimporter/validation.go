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
	"bytes"
	"fmt"
	"strings"

	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// Validate verifies that every field returned by a block build is internally
// consistent before it can participate in a ready-artifact attestation.
func (result BuildResult) Validate() error {
	if err := result.Descriptor.Validate(); err != nil {
		return fmt.Errorf("generated RootFS descriptor: %w", err)
	}
	encoded, err := rootfsblock.EncodeDescriptor(result.Descriptor)
	if err != nil || !bytes.Equal(encoded, result.DescriptorBytes) {
		return fmt.Errorf("generated RootFS descriptor bytes do not match the build result")
	}
	if result.DescriptorDigest != digest.FromBytes(result.DescriptorBytes) ||
		result.BaseBlockRoot.String() != result.Descriptor.MappingRoot.RootDigest ||
		result.LogicalSizeBytes != result.Descriptor.LogicalSizeBytes {
		return fmt.Errorf("generated RootFS artifact digests do not match the descriptor")
	}
	for _, item := range []struct {
		name  string
		value digest.Digest
	}{
		{name: "source", value: result.SourceOCIDigest},
		{name: "manifest", value: result.ManifestDigest},
		{name: "config", value: result.ConfigDigest},
		{name: "procd", value: result.ProcdDigest},
		{name: "descriptor", value: result.DescriptorDigest},
		{name: "base block root", value: result.BaseBlockRoot},
	} {
		if err := validateArtifactSHA256Digest(item.value); err != nil {
			return fmt.Errorf("generated RootFS %s digest: %w", item.name, err)
		}
	}
	refDigest, err := pinnedSourceDigest(result.SourceOCIRef)
	if err != nil || refDigest != result.SourceOCIDigest {
		return fmt.Errorf("generated RootFS source reference does not bind its OCI digest")
	}
	if result.Platform.OS != "linux" || !validPlatformPart(result.Platform.Architecture) ||
		(result.Platform.Variant != "" && !validPlatformPart(result.Platform.Variant)) ||
		result.Platform.OSVersion != "" || len(result.Platform.OSFeatures) != 0 {
		return fmt.Errorf("generated RootFS platform must be canonical Linux")
	}
	if len(result.LayerDigests) == 0 || len(result.LayerDigests) != len(result.DiffIDs) {
		return fmt.Errorf("generated RootFS layer and DiffID evidence is incomplete")
	}
	for index := range result.LayerDigests {
		if err := validateArtifactSHA256Digest(result.LayerDigests[index]); err != nil {
			return fmt.Errorf("generated RootFS layer %d digest: %w", index, err)
		}
		if err := validateArtifactSHA256Digest(result.DiffIDs[index]); err != nil {
			return fmt.Errorf("generated RootFS layer %d DiffID: %w", index, err)
		}
	}
	if result.UnpackedBytes < 0 || result.Files < 0 || result.Objects <= 0 ||
		result.Bytes <= 0 || len(result.References) == 0 {
		return fmt.Errorf("generated RootFS artifact has invalid counters or no immutable object inventory")
	}
	previous := ""
	var referencedBytes int64
	for _, reference := range result.References {
		if reference.Key <= previous {
			return fmt.Errorf("generated RootFS object inventory is not canonical")
		}
		if err := rootfsblock.ValidateObjectReference(reference); err != nil {
			return err
		}
		checksum, _ := digest.Parse(reference.Checksum)
		pathKind := "maps"
		if reference.Kind == rootfsblock.ObjectKindDataPack {
			pathKind = "packs"
		}
		if !strings.HasSuffix(reference.Key, "/"+pathKind+"/sha256/"+checksum.Encoded()) {
			return fmt.Errorf("generated RootFS object %q does not bind its kind and checksum", reference.Key)
		}
		if referencedBytes > result.Bytes-reference.Size {
			return fmt.Errorf("generated RootFS object inventory exceeds published bytes")
		}
		referencedBytes += reference.Size
		previous = reference.Key
	}
	if result.Objects < len(result.References) {
		return fmt.Errorf("generated RootFS object inventory exceeds publication count")
	}
	return nil
}

func pinnedSourceDigest(reference string) (digest.Digest, error) {
	if reference == "" || reference != strings.TrimSpace(reference) {
		return "", fmt.Errorf("OCI image reference must be canonical")
	}
	named, err := distref.ParseNormalizedNamed(reference)
	if err != nil || named.String() != reference {
		return "", fmt.Errorf("OCI image reference must be normalized")
	}
	digested, ok := named.(distref.Digested)
	if !ok {
		return "", fmt.Errorf("OCI image reference must be digest-pinned")
	}
	value := digest.Digest(digested.Digest())
	if err := validateArtifactSHA256Digest(value); err != nil {
		return "", fmt.Errorf("OCI image digest: %w", err)
	}
	return value, nil
}

// PinnedSourceDigest returns the canonical SHA-256 digest bound by a
// normalized OCI reference.
func PinnedSourceDigest(reference string) (digest.Digest, error) {
	return pinnedSourceDigest(reference)
}

func validateArtifactSHA256Digest(value digest.Digest) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if value.Algorithm() != digest.SHA256 || len(value.Encoded()) != 64 || strings.ToLower(value.String()) != value.String() {
		return fmt.Errorf("digest must be canonical SHA-256")
	}
	return nil
}

// ValidateArtifactSHA256Digest rejects digests that cannot participate in an
// immutable RootFS artifact identity.
func ValidateArtifactSHA256Digest(value digest.Digest) error {
	return validateArtifactSHA256Digest(value)
}
