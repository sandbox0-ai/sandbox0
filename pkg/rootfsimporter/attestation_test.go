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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestBuildResultAttestBindsProcdAndBlockIdentity(t *testing.T) {
	result := readyAttestationTestBuildResult(t)
	attestation, payload, artifactDigest, err := result.Attest(3, "procd-http-v1")
	require.NoError(t, err)
	require.Equal(t, digest.FromBytes(payload), artifactDigest)
	require.NotEqual(t, result.DescriptorDigest, artifactDigest)
	require.Equal(t, result.ProcdDigest.String(), attestation.ProcdDigest)
	require.Equal(t, result.DescriptorDigest.String(), attestation.DescriptorDigest)
	require.Equal(t, result.BaseBlockRoot.String(), attestation.BaseBlockRoot)
	decoded, err := DecodeReadyArtifactAttestation(payload)
	require.NoError(t, err)
	require.Equal(t, attestation, decoded)

	changedProtocol := result
	_, _, changedProtocolDigest, err := changedProtocol.Attest(3, "procd-http-v2")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedProtocolDigest)
	changedProcd := result
	changedProcd.ProcdDigest = digest.FromString("other-procd")
	_, _, changedProcdDigest, err := changedProcd.Attest(3, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedProcdDigest)
	_, _, changedFormatDigest, err := result.Attest(4, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedFormatDigest)
	changedManifest := result
	changedManifest.ManifestDigest = digest.FromString("other-manifest")
	_, _, changedManifestDigest, err := changedManifest.Attest(3, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedManifestDigest)
	changedPlatform := result
	changedPlatform.Platform.Variant = "v8"
	_, _, changedPlatformDigest, err := changedPlatform.Attest(3, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedPlatformDigest)
	changedReference := result
	changedReference.SourceOCIRef = "registry.example/mirror@" + result.SourceOCIDigest.String()
	_, _, changedReferenceDigest, err := changedReference.Attest(3, "procd-http-v1")
	require.NoError(t, err)
	require.NotEqual(t, artifactDigest, changedReferenceDigest)
}

func TestDecodeReadyArtifactAttestationRejectsUnknownTrailingAndNonCanonicalJSON(t *testing.T) {
	result := readyAttestationTestBuildResult(t)
	_, payload, _, err := result.Attest(1, "procd-http-v1")
	require.NoError(t, err)
	unknown := append(append([]byte(nil), payload[:len(payload)-1]...), []byte(`,"unknown":true}`)...)
	_, err = DecodeReadyArtifactAttestation(unknown)
	require.Error(t, err)
	_, err = DecodeReadyArtifactAttestation(append(append([]byte(nil), payload...), []byte(` {}`)...))
	require.ErrorContains(t, err, "exactly one")
	pretty := &bytes.Buffer{}
	require.NoError(t, jsonIndent(pretty, payload))
	_, err = DecodeReadyArtifactAttestation(pretty.Bytes())
	require.ErrorContains(t, err, "canonical")
}

func TestBuildResultAttestRejectsMissingProtocolAndInvalidResult(t *testing.T) {
	result := readyAttestationTestBuildResult(t)
	_, _, _, err := result.Attest(1, "")
	require.ErrorContains(t, err, "procd protocol")
	result.SourceOCIRef = "registry.example/unpinned:latest"
	_, _, _, err = result.Attest(1, "procd-http-v1")
	require.ErrorContains(t, err, "source reference")
}

func readyAttestationTestBuildResult(t *testing.T) BuildResult {
	t.Helper()
	objects := &attestationObjectPublisher{objects: make(map[string][]byte)}
	logical := bytes.Repeat([]byte{0x5a}, rootfsblock.LogicalBlockSize)
	built, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(logical), int64(len(logical)), objects,
		rootfsblock.BuildOptions{
			DataRangeBytes: rootfsblock.LogicalBlockSize,
			PackBytes:      rootfsblock.LogicalBlockSize,
			ObjectPrefix:   "rootfs/attestation-test",
		},
	)
	require.NoError(t, err)
	source := digest.FromString("source-image")
	return BuildResult{
		SourceOCIRef: "registry.example/sandbox@" + source.String(), SourceOCIDigest: source,
		ManifestDigest: digest.FromString("manifest"), ConfigDigest: digest.FromString("config"),
		Platform:      ocispec.Platform{OS: "linux", Architecture: "arm64"},
		LayerDigests:  []digest.Digest{digest.FromString("layer")},
		DiffIDs:       []digest.Digest{digest.FromString("diff-id")},
		ProcdDigest:   digest.FromString("production-procd"),
		UnpackedBytes: 4096, Files: 1, LogicalSizeBytes: int64(len(logical)),
		DescriptorDigest: digest.FromBytes(built.Payload),
		BaseBlockRoot:    digest.Digest(built.Descriptor.MappingRoot.RootDigest),
		Descriptor:       built.Descriptor, DescriptorBytes: built.Payload,
		Objects: built.Objects, Bytes: built.Bytes, References: built.References,
	}
}

type attestationObjectPublisher struct {
	objects map[string][]byte
}

func (p *attestationObjectPublisher) PutImmutable(_ context.Context, key string, payload []byte) error {
	if current, found := p.objects[key]; found && !bytes.Equal(current, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	p.objects[key] = append([]byte(nil), payload...)
	return nil
}

func jsonIndent(target *bytes.Buffer, payload []byte) error {
	return json.Indent(target, payload, "", "  ")
}
