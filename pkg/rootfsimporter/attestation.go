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
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	ReadyArtifactAttestationVersion  = 1
	ReadyArtifactStorageFormat       = "block-cow-v1"
	MaxReadyArtifactAttestationBytes = 64 << 10
)

// ReadyArtifactPlatform is the exact OCI platform selected during import.
type ReadyArtifactPlatform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

// ReadyArtifactAttestation is the immutable identity input for one ready base
// artifact. It binds the selected OCI graph, block descriptor, and exact procd
// compatibility instead of treating the descriptor digest as artifact truth.
// ManifestDigest and ConfigDigest transitively bind ordered layer digests and
// DiffIDs, so the attestation does not maintain a duplicate layer list.
type ReadyArtifactAttestation struct {
	Version          int                   `json:"version"`
	StorageFormat    string                `json:"storage_format"`
	FormatGeneration int                   `json:"format_generation"`
	SourceOCIRef     string                `json:"source_oci_ref"`
	SourceOCIDigest  string                `json:"source_oci_digest"`
	ManifestDigest   string                `json:"manifest_digest"`
	ConfigDigest     string                `json:"config_digest"`
	Platform         ReadyArtifactPlatform `json:"platform"`
	ProcdProtocol    string                `json:"procd_protocol"`
	ProcdDigest      string                `json:"procd_digest"`
	LogicalSizeBytes int64                 `json:"logical_size_bytes"`
	DescriptorDigest string                `json:"descriptor_digest"`
	BaseBlockRoot    string                `json:"base_block_root"`
}

// Attest constructs canonical ready-artifact bytes and their SHA-256 identity.
// The returned digest, not DescriptorDigest, is the final artifact identity.
func (result BuildResult) Attest(
	formatGeneration int,
	procdProtocol string,
) (ReadyArtifactAttestation, []byte, digest.Digest, error) {
	if err := result.Validate(); err != nil {
		return ReadyArtifactAttestation{}, nil, "", err
	}
	if formatGeneration <= 0 {
		return ReadyArtifactAttestation{}, nil, "", fmt.Errorf("RootFS format generation must be positive")
	}
	if err := validateProcdProtocol(procdProtocol); err != nil {
		return ReadyArtifactAttestation{}, nil, "", err
	}
	attestation := ReadyArtifactAttestation{
		Version: ReadyArtifactAttestationVersion, StorageFormat: ReadyArtifactStorageFormat,
		FormatGeneration: formatGeneration,
		SourceOCIRef:     result.SourceOCIRef, SourceOCIDigest: result.SourceOCIDigest.String(),
		ManifestDigest: result.ManifestDigest.String(), ConfigDigest: result.ConfigDigest.String(),
		Platform: ReadyArtifactPlatform{
			OS: result.Platform.OS, Architecture: result.Platform.Architecture, Variant: result.Platform.Variant,
		},
		ProcdProtocol: procdProtocol, ProcdDigest: result.ProcdDigest.String(),
		LogicalSizeBytes: result.LogicalSizeBytes,
		DescriptorDigest: result.DescriptorDigest.String(), BaseBlockRoot: result.BaseBlockRoot.String(),
	}
	payload, err := json.Marshal(attestation)
	if err != nil {
		return ReadyArtifactAttestation{}, nil, "", fmt.Errorf("encode ready RootFS artifact attestation: %w", err)
	}
	if len(payload) == 0 || len(payload) > MaxReadyArtifactAttestationBytes {
		return ReadyArtifactAttestation{}, nil, "", fmt.Errorf("ready RootFS artifact attestation exceeds %d bytes", MaxReadyArtifactAttestationBytes)
	}
	if err := attestation.Validate(); err != nil {
		return ReadyArtifactAttestation{}, nil, "", err
	}
	return attestation, payload, digest.FromBytes(payload), nil
}

// Validate rejects non-canonical or incomplete ready-artifact identities.
func (a ReadyArtifactAttestation) Validate() error {
	if a.Version != ReadyArtifactAttestationVersion || a.StorageFormat != ReadyArtifactStorageFormat ||
		a.FormatGeneration <= 0 || a.LogicalSizeBytes <= 0 {
		return fmt.Errorf("ready RootFS artifact version, format, generation, or size is invalid")
	}
	if err := validateProcdProtocol(a.ProcdProtocol); err != nil {
		return err
	}
	if err := validateReadyArtifactPlatform(a.Platform); err != nil {
		return err
	}
	refDigest, err := pinnedSourceDigest(a.SourceOCIRef)
	if err != nil || refDigest.String() != a.SourceOCIDigest {
		return fmt.Errorf("ready RootFS source reference does not bind its OCI digest")
	}
	for _, item := range []struct {
		name  string
		value string
	}{
		{name: "source OCI", value: a.SourceOCIDigest},
		{name: "manifest", value: a.ManifestDigest},
		{name: "config", value: a.ConfigDigest},
		{name: "procd", value: a.ProcdDigest},
		{name: "descriptor", value: a.DescriptorDigest},
		{name: "base block root", value: a.BaseBlockRoot},
	} {
		value, err := digest.Parse(item.value)
		if err != nil || validateArtifactSHA256Digest(value) != nil {
			return fmt.Errorf("ready RootFS %s digest is invalid", item.name)
		}
	}
	return nil
}

// DecodeReadyArtifactAttestation accepts exactly one strict bounded JSON value.
func DecodeReadyArtifactAttestation(payload []byte) (ReadyArtifactAttestation, error) {
	if len(payload) == 0 || len(payload) > MaxReadyArtifactAttestationBytes {
		return ReadyArtifactAttestation{}, fmt.Errorf("ready RootFS artifact attestation must contain 1..%d bytes", MaxReadyArtifactAttestationBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var attestation ReadyArtifactAttestation
	if err := decoder.Decode(&attestation); err != nil {
		return ReadyArtifactAttestation{}, fmt.Errorf("decode ready RootFS artifact attestation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return ReadyArtifactAttestation{}, fmt.Errorf("ready RootFS artifact attestation must contain exactly one JSON value")
	}
	if err := attestation.Validate(); err != nil {
		return ReadyArtifactAttestation{}, err
	}
	canonical, err := json.Marshal(attestation)
	if err != nil || !bytes.Equal(canonical, payload) {
		return ReadyArtifactAttestation{}, fmt.Errorf("ready RootFS artifact attestation is not canonical JSON")
	}
	return attestation, nil
}

func validateProcdProtocol(value string) error {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 {
		return fmt.Errorf("procd protocol must contain 1..128 canonical bytes")
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			strings.ContainsRune("._:-", character) {
			continue
		}
		return fmt.Errorf("procd protocol contains an invalid character")
	}
	return nil
}

// ValidateProcdProtocol rejects protocol identities that cannot participate in
// canonical ready-artifact attestation.
func ValidateProcdProtocol(value string) error {
	return validateProcdProtocol(value)
}

func validateReadyArtifactPlatform(value ReadyArtifactPlatform) error {
	if value.OS != "linux" || !validPlatformPart(value.Architecture) ||
		(value.Variant != "" && !validPlatformPart(value.Variant)) {
		return fmt.Errorf("ready RootFS platform must be canonical Linux")
	}
	return nil
}

func validPlatformPart(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' ||
			index > 0 && strings.ContainsRune("_.-", character) {
			continue
		}
		return false
	}
	return true
}
