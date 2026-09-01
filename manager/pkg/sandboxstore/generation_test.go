package sandboxstore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/stretchr/testify/require"
)

// PutReadyRootFSBaseArtifactRequest and the method below exist only in the
// sandboxstore test binary. Production publication must use the fenced durable
// importer and PublishReadyRootFSImport CAS.
type PutReadyRootFSBaseArtifactRequest struct {
	ArtifactDigest   string
	SourceOCIRef     string
	SourceOCIDigest  string
	BaseBlockRoot    string
	FormatGeneration int
	Platform         RootFSArtifactPlatform
	ProcdProtocol    string
	ProcdDigest      string
	LogicalSizeBytes int64
	Descriptor       []byte
}

func (s *PGSandboxStore) PutReadyRootFSBaseArtifact(
	ctx context.Context,
	req *PutReadyRootFSBaseArtifactRequest,
) (*RootFSBaseArtifact, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("rootfs generation store is not configured")
	}
	normalized, err := validateReadyRootFSBaseArtifact(req)
	if err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.rootfs_base_artifacts (
			artifact_digest, source_oci_ref, source_oci_digest, base_block_root,
			format_generation, oci_os, oci_architecture, oci_variant,
			procd_protocol, procd_digest, logical_size_bytes,
			state, descriptor, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'ready', $12, NOW(), NOW())
		ON CONFLICT (artifact_digest) DO NOTHING
	`, normalized.ArtifactDigest, normalized.SourceOCIRef, normalized.SourceOCIDigest,
		normalized.BaseBlockRoot, normalized.FormatGeneration, normalized.Platform.OS,
		normalized.Platform.Architecture, normalized.Platform.Variant,
		normalized.ProcdProtocol, normalized.ProcdDigest, normalized.LogicalSizeBytes,
		normalized.Descriptor)
	if err != nil {
		return nil, err
	}
	artifact, err := scanRootFSBaseArtifact(s.pool.QueryRow(ctx,
		rootFSBaseArtifactSelectSQL()+" WHERE artifact_digest = $1", normalized.ArtifactDigest))
	if err != nil {
		return nil, err
	}
	if !rootFSBaseArtifactMatchesRequest(artifact, normalized) {
		return nil, ErrRootFSBaseArtifactConflict
	}
	return artifact, nil
}

func validateReadyRootFSBaseArtifact(req *PutReadyRootFSBaseArtifactRequest) (*PutReadyRootFSBaseArtifactRequest, error) {
	if req == nil {
		return nil, fmt.Errorf("rootfs base artifact request is required")
	}
	normalized := *req
	normalized.ArtifactDigest = strings.TrimSpace(req.ArtifactDigest)
	normalized.SourceOCIRef = strings.TrimSpace(req.SourceOCIRef)
	normalized.SourceOCIDigest = strings.TrimSpace(req.SourceOCIDigest)
	normalized.BaseBlockRoot = strings.TrimSpace(req.BaseBlockRoot)
	normalized.Descriptor = append([]byte(nil), req.Descriptor...)
	for field, value := range map[string]string{
		"source_oci_ref": normalized.SourceOCIRef, "base_block_root": normalized.BaseBlockRoot,
	} {
		if value == "" {
			return nil, fmt.Errorf("%s is required", field)
		}
	}
	for field, value := range map[string]string{
		"artifact_digest": normalized.ArtifactDigest, "source_oci_digest": normalized.SourceOCIDigest,
	} {
		if _, err := digest.Parse(value); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
	}
	if normalized.FormatGeneration <= 0 {
		return nil, fmt.Errorf("format_generation must be positive")
	}
	if err := normalized.Platform.Validate(); err != nil {
		return nil, err
	}
	if err := rootfsimporter.ValidateProcdProtocol(normalized.ProcdProtocol); err != nil {
		return nil, err
	}
	procdDigest, err := digest.Parse(normalized.ProcdDigest)
	if err != nil || rootfsimporter.ValidateArtifactSHA256Digest(procdDigest) != nil {
		return nil, fmt.Errorf("procd_digest must be canonical SHA-256")
	}
	if len(normalized.Descriptor) == 0 || len(normalized.Descriptor) > RootFSGenerationDescriptorMaxBytes {
		return nil, fmt.Errorf("descriptor must contain 1..%d bytes", RootFSGenerationDescriptorMaxBytes)
	}
	descriptor, err := rootfsblock.DecodeDescriptor(normalized.Descriptor)
	if err != nil {
		return nil, fmt.Errorf("descriptor: %w", err)
	}
	if descriptor.MappingRoot.RootDigest != normalized.BaseBlockRoot || descriptor.CompositeTail != nil {
		return nil, fmt.Errorf("base artifact descriptor must point at the exact S3-materialized base block root")
	}
	if normalized.LogicalSizeBytes != 0 && normalized.LogicalSizeBytes != descriptor.LogicalSizeBytes {
		return nil, fmt.Errorf("logical_size_bytes must match the immutable descriptor")
	}
	normalized.LogicalSizeBytes = descriptor.LogicalSizeBytes
	return &normalized, nil
}

func rootFSBaseArtifactMatchesRequest(artifact *RootFSBaseArtifact, req *PutReadyRootFSBaseArtifactRequest) bool {
	return artifact != nil && req != nil && artifact.ArtifactDigest == req.ArtifactDigest &&
		artifact.SourceOCIRef == req.SourceOCIRef && artifact.SourceOCIDigest == req.SourceOCIDigest &&
		artifact.BaseBlockRoot == req.BaseBlockRoot && artifact.FormatGeneration == req.FormatGeneration &&
		artifact.Platform == req.Platform && artifact.ProcdProtocol == req.ProcdProtocol &&
		artifact.ProcdDigest == req.ProcdDigest && artifact.LogicalSizeBytes == req.LogicalSizeBytes &&
		artifact.State == RootFSBaseArtifactStateReady && string(artifact.Descriptor) == string(req.Descriptor)
}

func TestValidateReadyRootFSBaseArtifactRequiresImmutableDigests(t *testing.T) {
	req := readyRootFSBaseArtifactTestRequest()
	normalized, err := validateReadyRootFSBaseArtifact(req)
	require.NoError(t, err)
	require.Equal(t, req.ArtifactDigest, normalized.ArtifactDigest)
	require.Equal(t, req.SourceOCIDigest, normalized.SourceOCIDigest)
	require.NotSame(t, &req.Descriptor[0], &normalized.Descriptor[0])

	req.SourceOCIDigest = "ubuntu:latest"
	_, err = validateReadyRootFSBaseArtifact(req)
	require.ErrorContains(t, err, "source_oci_digest")

	req = readyRootFSBaseArtifactTestRequest()
	req.Platform.Architecture = "AMD64"
	_, err = validateReadyRootFSBaseArtifact(req)
	require.ErrorContains(t, err, "architecture")
}

func TestInitialRootFSGenerationIDSeparatesFilesystems(t *testing.T) {
	artifact := "sha256:" + strings.Repeat("a", 64)
	first := initialRootFSGenerationID("filesystem-a", artifact, 1)
	second := initialRootFSGenerationID("filesystem-b", artifact, 1)
	require.NotEqual(t, first, second)
	require.Equal(t, first, initialRootFSGenerationID("filesystem-a", artifact, 1))
}

func TestRestoredRootFSGenerationIDSeparatesFilesystemGenerationAndEpoch(t *testing.T) {
	first := restoredRootFSGenerationID("filesystem-a", "generation-a", 1)
	require.Equal(t, first, restoredRootFSGenerationID("filesystem-a", "generation-a", 1))
	require.NotEqual(t, first, restoredRootFSGenerationID("filesystem-b", "generation-a", 1))
	require.NotEqual(t, first, restoredRootFSGenerationID("filesystem-a", "generation-b", 1))
	require.NotEqual(t, first, restoredRootFSGenerationID("filesystem-a", "generation-a", 2))
}

func readyRootFSBaseArtifactTestRequest() *PutReadyRootFSBaseArtifactRequest {
	rootDigest := digest.FromString("base-block-root").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 1 << 30,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: rootDigest,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/base/map.page", Offset: 0, Length: 4096,
				Checksum: digest.FromString("base-map-page").String(),
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return &PutReadyRootFSBaseArtifactRequest{
		ArtifactDigest:   "sha256:" + strings.Repeat("a", 64),
		SourceOCIRef:     "registry.example/sandbox@sha256:" + strings.Repeat("b", 64),
		SourceOCIDigest:  "sha256:" + strings.Repeat("b", 64),
		BaseBlockRoot:    rootDigest,
		FormatGeneration: 1,
		Platform:         RootFSArtifactPlatform{OS: "linux", Architecture: "amd64"},
		ProcdProtocol:    "sandbox0.procd.test.v1",
		ProcdDigest:      "sha256:" + strings.Repeat("c", 64),
		Descriptor:       descriptor,
	}
}
