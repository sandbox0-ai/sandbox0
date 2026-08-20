package rootfsrebase

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestWorkerRequestAndResultBindExactGenerationAuthority(t *testing.T) {
	request := testWorkerRequest(t)
	require.NoError(t, request.Validate())
	requestDigest, err := request.Digest()
	require.NoError(t, err)
	health := sha256.Sum256([]byte("health"))
	apply := ApplyResult{
		Version: ApplyResultVersion, TargetNodeCount: 1,
		OldManifestDigest:    bytesToHex(sha256Digest("old")),
		SourceManifestDigest: bytesToHex(sha256Digest("source")),
		DiffDigest:           bytesToHex(sha256Digest("diff")),
		TargetManifestDigest: bytesToHex(sha256Digest("target")),
	}
	apply.HealthProof = bytesToHex(health[:])
	result := WorkerResult{
		Version: WorkerProtocolVersion, RequestDigest: requestDigest,
		GenerationID: request.TargetGenerationID, FilesystemID: request.FilesystemID,
		ParentGenerationID: request.SourceGenerationID,
		SourceOCIDigest:    request.TargetSourceOCIDigest,
		BaseArtifactDigest: request.TargetBaseArtifactDigest,
		BaseBlockRoot:      request.TargetBaseBlockRoot,
		CurrentBlockHead:   request.TargetBaseBlockRoot, WriterEpoch: request.TargetWriterEpoch,
		FormatGeneration: request.TargetFormatGeneration,
		DurabilityState:  rootfsblock.DurabilityS3, LocatorVersion: request.SourceLocatorVersion + 1,
		Descriptor:        append([]byte(nil), request.TargetBaseDescriptor...),
		HealthCheckDigest: health[:], Apply: apply,
	}
	require.NoError(t, result.SealProof())
	require.NoError(t, result.ValidateFor(request))

	changed := result
	changed.WriterEpoch++
	require.Error(t, changed.ValidateFor(request))
	changed = result
	changed.Descriptor = append([]byte(nil), result.Descriptor...)
	changed.Descriptor[0] ^= 0xff
	require.Error(t, changed.ValidateFor(request))
}

func TestWorkerRequestRejectsGeometryAndUnboundedDirtySet(t *testing.T) {
	request := testWorkerRequest(t)
	request.MaxChangedBlocks = MaxWorkerChangedBlocks + 1
	require.ErrorContains(t, request.Validate(), "max_changed_blocks")
	request = testWorkerRequest(t)
	target := decodeWorkerDescriptor(t, request.TargetBaseDescriptor)
	target.LogicalSizeBytes += rootfsblock.LogicalBlockSize
	request.TargetBaseDescriptor = encodeWorkerDescriptor(t, target)
	require.ErrorContains(t, request.Validate(), "geometry")
}

func testWorkerRequest(t *testing.T) WorkerRequest {
	t.Helper()
	sourceBaseRoot := digest.FromString("worker-source-base").String()
	sourceHead := digest.FromString("worker-source-head").String()
	targetBaseRoot := digest.FromString("worker-target-base").String()
	return WorkerRequest{
		Version: WorkerProtocolVersion, OperationID: "worker-operation", SandboxID: "sandbox-worker",
		TeamID: "team-worker", FilesystemID: "filesystem-worker", SourceGenerationID: "generation-source",
		SourceOCIDigest:          digest.FromString("worker-source-oci").String(),
		SourceBaseArtifactDigest: digest.FromString("worker-source-artifact").String(),
		SourceBaseBlockRoot:      sourceBaseRoot, SourceCurrentBlockHead: sourceHead,
		SourceFormatGeneration: 1, SourceLocatorVersion: 4,
		SourceBaseDescriptor:       testWorkerDescriptor(t, "source-base", sourceBaseRoot, 8),
		SourceGenerationDescriptor: testWorkerDescriptor(t, "source", sourceHead, 8),
		TargetGenerationID:         "generation-target",
		TargetSourceOCIDigest:      digest.FromString("worker-target-oci").String(),
		TargetBaseArtifactDigest:   digest.FromString("worker-target-artifact").String(),
		TargetBaseBlockRoot:        targetBaseRoot, TargetFormatGeneration: 1, TargetWriterEpoch: 5,
		TargetBaseDescriptor: testWorkerDescriptor(t, "target-base", targetBaseRoot, 8),
		RollbackExpiresAt:    time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		MaxChangedBlocks:     1024,
	}
}

func testWorkerDescriptor(t *testing.T, suffix, root string, blocks int64) []byte {
	t.Helper()
	return encodeWorkerDescriptor(t, rootfsblock.Descriptor{
		Version:          rootfsblock.DescriptorVersion,
		LogicalSizeBytes: blocks * rootfsblock.LogicalBlockSize,
		BlockSizeBytes:   rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/worker/" + suffix + "/map", Length: 4096,
				Checksum: digest.FromString("worker-map-" + suffix).String(),
			},
		},
	})
}

func encodeWorkerDescriptor(t *testing.T, descriptor rootfsblock.Descriptor) []byte {
	t.Helper()
	payload, err := rootfsblock.EncodeDescriptor(descriptor)
	require.NoError(t, err)
	return payload
}

func decodeWorkerDescriptor(t *testing.T, payload []byte) rootfsblock.Descriptor {
	t.Helper()
	descriptor, err := rootfsblock.DecodeDescriptor(payload)
	require.NoError(t, err)
	return descriptor
}

func bytesToHex(value []byte) string {
	const digits = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, item := range value {
		encoded[index*2] = digits[item>>4]
		encoded[index*2+1] = digits[item&0x0f]
	}
	return string(encoded)
}

func sha256Digest(value string) []byte {
	sum := sha256.Sum256([]byte(value))
	return sum[:]
}
