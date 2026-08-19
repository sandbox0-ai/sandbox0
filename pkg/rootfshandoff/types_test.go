package rootfshandoff

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestStageRequestValidate(t *testing.T) {
	request := validStageRequest()
	require.NoError(t, request.Validate())

	request.ExpectedPolicyToken.PolicyDigest = ""
	require.EqualError(t, request.Validate(), "expected_policy_token: policy_digest is required")

	request = validStageRequest()
	request.Parent = "not-a-chain-id"
	require.ErrorContains(t, request.Validate(), "parent must be a valid chain ID")

	request = validStageRequest()
	request.Parent = digest.SHA512.FromString("slot-1").String()
	require.ErrorContains(t, request.Validate(), "canonical sha256")

	request = validStageRequest()
	request.Identity.WriterGrantToken = "different-token"
	require.ErrorContains(t, request.Validate(), "does not match writer_grant_token")
}

func TestGenerationDescriptorRejectsNonSHA256IdentityDigests(t *testing.T) {
	require.ErrorContains(t, validateSHA256Digest(digest.SHA512.FromString("value").String()), "canonical sha256")
	require.ErrorContains(t, validateSHA256Digest(" "+digest.FromString("value").String()), "canonical sha256")
}

func TestReadyRequestNormalize(t *testing.T) {
	request := ReadyRequest{
		Parent: "parent", Source: "/run/sandbox0/rootfs/root-1",
		AppliedPolicyToken: validStageRequest().ExpectedPolicyToken,
	}
	normalized, err := request.Normalize()
	require.NoError(t, err)
	require.Equal(t, "bind", normalized.Type)
	require.Equal(t, []string{"rbind", "rw", "nosuid", "nodev"}, normalized.Options)

	request.Options = []string{"rbind", "rw"}
	_, err = request.Normalize()
	require.ErrorContains(t, err, "nosuid and nodev")

	request.Options = []string{"rbind", "rw", "nosuid", "nodev", "suid"}
	_, err = request.Normalize()
	require.ErrorContains(t, err, "not allowed")
}

func TestRetireResultBindsDurabilityAndBlockHead(t *testing.T) {
	descriptor := rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: digest.FromString("root").String(),
			Object: rootfsblock.ObjectRange{Key: "maps/root", Length: 1, Checksum: digest.FromString("page").String()},
		},
	}
	payload, err := rootfsblock.EncodeDescriptor(descriptor)
	require.NoError(t, err)
	result := RetireResult{
		Parent: "parent", RootFSID: "rootfs", WriterEpoch: 1, OperationID: "operation",
		CurrentBlockHead: descriptor.MappingRoot.RootDigest, DurabilityState: rootfsblock.DurabilityS3,
		Descriptor: payload, DetachProof: string(bytes.Repeat([]byte{'a'}, 64)),
	}
	require.NoError(t, result.Validate())

	result.CurrentBlockHead = digest.FromString("other").String()
	require.ErrorContains(t, result.Validate(), "mapping root")
	result.CurrentBlockHead = descriptor.MappingRoot.RootDigest
	result.DurabilityState = rootfsblock.DurabilityComposite
	require.ErrorContains(t, result.Validate(), "requires a composite tail")
}

func validStageRequest() StageRequest {
	request := StageRequest{
		BindingVersion:    WriterBindingVersion,
		Parent:            digest.FromString("slot-1").String(),
		InitialGeneration: "generation-1",
		Identity: Identity{
			NodeUID: "node-1", BootID: "boot-1", RuntimeGeneration: "runtime-1",
			PodUID: "pod-1", PodSandboxID: "sandbox-1", ContainerName: "app",
			Image: "gate-b@sha256:1", Snapshotter: "sandbox0-rootfs", RuntimeName: "io.containerd.runsc.v1",
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "rootfs-1", WriterEpoch: 1, WriterGrantID: "grant-1", WriterGrantToken: "test-token",
		},
	}
	request.Identity.WriterGrantTokenDigest = WriterGrantTokenDigest(request.Identity.WriterGrantToken)
	request.ExpectedPolicyToken = NetworkPolicyToken{
		PodUID: request.Identity.PodUID, PodSandboxID: request.Identity.PodSandboxID,
		ClaimID: request.Identity.ClaimID, NetworkEpoch: 1, PolicyDigest: "policy-1",
		PodIP: "10.0.0.2", CtldGeneration: "ctld-1", NetNSIdentity: "netns-1",
	}
	return request
}

func TestWriterBindingDigestBindsCompleteRequestWithoutPersistingToken(t *testing.T) {
	request := validStageRequest()
	first, err := request.BindingDigest()
	require.NoError(t, err)
	request.Identity.WriterGrantToken = "rotated-retry-token"
	second, err := request.BindingDigest()
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Empty(t, request.WithoutWriterGrantToken().Identity.WriterGrantToken)

	request.Identity.WriterGrantTokenDigest = WriterGrantTokenDigest(request.Identity.WriterGrantToken)
	issuedAgain, err := request.BindingDigest()
	require.NoError(t, err)
	require.NotEqual(t, first, issuedAgain)

	request.Identity.WriterEpoch++
	third, err := request.BindingDigest()
	require.NoError(t, err)
	require.NotEqual(t, first, third)
}

func TestRunningForkCheckpointProofBindsSequenceAndDescriptor(t *testing.T) {
	binding := sha256.Sum256([]byte("running-fork-binding"))
	proof := RunningForkCheckpointProof{
		Version: RunningForkCheckpointVersion, OperationID: "fork-operation",
		SourceSandboxID: "source", SourceFilesystemID: "source-rootfs", TargetSandboxID: "target",
		SourceWriterGrantID: "grant", SourceWriterEpoch: 7,
		BindingVersion: WriterBindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		ExpectedSourceGenerationID: "source-generation", CheckpointGenerationID: "checkpoint-generation",
		CheckpointSequence: 0, CheckpointDescriptorDigest: digest.FromString("checkpoint-descriptor").String(),
	}
	first, err := proof.Digest()
	require.NoError(t, err)
	proof.CheckpointSequence++
	second, err := proof.Digest()
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	proof.CheckpointDescriptorDigest = digest.FromString("changed-descriptor").String()
	third, err := proof.Digest()
	require.NoError(t, err)
	require.NotEqual(t, second, third)
}

func TestRunningForkCheckpointRequestRequiresCanonicalStableIDs(t *testing.T) {
	request := RunningForkCheckpointRequest{
		OperationID: "operation", SourceSandboxID: "source",
		TargetSandboxID: "target", TargetGenerationID: "generation",
	}
	require.NoError(t, request.Validate())
	request.OperationID = " operation"
	require.ErrorContains(t, request.Validate(), "canonical")
	request.OperationID = "operation"
	request.TargetSandboxID = request.SourceSandboxID
	require.ErrorContains(t, request.Validate(), "must differ")
}
