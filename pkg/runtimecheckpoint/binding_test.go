package runtimecheckpoint

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

func bindingFixture(t *testing.T) (rootfshandoff.StageRequest, runtimecontrol.Assignment, rootfshandoff.GenerationDescriptor) {
	t.Helper()
	d := digest.FromString("root").String()
	block := rootfsblock.Descriptor{Version: rootfsblock.DescriptorVersion,
		LogicalSizeBytes: 4096, BlockSizeBytes: 4096,
		MappingRoot: rootfsblock.MappingRootLocator{Version: rootfsblock.MappingPageVersion,
			RootDigest: d, Object: rootfsblock.ObjectRange{Key: "maps/root", Length: 1, Checksum: d}}}
	payload, err := rootfsblock.EncodeDescriptor(block)
	require.NoError(t, err)
	checkpoint := rootfshandoff.GenerationDescriptor{
		Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: "checkpoint", FilesystemID: "fs",
		SourceOCIDigest: d, BaseArtifactDigest: d, BaseBlockRoot: d, CurrentBlockHead: d, WriterEpoch: 2,
		FormatGeneration: rootfsblock.DescriptorVersion, DurabilityState: rootfsblock.DurabilityS3,
		LocatorVersion: 1, Descriptor: payload,
	}
	source := rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent: d, InitialGeneration: "initial",
		Identity: rootfshandoff.Identity{
			NodeUID: "source-node", BootID: "source-boot", RuntimeGeneration: "3", AllocationID: "source-allocation",
			NetworkIncarnationID: "network", TaskName: "slot", SourceOCIDigest: d,
			RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor", SlotNonce: "nonce",
			ClaimID: "claim", LaunchAttempt: "launch", RootFSID: "fs", WriterEpoch: 2,
			WriterGrantID: "grant", WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("test-token"),
		},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "source-allocation", NetworkIncarnationID: "network", ClaimID: "claim",
			NetworkEpoch: 1, PolicyDigest: d, SourceIP: "10.0.0.2", CtldGeneration: "ctld", NetNSIdentity: "netns",
		},
	}
	assignment := runtimecontrol.Assignment{SandboxID: "sandbox", TeamID: "team", RuntimeGeneration: 3, SecurityClass: "standard"}
	return source, assignment, checkpoint
}

func TestCheckpointBindingRejectsDifferentWriterFilesystemAndRuntime(t *testing.T) {
	d := digest.FromString("compatibility").String()
	source, assignment, checkpoint := bindingFixture(t)
	binding, err := Bind("migration", source, assignment, d, d, checkpoint)
	require.NoError(t, err)
	require.NoError(t, binding.Validate())
	for _, mutate := range []func(*rootfshandoff.StageRequest, *runtimecontrol.Assignment, *rootfshandoff.GenerationDescriptor){
		func(s *rootfshandoff.StageRequest, _ *runtimecontrol.Assignment, _ *rootfshandoff.GenerationDescriptor) {
			s.Identity.WriterGrantToken = "test-token"
		},
		func(_ *rootfshandoff.StageRequest, a *runtimecontrol.Assignment, _ *rootfshandoff.GenerationDescriptor) {
			a.RuntimeGeneration++
		},
		func(_ *rootfshandoff.StageRequest, _ *runtimecontrol.Assignment, c *rootfshandoff.GenerationDescriptor) {
			c.WriterEpoch++
		},
		func(_ *rootfshandoff.StageRequest, _ *runtimecontrol.Assignment, c *rootfshandoff.GenerationDescriptor) {
			c.FilesystemID = "other"
		},
		func(_ *rootfshandoff.StageRequest, _ *runtimecontrol.Assignment, c *rootfshandoff.GenerationDescriptor) {
			c.GenerationID = "initial"
		},
		func(_ *rootfshandoff.StageRequest, _ *runtimecontrol.Assignment, c *rootfshandoff.GenerationDescriptor) {
			c.SourceOCIDigest = digest.FromString("other").String()
		},
	} {
		s, a, c := bindingFixture(t)
		mutate(&s, &a, &c)
		_, err := Bind("migration", s, a, d, d, c)
		require.Error(t, err)
	}
	source.Identity.BootID = "successor-boot"
	next, err := Bind("migration", source, assignment, d, d, checkpoint)
	require.NoError(t, err)
	require.NotEqual(t, binding.SourceBindingDigest, next.SourceBindingDigest)
}
