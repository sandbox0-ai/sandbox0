package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
)

func TestNodeChannelPausedRebaseBindsExactWorkerAndResult(t *testing.T) {
	request, workerResult := testNodeChannelPausedRebase(t)
	target := testNodeChannelRebaseTarget()
	command, err := NewNodeChannelPausedRebaseCommand(target, request)
	if err != nil {
		t.Fatal(err)
	}
	result := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: command.RequestID,
		Kind: command.Kind, PausedRebase: &workerResult,
	}
	if err := result.ValidateFor(command); err != nil {
		t.Fatal(err)
	}
	result.PausedRebase = cloneNodeChannelRebaseResult(workerResult)
	result.PausedRebase.WriterEpoch++
	if err := result.ValidateFor(command); err == nil {
		t.Fatal("paused-rebase output for another writer epoch was accepted")
	}
	ackRequest := request
	ackRequest.AcknowledgeProofDigest = workerResult.ProofDigest
	ackCommand, err := NewNodeChannelPausedRebaseCommand(target, ackRequest)
	if err != nil {
		t.Fatal(err)
	}
	requestDigest, err := request.Worker.Digest()
	if err != nil {
		t.Fatal(err)
	}
	ackResult := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: ackCommand.RequestID, Kind: ackCommand.Kind,
		PausedRebaseAck: &rootfsrebase.WorkerAcknowledgement{
			RequestDigest: requestDigest, ProofDigest: workerResult.ProofDigest,
		},
	}
	if err := ackResult.ValidateFor(ackCommand); err != nil {
		t.Fatal(err)
	}
	ackResult.PausedRebaseAck.ProofDigest = digest.FromString("another-proof").String()
	if err := ackResult.ValidateFor(ackCommand); err == nil {
		t.Fatal("acknowledgement for another proof was accepted")
	}
	rejectRequest := request
	rejectRequest.Reject = true
	rejectCommand, err := NewNodeChannelPausedRebaseCommand(target, rejectRequest)
	if err != nil {
		t.Fatal(err)
	}
	rejection, err := rootfsrebase.RejectWithoutResult(request.Worker)
	if err != nil {
		t.Fatal(err)
	}
	rejectResult := NodeChannelResult{
		Version: NodeChannelVersion, RequestID: rejectCommand.RequestID, Kind: rejectCommand.Kind,
		PausedRebaseReject: &rejection,
	}
	if err := rejectResult.ValidateFor(rejectCommand); err != nil {
		t.Fatal(err)
	}
	rejectRequest.AcknowledgeProofDigest = rejection.ProofDigest
	if err := rejectRequest.Validate(); err == nil {
		t.Fatal("paused-rebase request accepted simultaneous rejection and acknowledgement")
	}

	changed := command
	changed.PausedRebase.Worker.MaxChangedBlocks--
	if err := changed.Validate(); err == nil {
		t.Fatal("mutated paused-rebase command retained its request digest")
	}
	target.SlotID = "runtime-slot-is-not-valid"
	if _, err := NewNodeChannelPausedRebaseCommand(target, request); err == nil {
		t.Fatal("paused-rebase command accepted a runtime-slot target")
	}
}

func TestNodeChannelHelloOrdersOptionalRebaseCapability(t *testing.T) {
	hello := testNodeChannelHello()
	hello.Capabilities = []NodeChannelCommandKind{
		NodeChannelCommandClaim, NodeChannelCommandCommandReady,
		NodeChannelCommandPausedRebase, NodeChannelCommandCleanup,
	}
	if err := hello.Validate(); err != nil || !hello.Supports(NodeChannelCommandPausedRebase) {
		t.Fatalf("rebase-capable hello = %+v, %v", hello, err)
	}
	hello.Capabilities = []NodeChannelCommandKind{
		NodeChannelCommandClaim, NodeChannelCommandCommandReady,
		NodeChannelCommandRunningFork, NodeChannelCommandPausedRebase, NodeChannelCommandCleanup,
	}
	if err := hello.Validate(); err != nil {
		t.Fatal(err)
	}
	hello.Capabilities[2], hello.Capabilities[3] = hello.Capabilities[3], hello.Capabilities[2]
	if err := hello.Validate(); err == nil {
		t.Fatal("misordered optional capabilities were accepted")
	}
}

func testNodeChannelPausedRebase(t *testing.T) (NodePausedRebaseControlRequest, rootfsrebase.WorkerResult) {
	t.Helper()
	sourceBaseRoot := digest.FromString("channel-rebase-source-base").String()
	sourceHead := digest.FromString("channel-rebase-source-head").String()
	targetBaseRoot := digest.FromString("channel-rebase-target-base").String()
	worker := rootfsrebase.WorkerRequest{
		Version: rootfsrebase.WorkerProtocolVersion, OperationID: "channel-rebase-operation",
		SandboxID: "sandbox-rebase", TeamID: "team-rebase", FilesystemID: "filesystem-rebase",
		SourceGenerationID: "generation-source", SourceOCIDigest: digest.FromString("source-oci").String(),
		SourceBaseArtifactDigest: digest.FromString("source-artifact").String(),
		SourceBaseBlockRoot:      sourceBaseRoot, SourceCurrentBlockHead: sourceHead,
		SourceFormatGeneration: 1, SourceLocatorVersion: 2,
		SourceBaseDescriptor:       testNodeChannelRebaseDescriptor(t, "source-base", sourceBaseRoot),
		SourceGenerationDescriptor: testNodeChannelRebaseDescriptor(t, "source", sourceHead),
		TargetGenerationID:         "generation-target",
		TargetSourceOCIDigest:      digest.FromString("target-oci").String(),
		TargetBaseArtifactDigest:   digest.FromString("target-artifact").String(),
		TargetBaseBlockRoot:        targetBaseRoot, TargetFormatGeneration: 1, TargetWriterEpoch: 3,
		TargetBaseDescriptor: testNodeChannelRebaseDescriptor(t, "target-base", targetBaseRoot),
		RollbackExpiresAt:    time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		MaxChangedBlocks:     1024,
	}
	requestDigest, err := worker.Digest()
	if err != nil {
		t.Fatal(err)
	}
	health := sha256.Sum256([]byte("channel-rebase-health"))
	apply := rootfsrebase.ApplyResult{
		Version: rootfsrebase.ApplyResultVersion, AppliedChanges: 1, TargetNodeCount: 1,
		OldManifestDigest: testNodeChannelHex("old"), SourceManifestDigest: testNodeChannelHex("source"),
		DiffDigest: testNodeChannelHex("diff"), TargetManifestDigest: testNodeChannelHex("target"),
		HealthProof: hex.EncodeToString(health[:]),
	}
	result := rootfsrebase.WorkerResult{
		Version: rootfsrebase.WorkerProtocolVersion, RequestDigest: requestDigest,
		GenerationID: worker.TargetGenerationID, FilesystemID: worker.FilesystemID,
		ParentGenerationID: worker.SourceGenerationID, SourceOCIDigest: worker.TargetSourceOCIDigest,
		BaseArtifactDigest: worker.TargetBaseArtifactDigest, BaseBlockRoot: worker.TargetBaseBlockRoot,
		CurrentBlockHead: worker.TargetBaseBlockRoot, WriterEpoch: worker.TargetWriterEpoch,
		FormatGeneration: worker.TargetFormatGeneration, DurabilityState: rootfsblock.DurabilityS3,
		LocatorVersion: worker.SourceLocatorVersion + 1, Descriptor: append([]byte(nil), worker.TargetBaseDescriptor...),
		HealthCheckDigest: health[:], DirtyBlocks: 1, Apply: apply,
	}
	if err := result.SealProof(); err != nil {
		t.Fatal(err)
	}
	if err := result.ValidateFor(worker); err != nil {
		t.Fatal(err)
	}
	return NodePausedRebaseControlRequest{Worker: worker}, result
}

func testNodeChannelRebaseTarget() NodeChannelTarget {
	return NodeChannelTarget{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}
}

func testNodeChannelRebaseDescriptor(t *testing.T, suffix, root string) []byte {
	t.Helper()
	payload, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: 8 * rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "rootfs/channel-rebase/" + suffix + "/map", Length: 4096,
				Checksum: digest.FromString("channel-rebase-map-" + suffix).String(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func testNodeChannelHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func cloneNodeChannelRebaseResult(result rootfsrebase.WorkerResult) *rootfsrebase.WorkerResult {
	result.Descriptor = append([]byte(nil), result.Descriptor...)
	result.HealthCheckDigest = append([]byte(nil), result.HealthCheckDigest...)
	return &result
}
