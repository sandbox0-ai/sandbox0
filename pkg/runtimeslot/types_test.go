package runtimeslot

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

func TestRegistrationRequestValidatesCanonicalPhysicalIdentity(t *testing.T) {
	request := testRegistrationRequest()
	require.NoError(t, request.Validate())

	request.RuntimeCompatibility = strings.ToUpper(request.RuntimeCompatibility)
	require.ErrorContains(t, request.Validate(), "canonical sha256")
	request = testRegistrationRequest()
	request.ControlEndpoint = "unix://remote/run/slot.sock"
	require.ErrorContains(t, request.Validate(), "absolute path")
	request = testRegistrationRequest()
	request.AllocationID = " allocation"
	require.ErrorContains(t, request.Validate(), "allocation_id")
}

func TestLifecycleRequestsRequireCanonicalProofs(t *testing.T) {
	proof := strings.Repeat("ab", 32)
	ready := ReadinessRequest{
		AllocationID: "allocation", NodeBootID: "boot",
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
	}
	require.NoError(t, ready.Validate())
	ready.StorageReadyDigest = strings.ToUpper(proof)
	require.ErrorContains(t, ready.Validate(), "canonical")
	ready.StorageReadyDigest = " " + proof
	require.ErrorContains(t, ready.Validate(), "canonical")

	starting := StartingRequest{
		AllocationID: "allocation", NodeBootID: "boot", OperationID: "operation",
		ClaimID: "claim", LaunchAttempt: "launch", RunscContainerID: "runsc",
		RootFSBindingDigest: proof, ClaimNetworkDigest: proof,
	}
	require.NoError(t, starting.Validate())
	starting.ClaimNetworkDigest = "short"
	require.ErrorContains(t, starting.Validate(), "claim_network_digest")

	command := CommandReadyRequest{
		AllocationID: "allocation", NodeBootID: "boot", OperationID: "operation",
		ClaimID: "claim", ProcdInstanceID: "procd", ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: proof,
	}
	require.NoError(t, command.Validate())
}

func TestObservationRequiresExactClaimShape(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	observation := Observation{
		SlotID: "slot", State: StateClaiming, Revision: 2,
		ServerTime: now, HeartbeatExpiresAt: now.Add(time.Minute),
		ClaimOperationID: "operation", ClaimID: "claim",
		ClaimLeaseExpiresAt: ptrTime(now.Add(10 * time.Second)),
	}
	require.NoError(t, observation.Validate())
	observation.ClaimID = ""
	require.Error(t, observation.Validate())
	observation.ClaimID = "claim"
	observation.ClaimLeaseExpiresAt = nil
	require.ErrorContains(t, observation.Validate(), "claim_lease_expires_at")
}

func TestPathsEscapeOpaqueSlotID(t *testing.T) {
	require.Equal(t, "/internal/v1/runtime-slots/slot%2Fwith%20space", SlotPath("slot/with space"))
	require.Equal(t, "/internal/v1/runtime-slots/slot%2Fwith%20space/command-ready", CommandReadyPath("slot/with space"))
}

func TestNomadRunscContainerIDIsStableAndBounded(t *testing.T) {
	require.Equal(t, NomadRunscContainerID("slot-1"), NomadRunscContainerID("slot-1"))
	require.NotEqual(t, NomadRunscContainerID("slot-1"), NomadRunscContainerID("slot-2"))
	require.Len(t, NomadRunscContainerID(strings.Repeat("slot", 1_000)), 35)
}

func TestRequestsDoNotCarryAuthenticatedNodeUID(t *testing.T) {
	payload, err := json.Marshal(testRegistrationRequest())
	require.NoError(t, err)
	require.NotContains(t, string(payload), "node_uid")
}

func TestCommandReadyProofBindsCanonicalProcdCommand(t *testing.T) {
	proof := CommandReadyProof{
		Version: CommandReadyProofVersion, SlotID: "slot", OperationID: "operation", ClaimID: "claim",
		LaunchAttempt: "attempt", RunscContainerID: "runsc", ProcdInstanceID: "procd",
		ProcdAddress:  "http://192.0.2.2:49983",
		RequestMethod: "PUT", RequestPath: ProcdCommandReadyProbePath, ResponseStatus: 200,
		ResponseBodyDigest: strings.Repeat("ab", 32),
	}
	first, err := proof.Digest()
	require.NoError(t, err)
	require.Len(t, first, 64)
	second, err := proof.Digest()
	require.NoError(t, err)
	require.Equal(t, first, second)

	changed := proof
	changed.ProcdInstanceID = "another-procd"
	third, err := changed.Digest()
	require.NoError(t, err)
	require.NotEqual(t, first, third)
	changed = proof
	changed.RequestPath = "/readyz"
	require.ErrorContains(t, changed.Validate(), "canonical procd probe")
	changed = proof
	changed.ResponseStatus = 503
	require.ErrorContains(t, changed.Validate(), "canonical procd probe")
	changed = proof
	changed.ProcdAddress = "https://192.0.2.2:49983"
	require.ErrorContains(t, changed.Validate(), "canonical HTTP origin")
	changed = proof
	changed.ProcdAddress = "http://192.0.2.2:12345"
	require.ErrorContains(t, changed.Validate(), "Nomad procd port")

	address, err := NomadProcdAddress("2001:db8::1")
	require.NoError(t, err)
	require.Equal(t, "http://[2001:db8::1]:49983", address)
	require.Error(t, ValidateNomadProcdAddress("http://[2001:0db8::1]:49983"))
	_, err = NomadProcdAddress(" 192.0.2.2")
	require.Error(t, err)
}

func TestNodeClaimControlRequestValidatesRegionalBinding(t *testing.T) {
	request := testNodeClaimControlRequest()
	require.NoError(t, request.ValidateRegional())

	changed := request
	changed.RootfsPath = "/host/development-root"
	require.ErrorContains(t, changed.ValidateRegional(), "forbidden")
	changed = request
	changed.OperationID = ""
	require.ErrorContains(t, changed.ValidateRegional(), "operation_id")
	changed = request
	changed.PolicyToken = "another-token"
	require.ErrorContains(t, changed.ValidateRegional(), "writer grant")
	changed = request
	changed.NetworkPolicy = `{"mode":"allow-all"}`
	require.ErrorContains(t, changed.ValidateRegional(), "network_policy")
	changed = request
	changed.NetworkPolicy = strings.Repeat("x", MaxNetworkPolicyBytes+1)
	require.ErrorContains(t, changed.ValidateRegional(), "network policy exceeds 64 KiB")
	changed = request
	changed.Runtime = nil
	require.ErrorContains(t, changed.ValidateRegional(), "runtime assignment")
	changed = request
	changed.Runtime = &runtimecontrol.Assignment{
		SandboxID: "sandbox-1", RuntimeGeneration: 1,
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "another-sandbox"},
	}
	require.ErrorContains(t, changed.ValidateRegional(), "sandbox environment")
	changed = request
	changedRuntime := *request.Runtime
	changedRuntime.EnvVars = map[string]string{
		runtimecontrol.EnvSandboxID: "sandbox-1",
		"MODE":                      "changed",
	}
	changed.Runtime = &changedRuntime
	require.ErrorContains(t, changed.ValidateRegional(), "runtime assignment revision")
	changed = request
	oversizedRuntime := *request.Runtime
	oversizedRuntime.EnvVars = map[string]string{
		runtimecontrol.EnvSandboxID: "sandbox-1",
		"OVERSIZED":                 strings.Repeat("x", MaxRuntimeAssignmentBytes),
	}
	changed.Runtime = &oversizedRuntime
	require.ErrorContains(t, changed.ValidateRegional(), "exceeds 64 KiB")
}

func TestNodeCleanupProofBindsExactRequestAndAbsenceFacts(t *testing.T) {
	request := NodeCleanupControlRequest{
		OperationID: "cleanup-1", WriterOperationID: "writer-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: "runsc-1", WriterGrantID: "grant-1",
		WriterFenceDigest: strings.Repeat("ab", 32),
	}
	require.NoError(t, request.Validate())
	proof := NodeCleanupControlProof{
		Version: NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, SlotID: request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterFenceDigest: request.WriterFenceDigest,
		RootFSCrashOperationID: request.WriterOperationID, RootFSCrashProofDigest: strings.Repeat("cd", 32),
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	digest, err := proof.Digest()
	require.NoError(t, err)
	proof.ProofDigest = digest
	require.NoError(t, proof.Validate())
	require.Equal(t, request, proof.Request())

	changed := proof
	changed.NetNSIdentity = "netns-v1:3:4"
	require.ErrorContains(t, changed.Validate(), "digest")
	changed = proof
	changed.NetworkPolicyAbsent = false
	require.ErrorContains(t, changed.Validate(), "physical absence")
	changed = proof
	changed.WriterFenceDigest = strings.Repeat("AB", 32)
	require.ErrorContains(t, changed.Validate(), "canonical")
	changed = proof
	changed.RootFSCrashOperationID = "another-writer-operation"
	require.ErrorContains(t, changed.Validate(), "writer operation")
}

func testRegistrationRequest() RegistrationRequest {
	return RegistrationRequest{
		ClusterID: "cluster", AllocationID: "allocation", AllocationNamespace: "default",
		NodeID: "node", NodeBootID: "boot", NetNSIdentity: "netns",
		ControlEndpoint:      "unix:///run/sandbox0/slot.sock",
		RuntimeCompatibility: digest.FromString("amd64/runsc/directfs").String(),
	}
}

func testNodeClaimControlRequest() NodeClaimControlRequest {
	networkPolicy := `{"mode":"block-all"}`
	token := strings.Repeat("writer-token-", 4)
	stage := &rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent:         "sha256:" + strings.Repeat("a", 64), InitialGeneration: "generation-1",
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			PodUID: "allocation-1", PodSandboxID: "allocation-network-1", ClaimID: "claim-1",
			NetworkEpoch: 4, PolicyDigest: NetworkPolicyDigest(networkPolicy), PodIP: "192.0.2.2",
			CtldGeneration: "ctld-1", NetNSIdentity: "1:2",
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-uid-1", BootID: "boot-1", RuntimeGeneration: "runtime-1",
			PodUID: "allocation-1", PodSandboxID: "allocation-network-1", ContainerName: "slot",
			Image: "procd-image-1", Snapshotter: "nomad-driver", RuntimeName: "sandbox0-gvisor",
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "filesystem-1", WriterEpoch: 4, WriterGrantID: "grant-1",
			WriterGrantToken: token, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(token),
		},
	}
	runtime := &runtimecontrol.Assignment{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1"},
	}
	revision, err := runtime.Revision()
	if err != nil {
		panic(err)
	}
	stage.Labels = map[string]string{RuntimeAssignmentRevisionLabel: revision}
	return NodeClaimControlRequest{
		OperationID: "operation-1", ClaimID: stage.Identity.ClaimID,
		PolicyToken: token, WriterEpoch: strconv.FormatInt(stage.Identity.WriterEpoch, 10),
		Stage: stage, NetworkPolicy: networkPolicy,
		Runtime: runtime,
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
