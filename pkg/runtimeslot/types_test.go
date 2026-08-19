package runtimeslot

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
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
		ClaimID: "claim", ProcdInstanceID: "procd", CommandReadyDigest: proof,
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

func TestRequestsDoNotCarryAuthenticatedNodeUID(t *testing.T) {
	payload, err := json.Marshal(testRegistrationRequest())
	require.NoError(t, err)
	require.NotContains(t, string(payload), "node_uid")
}

func testRegistrationRequest() RegistrationRequest {
	return RegistrationRequest{
		ClusterID: "cluster", AllocationID: "allocation", AllocationNamespace: "default",
		NodeID: "node", NodeBootID: "boot", NetNSIdentity: "netns",
		ControlEndpoint:      "unix:///run/sandbox0/slot.sock",
		RuntimeCompatibility: digest.FromString("amd64/runsc/directfs").String(),
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
