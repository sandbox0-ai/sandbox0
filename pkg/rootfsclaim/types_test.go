package rootfsclaim

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestStageRequestRejectsCrossPodGrant(t *testing.T) {
	request := validStageRequest()
	request.Pod.UID = "other-pod"
	require.ErrorContains(t, request.Validate(), "does not match")
}

func TestReadyRequestDoesNotAcceptManagerOwnedRuntimeFacts(t *testing.T) {
	payload := []byte(`{"pod":{"namespace":"ns","name":"pod","uid":"uid","node_uid":"node","slot_nonce":"slot"},"parent":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source":"/host/root"}`)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request ReadyRequest
	require.Error(t, decoder.Decode(&request))
}

func TestProtocolPathsEscapeOpaqueIDs(t *testing.T) {
	require.Equal(t, "/internal/v1/rootfs-claims/claim%2Fone/stage", StagePath("claim/one"))
	require.Equal(t, "/internal/v1/rootfs-slots/slot%2Fone/gate", GatePath("slot/one"))
}

func validStageRequest() StageRequest {
	token := "01234567890123456789012345678901"
	identity := rootfshandoff.Identity{
		NodeUID: "node", BootID: "boot", RuntimeGeneration: "runtime", PodUID: "pod-uid",
		PodSandboxID: "sandbox", ContainerName: "procd", Image: "registry/gate@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Snapshotter: "sandbox0-rootfs", RuntimeName: "io.containerd.runsc.v1", SlotNonce: "slot", ClaimID: "claim",
		LaunchAttempt: "attempt", RootFSID: "rootfs", WriterEpoch: 1, WriterGrantID: "grant",
		WriterGrantToken: token, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(token),
	}
	policy := rootfshandoff.NetworkPolicyToken{
		PodUID: identity.PodUID, PodSandboxID: identity.PodSandboxID, ClaimID: identity.ClaimID,
		NetworkEpoch: 1, PolicyDigest: "policy", PodIP: "10.0.0.2", CtldGeneration: "ctld", NetNSIdentity: "1:2",
	}
	return StageRequest{
		Pod: PodIdentity{Namespace: "ns", Name: "pod", UID: identity.PodUID, NodeUID: identity.NodeUID, SlotNonce: identity.SlotNonce},
		Handoff: rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion,
			Parent:            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			InitialGeneration: "generation", ExpectedPolicyToken: policy, Identity: identity},
	}
}
