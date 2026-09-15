package runtimeslotauthority

import (
	"net/http"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestRegisterRejectsForeignPlacementBeforeMutation(t *testing.T) {
	for _, field := range []string{"cluster", "node"} {
		t.Run(field, func(t *testing.T) {
			identity := nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}
			body := protocol.RegistrationRequest{
				ClusterID: "cluster", NodeID: "nomad-node", AllocationID: "allocation",
				AllocationNamespace: "default", NodeBootID: "boot", NetNSIdentity: "netns",
				ControlEndpoint:      "unix:///run/sandbox0/slot.sock",
				RuntimeCompatibility: digest.FromString("amd64/runsc/directfs").String(),
			}
			if field == "cluster" {
				body.ClusterID = "foreign-cluster"
			} else {
				body.NodeID = "foreign-node"
			}
			store := &fakeStore{slot: testSlot()}
			response := doJSON(t, testHandler(t, &fakeVerifier{identity: identity}, store), http.MethodPut, protocol.SlotPath("slot"), body, "token")
			require.Equal(t, http.StatusForbidden, response.Code)
			require.Nil(t, store.register, "foreign placement must not create a regional slot")
		})
	}
}

func TestSlotAuthorityRequiresCompletePlacementIdentity(t *testing.T) {
	for _, identity := range []nodeauth.Identity{
		{NodeUID: "node-uid"},
		{ClusterID: "cluster", NodeUID: "node-uid"},
		{NodeID: "nomad-node", NodeUID: "node-uid"},
	} {
		store := &fakeStore{slot: testSlot()}
		response := doRequest(t, testHandler(t, &fakeVerifier{identity: identity}, store), http.MethodGet, protocol.SlotPath("slot"), nil, "token", "")
		require.Equal(t, http.StatusForbidden, response.Code)
		require.Empty(t, store.getIDs)
	}
}

func TestSlotAuthorityRejectsChangedPlacementWithSameNodeUID(t *testing.T) {
	for _, field := range []string{"cluster", "node"} {
		t.Run(field, func(t *testing.T) {
			slot := testSlot()
			slot.ClusterID, slot.NodeID = "cluster", "nomad-node"
			if field == "cluster" {
				slot.ClusterID = "foreign-cluster"
			} else {
				slot.NodeID = "foreign-node"
			}
			store := &fakeStore{slot: slot}
			handler := testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, store)
			response := doRequest(t, handler, http.MethodGet, protocol.SlotPath("slot"), nil, "token", "")
			require.Equal(t, http.StatusForbidden, response.Code)
			response = doJSON(t, handler, http.MethodPut, protocol.HeartbeatPath("slot"), protocol.HeartbeatRequest{AllocationID: "allocation", NodeBootID: "boot"}, "token")
			require.Equal(t, http.StatusForbidden, response.Code)
			require.Nil(t, store.heartbeat)
		})
	}
}
