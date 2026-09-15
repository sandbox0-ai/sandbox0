package runtimeslotauthority

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type fakeRegistrationAbortStore struct {
	*fakeStore
	requests []protocol.RegistrationAbortRequest
	response protocol.RegistrationAbortResponse
}

func (s *fakeRegistrationAbortStore) AbortRuntimeSlotRegistration(_ context.Context, r protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
	s.requests = append(s.requests, r)
	return s.response, nil
}

func testAbortRequest() protocol.RegistrationAbortRequest {
	return protocol.RegistrationAbortRequest{SlotID: "allocation/slot/identity", ClusterID: "cluster", AllocationID: "allocation", NodeID: "nomad-node", NodeUID: "node-uid", NodeBootID: "old-boot", NetNSIdentity: "netns"}
}

func TestRegistrationAbortAuthorizesExactNodeAndSlotBeforeMutation(t *testing.T) {
	for _, field := range []string{"cluster", "node", "uid", "path", "valid"} {
		t.Run(field, func(t *testing.T) {
			request := testAbortRequest()
			path := protocol.RegistrationAbortPath(request.SlotID)
			store := &fakeRegistrationAbortStore{fakeStore: &fakeStore{}, response: protocol.RegistrationAbortResponse{Registered: true}}
			handler := testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, store)
			switch field {
			case "cluster":
				request.ClusterID = "foreign"
			case "node":
				request.NodeID = "foreign"
			case "uid":
				request.NodeUID = "foreign"
			case "path":
				path = protocol.RegistrationAbortPath("another-slot")
			}
			response := doJSON(t, handler, http.MethodPut, path, request, "token")
			if field == "valid" {
				require.Equal(t, http.StatusOK, response.Code)
				require.Equal(t, []protocol.RegistrationAbortRequest{request}, store.requests)
			} else {
				require.Equal(t, http.StatusForbidden, response.Code)
				require.Empty(t, store.requests)
			}
		})
	}
}

func TestRegistrationAbortClientValidatesAuthenticatedFence(t *testing.T) {
	request := testAbortRequest()
	cleanup, err := request.CleanupRequest()
	require.NoError(t, err)
	store := &fakeRegistrationAbortStore{fakeStore: &fakeStore{}, response: protocol.RegistrationAbortResponse{Cleanup: &cleanup}}
	handler := testHandler(t, &fakeVerifier{identity: nodeauth.Identity{ClusterID: "cluster", NodeID: "nomad-node", NodeUID: "node-uid"}}, store)
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	client := testClient(t, server)
	response, err := client.AbortRegistration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, cleanup, *response.Cleanup)
	require.False(t, response.Completed)
	store.response.Cleanup = nil
	_, err = client.AbortRegistration(t.Context(), request)
	require.Error(t, err, "a missing exact cleanup is not permission to mutate the node")
}
