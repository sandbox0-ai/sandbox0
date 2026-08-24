package runtimeslotauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type fakeVerifier struct {
	identity nodeauth.Identity
	err      error
	token    string
	calls    int
}

func (f *fakeVerifier) Verify(_ context.Context, token string) (nodeauth.Identity, error) {
	f.calls++
	f.token = token
	return f.identity, f.err
}

type fakeStore struct {
	slot          *sandboxstore.RuntimeSlot
	getErr        error
	registerErr   error
	transitionErr error
	register      *sandboxstore.RegisterRuntimeSlotRequest
	ready         *sandboxstore.ReportRuntimeSlotReadyRequest
	heartbeat     *sandboxstore.HeartbeatRuntimeSlotRequest
	starting      *sandboxstore.StartRuntimeSlotRequest
	commandReady  *sandboxstore.MarkRuntimeSlotCommandReadyRequest
	getIDs        []string
}

func (f *fakeStore) RegisterRuntimeSlot(_ context.Context, request *sandboxstore.RegisterRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	f.register = request
	return f.slot, f.registerErr
}

func (f *fakeStore) GetRuntimeSlot(_ context.Context, slotID string) (*sandboxstore.RuntimeSlot, error) {
	f.getIDs = append(f.getIDs, slotID)
	return f.slot, f.getErr
}

func (f *fakeStore) ReportRuntimeSlotReady(_ context.Context, request *sandboxstore.ReportRuntimeSlotReadyRequest) (*sandboxstore.RuntimeSlot, error) {
	f.ready = request
	return f.slot, f.transitionErr
}

func (f *fakeStore) HeartbeatRuntimeSlot(_ context.Context, request *sandboxstore.HeartbeatRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	f.heartbeat = request
	return f.slot, f.transitionErr
}

func (f *fakeStore) StartRuntimeSlot(_ context.Context, request *sandboxstore.StartRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	f.starting = request
	return f.slot, f.transitionErr
}

func (f *fakeStore) MarkRuntimeSlotCommandReady(_ context.Context, request *sandboxstore.MarkRuntimeSlotCommandReadyRequest) (*sandboxstore.RuntimeSlot, error) {
	f.commandReady = request
	return f.slot, f.transitionErr
}

func TestRegisterDerivesNodeIdentityAndReturnsAuthorityTime(t *testing.T) {
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid", AgentUID: "agent-instance"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)
	body := protocol.RegistrationRequest{
		ClusterID: "cluster", AllocationID: "allocation", AllocationNamespace: "default",
		NodeID: "nomad-node", NodeBootID: "boot", NetNSIdentity: "netns",
		ControlEndpoint:      "unix:///run/sandbox0/slot.sock",
		RuntimeCompatibility: digest.FromString("amd64/runsc/directfs").String(),
	}
	response := doJSON(t, handler, http.MethodPut, protocol.SlotPath("slot"), body, "token")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "token", verifier.token)
	require.NotNil(t, store.register)
	require.Equal(t, "node-uid", store.register.NodeUID)
	require.Equal(t, time.Minute, store.register.HeartbeatTTL)

	var observation protocol.Observation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &observation))
	require.NoError(t, observation.Validate())
	require.Equal(t, store.slot.AuthorityObservedAt, observation.ServerTime)
}

func TestObserveAndTransitionsAuthorizeExactNodeIncarnation(t *testing.T) {
	proof := strings.Repeat("ab", 32)
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)

	observed := doRequest(t, handler, http.MethodGet, protocol.SlotPath("slot"), nil, "token", "")
	require.Equal(t, http.StatusOK, observed.Code)

	ready := protocol.ReadinessRequest{
		AllocationID: "allocation", NodeBootID: "boot", RuntimeReadyDigest: proof,
		NetworkReadyDigest: strings.Repeat("bc", 32), StorageReadyDigest: strings.Repeat("cd", 32),
	}
	response := doJSON(t, handler, http.MethodPut, protocol.ReadyPath("slot"), ready, "token")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, bytes.Repeat([]byte{0xab}, 32), store.ready.RuntimeReadyDigest)
	require.Equal(t, "node-uid", store.ready.NodeUID)

	heartbeat := protocol.HeartbeatRequest{AllocationID: "allocation", NodeBootID: "boot"}
	response = doJSON(t, handler, http.MethodPut, protocol.HeartbeatPath("slot"), heartbeat, "token")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, time.Minute, store.heartbeat.TTL)

	starting := protocol.StartingRequest{
		AllocationID: "allocation", NodeBootID: "boot", OperationID: "operation", ClaimID: "claim",
		LaunchAttempt: "launch", RunscContainerID: "runsc", RootFSBindingDigest: proof,
		ClaimNetworkDigest: strings.Repeat("de", 32), ResourceLeaseID: "resource-lease",
		ResourceLeaseDigest: strings.Repeat("ad", 32),
	}
	response = doJSON(t, handler, http.MethodPut, protocol.StartingPath("slot"), starting, "token")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, bytes.Repeat([]byte{0xde}, 32), store.starting.ClaimNetworkDigest)
	require.Equal(t, "node-uid", store.starting.NodeUID)

	command := protocol.CommandReadyRequest{
		AllocationID: "allocation", NodeBootID: "boot", OperationID: "operation", ClaimID: "claim",
		ProcdInstanceID: "procd", ProcdAddress: "http://192.0.2.2:49983",
		CommandReadyDigest: strings.Repeat("ef", 32),
	}
	response = doJSON(t, handler, http.MethodPut, protocol.CommandReadyPath("slot"), command, "token")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, bytes.Repeat([]byte{0xef}, 32), store.commandReady.CommandReadyDigest)
	require.Equal(t, command.ProcdAddress, store.commandReady.ProcdAddress)
}

func TestTransitionRejectsWrongAuthenticatedNodeBeforeMutation(t *testing.T) {
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "other-node"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)
	body := protocol.HeartbeatRequest{AllocationID: "allocation", NodeBootID: "boot"}
	response := doJSON(t, handler, http.MethodPut, protocol.HeartbeatPath("slot"), body, "token")
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Nil(t, store.heartbeat)
	requireErrorCode(t, response, protocol.ErrorPermissionDenied)
}

func TestTransitionRejectsChangedPhysicalIncarnation(t *testing.T) {
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)
	body := protocol.HeartbeatRequest{AllocationID: "allocation", NodeBootID: "other-boot"}
	response := doJSON(t, handler, http.MethodPut, protocol.HeartbeatPath("slot"), body, "token")
	require.Equal(t, http.StatusConflict, response.Code)
	require.Nil(t, store.heartbeat)
	requireErrorCode(t, response, protocol.ErrorConflict)
}

func TestHandlerRequiresBearerStrictJSONAndPUT(t *testing.T) {
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)

	body := protocol.HeartbeatRequest{AllocationID: "allocation", NodeBootID: "boot"}
	response := doRequest(t, handler, http.MethodPut, protocol.HeartbeatPath("slot"), body, "", "application/json")
	require.Equal(t, http.StatusUnauthorized, response.Code)

	response = doRequest(t, handler, http.MethodPost, protocol.HeartbeatPath("slot"), body, "token", "application/json")
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Equal(t, http.MethodPut, response.Header().Get("Allow"))

	payload := []byte(`{"allocation_id":"allocation","node_boot_id":"boot","unknown":true}`)
	request := httptest.NewRequest(http.MethodPut, protocol.HeartbeatPath("slot"), bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Nil(t, store.heartbeat)
}

func TestStoreErrorsHaveStableClasses(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{sandboxstore.ErrRuntimeSlotNotFound, http.StatusNotFound, protocol.ErrorNotFound},
		{sandboxstore.ErrRuntimeSlotConflict, http.StatusConflict, protocol.ErrorConflict},
		{sandboxstore.ErrRuntimeSlotInvalid, http.StatusPreconditionFailed, protocol.ErrorFailedPrecondition},
		{errors.New("postgres unavailable"), http.StatusServiceUnavailable, protocol.ErrorUnavailable},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			handler := testHandler(t,
				&fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}},
				&fakeStore{getErr: test.err},
			)
			response := doRequest(t, handler, http.MethodGet, protocol.SlotPath("slot"), nil, "token", "")
			require.Equal(t, test.status, response.Code)
			requireErrorCode(t, response, test.code)
		})
	}
}

func TestRouteUsesCanonicalEscapedOpaqueSlotID(t *testing.T) {
	store := &fakeStore{slot: testSlot()}
	store.slot.ID = "slot/with space"
	handler := testHandler(t, &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}, store)
	response := doRequest(t, handler, http.MethodGet, protocol.SlotPath(store.slot.ID), nil, "token", "")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, []string{store.slot.ID}, store.getIDs)
}

func testHandler(t *testing.T, verifier nodeauth.Verifier, store Store) http.Handler {
	t.Helper()
	handler, err := NewHandler(HandlerConfig{Verifier: verifier, Store: store, HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	return handler
}

func testSlot() *sandboxstore.RuntimeSlot {
	now := time.Unix(1_700_000_000, 0).UTC()
	return &sandboxstore.RuntimeSlot{
		ID: "slot", AllocationID: "allocation", NodeUID: "node-uid", NodeBootID: "boot",
		State: sandboxstore.RuntimeSlotStateRegistered, Revision: 1,
		HeartbeatExpiresAt: now.Add(time.Minute), AuthorityObservedAt: now,
	}
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any, token string) *httptest.ResponseRecorder {
	t.Helper()
	return doRequest(t, handler, method, path, body, token, "application/json")
}

func doRequest(
	t *testing.T,
	handler http.Handler,
	method, path string,
	body any,
	token, contentType string,
) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		require.NoError(t, err)
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func requireErrorCode(t *testing.T, response *httptest.ResponseRecorder, code string) {
	t.Helper()
	var body protocol.ErrorResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.NoError(t, body.Validate())
	require.Equal(t, code, body.Code)
}
