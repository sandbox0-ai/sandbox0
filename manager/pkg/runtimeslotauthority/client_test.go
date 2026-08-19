package runtimeslotauthority

import (
	"context"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestClientRoundTripsAuthenticatedRegistrationAndHeartbeat(t *testing.T) {
	verifier := &fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}
	store := &fakeStore{slot: testSlot()}
	handler := testHandler(t, verifier, store)
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	client := testClient(t, server)

	registration := protocol.RegistrationRequest{
		ClusterID: "cluster", AllocationID: "allocation", AllocationNamespace: "default",
		NodeID: "node", NodeBootID: "boot", NetNSIdentity: "netns",
		ControlEndpoint:      "unix:///run/sandbox0/slot.sock",
		RuntimeCompatibility: digest.FromString("amd64/runsc/directfs").String(),
	}
	observation, err := client.Register(context.Background(), "slot", registration)
	require.NoError(t, err)
	require.Equal(t, protocol.StateRegistered, observation.State)
	require.Equal(t, "projected-token", verifier.token)
	require.Equal(t, "node-uid", store.register.NodeUID)

	observation, err = client.Heartbeat(context.Background(), "slot", protocol.HeartbeatRequest{
		AllocationID: "allocation", NodeBootID: "boot",
	})
	require.NoError(t, err)
	require.Equal(t, "slot", observation.SlotID)
	require.NotNil(t, store.heartbeat)
}

func TestClientMapsStableConflictAndValidatesBeforeNetwork(t *testing.T) {
	store := &fakeStore{slot: testSlot(), transitionErr: sandboxstore.ErrRuntimeSlotConflict}
	server := httptest.NewTLSServer(testHandler(t,
		&fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}, store,
	))
	defer server.Close()
	client := testClient(t, server)

	_, err := client.Heartbeat(context.Background(), "slot", protocol.HeartbeatRequest{
		AllocationID: "allocation", NodeBootID: "boot",
	})
	require.Error(t, err)
	require.True(t, errdefs.IsFailedPrecondition(err))

	store.heartbeat = nil
	_, err = client.Ready(context.Background(), "slot", protocol.ReadinessRequest{
		AllocationID: "allocation", NodeBootID: "boot",
		RuntimeReadyDigest: "invalid", NetworkReadyDigest: strings.Repeat("ab", 32),
		StorageReadyDigest: strings.Repeat("cd", 32),
	})
	require.Error(t, err)
	require.True(t, errdefs.IsInvalidArgument(err))
	require.Nil(t, store.ready)
}

func TestClientRejectsObservationForAnotherSlot(t *testing.T) {
	store := &fakeStore{slot: testSlot()}
	store.slot.ID = "different-slot"
	server := httptest.NewTLSServer(testHandler(t,
		&fakeVerifier{identity: nodeauth.Identity{NodeUID: "node-uid"}}, store,
	))
	defer server.Close()
	client := testClient(t, server)
	_, err := client.Observe(context.Background(), "slot")
	require.Error(t, err)
	require.True(t, errdefs.IsUnavailable(err))
}

func testClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	directory := t.TempDir()
	caFile := filepath.Join(directory, "ca.pem")
	tokenFile := filepath.Join(directory, "token")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: server.Certificate().Raw,
	}), 0o600))
	require.NoError(t, os.WriteFile(tokenFile, []byte("projected-token\n"), 0o600))
	client, err := NewClient(ClientConfig{
		BaseURL: server.URL, CAFile: caFile, TokenFile: tokenFile, Timeout: time.Second,
	})
	require.NoError(t, err)
	return client
}
