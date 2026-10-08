package runtimeslotnomad

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNomadRequestsReuseTLSConnectionAndReloadRotatedCredentials(t *testing.T) {
	state := &nomadTestServerState{token: "token", serverPresent: true, desiredStatus: "run"}
	server, resolver, tokenFile := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)
	for range 4 {
		_, err := api.ServerAllocation(t.Context(), httpTestTarget())
		require.NoError(t, err)
	}
	state.mu.Lock()
	connections := len(state.connections)
	state.token = "rotated-token"
	state.mu.Unlock()
	require.Equal(t, 1, connections, "repeated calls must reuse their authenticated connection")
	require.NoError(t, os.WriteFile(tokenFile, []byte("rotated-token"), 0600))
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.NoError(t, err, "ACL token rotation must take effect on a reused connection")
	ca, err := os.ReadFile(resolver.server.CAFile)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(resolver.server.CAFile, []byte("invalid CA"), 0600))
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.Error(t, err, "an older authenticated connection must not bypass changed trust")
	require.NoError(t, os.WriteFile(resolver.server.CAFile, ca, 0600))
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.NoError(t, err)
	require.NoError(t, os.Remove(resolver.server.ClientKeyFile))
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.Error(t, err, "missing client credentials must not fall back to the warm connection")
}

func TestNomadTransportExpiryRebuildsBeforeAnotherOperation(t *testing.T) {
	state := &nomadTestServerState{token: "token", serverPresent: true, desiredStatus: "run"}
	server, resolver, _ := newNomadMTLSTestServer(t, state)
	defer server.Close()
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.NoError(t, err)
	api.clients.mu.Lock()
	entry := api.clients.entries[resolver.server]
	require.NotNil(t, entry)
	entry.transport.shortenExpiry(time.Now().Add(-time.Second))
	api.clients.mu.Unlock()
	_, err = api.ServerAllocation(t.Context(), httpTestTarget())
	require.NoError(t, err)
	state.mu.Lock()
	connections := len(state.connections)
	state.mu.Unlock()
	require.Equal(t, 2, connections)
}
