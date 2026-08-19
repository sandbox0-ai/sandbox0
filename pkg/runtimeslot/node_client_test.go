//go:build linux

package runtimeslot

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func TestNodeClientCallsRootOwnedUnixControlSocket(t *testing.T) {
	var mu sync.Mutex
	paths := make([]string, 0, 2)
	client, endpoint := startNodeControlTestServer(t, func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		paths = append(paths, request.Method+" "+request.URL.Path)
		mu.Unlock()
		if got := request.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(NodeControlResponse{Phase: string(StateActive)})
	})

	claim := testNodeClaimControlRequest()
	response, err := client.Claim(t.Context(), endpoint, claim)
	require.NoError(t, err)
	require.Equal(t, string(StateActive), response.Phase)
	proof := CommandReadyProof{
		Version: CommandReadyProofVersion, SlotID: "slot-1", OperationID: claim.OperationID,
		ClaimID: claim.ClaimID, LaunchAttempt: claim.Stage.Identity.LaunchAttempt,
		RunscContainerID: "runsc-1", ProcdInstanceID: "procd-1", RequestMethod: http.MethodPut,
		RequestPath: ProcdCommandReadyProbePath, ResponseStatus: http.StatusOK,
		ResponseBodyDigest: strings.Repeat("ab", 32),
	}
	_, err = client.CommandReady(t.Context(), endpoint, CommandReadyControlRequest{Proof: proof})
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"PUT /claim", "PUT /command-ready"}, paths)
}

func TestNodeClientRejectsSocketOutsideAllowedRootAndInsecureMode(t *testing.T) {
	allowed := t.TempDir()
	client, err := newNodeClient(NodeClientConfig{AllowedSocketRoot: allowed}, uint32(os.Geteuid()))
	require.NoError(t, err)

	outside := filepath.Join(t.TempDir(), "outside.sock")
	listener, err := net.Listen("unix", outside)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	require.NoError(t, os.Chmod(outside, 0o600))
	_, err = client.Claim(t.Context(), "unix://"+outside, testNodeClaimControlRequest())
	require.Error(t, err)
	require.True(t, errdefs.IsPermissionDenied(err), err)

	insecure := filepath.Join(allowed, "insecure.sock")
	insecureListener, err := net.Listen("unix", insecure)
	require.NoError(t, err)
	t.Cleanup(func() { _ = insecureListener.Close() })
	require.NoError(t, os.Chmod(insecure, 0o660))
	_, err = client.Claim(t.Context(), "unix://"+insecure, testNodeClaimControlRequest())
	require.Error(t, err)
	require.True(t, errdefs.IsPermissionDenied(err), err)
}

func TestNodeClientMapsStableOperationErrors(t *testing.T) {
	var mu sync.Mutex
	status := http.StatusBadRequest
	client, endpoint := startNodeControlTestServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		current := status
		mu.Unlock()
		writer.WriteHeader(current)
		_ = json.NewEncoder(writer).Encode(map[string]string{"error": "rejected"})
	})
	tests := []struct {
		status int
		is     func(error) bool
	}{
		{http.StatusBadRequest, errdefs.IsInvalidArgument},
		{http.StatusForbidden, errdefs.IsPermissionDenied},
		{http.StatusNotFound, errdefs.IsNotFound},
		{http.StatusConflict, errdefs.IsFailedPrecondition},
		{http.StatusServiceUnavailable, errdefs.IsUnavailable},
	}
	for _, test := range tests {
		mu.Lock()
		status = test.status
		mu.Unlock()
		_, err := client.Claim(t.Context(), endpoint, testNodeClaimControlRequest())
		require.Error(t, err)
		require.True(t, test.is(err), "status %d: %v", test.status, err)
	}
}

func TestNodeClientRejectsAmbiguousSuccessResponse(t *testing.T) {
	responses := []string{
		`{"phase":"warm"}`,
		`{"phase":"active","unknown":true}`,
		`{"phase":"active"} {}`,
		strings.Repeat("x", maxNodeControlBytes+1),
	}
	var mu sync.Mutex
	index := 0
	client, endpoint := startNodeControlTestServer(t, func(writer http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		payload := responses[index]
		index++
		mu.Unlock()
		_, _ = writer.Write([]byte(payload))
	})
	for range responses {
		_, err := client.Claim(t.Context(), endpoint, testNodeClaimControlRequest())
		require.Error(t, err)
		require.True(t, errdefs.IsUnavailable(err), err)
	}
}

func startNodeControlTestServer(
	t *testing.T,
	handler http.HandlerFunc,
) (*NodeClient, string) {
	t.Helper()
	root := t.TempDir()
	socket := filepath.Join(root, "slot.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	server := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("serve node control test socket: %v", serveErr)
		}
	}()
	t.Cleanup(func() { _ = server.Close() })
	client, err := newNodeClient(NodeClientConfig{AllowedSocketRoot: root, Timeout: time.Second}, uint32(os.Geteuid()))
	require.NoError(t, err)
	return client, "unix://" + socket
}
