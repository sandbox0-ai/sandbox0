package nomadruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type stalledMigrationPeerTestRuntime struct {
	nodeRuntimeBackend
	finished chan error
}

func (r *stalledMigrationPeerTestRuntime) WriteMigrationPeerImage(_ context.Context, _ runtimecheckpoint.Binding, _ runtimecheckpoint.Reference, _ string, output io.Writer) error {
	// Deliberately rely on the HTTP write deadline, not context polling, to
	// exercise cancellation of a socket blocked by a non-reading destination.
	block := make([]byte, 64<<10)
	for {
		if _, err := output.Write(block); err != nil {
			r.finished <- err
			return err
		}
	}
}

func TestMigrationPeerCleanupPreemptsNonReadingDestination(t *testing.T) {
	source, target, request, _, _ := migrationPeerFixture(t)
	stalled := &stalledMigrationPeerTestRuntime{nodeRuntimeBackend: source.runtime, finished: make(chan error, 1)}
	source.runtime = stalled
	client, err := runtimecheckpoint.NewPeerClient(request.Receipt.Peer, target.migrationPeer.identity)
	require.NoError(t, err)
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	slot := request.Publication.Capture.Request.Target.SlotID
	payload, err := json.Marshal(migrationPeerRead{SlotID: slot, RequestDigest: request.Receipt.RequestDigest})
	require.NoError(t, err)
	read, err := http.NewRequestWithContext(ctx, http.MethodPost, request.Receipt.Peer.Address+runtimecheckpoint.PeerImagePath, bytes.NewReader(payload))
	require.NoError(t, err)
	response, err := client.Do(read)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	// Keep the response open without reading, then exercise the same custody
	// preemption used by regional cleanup. Closing the client would hide a bug.
	cleanup, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	require.NoError(t, source.beginExternalReconciliation(cleanup, slot))
	defer source.endReconciliation(slot)
	select {
	case err := <-stalled.finished:
		require.Error(t, err)
	case <-cleanup.Done():
		t.Fatal("cleanup did not interrupt the blocked peer write")
	}
	record, err := source.journal.Get(slot)
	require.NoError(t, err)
	require.Equal(t, request.Receipt, *record.Migration.Publication, "interrupted transport must retain recoverable publication custody")
}

func (r *migrationImageTestRuntime) WriteMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, directory string, output io.Writer) error {
	return r.store.WritePeerImage(ctx, binding, ref, directory, output)
}

func migrationPeerFixture(t *testing.T) (*nodeRuntime, *nodeRuntime, protocol.MigrationImagePrepareRequest, *migrationImageDownloadTestRuntime, *httptest.Server) {
	t.Helper()
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	var source *nodeRuntime
	var server *httptest.Server
	target, request, downloads := migrationImageDestinationFixture(t, func(node *nodeRuntime, publication *protocol.MigrationPublicationRequest, _ *migrationImageTestRuntime) {
		source = node
		cert, err := runtimecheckpoint.NewPeerIdentity()
		require.NoError(t, err)
		server = httptest.NewUnstartedServer(http.HandlerFunc(node.serveMigrationPeer))
		server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert}
		server.StartTLS()
		t.Cleanup(server.Close)
		endpoint, err := runtimecheckpoint.NewPeerEndpoint(strings.TrimPrefix(server.URL, "https://"), cert)
		require.NoError(t, err)
		node.migrationPeer = &migrationPeer{identity: cert, endpoint: endpoint}
		publication.DestinationPeerCertificateSHA256 = runtimecheckpoint.PeerCertificateDigest(identity)
	})
	target.migrationPeer = &migrationPeer{identity: identity}
	return source, target, request, downloads, server
}

func TestMigrationPeerPreparesDestinationWithoutRegionalDownload(t *testing.T) {
	_, target, request, downloads, _ := migrationPeerFixture(t)
	downloads.fail = true
	prepared, err := target.PrepareMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(request))
	require.Zero(t, downloads.calls, "successful node-to-node transfer must bypass the regional download")
	record, err := target.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, prepared, record.MigrationDestination.Prepared)
	data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
}

func TestMigrationPeerFailureFallsBackToExactRegionalImage(t *testing.T) {
	for _, failure := range []string{"source unavailable", "source image corrupt", "target restarted"} {
		t.Run(failure, func(t *testing.T) {
			source, target, request, downloads, server := migrationPeerFixture(t)
			switch failure {
			case "source unavailable":
				server.Close()
			case "source image corrupt":
				custody, err := source.GetMigrationCapture(t.Context(), request.Publication.Capture.Request.Target.SlotID)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(custody.ImageDirectory, "checkpoint.img"), []byte("corrupt!-memory"), 0o600))
			case "target restarted":
				cert, err := runtimecheckpoint.NewPeerIdentity()
				require.NoError(t, err)
				target.migrationPeer.identity = cert
			}
			prepared, err := target.PrepareMigrationImage(t.Context(), request)
			require.NoError(t, err)
			require.NoError(t, prepared.ValidateFor(request))
			require.Equal(t, 1, downloads.calls)
			record, err := target.journal.Get(request.Target.SlotID)
			require.NoError(t, err)
			data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
			require.NoError(t, err)
			require.Equal(t, "retained-memory", string(data))
		})
	}
}

func TestMigrationPeerRejectsAnotherDestinationAndOperation(t *testing.T) {
	_, target, request, _, _ := migrationPeerFixture(t)
	other, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	for _, test := range []struct {
		name     string
		identity tls.Certificate
		digest   string
	}{
		{"other destination", other, request.Receipt.RequestDigest},
		{"other operation", target.migrationPeer.identity, strings.Repeat("f", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := runtimecheckpoint.NewPeerClient(request.Receipt.Peer, test.identity)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			payload, err := json.Marshal(migrationPeerRead{SlotID: request.Publication.Capture.Request.Target.SlotID, RequestDigest: test.digest})
			require.NoError(t, err)
			response, err := client.Post(request.Receipt.Peer.Address+runtimecheckpoint.PeerImagePath, "application/json", bytes.NewReader(payload))
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusForbidden, response.StatusCode)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.NotContains(t, string(body), "retained-memory")
		})
	}
}
