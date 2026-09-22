package runtimeslotnode

import (
	"context"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationCaptureFailureChannelExecutor struct {
	migrationFailureCleanupChannelExecutor
}

func (e *migrationCaptureFailureChannelExecutor) CleanupFailedMigrationCapture(_ context.Context, r protocol.MigrationCaptureFailureRequest) (*protocol.MigrationCaptureFailureProof, error) {
	e.calls.Add(1)
	p, err := migrationChannelPhysicalCleanupProof(r.Cleanup)
	if err != nil {
		return nil, err
	}
	want, err := r.Digest()
	return &protocol.MigrationCaptureFailureProof{RequestDigest: want, Cleanup: p}, err
}
func (e *migrationCaptureFailureChannelExecutor) FinalizeFailedMigrationCapture(_ context.Context, r protocol.MigrationCaptureFailureFinalizeRequest) (*protocol.MigrationCaptureFailureFinalizeProof, error) {
	e.finalizations.Add(1)
	want, err := r.Digest()
	return &protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}, err
}

func migrationCaptureFailureChannelRequest(t *testing.T) protocol.MigrationCaptureFailureRequest {
	t.Helper()
	base := migrationFailureChannelRequest(t)
	capture := base.Failure.Request.Restore.Image.Publication.Capture
	capture.Request.Target = base.Failure.Request.Restore.Image.Target
	capture.Request.ResourceLeaseDigest = base.Cleanup.ResourceLeaseDigest
	capture.State, capture.RootFS = protocol.MigrationCaptureUncertain, nil
	var err error
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	c := base.Cleanup
	c.OperationID = protocol.MigrationCaptureFailureOperationID(capture.Request.OperationID)
	c.WriterOperationID, c.WriterAuthorityDigest = c.OperationID, capture.RequestDigest
	r := protocol.MigrationCaptureFailureRequest{Capture: capture, Cleanup: c}
	_, err = r.Digest()
	require.NoError(t, err)
	return r
}

func TestMigrationCaptureFailureRequiresAuthenticatedCapability(t *testing.T) {
	for _, supported := range []bool{false, true} {
		hub, err := NewChannelHub(channelTestVerifier{}, channelTestCapacityStore{})
		require.NoError(t, err)
		server, files := newNodeChannelTLSServer(t, hub)
		executor := &migrationCaptureFailureChannelExecutor{}
		config := protocol.NodeChannelAgentConfig{BaseURL: server.URL, CAFile: files.ca, ClientCertFile: files.clientCert, ClientKeyFile: files.clientKey,
			TokenFile: files.token, PeerURISAN: testNodeChannelServerURI, NodeUID: "node-uid-1", NodeBootIDFile: files.boot, ClusterID: "cluster-1", NodeID: "node-1",
			Executor: &channelTestExecutor{}, Capacity: channelTestCapacity(), AgentInstanceID: "agent-1", ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond}
		if supported {
			config.MigrationCaptureFailureExecutor = executor
		}
		agent, err := protocol.NewNodeChannelAgent(config)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- agent.Run(ctx) }()
		t.Cleanup(func() { cancel(); <-done; server.Close(); hub.Close() })
		waitNodeChannelConnected(t, hub, "cluster-1", "node-1", "node-uid-1", "boot-1")
		request := migrationCaptureFailureChannelRequest(t)
		proof, err := hub.CleanupFailedMigrationCapture(ctx, request)
		if supported {
			require.NoError(t, err)
			require.NoError(t, proof.ValidateFor(request))
		} else {
			require.Error(t, err)
			require.Nil(t, proof)
		}
		expected := int32(0)
		if supported {
			expected = 1
		}
		require.Equal(t, expected, executor.calls.Load())
		changed := request
		changed.Capture.Request.Target.NodeBootID = "old-boot"
		changed.Capture.RequestDigest, _ = changed.Capture.Request.Digest()
		changed.Cleanup.NodeBootID = "old-boot"
		changed.Cleanup.Resources.NodeBootID = "old-boot"
		_, err = hub.CleanupFailedMigrationCapture(ctx, changed)
		require.Error(t, err)
		require.Equal(t, expected, executor.calls.Load())
		fixture := &migrationCaptureFailureChannelExecutor{}
		proof, err = fixture.CleanupFailedMigrationCapture(ctx, request)
		require.NoError(t, err)
		final := protocol.MigrationCaptureFailureFinalizeRequest{Request: request, Proof: *proof}
		finalized, err := hub.FinalizeFailedMigrationCapture(ctx, final)
		if supported {
			require.NoError(t, err)
			require.NoError(t, finalized.ValidateFor(final))
		} else {
			require.Error(t, err)
			require.Nil(t, finalized)
		}
		require.Equal(t, expected, executor.finalizations.Load())
		final.Proof.Cleanup.NetworkPolicyAbsent = false
		_, err = hub.FinalizeFailedMigrationCapture(ctx, final)
		require.Error(t, err)
		require.Equal(t, expected, executor.finalizations.Load())
	}
}
