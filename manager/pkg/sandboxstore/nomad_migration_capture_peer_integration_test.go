package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type capturePeerStagingWorkerNode struct {
	*stagingWorkNode
	peers    map[bool]runtimecheckpoint.PeerEndpoint
	fail     string
	prepared []bool
}

func (n *capturePeerStagingWorkerNode) ReserveMigrationStaging(ctx context.Context, request protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error) {
	r, err := n.stagingWorkNode.ReserveMigrationStaging(ctx, request)
	if err != nil {
		return nil, err
	}
	r.Peer = n.peers[request.IsSource()]
	r.PeerCertificateSHA256, err = r.Peer.CertificateDigest()
	return r, err
}

func (n *capturePeerStagingWorkerNode) PrepareMigrationCapturePeer(ctx context.Context, request protocol.MigrationCapturePeerRequest) (*protocol.MigrationCapturePeerPrepared, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var payload []byte
	var receiverReady bool
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT capture_peer_request,capture_peer_destination_receipt IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Staging.Source.OperationID).Scan(&payload, &receiverReady))
	var retained protocol.MigrationCapturePeerRequest
	require.NoError(n.t, json.Unmarshal(payload, &retained))
	if !request.Staging.IsSource() {
		retained.Staging.Target = retained.Staging.Destination
	}
	require.Equal(n.t, retained, request, "exact regional permission must precede dispatch")
	if request.Staging.IsSource() {
		require.True(n.t, receiverReady)
	}
	n.prepared = append(n.prepared, request.Staging.IsSource())
	receipt := capturePeerPrepared(n.t, request)
	if n.fail == "source" && request.Staging.IsSource() || n.fail == "destination" && !request.Staging.IsSource() {
		return receipt, errors.New("accepted node grant but response was lost")
	}
	return receipt, nil
}

func TestCapturePeerStagingWorkerDispatchAndFallbackIntegration(t *testing.T) {
	for _, failure := range []string{"none", "source", "destination"} {
		t.Run(failure, func(t *testing.T) {
			f, a := migrationSourceExecutionFixture(t, "capture-peer-worker-"+failure)
			retainMigrationCPUFixture(t, f, a)
			n := &capturePeerStagingWorkerNode{stagingWorkNode: &stagingWorkNode{t: t, f: f, calls: map[string]int{}}, peers: map[bool]runtimecheckpoint.PeerEndpoint{}, fail: failure}
			for _, source := range []bool{true, false} {
				cert, err := runtimecheckpoint.NewPeerIdentity()
				require.NoError(t, err)
				address := "10.1.1.1:18992"
				if !source {
					address = "10.1.1.2:18992"
				}
				n.peers[source], err = runtimecheckpoint.NewPeerEndpoint(address, cert)
				require.NoError(t, err)
			}
			for range 6 {
				worker, err := nomadmigration.NewStaging(NewPGSandboxStore(f.pool), n)
				require.NoError(t, err)
				_, err = worker.RunOnce(f.ctx)
				require.NoError(t, err)
			}
			if failure == "destination" {
				require.Equal(t, []bool{false}, n.prepared)
			} else {
				require.Equal(t, []bool{false, true}, n.prepared)
			}
			ids, err := f.store.ListNomadMigrationStaging(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			ids, err = f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
			require.NoError(t, err)
			require.Contains(t, ids, a.OperationID)
			var disabled, sourceReceipt bool
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT capture_peer_disabled,capture_peer_source_receipt IS NOT NULL
                FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&disabled, &sourceReceipt))
			require.Equal(t, failure != "none", disabled)
			require.Equal(t, failure == "none", sourceReceipt)
		})
	}
}

func retainCapturePeerStaging(t *testing.T, f *nomadPauseStoreFixture, a runtimecontrol.MigrationAssignment) {
	t.Helper()
	for range 2 {
		request, err := f.store.AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
		require.NoError(t, err)
		require.NotNil(t, request)
		require.Equal(t, 1, request.CaptureUpload.Version)
		cert, err := runtimecheckpoint.NewPeerIdentity()
		require.NoError(t, err)
		address := "10.1.1.1:18992"
		if !request.IsSource() {
			address = "10.1.1.2:18992"
		}
		endpoint, err := runtimecheckpoint.NewPeerEndpoint(address, cert)
		require.NoError(t, err)
		want, err := request.Digest()
		require.NoError(t, err)
		receipt := protocol.MigrationStagingReserved{RequestDigest: want, PeerCertificateSHA256: runtimecheckpoint.PeerCertificateDigest(cert), Peer: endpoint}
		require.NoError(t, f.store.CommitNomadSandboxMigrationStaging(f.ctx, a, *request, receipt))
		// A lost final staging reply cannot replace the peer decision.
		require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationStaging(f.ctx, a, *request, receipt))
	}
}

func capturePeerPrepared(t *testing.T, request protocol.MigrationCapturePeerRequest) *protocol.MigrationCapturePeerPrepared {
	t.Helper()
	want, err := request.Digest()
	require.NoError(t, err)
	return &protocol.MigrationCapturePeerPrepared{RequestDigest: want}
}

func TestCapturePeerRegionalDecisionPrecedesSourcePreparationIntegration(t *testing.T) {
	for _, outcome := range []string{"accepted", "destination-failed", "source-failed", "aborted", "expired"} {
		t.Run(outcome, func(t *testing.T) {
			f, a := migrationSourceExecutionFixture(t, "capture-peer-"+outcome)
			retainMigrationCPUFixture(t, f, a)
			retainCapturePeerStaging(t, f, a)
			policy := migrationSourcePolicy(f.sandboxID, a.Target.TeamID)
			ids, err := f.store.ListNomadMigrationStaging(f.ctx, "", 8)
			require.NoError(t, err)
			require.Contains(t, ids, a.OperationID)
			ids, err = f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			destination, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationCapturePeer(f.ctx, a)
			require.NoError(t, err)
			require.NotNil(t, destination)
			require.False(t, destination.Staging.IsSource())
			source := *destination
			source.Staging.Target = source.Staging.Source.Target
			require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, source, capturePeerPrepared(t, source)), ErrNomadSandboxMigrationConflict)
			changed := *destination
			changed.Source.Peer.Address = "https://10.1.1.3:18992"
			require.NoError(t, changed.Validate())
			require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, changed, capturePeerPrepared(t, changed)), ErrNomadSandboxMigrationConflict)
			for _, mutation := range []string{
				"capture_peer_request=NULL",
				"capture_peer_request=jsonb_set(capture_peer_request,'{destination,peer,address}','\"https://10.1.1.3:18992\"')",
				"capture_peer_source_receipt=jsonb_build_object('request_digest',capture_peer_source_digest)",
			} {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET `+mutation+` WHERE operation_id=$1`, a.OperationID)
				require.Error(t, err)
			}
			if outcome == "aborted" {
				require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, a.OperationID, "peer preparation canceled"))
				_, err = f.store.AuthorizeNomadSandboxMigrationCapturePeer(f.ctx, a)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				require.ErrorIs(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, *destination, capturePeerPrepared(t, *destination)), ErrNomadSandboxMigrationConflict)
				for range 2 {
					release, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, a.OperationID)
					require.NoError(t, err)
					require.NotNil(t, release)
					want, err := release.Digest()
					require.NoError(t, err)
					require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *release, protocol.MigrationStagingReleased{RequestDigest: want}))
				}
				ids, err = f.store.ListNomadMigrationStagingReleases(f.ctx, "", 8)
				require.NoError(t, err)
				require.Empty(t, ids)
				return
			}
			if outcome == "expired" {
				ageMigrationCPUPreflight(t, f, a.OperationID)
				_, err = f.store.AuthorizeNomadSandboxMigrationCapturePeer(f.ctx, a)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				return
			}
			if outcome == "destination-failed" {
				require.NoError(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, *destination, nil))
				// A late success after fallback cannot revive early transfer.
				require.NoError(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, *destination, capturePeerPrepared(t, *destination)))
			} else {
				// Multiple replicas and replay after a committed but lost reply
				// must retain one exact acknowledgement and the same next side.
				var wg sync.WaitGroup
				errs := make([]error, 3)
				receipt := capturePeerPrepared(t, *destination)
				for i := range errs {
					wg.Go(func() {
						errs[i] = NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationCapturePeer(f.ctx, a, *destination, receipt)
					})
				}
				wg.Wait()
				for _, err := range errs {
					require.NoError(t, err)
				}
				require.NoError(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, *destination, nil), "stale failure cannot erase accepted destination")
				next, err := f.store.AuthorizeNomadSandboxMigrationCapturePeer(f.ctx, a)
				require.NoError(t, err)
				require.Equal(t, source, *next)
				_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
				var prepared *protocol.MigrationCapturePeerPrepared
				if outcome == "accepted" {
					prepared = capturePeerPrepared(t, source)
				}
				require.NoError(t, f.store.CommitNomadSandboxMigrationCapturePeer(f.ctx, a, source, prepared))
			}
			next, err := f.store.AuthorizeNomadSandboxMigrationCapturePeer(f.ctx, a)
			require.NoError(t, err)
			require.Nil(t, next)
			ids, err = f.store.ListNomadMigrationStaging(f.ctx, "", 8)
			require.NoError(t, err)
			require.Empty(t, ids)
			ids, err = f.store.ListNomadMigrationSourceExecutions(f.ctx, "", 8)
			require.NoError(t, err)
			require.Contains(t, ids, a.OperationID)
			var disabled bool
			var destinationAck, sourceAck []byte
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT capture_peer_disabled,capture_peer_destination_receipt,capture_peer_source_receipt
                FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, a.OperationID).Scan(&disabled, &destinationAck, &sourceAck))
			require.Equal(t, outcome != "accepted", disabled)
			require.Equal(t, outcome != "destination-failed", len(destinationAck) != 0)
			require.Equal(t, outcome == "accepted", len(sourceAck) != 0)
			if outcome == "accepted" {
				var receipt protocol.MigrationCapturePeerPrepared
				require.NoError(t, json.Unmarshal(sourceAck, &receipt))
				require.NoError(t, receipt.ValidateFor(source))
			}
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_peer_disabled=NOT capture_peer_disabled WHERE operation_id=$1`, a.OperationID)
			require.Error(t, err, "the decision cannot change after source preparation")
		})
	}
}
