package sandboxstore

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationSourceRecoveryAdvancesPeerPublicationIntegration(t *testing.T) {
	peer := digest.FromString("reserved-destination-certificate").String()
	f, _, request := migrationPublicationStoreFixture(t, "source-recovery-peer", peer)
	node := &sourceRecoveryTestNode{t: t, request: request}
	worker, err := nomadmigration.NewSourceRecovery(f.store, node)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err, "committed peer publication must wake the transfer lane without an error backoff")
	require.Equal(t, 1, result.Advanced)
	transfer, err := f.store.GetNomadMigrationTransfer(f.ctx, request.Assignment.OperationID)
	require.NoError(t, err)
	require.NotNil(t, transfer)
	require.Equal(t, request, transfer.Publication)
	require.Equal(t, peer, transfer.Publication.DestinationPeerCertificateSHA256)
}

type sourceRecoveryTestNode struct {
	mu      sync.Mutex
	t       *testing.T
	request protocol.MigrationPublicationRequest
	calls   int
	lost    bool
	outcome string
	before  func()
}

func (n *sourceRecoveryTestNode) RecoverMigrationCapture(_ context.Context, r protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls++
	require.Equal(n.t, n.request.Capture.Request, r)
	if n.lost {
		n.lost = false
		return nil, errors.New("lost source recovery reply")
	}
	if n.before != nil {
		n.before()
		n.before = nil
	}
	capture := n.request.Capture
	switch n.outcome {
	case "missing":
		return nil, nil
	case "intent":
		capture.State = protocol.MigrationCaptureIntent
		capture.RootFS = nil
	case "uncertain":
		capture.State = protocol.MigrationCaptureUncertain
		capture.RootFS = nil
	case "missing-cut":
		capture.RootFS = nil
	case "another-source":
		capture.Request.ProcdInstanceID = "replacement"
		capture.RequestDigest, _ = capture.Request.Digest()
		capture.RootFS = nil
	}
	return &capture, nil
}

type sourceRecoveryTestStore struct {
	*PGSandboxStore
	boundary string
}

func (s *sourceRecoveryTestStore) AuthorizeNomadSandboxMigrationPublication(ctx context.Context, a runtimecontrol.MigrationAssignment, c protocol.MigrationCapture, cpu string) (*protocol.MigrationPublicationRequest, error) {
	boundary := s.boundary
	s.boundary = ""
	if boundary == "before" {
		return nil, errors.New("before publication authorization")
	}
	request, err := s.PGSandboxStore.AuthorizeNomadSandboxMigrationPublication(ctx, a, c, cpu)
	if err != nil {
		return nil, err
	}
	if boundary == "after" {
		return nil, errors.New("lost publication authorization response")
	}
	return request, nil
}

func TestNomadMigrationSourceRecoverySurvivesResponseLossIntegration(t *testing.T) {
	for _, boundary := range []string{"node", "before", "after"} {
		t.Run(boundary, func(t *testing.T) {
			f, _, request := migrationPublicationStoreFixture(t, "source-recovery-"+boundary)
			ageMigrationCPUPreflight(t, f, request.Assignment.OperationID)
			n := &sourceRecoveryTestNode{t: t, request: request, lost: boundary == "node"}
			store := &sourceRecoveryTestStore{PGSandboxStore: f.store, boundary: boundary}
			worker, err := nomadmigration.NewSourceRecovery(store, n)
			require.NoError(t, err)
			result, err := worker.RunOnce(f.ctx)
			require.Error(t, err)
			require.Equal(t, 1, result.Failed)
			store.PGSandboxStore = NewPGSandboxStore(f.pool)
			worker, err = nomadmigration.NewSourceRecovery(store, n)
			require.NoError(t, err)
			result, err = worker.RunOnce(f.ctx)
			require.NoError(t, err)
			if boundary == "after" {
				require.Zero(t, result.Candidates)
				require.Equal(t, 1, n.calls)
			} else {
				require.Equal(t, 1, result.Advanced)
				require.Equal(t, 2, n.calls)
			}
			work, err := f.store.GetNomadMigrationSourceRecovery(f.ctx, request.Assignment.OperationID)
			require.NoError(t, err)
			require.Nil(t, work)
			transfer, err := f.store.GetNomadMigrationTransfer(f.ctx, request.Assignment.OperationID)
			require.NoError(t, err)
			require.NotNil(t, transfer, "the existing transfer worker owns the next step")
			require.Equal(t, request, transfer.Publication)
			require.Nil(t, transfer.Published, "source recovery does not pretend upload succeeded")
			filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
			require.NoError(t, err)
			require.Equal(t, f.initialGenerationID, filesystem.HeadGenerationID)
			require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
			var generation int64
			require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT runtime_generation FROM manager.sandboxes WHERE sandbox_id=$1`, f.sandboxID).Scan(&generation))
			require.EqualValues(t, 1, generation)
		})
	}
}

func TestNomadMigrationSourceRecoveryRejectsIncompleteEvidenceIntegration(t *testing.T) {
	for _, outcome := range []string{"missing", "intent", "missing-cut", "another-source"} {
		t.Run(outcome, func(t *testing.T) {
			f, _, request := migrationPublicationStoreFixture(t, "source-recovery-"+outcome)
			n := &sourceRecoveryTestNode{t: t, request: request, outcome: outcome}
			worker, err := nomadmigration.NewSourceRecovery(f.store, n)
			require.NoError(t, err)
			result, err := worker.RunOnce(f.ctx)
			if outcome == "intent" {
				require.NoError(t, err)
				require.Equal(t, 1, result.Skipped)
			} else {
				require.Error(t, err)
				require.Equal(t, 1, result.Failed)
			}
			transfer, err := f.store.GetNomadMigrationTransfer(f.ctx, request.Assignment.OperationID)
			require.NoError(t, err)
			require.Nil(t, transfer)
			work, err := f.store.GetNomadMigrationSourceRecovery(f.ctx, request.Assignment.OperationID)
			require.NoError(t, err)
			require.NotNil(t, work, "ambiguous source retains its pending operation")
		})
	}
}

func TestNomadMigrationSourceRecoveryRechecksWriterBeforePublicationIntegration(t *testing.T) {
	f, _, request := migrationPublicationStoreFixture(t, "source-recovery-writer")
	n := &sourceRecoveryTestNode{t: t, request: request, before: func() {
		_, err := f.pool.Exec(f.ctx, `UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+1 WHERE filesystem_id=$1`, f.filesystem.ID)
		require.NoError(t, err)
	}}
	worker, err := nomadmigration.NewSourceRecovery(f.store, n)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.Error(t, err)
	require.Equal(t, 1, result.Failed)
	transfer, err := f.store.GetNomadMigrationTransfer(f.ctx, request.Assignment.OperationID)
	require.NoError(t, err)
	require.Nil(t, transfer)
}

func TestNomadMigrationSourceRecoveryConcurrentReplicasIntegration(t *testing.T) {
	f, _, request := migrationPublicationStoreFixture(t, "source-recovery-concurrent")
	n := &sourceRecoveryTestNode{t: t, request: request}
	var wg sync.WaitGroup
	failures := make([]error, 4)
	for i := range failures {
		wg.Go(func() {
			worker, err := nomadmigration.NewSourceRecovery(NewPGSandboxStore(f.pool), n)
			if err == nil {
				_, err = worker.RunOnce(f.ctx)
			}
			failures[i] = err
		})
	}
	wg.Wait()
	for _, err := range failures {
		require.NoError(t, err)
	}
	transfer, err := f.store.GetNomadMigrationTransfer(f.ctx, request.Assignment.OperationID)
	require.NoError(t, err)
	require.Equal(t, request, transfer.Publication)
	ids, err := f.store.ListNomadMigrationSourceRecoveries(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestNomadMigrationSourceRecoveryRequiresCaptureAuthorityIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "source-recovery-authority")
	migrationReadyTarget(t, f, a.OperationID, "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	retainMigrationEligibilityFixture(t, f, a)
	for _, prepare := range []bool{false, true} {
		if prepare {
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, migrationSourcePolicy(f.sandboxID, a.Target.TeamID))
			require.NoError(t, err)
		}
		ids, err := f.store.ListNomadMigrationSourceRecoveries(f.ctx, "", 8)
		require.NoError(t, err)
		require.Empty(t, ids)
		work, err := f.store.GetNomadMigrationSourceRecovery(f.ctx, a.OperationID)
		require.NoError(t, err)
		require.Nil(t, work)
	}
}
