package sandboxstore

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type transferTestNode struct {
	mu          sync.Mutex
	t           *testing.T
	f           *nomadPauseStoreFixture
	publication protocol.MigrationPublicationRequest
	calls       map[string]int
	fail        string
	invalid     string
	proof       *protocol.MigrationSourceFenceProof
}

func (n *transferTestNode) called(phase string) error {
	n.calls[phase]++
	if n.fail == phase {
		n.fail = ""
		return errors.New("injected lost node response")
	}
	return nil
}

func (n *transferTestNode) PublishMigration(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	require.Equal(n.t, n.publication, request)
	var stored string
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT publication_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Assignment.OperationID).Scan(&stored))
	want, err := request.Digest()
	require.NoError(n.t, err)
	require.Equal(n.t, want, stored, "publication must have regional intent before dispatch")
	receipt := migrationPublicationReceipt(n.t, request)
	if n.invalid == "publish" {
		receipt.RequestDigest = "invalid"
	}
	return &receipt, n.called("publish")
}
func (n *transferTestNode) PrepareMigrationImage(ctx context.Context, request protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	var stored string
	var published bool
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT target_image_digest,publication_receipt IS NOT NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Publication.Assignment.OperationID).Scan(&stored, &published))
	require.True(n.t, published)
	digest, err := request.Digest()
	require.NoError(n.t, err)
	require.Equal(n.t, digest, stored, "destination command must precede dispatch")
	receipt := &protocol.MigrationImagePrepared{RequestDigest: digest, ManifestDigest: request.Receipt.Reference.ManifestDigest, TotalBytes: 1024}
	if n.invalid == "image" {
		receipt.RequestDigest = "invalid"
	}
	return receipt, n.called("image")
}
func (n *transferTestNode) FenceMigrationSource(ctx context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	state, err := n.f.store.GetNomadMigrationTransfer(ctx, request.PublicationRequest.Assignment.OperationID)
	require.NoError(n.t, err)
	// Another exact worker may have already committed this immutable proof.
	if state != nil {
		require.Equal(n.t, &request, state.Fence)
		require.NotNil(n.t, state.Prepared)
	}
	grant, err := n.f.store.GetRootFSWriterGrant(ctx, n.f.issue.GrantID)
	require.NoError(n.t, err)
	require.Contains(n.t, []string{RootFSWriterGrantStateRetiring, RootFSWriterGrantStateRetired}, grant.State, "writer renewal must be fenced before physical detachment")
	if n.proof == nil {
		detach, err := request.RootFSRequest()
		require.NoError(n.t, err)
		root, err := rootfshandoff.NewMigrationRootFSDetachProof(detach, rootfshandoff.CrashFenceSessionObservation{
			Parent: n.f.issue.GateParent, RootFSID: n.f.filesystem.ID, WriterEpoch: n.f.writerEpoch, OperationID: request.PublicationRequest.Assignment.OperationID,
			BindingDigest: request.PublicationRequest.Capture.Request.BindingDigest, SessionState: rootfshandoff.StateTombstoned, BranchPath: "/private/transfer.wal",
			DeviceBound: true, DevicePath: "/dev/nbd0", LiveSessionAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)})
		require.NoError(n.t, err)
		digest, err := request.Digest()
		require.NoError(n.t, err)
		n.proof = &protocol.MigrationSourceFenceProof{RequestDigest: digest, RootFS: root, ContainerID: protocol.NomadRunscContainerID(n.f.slotID), MountNamespaceID: "mnt:[1]", ContainerAbsent: true, StableMountAbsent: true}
		n.proof.Digest, err = n.proof.ProofDigest()
		require.NoError(n.t, err)
	}
	proof := *n.proof
	if n.invalid == "fence" {
		proof.ContainerAbsent = false
	}
	return &proof, n.called("fence")
}

type transferFailureStore struct {
	*PGSandboxStore
	before, after string
}

func (s *transferFailureStore) commit(phase string, fn func() error) error {
	if s.before == phase {
		s.before = ""
		return errors.New("injected before database commit")
	}
	if err := fn(); err != nil {
		return err
	}
	if s.after == phase {
		s.after = ""
		return errors.New("injected lost database acknowledgement")
	}
	return nil
}
func (s *transferFailureStore) CommitNomadSandboxMigrationPublication(ctx context.Context, r protocol.MigrationPublicationRequest, p protocol.MigrationPublication) error {
	return s.commit("publish", func() error { return s.PGSandboxStore.CommitNomadSandboxMigrationPublication(ctx, r, p) })
}
func (s *transferFailureStore) CommitNomadSandboxMigrationImagePreparation(ctx context.Context, r protocol.MigrationImagePrepareRequest, p protocol.MigrationImagePrepared) error {
	return s.commit("image", func() error { return s.PGSandboxStore.CommitNomadSandboxMigrationImagePreparation(ctx, r, p) })
}
func (s *transferFailureStore) CommitNomadSandboxMigrationSourceFence(ctx context.Context, r protocol.MigrationSourceFenceRequest, p protocol.MigrationSourceFenceProof) error {
	return s.commit("fence", func() error { return s.PGSandboxStore.CommitNomadSandboxMigrationSourceFence(ctx, r, p) })
}

func TestNomadMigrationTransferWorkerRecoversEveryLostResponseIntegration(t *testing.T) {
	f, reservation, publication := migrationPublicationStoreFixture(t, "transfer-recovery")
	ids, err := f.store.ListNomadMigrationTransfers(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids, "capture authorization alone must not enter the transfer queue")
	state, err := f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
	require.NoError(t, err)
	require.Nil(t, state)
	_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	node := &transferTestNode{t: t, f: f, publication: publication, calls: map[string]int{}}
	for _, phase := range []string{"publish", "image", "fence"} {
		for _, loss := range []string{"node", "before", "after"} {
			store := &transferFailureStore{PGSandboxStore: NewPGSandboxStore(f.pool)}
			switch loss {
			case "node":
				node.fail = phase
			case "before":
				store.before = phase
			case "after":
				store.after = phase
			}
			worker, err := nomadmigration.New(store, node)
			require.NoError(t, err)
			result, err := worker.RunOnce(f.ctx)
			require.Error(t, err, phase+"/"+loss)
			require.Equal(t, 1, result.Failed)
			require.Zero(t, result.Advanced, "lost acknowledgement must not be reported as a successful transition")
		}
	}
	require.Equal(t, map[string]int{"publish": 3, "image": 3, "fence": 3}, node.calls)
	state, err = f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
	require.NoError(t, err)
	require.Nil(t, state)
	ids, err = f.store.ListNomadMigrationTransfers(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
	lifecycle, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseCommitting, lifecycle.Phase)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, publication.Capture.RootFS.Generation.GenerationID, filesystem.HeadGenerationID)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.WriterGrantID)
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, int64(1), sandbox.RuntimeGeneration)
	var active int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&active))
	require.Equal(t, 2, active, "transfer cannot release either carrier")
}

func TestNomadMigrationTransferWorkerConcurrentReplicasIntegration(t *testing.T) {
	f, _, publication := migrationPublicationStoreFixture(t, "transfer-replicas")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	node := &transferTestNode{t: t, f: f, publication: publication, calls: map[string]int{}}
	for range 3 {
		var wg sync.WaitGroup
		for range 4 {
			wg.Go(func() {
				worker, err := nomadmigration.New(NewPGSandboxStore(f.pool), node)
				require.NoError(t, err)
				// A competing phase commit can make a stale CAS fail. The next
				// pass rereads PostgreSQL rather than recreating earlier work.
				_, _ = worker.RunOnce(f.ctx)
			})
		}
		wg.Wait()
	}
	state, err := f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
	require.NoError(t, err)
	require.Nil(t, state)
	node.mu.Lock()
	defer node.mu.Unlock()
	for _, phase := range []string{"publish", "image", "fence"} {
		require.Positive(t, node.calls[phase])
	}
}

func TestNomadMigrationTransferInvalidReceiptsCannotAdvanceIntegration(t *testing.T) {
	f, _, publication := migrationPublicationStoreFixture(t, "transfer-invalid")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	node := &transferTestNode{t: t, f: f, publication: publication, calls: map[string]int{}}
	worker, err := nomadmigration.New(f.store, node)
	require.NoError(t, err)
	for _, phase := range []string{"publish", "image", "fence"} {
		node.invalid = phase
		result, err := worker.RunOnce(f.ctx)
		require.Error(t, err)
		require.Equal(t, 1, result.Failed)
		state, err := f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
		require.NoError(t, err)
		require.NotNil(t, state)
		switch phase {
		case "publish":
			require.Nil(t, state.Published)
			require.Nil(t, state.Image)
		case "image":
			require.Nil(t, state.Prepared)
			require.Nil(t, state.Fence)
		case "fence":
			require.NotNil(t, state.Fence)
		}
		node.invalid = ""
		result, err = worker.RunOnce(f.ctx)
		require.NoError(t, err)
		require.Equal(t, 1, result.Advanced)
	}
	ids, err := f.store.ListNomadMigrationTransfers(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestNomadMigrationTransferReadDoesNotRefreshDestinationAdmissionIntegration(t *testing.T) {
	f, reservation, publication := migrationPublicationStoreFixture(t, "transfer-expiry")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	node := &transferTestNode{t: t, f: f, publication: publication, calls: map[string]int{}}
	worker, err := nomadmigration.New(f.store, node)
	require.NoError(t, err)
	_, err = worker.RunOnce(f.ctx)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	state, err := f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
	require.NoError(t, err)
	require.NotNil(t, state)
	result, err := worker.RunOnce(f.ctx)
	require.Error(t, err)
	require.Equal(t, 1, result.Failed)
	require.Zero(t, node.calls["image"])
	require.Zero(t, node.calls["fence"])
	grant, err := f.store.GetRootFSWriterGrant(f.ctx, f.issue.GrantID)
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateConsumed, grant.State)
	ids, err := f.store.ListNomadMigrationTransfers(f.ctx, publication.Assignment.OperationID, 8)
	require.NoError(t, err)
	require.Empty(t, ids, "cursor is exclusive")
	_, err = f.store.ListNomadMigrationTransfers(f.ctx, "", 0)
	require.Error(t, err)
}
