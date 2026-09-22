package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationPrefetchStoreFixture(t *testing.T, suffix string) (*nomadPauseStoreFixture, *NomadSandboxMigrationReservation, protocol.MigrationPublicationRequest, protocol.MigrationPublicationPlan) {
	t.Helper()
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	f, reservation, publication := migrationPublicationStoreFixture(t, suffix, digest.FromString("reserved-target-peer").String())
	receipt := migrationPublicationReceipt(t, publication)
	endpoint, err := runtimecheckpoint.NewPeerEndpoint("10.0.1.2:19443", identity)
	require.NoError(t, err)
	return f, reservation, publication, protocol.MigrationPublicationPlan{RequestDigest: receipt.RequestDigest,
		Binding: receipt.Binding, Reference: receipt.Reference, Peer: endpoint}
}

type prefetchTransferNode struct {
	*transferTestNode
	plan                                protocol.MigrationPublicationPlan
	planError, prefetchError            error
	waitCancellation                    bool
	prefetchStarted, publicationStarted chan struct{}
	prefetchStopped                     chan struct{}
	planCalls, prefetchCalls            atomic.Int32
}

func (n *prefetchTransferNode) PlanMigrationPublication(_ context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error) {
	n.planCalls.Add(1)
	if n.planError != nil {
		return nil, n.planError
	}
	copy := n.plan
	return &copy, copy.ValidateFor(request)
}

func (n *prefetchTransferNode) PrefetchMigrationImage(ctx context.Context, request protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	n.prefetchCalls.Add(1)
	if n.prefetchStopped != nil {
		defer close(n.prefetchStopped)
	}
	var stored string
	err := n.f.pool.QueryRow(ctx, `SELECT target_image_prefetch_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.Publication.Assignment.OperationID).Scan(&stored)
	if err != nil {
		return nil, err
	}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if want != stored {
		return nil, errors.New("prefetch dispatched before its exact database authorization")
	}
	close(n.prefetchStarted)
	if n.waitCancellation {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	select {
	case <-n.publicationStarted:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if n.prefetchError != nil {
		return nil, n.prefetchError
	}
	return &protocol.MigrationImagePrefetched{RequestDigest: want, ManifestDigest: request.Plan.Reference.ManifestDigest, TotalBytes: 1024}, nil
}

func (n *prefetchTransferNode) PublishMigration(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	if n.planError == nil || n.prefetchCalls.Load() != 0 {
		select {
		case <-n.prefetchStarted:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	close(n.publicationStarted)
	p, err := n.transferTestNode.PublishMigration(ctx, request)
	if p != nil {
		p.Peer = n.plan.Peer
	}
	return p, err
}

func TestNomadMigrationPrefetchWorkerOverlapsAndPreservesPublicationGateIntegration(t *testing.T) {
	for _, mode := range []string{"success", "unsupported", "peer-failure", "lost-prefetch-ack", "upload-failure"} {
		t.Run(mode, func(t *testing.T) {
			f, _, publication, plan := migrationPrefetchStoreFixture(t, "prefetch-worker-"+mode)
			_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
			require.NoError(t, err)
			node := &prefetchTransferNode{transferTestNode: &transferTestNode{t: t, f: f, publication: publication, calls: map[string]int{}},
				plan: plan, prefetchStarted: make(chan struct{}), publicationStarted: make(chan struct{}), prefetchStopped: make(chan struct{})}
			switch mode {
			case "unsupported":
				node.planError = errors.New("node lacks planning capability")
			case "peer-failure":
				node.prefetchError = errors.New("peer unavailable")
			case "lost-prefetch-ack":
				node.waitCancellation = true
			case "upload-failure":
				node.fail = "publish"
				node.waitCancellation = true
			}
			worker, err := nomadmigration.New(f.store, node)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
			defer cancel()
			result, err := worker.RunOnce(ctx)
			state, readErr := f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
			require.NoError(t, readErr)
			require.Nil(t, state.Prepared)
			require.Nil(t, state.Fence)
			if mode == "upload-failure" {
				require.Error(t, err)
				require.Equal(t, 1, result.Failed)
				require.Nil(t, state.Published, "cache activity cannot acknowledge a failed regional upload")
				require.NotNil(t, state.Prefetch)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, result.Advanced)
			require.NotNil(t, state.Published)
			if mode == "lost-prefetch-ack" {
				require.NoError(t, ctx.Err(), "optional cache acknowledgement must not consume the operation deadline")
				select {
				case <-node.prefetchStopped:
				default:
					t.Fatal("prefetch observer must exit before publication returns")
				}
			}
			if mode == "unsupported" {
				require.Nil(t, state.Prefetch)
				require.Zero(t, node.prefetchCalls.Load())
			} else {
				require.NotNil(t, state.Prefetch)
				require.Equal(t, int32(1), node.prefetchCalls.Load())
			}
			// A separate pass must still authorize and commit ordinary image
			// preparation, even after an acknowledged or failed speculative fill.
			result, err = worker.RunOnce(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, result.Advanced)
			state, err = f.store.GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
			require.NoError(t, err)
			require.NotNil(t, state.Prepared)
			require.Nil(t, state.Fence)
		})
	}
}

func TestNomadMigrationPrefetchRequiresAuthorityAndDoesNotPublishIntegration(t *testing.T) {
	f, reservation, publication, plan := migrationPrefetchStoreFixture(t, "prefetch-authority")
	_, err := f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	request, err := f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
	require.NoError(t, err)
	require.NoError(t, request.Validate())
	require.Equal(t, reservation.TargetSlot.ID, request.Staging.Target.SlotID)
	require.Equal(t, plan, request.Plan)
	var payload []byte
	var published, prepared, fenced bool
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT target_image_prefetch_request,
        publication_receipt IS NOT NULL,target_image_request IS NOT NULL,source_fence_request IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, publication.Assignment.OperationID).
		Scan(&payload, &published, &prepared, &fenced))
	require.False(t, published)
	require.False(t, prepared)
	require.False(t, fenced)
	var stored protocol.MigrationImagePrefetchRequest
	require.NoError(t, json.Unmarshal(payload, &stored))
	require.Equal(t, *request, stored)
	transfer, err := NewPGSandboxStore(f.pool).GetNomadMigrationTransfer(f.ctx, publication.Assignment.OperationID)
	require.NoError(t, err)
	require.Equal(t, request, transfer.Prefetch, "manager restart recovers the committed cache authorization")
	require.Nil(t, transfer.Published)
	require.Nil(t, transfer.Image)
	require.Nil(t, transfer.Fence)
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "prefetch authority is not a durable publication")
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, int64(1), sandbox.RuntimeGeneration)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.WriterGrantID)
	require.Empty(t, target.SandboxID)
	for _, query := range []string{
		`UPDATE manager.sandbox_runtime_migrations SET target_image_prefetch_request=NULL,target_image_prefetch_digest=NULL WHERE operation_id=$1`,
		`UPDATE manager.sandbox_runtime_migrations SET target_image_prefetch_digest=repeat('f',64) WHERE operation_id=$1`,
	} {
		_, err = f.pool.Exec(f.ctx, query, publication.Assignment.OperationID)
		require.Error(t, err, "committed prefetch authority must survive retries and restart")
	}
	actual := migrationPublicationReceipt(t, publication)
	actual.Peer = plan.Peer
	require.NoError(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, publication, actual))
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.NoError(t, err, "normal preparation remains gated by its real publication receipt")
}

func TestNomadMigrationPrefetchConcurrentRetriesRetainExactPlanIntegration(t *testing.T) {
	f, _, publication, plan := migrationPrefetchStoreFixture(t, "prefetch-retry")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	const workers = 4
	results := make([]*protocol.MigrationImagePrefetchRequest, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
		})
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, results[0], results[i])
	}
	changed := plan
	changed.Reference.ManifestDigest = digest.FromString("replacement-image").String()
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	changed = plan
	changed.Peer.Address = "https://10.0.1.3:19443"
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET staging_destination_release_requested=true WHERE operation_id=$1`, publication.Assignment.OperationID)
	require.Error(t, err, "prefetch authority does not permit releasing target staging without cleanup evidence")
	retry, err := f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
	require.NoError(t, err)
	require.Equal(t, results[0], retry)
}

func TestNomadMigrationPrefetchRejectsExpiredTargetAndChangedPublicationIntegration(t *testing.T) {
	f, reservation, publication, plan := migrationPrefetchStoreFixture(t, "prefetch-target")
	_, err := f.store.AuthorizeNomadSandboxMigrationPublication(f.ctx, publication.Assignment, publication.Capture, publication.CPUFeaturesDigest)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()+INTERVAL '1 minute' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	actual := migrationPublicationReceipt(t, publication)
	actual.Peer = plan.Peer
	require.NoError(t, f.store.CommitNomadSandboxMigrationPublication(f.ctx, publication, actual))
	changed := plan
	changed.Reference.ManifestDigest = digest.FromString("not-the-published-image").String()
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, changed)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePreparation(f.ctx, publication.Assignment)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadSandboxMigrationImagePrefetch(f.ctx, publication, plan)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "prefetch cannot start beside existing destination image custody")
}
