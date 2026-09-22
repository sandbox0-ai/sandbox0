package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func retainMigrationStagingFixture(t *testing.T, f *nomadPauseStoreFixture, a runtimecontrol.MigrationAssignment, destinationPeer ...string) {
	t.Helper()
	for range 2 {
		r, err := f.store.AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
		require.NoError(t, err)
		require.NotNil(t, r)
		digest, err := r.Digest()
		require.NoError(t, err)
		receipt := protocol.MigrationStagingReserved{RequestDigest: digest}
		if !r.IsSource() && len(destinationPeer) > 0 {
			receipt.PeerCertificateSHA256 = destinationPeer[0]
		}
		require.NoError(t, f.store.CommitNomadSandboxMigrationStaging(f.ctx, a, *r, receipt))
	}
}

func retainMigrationEligibilityFixture(t *testing.T, f *nomadPauseStoreFixture, a runtimecontrol.MigrationAssignment) *protocol.MigrationCPULaunch {
	t.Helper()
	launch := retainMigrationCPUFixture(t, f, a)
	retainMigrationStagingFixture(t, f, a)
	return launch
}

// The node double verifies command-before-dispatch ordering in real PostgreSQL.
// Physical pool and authenticated-channel behavior have separate node tests.
type stagingWorkNode struct {
	mu    sync.Mutex
	t     *testing.T
	f     *nomadPauseStoreFixture
	calls map[string]int
	fail  string
}

func (n *stagingWorkNode) observe(ctx context.Context, r protocol.MigrationStagingRequest, release bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	var payload []byte
	var sourceReady, targetReady, sourceRelease, targetRelease bool
	require.NoError(n.t, n.f.pool.QueryRow(ctx, `SELECT staging_request,staging_source_receipt IS NOT NULL,staging_destination_receipt IS NOT NULL,
        staging_source_release_requested,staging_destination_release_requested FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, r.Source.OperationID).
		Scan(&payload, &sourceReady, &targetReady, &sourceRelease, &targetRelease))
	var stored protocol.MigrationStagingRequest
	require.NoError(n.t, json.Unmarshal(payload, &stored))
	if !r.IsSource() {
		stored.Target = stored.Destination
	}
	require.Equal(n.t, stored, r)
	if release {
		require.True(n.t, r.IsSource() && sourceRelease || !r.IsSource() && targetRelease, "release command must precede dispatch")
	} else if r.Source.Target.NodeUID < r.Destination.NodeUID {
		if !r.IsSource() {
			require.True(n.t, sourceReady)
		}
	} else if r.IsSource() {
		require.True(n.t, targetReady)
	}
	key := fmt.Sprintf("%t/%t", release, r.IsSource())
	n.calls[key]++
	if n.fail == key {
		n.fail = ""
		return errors.New("lost staging node reply")
	}
	return nil
}

func (n *stagingWorkNode) ReserveMigrationStaging(ctx context.Context, r protocol.MigrationStagingRequest) (*protocol.MigrationStagingReserved, error) {
	if err := n.observe(ctx, r, false); err != nil {
		return nil, err
	}
	digest, err := r.Digest()
	return &protocol.MigrationStagingReserved{RequestDigest: digest}, err
}

func (n *stagingWorkNode) ReleaseMigrationStaging(ctx context.Context, r protocol.MigrationStagingRequest) error {
	return n.observe(ctx, r, true)
}

type stagingWorkStore struct {
	*PGSandboxStore
	role, release bool
	boundary      string
}

func (s *stagingWorkStore) commit(source, release bool, run func() error) error {
	boundary := ""
	if source == s.role && release == s.release {
		boundary, s.boundary = s.boundary, ""
	}
	if boundary == "before" {
		return errors.New("before staging receipt commit")
	}
	if err := run(); err != nil {
		return err
	}
	if boundary == "after" {
		return errors.New("lost staging receipt commit reply")
	}
	return nil
}

func (s *stagingWorkStore) CommitNomadSandboxMigrationStaging(ctx context.Context, a runtimecontrol.MigrationAssignment, r protocol.MigrationStagingRequest, p protocol.MigrationStagingReserved) error {
	return s.commit(r.IsSource(), false, func() error { return s.PGSandboxStore.CommitNomadSandboxMigrationStaging(ctx, a, r, p) })
}

func (s *stagingWorkStore) CommitNomadSandboxMigrationStagingRelease(ctx context.Context, r protocol.MigrationStagingRequest, p protocol.MigrationStagingReleased) error {
	return s.commit(r.IsSource(), true, func() error { return s.PGSandboxStore.CommitNomadSandboxMigrationStagingRelease(ctx, r, p) })
}

func TestNomadMigrationStagingWorkersRecoverLostRepliesIntegration(t *testing.T) {
	for _, release := range []bool{false, true} {
		for _, role := range []bool{false, true} {
			for _, boundary := range []string{"node", "before", "after"} {
				t.Run(fmt.Sprintf("release-%t-source-%t-%s", release, role, boundary), func(t *testing.T) {
					f, a := migrationStoreFixture(t, "staging-recovery")
					migrationReadyTarget(t, f, "staging-recovery", "b")
					_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
					require.NoError(t, err)
					retainMigrationCPUFixture(t, f, a)
					n := &stagingWorkNode{t: t, f: f, calls: map[string]int{}}
					store := &stagingWorkStore{PGSandboxStore: f.store, role: role, release: release}
					if boundary == "node" {
						n.fail = fmt.Sprintf("%t/%t", release, role)
					} else {
						store.boundary = boundary
					}
					if release {
						// A committed command may have reached either node, even
						// when neither reply made it back to the manager.
						_, err = f.store.AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
						require.NoError(t, err)
						require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, a.OperationID, "unused staging"))
						ageMigrationCPUPreflight(t, f, a.OperationID)
					}
					failed := 0
					for range 4 {
						store.PGSandboxStore = NewPGSandboxStore(f.pool)
						var c *nomadmigration.Coordinator
						if release {
							c, err = nomadmigration.NewStagingRelease(store, n)
						} else {
							c, err = nomadmigration.NewStaging(store, n)
						}
						require.NoError(t, err)
						_, err = c.RunOnce(f.ctx)
						if err != nil {
							failed++
						}
					}
					require.Equal(t, 1, failed)
					var ids []string
					if release {
						ids, err = f.store.ListNomadMigrationStagingReleases(f.ctx, "", 8)
					} else {
						ids, err = f.store.ListNomadMigrationStaging(f.ctx, "", 8)
					}
					require.NoError(t, err)
					require.Empty(t, ids)
					for _, side := range []bool{false, true} {
						expected := 1
						if side == role && boundary != "after" {
							expected++
						}
						require.Equal(t, expected, n.calls[fmt.Sprintf("%t/%t", release, side)])
					}
					var prepared, captured bool
					var generation int64
					require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT m.preparation_request IS NOT NULL,m.capture_request IS NOT NULL,s.runtime_generation
                            FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
                            JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id WHERE m.operation_id=$1`, a.OperationID).Scan(&prepared, &captured, &generation))
					require.False(t, prepared)
					require.False(t, captured)
					require.EqualValues(t, 1, generation)
				})
			}
		}
	}
}

func TestNomadMigrationStagingRequiresBothOrderedReceiptsIntegration(t *testing.T) {
	for _, destination := range []string{"0", "z"} {
		t.Run(destination, func(t *testing.T) {
			f, a := migrationStoreFixture(t, "staging-order")
			migrationReadyTarget(t, f, "staging-order", destination)
			_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
			require.NoError(t, err)
			_, err = f.store.AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
			require.Error(t, err, "staging cannot replace CPU eligibility")
			retainMigrationCPUFixture(t, f, a)
			policy := migrationSourcePolicy(f.sandboxID, a.Target.TeamID)
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			first, err := f.store.AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
			require.NoError(t, err)
			require.Equal(t, first.Source.Target.NodeUID < first.Destination.NodeUID, first.IsSource())
			second := *first
			if first.IsSource() {
				second.Target = first.Destination
			} else {
				second.Target = first.Source.Target
			}
			digest, err := second.Digest()
			require.NoError(t, err)
			require.ErrorIs(t, f.store.CommitNomadSandboxMigrationStaging(f.ctx, a, second, protocol.MigrationStagingReserved{RequestDigest: digest}), ErrNomadSandboxMigrationConflict)
			digest, err = first.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationStaging(f.ctx, a, *first, protocol.MigrationStagingReserved{RequestDigest: digest}))
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			next, err := NewPGSandboxStore(f.pool).AuthorizeNomadSandboxMigrationStaging(f.ctx, a)
			require.NoError(t, err)
			require.Equal(t, second, *next)
			digest, err = next.Digest()
			require.NoError(t, err)
			require.NoError(t, f.store.CommitNomadSandboxMigrationStaging(f.ctx, a, *next, protocol.MigrationStagingReserved{RequestDigest: digest}))
			_, err = f.store.AuthorizeNomadSandboxMigrationPreparation(f.ctx, a, policy)
			require.NoError(t, err)
			_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET staging_source_release_requested=true WHERE operation_id=$1`, a.OperationID)
			require.Error(t, err, "preparation is not image-absence evidence")
			for _, mutation := range []string{"staging_request=NULL", "staging_request=jsonb_set(staging_request,'{bytes}','1048576')", "staging_source_receipt=NULL"} {
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_migrations SET `+mutation+` WHERE operation_id=$1`, a.OperationID)
				require.Error(t, err)
			}
			_, err = f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, a.OperationID)
			require.NoError(t, err)
		})
	}
}

func TestNomadMigrationStagingReleaseRequiresPhysicalEvidenceIntegration(t *testing.T) {
	f, adoption, adopted := migrationFinalizationStoreFixture(t, "staging-finalization")
	request, err := f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.Nil(t, request, "generation commit does not prove image removal")
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, adoption, adopted))
	request, err = f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.False(t, request.IsSource())
	digest, err := request.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *request, protocol.MigrationStagingReleased{RequestDigest: digest}))
	request, err = f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.Nil(t, request, "source remains retained until its own physical finalization")
	finalize, err := f.store.AuthorizeNomadSandboxMigrationSourceFinalization(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFinalization(f.ctx, *finalize, migrationFinalizationStoreProof(t, *finalize)))
	ageMigrationCPUPreflight(t, f, adoption.OperationID)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='terminating',hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	request, err = f.store.AuthorizeNomadSandboxMigrationStagingRelease(f.ctx, adoption.OperationID)
	require.NoError(t, err)
	require.True(t, request.IsSource())
	digest, err = request.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationStagingRelease(f.ctx, *request, protocol.MigrationStagingReleased{RequestDigest: digest}))
	ids, err := f.store.ListNomadMigrationStagingReleases(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestNomadMigrationStagingConcurrentReplicasIntegration(t *testing.T) {
	f, a := migrationStoreFixture(t, "staging-concurrent")
	migrationReadyTarget(t, f, "staging-concurrent", "b")
	_, err := f.store.ReserveNomadSandboxMigration(f.ctx, a)
	require.NoError(t, err)
	retainMigrationCPUFixture(t, f, a)
	n := &stagingWorkNode{t: t, f: f, calls: map[string]int{}}
	for range 3 {
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Go(func() {
				c, err := nomadmigration.NewStaging(NewPGSandboxStore(f.pool), n)
				if err == nil {
					_, err = c.RunOnce(f.ctx)
				}
				errs[i] = err
			})
		}
		wg.Wait()
		for _, err := range errs {
			require.NoError(t, err)
		}
	}
	ids, err := f.store.ListNomadMigrationStaging(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}
