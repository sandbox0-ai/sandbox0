package sandboxstore

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type handoverWorkProcd struct {
	t               *testing.T
	request         procdapi.RuntimeMigrationRequest
	address         string
	calls, probes   int
	lost, probeLost bool
	invalid         string
}

func (p *handoverWorkProcd) GenerateMigrationToken(r procdapi.RuntimeMigrationRequest) (string, error) {
	require.Equal(p.t, p.request, r)
	return r.Permission()
}
func (p *handoverWorkProcd) GenerateToken(team, user, sandbox string) (string, error) {
	require.Equal(p.t, p.request.Assignment.Target.TeamID, team)
	require.Equal(p.t, p.request.Assignment.Target.SandboxID, sandbox)
	require.Empty(p.t, user)
	return "probe-token", nil
}
func (p *handoverWorkProcd) MigrateRuntime(_ context.Context, address string, r procdapi.RuntimeMigrationRequest, token string) (*procdapi.RuntimeMigrationResponse, error) {
	p.calls++
	require.Equal(p.t, p.address, address)
	require.Equal(p.t, p.request, r)
	permission, err := r.Permission()
	require.NoError(p.t, err)
	require.Equal(p.t, permission, token)
	if p.lost {
		p.lost = false
		return nil, errors.New("lost procd handover response")
	}
	receipt := migrationHandoverResponse(p.t, r)
	switch p.invalid {
	case "missing":
		return nil, nil
	case "digest":
		receipt.RequestDigest = strings.Repeat("a", 64)
	case "instance":
		receipt.InstanceID = "replacement-procd"
	}
	return &receipt, nil
}
func (p *handoverWorkProcd) ProbeCommandReady(_ context.Context, address, token string) (*procdapi.CommandReadyProbeResult, error) {
	p.probes++
	require.Equal(p.t, p.address, address)
	require.Equal(p.t, "probe-token", token)
	if p.probeLost {
		p.probeLost = false
		return nil, errors.New("lost command probe response")
	}
	result := &procdapi.CommandReadyProbeResult{CommandReadyProbeResponse: procdapi.CommandReadyProbeResponse{InstanceID: p.request.InstanceID, Status: "ready"}, ResponseBodyDigest: strings.Repeat("8", 64)}
	if p.invalid == "probe-instance" {
		result.InstanceID = "replacement-procd"
	}
	return result, nil
}

type handoverWorkStore struct {
	*PGSandboxStore
	before, after bool
}

func (s *handoverWorkStore) CommitNomadSandboxMigrationHandover(ctx context.Context, r procdapi.RuntimeMigrationRequest, p procdapi.RuntimeMigrationResponse) error {
	if s.before {
		s.before = false
		return errors.New("before procd receipt commit")
	}
	if err := s.PGSandboxStore.CommitNomadSandboxMigrationHandover(ctx, r, p); err != nil {
		return err
	}
	if s.after {
		s.after = false
		return errors.New("lost procd receipt commit response")
	}
	return nil
}

type handoverWorkNode struct {
	t             *testing.T
	store         *PGSandboxStore
	restored      protocol.MigrationRestoreObservation
	ready         MarkRuntimeSlotCommandReadyRequest
	calls         int
	before, after bool
}

func (n *handoverWorkNode) CommandReady(ctx context.Context, target protocol.NodeChannelTarget, r protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error) {
	n.calls++
	require.Equal(n.t, n.restored.Request.Image.Target, target)
	require.Equal(n.t, n.ready.SlotID, r.Proof.SlotID)
	require.Equal(n.t, n.ready.OperationID, r.Proof.OperationID)
	require.Equal(n.t, n.ready.ClaimID, r.Proof.ClaimID)
	require.Equal(n.t, n.restored.Request.Stage.Identity.LaunchAttempt, r.Proof.LaunchAttempt)
	require.Equal(n.t, protocol.NomadRunscContainerID(target.SlotID), r.Proof.RunscContainerID)
	require.Equal(n.t, n.ready.ProcdInstanceID, r.Proof.ProcdInstanceID)
	require.Equal(n.t, n.ready.ProcdAddress, r.Proof.ProcdAddress)
	require.NoError(n.t, r.Proof.Validate())
	if n.before {
		n.before = false
		return protocol.NodeControlResponse{}, errors.New("before readiness commit")
	}
	digest, err := r.Proof.Digest()
	require.NoError(n.t, err)
	ready := n.ready
	ready.CommandReadyDigest, err = hex.DecodeString(digest)
	require.NoError(n.t, err)
	slot, err := n.store.MarkRuntimeSlotCommandReady(ctx, &ready)
	if err != nil {
		return protocol.NodeControlResponse{}, err
	}
	require.NotNil(n.t, slot.MigrationAdoption)
	adoptionDigest, err := slot.MigrationAdoption.Digest()
	require.NoError(n.t, err)
	proof := protocol.MigrationAdoptionProof{RequestDigest: adoptionDigest, ImageAbsent: true}
	require.NoError(n.t, n.store.CommitNomadSandboxMigrationAdoption(ctx, *slot.MigrationAdoption, proof))
	if n.after {
		n.after = false
		return protocol.NodeControlResponse{}, errors.New("lost readiness commit response")
	}
	return protocol.NodeControlResponse{Phase: string(protocol.StateActive), MigrationAdoption: &protocol.MigrationAdoptionReceipt{Request: *slot.MigrationAdoption, Proof: proof}}, nil
}

func TestNomadMigrationHandoverWorkerRecoversEachDeliveryBoundaryIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "handover-worker")
	request, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	procd := &handoverWorkProcd{t: t, request: *request, address: ready.ProcdAddress, lost: true}
	node := &handoverWorkNode{t: t, store: f.store, restored: restored, ready: *ready}
	original, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	run := func(store nomadmigration.HandoverStore) (nomadmigration.Result, error) {
		// Each retry loses all coordinator memory, as on manager failover.
		worker, err := nomadmigration.NewHandover(store, procd, procd, node)
		require.NoError(t, err)
		return worker.RunOnce(f.ctx)
	}
	for _, step := range []string{"procd", "before-receipt", "after-receipt", "probe", "probe-instance", "before-ready", "after-ready"} {
		store := &handoverWorkStore{PGSandboxStore: NewPGSandboxStore(f.pool), before: step == "before-receipt", after: step == "after-receipt"}
		procd.probeLost = step == "probe"
		procd.invalid = ""
		if step == "probe-instance" {
			procd.invalid = step
		}
		node.before, node.after = step == "before-ready", step == "after-ready"
		result, err := run(store)
		require.Error(t, err, step)
		require.Equal(t, 1, result.Failed, step)
		current, err := f.store.GetSandbox(f.ctx, f.sandboxID)
		require.NoError(t, err)
		generation := original.RuntimeGeneration
		if step == "after-ready" {
			generation++
		}
		require.Equal(t, generation, current.RuntimeGeneration, step)
		require.Equal(t, original.ExpiresAt, current.ExpiresAt)
		require.Equal(t, original.HardExpiresAt, current.HardExpiresAt)
	}
	require.Equal(t, 3, procd.calls, "committed receipt suppresses further procd handovers")
	require.Equal(t, 2, node.calls, "failed probes cannot publish readiness")
	result, err := run(NewPGSandboxStore(f.pool))
	require.NoError(t, err)
	require.Zero(t, result.Candidates, "generation commit removes work despite lost node acknowledgement")
	h, err := f.store.GetNomadMigrationHandover(f.ctx, request.Assignment.OperationID)
	require.NoError(t, err)
	require.Nil(t, h)
	var leases int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&leases))
	require.Equal(t, 2, leases, "source cleanup still requires physical evidence")
}

func TestNomadMigrationHandoverWorkerRejectsInvalidProcdEvidenceIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "handover-invalid")
	ids, err := f.store.ListNomadMigrationHandovers(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids, "restore alone cannot authorize handover delivery")
	command, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	procd := &handoverWorkProcd{t: t, request: *command, address: ready.ProcdAddress}
	node := &handoverWorkNode{t: t, store: f.store, restored: restored, ready: *ready}
	for _, invalid := range []string{"missing", "digest", "instance"} {
		procd.invalid = invalid
		worker, err := nomadmigration.NewHandover(f.store, procd, procd, node)
		require.NoError(t, err)
		_, err = worker.RunOnce(f.ctx)
		require.Error(t, err)
		h, err := f.store.GetNomadMigrationHandover(f.ctx, command.Assignment.OperationID)
		require.NoError(t, err)
		require.Nil(t, h.Receipt)
	}
	require.Zero(t, procd.probes)
	require.Zero(t, node.calls)
	procd.invalid = ""
	worker, err := nomadmigration.NewHandover(f.store, procd, procd, node)
	require.NoError(t, err)
	result, err := worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
	result, err = worker.RunOnce(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, result.Advanced)
}

func TestNomadMigrationHandoverWorkerRechecksLiveAuthorityIntegration(t *testing.T) {
	for _, change := range []string{"ttl", "writer", "heartbeat", "claim", "termination"} {
		t.Run(change, func(t *testing.T) {
			f, restored, ready := migrationHandoverStoreFixture(t, "handover-live-"+change)
			command, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
			require.NoError(t, err)
			query, id := "", f.sandboxID
			switch change {
			case "ttl":
				query = `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`
			case "termination":
				query = `UPDATE manager.sandboxes SET desired_state='terminating' WHERE sandbox_id=$1`
			case "writer":
				query, id = `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE grant_id=$1`, restored.Request.Stage.Identity.WriterGrantID
			case "heartbeat":
				query, id = `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID
			case "claim":
				query, id = `UPDATE manager.runtime_slots SET claim_lease_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, ready.SlotID
			}
			_, err = f.pool.Exec(f.ctx, query, id)
			require.NoError(t, err)
			procd := &handoverWorkProcd{t: t, request: *command, address: ready.ProcdAddress}
			node := &handoverWorkNode{t: t, store: f.store, restored: restored, ready: *ready}
			worker, err := nomadmigration.NewHandover(f.store, procd, procd, node)
			require.NoError(t, err)
			_, err = worker.RunOnce(f.ctx)
			require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
			require.Zero(t, procd.calls)
			require.Zero(t, procd.probes)
			require.Zero(t, node.calls)
		})
	}
}

func TestNomadMigrationHandoverWorkerConcurrentReplicasPublishOnceIntegration(t *testing.T) {
	f, restored, ready := migrationHandoverStoreFixture(t, "handover-replicas")
	command, err := f.store.AuthorizeNomadSandboxMigrationHandover(f.ctx, restored)
	require.NoError(t, err)
	for range 2 {
		var wg sync.WaitGroup
		outcomes := make([]error, 4)
		for i := range outcomes {
			wg.Go(func() {
				store := NewPGSandboxStore(f.pool)
				procd := &handoverWorkProcd{t: t, request: *command, address: ready.ProcdAddress}
				node := &handoverWorkNode{t: t, store: store, restored: restored, ready: *ready}
				worker, err := nomadmigration.NewHandover(store, procd, procd, node)
				if err == nil {
					_, err = worker.RunOnce(f.ctx)
				}
				outcomes[i] = err
			})
		}
		wg.Wait()
		for _, err := range outcomes {
			if err != nil {
				require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "a concurrent generation commit may invalidate a pending read")
			}
		}
	}
	sandbox, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, command.Assignment.Target.RuntimeGeneration, sandbox.RuntimeGeneration)
	ids, err := f.store.ListNomadMigrationHandovers(f.ctx, "", 8)
	require.NoError(t, err)
	require.Empty(t, ids)
}
