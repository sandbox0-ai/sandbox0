package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
)

type fakeReplica struct {
	replicaID string
	appendErr error
	snapErr   error
	appends   []Entry
	snapshots []Snapshot
}

func (r *fakeReplica) ID() string { return r.replicaID }

func (r *fakeReplica) Append(_ context.Context, _ string, _ uint64, entry Entry, _ uint64) error {
	if r.appendErr != nil {
		return r.appendErr
	}
	r.appends = append(r.appends, entry)
	return nil
}

func (r *fakeReplica) InstallSnapshot(_ context.Context, _ string, snapshot Snapshot) error {
	if r.snapErr != nil {
		return r.snapErr
	}
	r.snapshots = append(r.snapshots, snapshot)
	return nil
}

func TestGroupAppendRequiresLeaderAndQuorum(t *testing.T) {
	t.Parallel()

	manager := NewManager("node-a", "10.0.0.1:8080", router.NewVolumeRouter())
	group := manager.EnsureGroup("vol-1", []Member{
		{ID: "node-a", Addr: "10.0.0.1:8080"},
		{ID: "node-b", Addr: "10.0.0.2:8080"},
		{ID: "node-c", Addr: "10.0.0.3:8080"},
	})
	replicaB := &fakeReplica{replicaID: "node-b"}
	replicaC := &fakeReplica{replicaID: "node-c", appendErr: errors.New("network partition")}
	if err := group.RegisterReplica(replicaB); err != nil {
		t.Fatalf("RegisterReplica(node-b) error = %v", err)
	}
	if err := group.RegisterReplica(replicaC); err != nil {
		t.Fatalf("RegisterReplica(node-c) error = %v", err)
	}
	if _, err := group.Campaign("node-a"); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	entry, lease, err := group.Append(context.Background(), "node-a", []byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if entry.Index != 1 || lease.Committed != 1 {
		t.Fatalf("entry=%+v lease=%+v, want committed index 1", entry, lease)
	}
	if len(replicaB.appends) != 1 {
		t.Fatalf("replicaB appends = %d, want 1", len(replicaB.appends))
	}

	replicaB.appendErr = errors.New("disk full")
	if _, _, err := group.Append(context.Background(), "node-a", []byte("second")); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("Append() err = %v, want %v", err, ErrQuorumUnavailable)
	}
}

func TestGroupElectionFencesStaleLeaderAndPublishesRoute(t *testing.T) {
	t.Parallel()

	volumeRouter := router.NewVolumeRouter()
	manager := NewManager("node-a", "10.0.0.1:8080", volumeRouter)
	group := manager.EnsureGroup("vol-1", []Member{
		{ID: "node-a", Addr: "10.0.0.1:8080"},
		{ID: "node-b", Addr: "10.0.0.2:8080"},
	})
	replicaB := &fakeReplica{replicaID: "node-b"}
	if err := group.RegisterReplica(replicaB); err != nil {
		t.Fatalf("RegisterReplica(node-b) error = %v", err)
	}

	leaseA, err := group.Campaign("node-a")
	if err != nil {
		t.Fatalf("Campaign(node-a) error = %v", err)
	}
	if err := group.ValidateLease(leaseA); err != nil {
		t.Fatalf("ValidateLease(leaseA) error = %v", err)
	}
	route := volumeRouter.Resolve("vol-1")
	if !route.LocalPrimary || route.PrimaryNodeID != "node-a" || route.Epoch != leaseA.Epoch {
		t.Fatalf("route after node-a campaign = %+v", route)
	}

	leaseB, err := group.Campaign("node-b")
	if err != nil {
		t.Fatalf("Campaign(node-b) error = %v", err)
	}
	if leaseB.Epoch <= leaseA.Epoch || leaseB.Term <= leaseA.Term {
		t.Fatalf("leaseB = %+v, want higher term and epoch than %+v", leaseB, leaseA)
	}
	if err := group.ValidateLease(leaseA); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("ValidateLease(stale) err = %v, want %v", err, ErrLeaseExpired)
	}
	if _, _, err := group.Append(context.Background(), "node-a", []byte("stale")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Append(stale leader) err = %v, want %v", err, ErrNotLeader)
	}

	route = volumeRouter.Resolve("vol-1")
	if route.LocalPrimary || route.PrimaryNodeID != "node-b" || route.PrimaryAddr != "10.0.0.2:8080" || route.Epoch != leaseB.Epoch {
		t.Fatalf("route after node-b campaign = %+v", route)
	}
}

func TestGroupInstallSnapshotAdvancesLaggingFollowers(t *testing.T) {
	t.Parallel()

	manager := NewManager("node-a", "10.0.0.1:8080", router.NewVolumeRouter())
	group := manager.EnsureGroup("vol-1", []Member{
		{ID: "node-a", Addr: "10.0.0.1:8080"},
		{ID: "node-b", Addr: "10.0.0.2:8080"},
		{ID: "node-c", Addr: "10.0.0.3:8080"},
	})
	replicaB := &fakeReplica{replicaID: "node-b"}
	replicaC := &fakeReplica{replicaID: "node-c"}
	if err := group.RegisterReplica(replicaB); err != nil {
		t.Fatalf("RegisterReplica(node-b) error = %v", err)
	}
	if err := group.RegisterReplica(replicaC); err != nil {
		t.Fatalf("RegisterReplica(node-c) error = %v", err)
	}
	if _, err := group.Campaign("node-a"); err != nil {
		t.Fatalf("Campaign() error = %v", err)
	}

	lease, err := group.InstallSnapshot(context.Background(), "node-a", Snapshot{
		Index: 7,
		Term:  1,
		Data:  []byte("checkpoint"),
	})
	if err != nil {
		t.Fatalf("InstallSnapshot() error = %v", err)
	}
	if lease.Committed != 7 {
		t.Fatalf("lease committed = %d, want 7", lease.Committed)
	}
	if len(replicaB.snapshots) != 1 || replicaB.snapshots[0].Index != 7 {
		t.Fatalf("replicaB snapshots = %+v", replicaB.snapshots)
	}
	if len(replicaC.snapshots) != 1 || replicaC.snapshots[0].Index != 7 {
		t.Fatalf("replicaC snapshots = %+v", replicaC.snapshots)
	}

	status := group.Status()
	if status.CommitIndex != 7 || status.LastIndex != 7 {
		t.Fatalf("status = %+v, want commit and last index 7", status)
	}
	if status.Progress["node-b"].MatchIndex != 7 || status.Progress["node-c"].MatchIndex != 7 {
		t.Fatalf("progress = %+v", status.Progress)
	}
}
