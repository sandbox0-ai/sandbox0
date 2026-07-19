package fsserver

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type objectRejectingQuotaStore struct {
	limit int64
}

func (s objectRejectingQuotaStore) EffectivePolicy(_ context.Context, teamID string, key teamquota.Key) (*teamquota.Policy, error) {
	return &teamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   teamquota.KindCapacity,
		Limit:  s.limit,
	}, nil
}

func (s objectRejectingQuotaStore) ReserveDelta(_ context.Context, request teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	committed := request.Observed.Clone()
	target := committed.Clone()
	for key, delta := range request.Delta {
		target[key] += delta
	}
	if requested := target[teamquota.KeyStorageObjectCount]; requested > s.limit {
		return nil, &teamquota.ExceededError{
			TeamID:    request.Owner.TeamID,
			Key:       teamquota.KeyStorageObjectCount,
			Limit:     s.limit,
			Committed: committed[teamquota.KeyStorageObjectCount],
			Requested: request.Delta[teamquota.KeyStorageObjectCount],
		}
	}
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Committed: committed,
		Target:    target,
		Reserved:  request.Delta.Clone(),
	}, nil
}

func (objectRejectingQuotaStore) Commit(context.Context, teamquota.OperationRef) error {
	return nil
}

func (objectRejectingQuotaStore) CommitExact(
	context.Context,
	teamquota.OperationRef,
	teamquota.Values,
) error {
	return nil
}

func (objectRejectingQuotaStore) Abort(context.Context, teamquota.OperationRef, string) error {
	return nil
}

func (objectRejectingQuotaStore) BeginRelease(_ context.Context, request teamquota.ReleaseRequest) (*teamquota.Reservation, error) {
	return &teamquota.Reservation{Owner: request.Owner, Operation: request.Operation, Target: request.Target}, nil
}

func (objectRejectingQuotaStore) ConfirmRelease(context.Context, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (objectRejectingQuotaStore) ConfirmReleaseTx(context.Context, pgx.Tx, teamquota.OperationRef, teamquota.RuntimeRef) error {
	return nil
}

func (objectRejectingQuotaStore) ReconcileTargetIfRevision(
	context.Context,
	teamquota.Owner,
	teamquota.Values,
	teamquota.RuntimeRef,
	int64,
) (bool, error) {
	return true, nil
}

func (objectRejectingQuotaStore) GetRecoveryAllocation(context.Context, teamquota.Owner) (*teamquota.RecoveryAllocation, error) {
	return nil, nil
}

func TestCreateFailsBeforeS0FSStateMutationWhenObjectQuotaIsExceeded(t *testing.T) {
	volCtx := newMountedS0FSVolumeContext(t, "volume-quota", "team-quota")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"volume-quota": volCtx,
		},
	}, nil, nil)
	server.SetStorageQuota(storagequota.New(objectRejectingQuotaStore{limit: 5}, "test-region"))

	_, err := server.Create(authContext("team-quota", ""), &pb.CreateRequest{
		VolumeId: "volume-quota",
		Parent:   s0fs.RootInode,
		Name:     "blocked",
		Mode:     0o644,
	})
	if got := fserror.CodeOf(err); got != fserror.ResourceExhausted {
		t.Fatalf("Create() error = %v, code = %v, want ResourceExhausted", err, got)
	}
	if _, lookupErr := volCtx.S0FS.Lookup(s0fs.RootInode, "blocked"); !errors.Is(lookupErr, s0fs.ErrNotFound) {
		t.Fatalf("Lookup(blocked) error = %v, want ErrNotFound", lookupErr)
	}
}
