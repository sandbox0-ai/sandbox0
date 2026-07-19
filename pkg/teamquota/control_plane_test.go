package teamquota_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotatestutil "github.com/sandbox0-ai/sandbox0/pkg/teamquota/testutil"
)

type recordingControlPlaneStore struct {
	*teamquotatestutil.PermissiveCapacityStore
	reserveRequest *teamquota.ReserveRequest
	releaseRequest *teamquota.ReleaseRequest
	committed      *teamquota.OperationRef
	confirmed      *teamquota.OperationRef
}

func newRecordingControlPlaneStore() *recordingControlPlaneStore {
	return &recordingControlPlaneStore{
		PermissiveCapacityStore: teamquotatestutil.NewPermissiveCapacityStore(),
	}
}

func (s *recordingControlPlaneStore) ReserveTargetTx(
	ctx context.Context,
	tx pgx.Tx,
	request teamquota.ReserveRequest,
) (*teamquota.Reservation, error) {
	copied := request
	copied.Target = request.Target.Clone()
	s.reserveRequest = &copied
	return s.PermissiveCapacityStore.ReserveTargetTx(ctx, tx, request)
}

func (s *recordingControlPlaneStore) CommitTx(
	ctx context.Context,
	tx pgx.Tx,
	ref teamquota.OperationRef,
) error {
	copied := ref
	s.committed = &copied
	return s.PermissiveCapacityStore.CommitTx(ctx, tx, ref)
}

func (s *recordingControlPlaneStore) BeginReleaseTx(
	ctx context.Context,
	tx pgx.Tx,
	request teamquota.ReleaseRequest,
) (*teamquota.Reservation, error) {
	copied := request
	copied.Target = request.Target.Clone()
	s.releaseRequest = &copied
	return s.PermissiveCapacityStore.BeginReleaseTx(ctx, tx, request)
}

func (s *recordingControlPlaneStore) ConfirmReleaseTx(
	ctx context.Context,
	tx pgx.Tx,
	ref teamquota.OperationRef,
	runtime teamquota.RuntimeRef,
) error {
	copied := ref
	s.confirmed = &copied
	return s.PermissiveCapacityStore.ConfirmReleaseTx(ctx, tx, ref, runtime)
}

func TestControlPlaneObjectQuotaTransitionsUseAggregateCount(t *testing.T) {
	store := newRecordingControlPlaneStore()
	owner := teamquota.ControlPlaneObjectOwner(
		" team-1 ",
		" "+teamquota.ControlPlaneOwnerKindAPIKey+" ",
		" key-1 ",
	)

	reserveRef, err := teamquota.ReserveControlPlaneObjectTargetTx(
		context.Background(),
		store,
		nil,
		owner,
		"create_api_key",
		1,
	)
	if err != nil {
		t.Fatalf("ReserveControlPlaneObjectTargetTx() error = %v", err)
	}
	if store.reserveRequest == nil {
		t.Fatal("reserve request was not recorded")
	}
	if got := store.reserveRequest.Target[teamquota.KeyControlPlaneObjectCount]; got != 1 {
		t.Fatalf("control-plane target = %d, want 1", got)
	}
	if len(store.reserveRequest.Target) != 1 {
		t.Fatalf("reserve target = %#v, want one aggregate key", store.reserveRequest.Target)
	}
	if got := store.reserveRequest.Owner; got.TeamID != "team-1" || got.Kind != "api_key" || got.ID != "key-1" {
		t.Fatalf("normalized owner = %#v", got)
	}
	if err := teamquota.CommitControlPlaneObjectTargetTx(context.Background(), store, nil, reserveRef); err != nil {
		t.Fatalf("CommitControlPlaneObjectTargetTx() error = %v", err)
	}
	if store.committed == nil || *store.committed != reserveRef {
		t.Fatalf("committed ref = %#v, want %#v", store.committed, reserveRef)
	}

	releaseRef, err := teamquota.BeginControlPlaneObjectReleaseTx(
		context.Background(),
		store,
		nil,
		owner,
		"delete_api_key",
		0,
	)
	if err != nil {
		t.Fatalf("BeginControlPlaneObjectReleaseTx() error = %v", err)
	}
	if store.releaseRequest == nil || store.releaseRequest.Target[teamquota.KeyControlPlaneObjectCount] != 0 {
		t.Fatalf("release request = %#v, want zero aggregate target", store.releaseRequest)
	}
	if err := teamquota.ConfirmControlPlaneObjectReleaseTx(context.Background(), store, nil, releaseRef); err != nil {
		t.Fatalf("ConfirmControlPlaneObjectReleaseTx() error = %v", err)
	}
	if store.confirmed == nil || *store.confirmed != releaseRef {
		t.Fatalf("confirmed ref = %#v, want %#v", store.confirmed, releaseRef)
	}
}

func TestControlPlaneObjectQuotaFailsClosedWithoutStore(t *testing.T) {
	_, err := teamquota.ReserveControlPlaneObjectTargetTx(
		context.Background(),
		nil,
		nil,
		teamquota.ControlPlaneObjectOwner("team-1", teamquota.ControlPlaneOwnerKindAPIKey, "key-1"),
		"create_api_key",
		1,
	)
	if !teamquota.IsUnavailable(err) {
		t.Fatalf("reserve error = %v, want unavailable", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected wrapped context error: %v", err)
	}
}
