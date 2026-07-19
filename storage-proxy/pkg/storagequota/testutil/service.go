// Package testutil provides explicit unit-test dependencies for storage quota
// consumers. Production runtimes must use the PostgreSQL TeamQuota repository.
package testutil

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
)

type permissiveStore struct {
	state *memoryState
}

type limitedStore struct {
	permissiveStore
	key   teamquota.Key
	limit int64
}

type memoryState struct {
	mu        sync.Mutex
	committed map[string]teamquota.Values
	pending   map[string]teamquota.Values
}

func newMemoryState() *memoryState {
	return &memoryState{
		committed: make(map[string]teamquota.Values),
		pending:   make(map[string]teamquota.Values),
	}
}

// NewService returns a permissive in-memory coordinator for tests whose quota
// behavior is not under test.
func NewService(clusterID string) *storagequota.Service {
	return storagequota.New(permissiveStore{state: newMemoryState()}, clusterID)
}

// NewLimitedService returns a coordinator that rejects targets above one
// selected key's limit while leaving other keys permissive.
func NewLimitedService(clusterID string, key teamquota.Key, limit int64) *storagequota.Service {
	return storagequota.New(limitedStore{
		permissiveStore: permissiveStore{state: newMemoryState()},
		key:             key,
		limit:           limit,
	}, clusterID)
}

func (s limitedStore) EffectivePolicy(_ context.Context, teamID string, key teamquota.Key) (*teamquota.Policy, error) {
	limit := int64(^uint64(0) >> 1)
	if key == s.key {
		limit = s.limit
	}
	return &teamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   teamquota.KindCapacity,
		Limit:  limit,
	}, nil
}

func (s limitedStore) ReserveDelta(_ context.Context, request teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	return s.reserveDelta(request, s.key, s.limit)
}

func (permissiveStore) EffectivePolicy(_ context.Context, teamID string, key teamquota.Key) (*teamquota.Policy, error) {
	return &teamquota.Policy{
		TeamID: teamID,
		Key:    key,
		Kind:   teamquota.KindCapacity,
		Limit:  int64(^uint64(0) >> 1),
	}, nil
}

func (s permissiveStore) ReserveDelta(_ context.Context, request teamquota.DeltaRequest) (*teamquota.Reservation, error) {
	return s.reserveDelta(request, "", 0)
}

func (s permissiveStore) reserveDelta(
	request teamquota.DeltaRequest,
	limitedKey teamquota.Key,
	limit int64,
) (*teamquota.Reservation, error) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	key := testOwnerKey(request.Owner)
	committed := s.state.committed[key].Clone()
	if committed == nil {
		committed = request.Observed.Clone()
	}
	if committed == nil {
		committed = make(teamquota.Values, len(request.Delta))
	}
	target := committed.Clone()
	for quotaKey, delta := range request.Delta {
		target[quotaKey] += delta
	}
	if limitedKey != "" && target[limitedKey] > limit {
		return nil, &teamquota.ExceededError{
			TeamID:    request.Owner.TeamID,
			Key:       limitedKey,
			Limit:     limit,
			Committed: committed[limitedKey],
			Requested: request.Delta[limitedKey],
		}
	}
	s.state.pending[key] = target.Clone()
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Committed: committed,
		Target:    target,
		Reserved:  request.Delta.Clone(),
	}, nil
}

func (s permissiveStore) Commit(_ context.Context, ref teamquota.OperationRef) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	key := testOwnerKey(ref.Owner)
	if pending := s.state.pending[key]; pending != nil {
		s.state.committed[key] = pending.Clone()
		delete(s.state.pending, key)
	}
	return nil
}

func (s permissiveStore) CommitExact(
	_ context.Context,
	ref teamquota.OperationRef,
	exact teamquota.Values,
) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	key := testOwnerKey(ref.Owner)
	s.state.committed[key] = exact.Clone()
	delete(s.state.pending, key)
	return nil
}

func (s permissiveStore) Abort(_ context.Context, ref teamquota.OperationRef, _ string) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	delete(s.state.pending, testOwnerKey(ref.Owner))
	return nil
}

func (s permissiveStore) BeginRelease(_ context.Context, request teamquota.ReleaseRequest) (*teamquota.Reservation, error) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.pending[testOwnerKey(request.Owner)] = request.Target.Clone()
	return &teamquota.Reservation{
		Owner:     request.Owner,
		Operation: request.Operation,
		Target:    request.Target,
	}, nil
}

func (s permissiveStore) ConfirmRelease(ctx context.Context, ref teamquota.OperationRef, _ teamquota.RuntimeRef) error {
	return s.Commit(ctx, ref)
}

func (s permissiveStore) ConfirmReleaseTx(ctx context.Context, _ pgx.Tx, ref teamquota.OperationRef, _ teamquota.RuntimeRef) error {
	return s.Commit(ctx, ref)
}

func (s permissiveStore) ReconcileTargetIfRevision(
	_ context.Context,
	owner teamquota.Owner,
	target teamquota.Values,
	_ teamquota.RuntimeRef,
	expectedRevision int64,
) (bool, error) {
	if expectedRevision != 0 {
		return false, nil
	}
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.committed[testOwnerKey(owner)] = target.Clone()
	return true, nil
}

func (permissiveStore) GetRecoveryAllocation(context.Context, teamquota.Owner) (*teamquota.RecoveryAllocation, error) {
	return nil, nil
}

func testOwnerKey(owner teamquota.Owner) string {
	return owner.TeamID + "\x00" + owner.Kind + "\x00" + owner.ID
}
