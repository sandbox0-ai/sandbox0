package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type memorySandboxStore struct {
	mu                 sync.Mutex
	records            map[string]*sandboxstore.SandboxRecord
	lifecycleTxns      map[string]*sandboxstore.SandboxLifecycleTxn
	rootFSFilesystems  map[string]*sandboxstore.RootFSFilesystem
	rootFSSnapshots    map[string]*sandboxstore.RootFSSnapshot
	runtimeSlots       map[string]*sandboxstore.RuntimeSlot
	credentialBindings map[string][]egressauthstore.CredentialBinding
	credentialDigests  map[string]string
	deletes            []string
	saves              int
	pauses             int
	lockCalls          int
	activeTxnGets      int
	lockStarted        chan struct{}
	blockLock          chan struct{}
}

func (s *memorySandboxStore) GetRuntimeSlotBySandboxID(_ context.Context, sandboxID string) (*sandboxstore.RuntimeSlot, error) {
	if s == nil {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := s.runtimeSlots[sandboxID]
	if slot == nil {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	clone := *slot
	clone.CommandReadyDigest = append([]byte(nil), slot.CommandReadyDigest...)
	return &clone, nil
}

func (s *memorySandboxStore) GetNomadSandboxCredentialBindings(
	_ context.Context,
	_, sandboxID string,
) (*sandboxstore.NomadSandboxCredentialBindings, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bindings := credentialbinding.CloneStore(s.credentialBindings[sandboxID])
	digest := s.credentialDigests[sandboxID]
	if digest == "" {
		digest = credentialbinding.DigestStore(bindings)
	}
	return &sandboxstore.NomadSandboxCredentialBindings{Digest: digest, Bindings: bindings}, nil
}

type memorySandboxStoreTx struct {
	store *memorySandboxStore
}

func (t memorySandboxStoreTx) SaveSandbox(_ context.Context, record *sandboxstore.SandboxRecord) error {
	return t.store.UpsertSandbox(context.Background(), record)
}

func (s *memorySandboxStore) UpsertSandbox(_ context.Context, record *sandboxstore.SandboxRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	if existing := s.records[record.ID]; existing != nil &&
		(existing.DesiredState == sandboxstore.SandboxDesiredStateTerminating || existing.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !existing.DeletedAt.IsZero()) {
		return nil
	}
	clone := cloneSandboxRecord(record)
	if existing := s.records[record.ID]; existing != nil && clone.HotClaimCompletedAt.IsZero() {
		clone.HotClaimCompletedAt = existing.HotClaimCompletedAt
	}
	s.records[record.ID] = clone
	return nil
}

func (s *memorySandboxStore) GetSandbox(_ context.Context, sandboxID string) (*sandboxstore.SandboxRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		return nil, nil
	}
	return cloneSandboxRecord(s.records[sandboxID]), nil
}

func (s *memorySandboxStore) ListSandboxes(_ context.Context, req *sandboxstore.ListSandboxesRequest) ([]*sandboxstore.SandboxRecord, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		return nil, nil
	}
	var records []*sandboxstore.SandboxRecord
	for _, record := range s.records {
		if record == nil {
			continue
		}
		if req != nil {
			if req.TeamID != "" && record.TeamID != req.TeamID {
				continue
			}
			if req.TemplateID != "" && record.TemplateID != req.TemplateID {
				continue
			}
		}
		records = append(records, cloneSandboxRecord(record))
	}
	return records, nil
}

func (s *memorySandboxStore) ListHardExpiredSandboxes(context.Context, time.Time, int) ([]*sandboxstore.SandboxRecord, error) {
	return nil, nil
}

func (s *memorySandboxStore) ListActiveLifecycleTxns(_ context.Context, kind string, limit int) ([]*sandboxstore.SandboxLifecycleTxn, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecycleTxns == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = len(s.lifecycleTxns)
	}
	txns := make([]*sandboxstore.SandboxLifecycleTxn, 0, len(s.lifecycleTxns))
	for _, txn := range s.lifecycleTxns {
		if txn == nil || txn.Kind != kind || !sandboxLifecyclePhaseActive(txn.Phase) {
			continue
		}
		txns = append(txns, sandboxstore.CloneSandboxLifecycleTxn(txn))
		if len(txns) >= limit {
			break
		}
	}
	return txns, nil
}

func (s *memorySandboxStore) ListPendingRuntimeRecoverySandboxIDs(_ context.Context, limit int) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = len(s.records)
	}
	var sandboxIDs []string
	for sandboxID, record := range s.records {
		if record == nil || record.DesiredState != sandboxstore.SandboxDesiredStatePaused || !record.DeletedAt.IsZero() {
			continue
		}
		var latest *sandboxstore.SandboxLifecycleTxn
		for _, txn := range s.lifecycleTxns {
			if txn == nil || txn.SandboxID != sandboxID || txn.Phase != sandboxstore.SandboxLifecyclePhaseCommitted {
				continue
			}
			if latest == nil || txn.Epoch > latest.Epoch {
				latest = txn
			}
		}
		if latest == nil || latest.Kind != sandboxstore.SandboxLifecycleKindPause ||
			!sandboxLifecycleSourceReconstructsRuntime(latest.Source) {
			continue
		}
		sandboxIDs = append(sandboxIDs, sandboxID)
		if len(sandboxIDs) >= limit {
			break
		}
	}
	return sandboxIDs, nil
}

func (s *memorySandboxStore) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*sandboxstore.SandboxLifecycleTxn, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTxnGets++
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (s *memorySandboxStore) MarkSandboxDeleted(_ context.Context, sandboxID string, deletedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.records == nil {
		s.records = make(map[string]*sandboxstore.SandboxRecord)
	}
	record := s.records[sandboxID]
	if record == nil {
		record = &sandboxstore.SandboxRecord{ID: sandboxID}
		s.records[sandboxID] = record
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateDeleted
	record.DeletedAt = deletedAt
	record.RuntimeID = ""
	record.RuntimeNamespace = ""
	for _, txn := range s.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
			txn.Error = "sandbox deleted"
			txn.AbortedAt = deletedAt
		}
	}
	delete(s.rootFSFilesystems, sandboxID)
	s.deletes = append(s.deletes, sandboxID)
	return nil
}

func (s *memorySandboxStore) WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error {
	if s == nil {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	s.mu.Lock()
	s.lockCalls++
	if s.lockStarted != nil {
		select {
		case s.lockStarted <- struct{}{}:
		default:
		}
	}
	blockLock := s.blockLock
	record := cloneSandboxRecord(s.records[sandboxID])
	s.mu.Unlock()
	if blockLock != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blockLock:
		}
	}
	if record == nil {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	snapshot := s.snapshot()
	if err := fn(ctx, memorySandboxStoreTx{store: s}, record); err != nil {
		s.restore(snapshot)
		return err
	}
	return nil
}

type memorySandboxStoreSnapshot struct {
	records            map[string]*sandboxstore.SandboxRecord
	lifecycleTxns      map[string]*sandboxstore.SandboxLifecycleTxn
	rootFSFilesystems  map[string]*sandboxstore.RootFSFilesystem
	credentialBindings map[string][]egressauthstore.CredentialBinding
	credentialDigests  map[string]string
}

func (s *memorySandboxStore) snapshot() memorySandboxStoreSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return memorySandboxStoreSnapshot{
		records:            cloneSandboxRecordMap(s.records),
		lifecycleTxns:      cloneSandboxLifecycleTxnMap(s.lifecycleTxns),
		rootFSFilesystems:  cloneRootFSFilesystemMap(s.rootFSFilesystems),
		credentialBindings: cloneMemoryCredentialBindings(s.credentialBindings),
		credentialDigests:  cloneCredentialDigestMap(s.credentialDigests),
	}
}

func (s *memorySandboxStore) restore(snapshot memorySandboxStoreSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = snapshot.records
	s.lifecycleTxns = snapshot.lifecycleTxns
	s.rootFSFilesystems = snapshot.rootFSFilesystems
	s.credentialBindings = snapshot.credentialBindings
	s.credentialDigests = snapshot.credentialDigests
}

func (t memorySandboxStoreTx) ReplaceNomadSandboxCredentialBindings(
	_ context.Context,
	_, sandboxID string,
	bindings []egressauthstore.CredentialBinding,
) (string, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.credentialBindings == nil {
		t.store.credentialBindings = make(map[string][]egressauthstore.CredentialBinding)
	}
	if t.store.credentialDigests == nil {
		t.store.credentialDigests = make(map[string]string)
	}
	t.store.credentialBindings[sandboxID] = credentialbinding.CloneStore(bindings)
	digest := credentialbinding.DigestStore(bindings)
	t.store.credentialDigests[sandboxID] = digest
	return digest, nil
}

func cloneMemoryCredentialBindings(
	in map[string][]egressauthstore.CredentialBinding,
) map[string][]egressauthstore.CredentialBinding {
	if in == nil {
		return nil
	}
	out := make(map[string][]egressauthstore.CredentialBinding, len(in))
	for key, bindings := range in {
		out[key] = credentialbinding.CloneStore(bindings)
	}
	return out
}

func cloneCredentialDigestMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (s *memorySandboxStore) setSandboxDesiredState(sandboxID, desiredState string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record := s.records[sandboxID]; record != nil {
		record.DesiredState = desiredState
	}
}

func (t memorySandboxStoreTx) SaveRuntime(_ context.Context, sandboxID, runtimeNamespace, runtimeID string, generation int64, expiresAt, hardExpiresAt time.Time, ownerKind string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.RuntimeNamespace = runtimeNamespace
	record.RuntimeID = runtimeID
	record.DesiredState = sandboxstore.SandboxDesiredStateActive
	record.RuntimeGeneration = generation
	record.ExpiresAt = expiresAt
	record.HardExpiresAt = hardExpiresAt
	if ownerKind != "" {
		record.OwnerKind = ownerKind
	}
	t.store.saves++
	return nil
}

func (t memorySandboxStoreTx) MarkHotClaimCompleted(_ context.Context, sandboxID string, completedAt time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || record.DesiredState == sandboxstore.SandboxDesiredStateDeleted || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.HotClaimCompletedAt = completedAt
	return nil
}

func (t memorySandboxStoreTx) MarkRuntimePaused(_ context.Context, sandboxID string, generation int64, _ time.Time) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || record.DesiredState == sandboxstore.SandboxDesiredStateTerminating || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.RuntimeNamespace = ""
	record.RuntimeID = ""
	record.DesiredState = sandboxstore.SandboxDesiredStatePaused
	if record.RuntimeGeneration < generation {
		record.RuntimeGeneration = generation
	}
	t.store.pauses++
	return nil
}

func (t memorySandboxStoreTx) MarkRuntimeTerminating(_ context.Context, sandboxID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	record := t.store.records[sandboxID]
	if record == nil || !record.DeletedAt.IsZero() {
		return sandboxstore.ErrSandboxRecordNotFound
	}
	record.DesiredState = sandboxstore.SandboxDesiredStateTerminating
	return nil
}

func (t memorySandboxStoreTx) GetActiveLifecycleTxn(_ context.Context, sandboxID string) (*sandboxstore.SandboxLifecycleTxn, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	for _, txn := range t.store.lifecycleTxns {
		if txn != nil && txn.SandboxID == sandboxID && sandboxLifecyclePhaseActive(txn.Phase) {
			return sandboxstore.CloneSandboxLifecycleTxn(txn), nil
		}
	}
	return nil, nil
}

func (t memorySandboxStoreTx) BeginLifecycleTxn(_ context.Context, txn *sandboxstore.SandboxLifecycleTxn) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.lifecycleTxns == nil {
		t.store.lifecycleTxns = make(map[string]*sandboxstore.SandboxLifecycleTxn)
	}
	record := t.store.records[txn.SandboxID]
	record.LifecycleEpoch++
	txn.Epoch = record.LifecycleEpoch
	if txn.Phase == "" {
		txn.Phase = sandboxstore.SandboxLifecyclePhasePreparing
	}
	if txn.Source == "" {
		txn.Source = sandboxstore.SandboxLifecycleSourceManual
	}
	t.store.lifecycleTxns[txn.ID] = sandboxstore.CloneSandboxLifecycleTxn(txn)
	return nil
}

func (t memorySandboxStoreTx) SetLifecycleTxnRuntime(_ context.Context, txnID, namespace, podName string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		txn.ToRuntimeNamespace = namespace
		txn.ToRuntimeID = podName
	}
	return nil
}

func (t memorySandboxStoreTx) UpdateLifecycleTxnPhase(_ context.Context, txnID, phase string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.Phase = phase
	}
	return nil
}

func (t memorySandboxStoreTx) SetLifecycleTxnPreparedGeneration(_ context.Context, txnID, preparedGenerationID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.PreparedGenerationID = preparedGenerationID
	}
	return nil
}

func (t memorySandboxStoreTx) RequestLifecycleTxnCancel(_ context.Context, txnID, reason string) (bool, error) {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	txn := t.store.lifecycleTxns[txnID]
	if !sandboxLifecycleTxnCancelableAutoPause(txn) {
		return false, nil
	}
	if txn.CancelRequestedAt.IsZero() {
		txn.CancelRequestedAt = time.Now()
	}
	if txn.CancelReason == "" {
		txn.CancelReason = reason
	}
	return true, nil
}

func (t memorySandboxStoreTx) CommitLifecycleTxn(_ context.Context, txnID, preparedGenerationID string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		if sandboxLifecycleTxnCancelRequested(txn) {
			return fmt.Errorf("active lifecycle txn %s not found", txnID)
		}
		txn.Phase = sandboxstore.SandboxLifecyclePhaseCommitted
		txn.PreparedGenerationID = preparedGenerationID
	}
	return nil
}

func (t memorySandboxStoreTx) AbortLifecycleTxn(_ context.Context, txnID, reason string) error {
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if txn := t.store.lifecycleTxns[txnID]; txn != nil && sandboxLifecyclePhaseActive(txn.Phase) {
		txn.Phase = sandboxstore.SandboxLifecyclePhaseAborted
		txn.Error = reason
	}
	return nil
}

func cloneSandboxRecord(record *sandboxstore.SandboxRecord) *sandboxstore.SandboxRecord {
	if record == nil {
		return nil
	}
	clone := *record
	clone.Config = *CloneSandboxConfig(&record.Config)
	return &clone
}

func cloneSandboxRecordMap(records map[string]*sandboxstore.SandboxRecord) map[string]*sandboxstore.SandboxRecord {
	if records == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.SandboxRecord, len(records))
	for key, record := range records {
		cloned[key] = cloneSandboxRecord(record)
	}
	return cloned
}

func cloneSandboxLifecycleTxnMap(txns map[string]*sandboxstore.SandboxLifecycleTxn) map[string]*sandboxstore.SandboxLifecycleTxn {
	if txns == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.SandboxLifecycleTxn, len(txns))
	for key, txn := range txns {
		cloned[key] = sandboxstore.CloneSandboxLifecycleTxn(txn)
	}
	return cloned
}

func sandboxLifecyclePhaseActive(phase string) bool {
	return phase != sandboxstore.SandboxLifecyclePhaseCommitted && phase != sandboxstore.SandboxLifecyclePhaseAborted
}

func cloneRootFSFilesystemMap(filesystems map[string]*sandboxstore.RootFSFilesystem) map[string]*sandboxstore.RootFSFilesystem {
	if filesystems == nil {
		return nil
	}
	cloned := make(map[string]*sandboxstore.RootFSFilesystem, len(filesystems))
	for key, filesystem := range filesystems {
		cloned[key] = cloneRootFSFilesystemForTest(filesystem)
	}
	return cloned
}

func cloneRootFSFilesystemForTest(filesystem *sandboxstore.RootFSFilesystem) *sandboxstore.RootFSFilesystem {
	if filesystem == nil {
		return nil
	}
	clone := *filesystem
	return &clone
}
func (s *memorySandboxStore) CreateRootFSSnapshot(_ context.Context, req *sandboxstore.CreateRootFSSnapshotRequest) (*sandboxstore.RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootFSSnapshots == nil {
		s.rootFSSnapshots = make(map[string]*sandboxstore.RootFSSnapshot)
	}
	filesystem := s.rootFSFilesystems[req.SandboxID]
	if filesystem == nil || filesystem.HeadGenerationID == "" {
		return nil, sandboxstore.ErrRootFSFilesystemNotFound
	}
	record := s.records[req.SandboxID]
	if record == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	snapshot := &sandboxstore.RootFSSnapshot{
		ID: req.SnapshotID, FilesystemID: filesystem.ID, TeamID: record.TeamID,
		SourceSandboxID: req.SandboxID, HeadGenerationID: filesystem.HeadGenerationID,
		BaseArtifactDigest: filesystem.BaseArtifactDigest,
		FormatGeneration:   filesystem.FormatGeneration, SourceOCIDigest: filesystem.BaseImageDigest,
		Name: req.Name, Description: req.Description,
		CreatedAt: time.Now().UTC(), ExpiresAt: req.ExpiresAt,
	}
	s.rootFSSnapshots[snapshot.ID] = cloneRootFSSnapshotForTest(snapshot)
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) ListRootFSSnapshots(_ context.Context, req *sandboxstore.ListRootFSSnapshotsRequest) ([]*sandboxstore.RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snapshots []*sandboxstore.RootFSSnapshot
	for _, snapshot := range s.rootFSSnapshots {
		if snapshot == nil || snapshot.SourceSandboxID != req.SandboxID {
			continue
		}
		if req.TeamID != "" && snapshot.TeamID != req.TeamID {
			continue
		}
		snapshots = append(snapshots, cloneRootFSSnapshotForTest(snapshot))
	}
	return snapshots, nil
}

func (s *memorySandboxStore) GetRootFSSnapshot(_ context.Context, snapshotID, teamID string) (*sandboxstore.RootFSSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	return cloneRootFSSnapshotForTest(snapshot), nil
}

func (s *memorySandboxStore) DeleteRootFSSnapshot(_ context.Context, snapshotID, teamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[snapshotID]
	if snapshot == nil || (teamID != "" && snapshot.TeamID != teamID) {
		return sandboxstore.ErrRootFSSnapshotNotFound
	}
	delete(s.rootFSSnapshots, snapshotID)
	return nil
}

func (s *memorySandboxStore) ForkRootFSFilesystem(_ context.Context, req *sandboxstore.ForkRootFSFilesystemRequest) (*sandboxstore.RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	source := s.rootFSFilesystems[req.SourceSandboxID]
	if source == nil || source.HeadGenerationID == "" {
		return nil, sandboxstore.ErrRootFSFilesystemNotFound
	}
	target := s.records[req.TargetSandboxID]
	if target == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	targetTeamID := req.TargetTeamID
	if targetTeamID == "" {
		targetTeamID = target.TeamID
	}
	if s.rootFSFilesystems == nil {
		s.rootFSFilesystems = make(map[string]*sandboxstore.RootFSFilesystem)
	}
	filesystem := &sandboxstore.RootFSFilesystem{
		ID:                 req.TargetSandboxID,
		TeamID:             targetTeamID,
		SourceFilesystemID: source.ID,
		HeadGenerationID:   source.HeadGenerationID,
		WriterEpoch:        source.WriterEpoch,
		BaseArtifactDigest: source.BaseArtifactDigest,
		FormatGeneration:   source.FormatGeneration,
		BaseImageRef:       source.BaseImageRef,
		BaseImageDigest:    source.BaseImageDigest,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	s.rootFSFilesystems[filesystem.ID] = cloneRootFSFilesystemForTest(filesystem)
	return cloneRootFSFilesystemForTest(filesystem), nil
}

func (s *memorySandboxStore) RestoreRootFSFromSnapshot(_ context.Context, req *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.rootFSSnapshots[req.SnapshotID]
	if snapshot == nil || (req.TeamID != "" && snapshot.TeamID != req.TeamID) {
		return nil, sandboxstore.ErrRootFSSnapshotNotFound
	}
	target := s.records[req.SandboxID]
	if target == nil {
		return nil, sandboxstore.ErrSandboxRecordNotFound
	}
	filesystem := &sandboxstore.RootFSFilesystem{
		ID:                 req.SandboxID,
		TeamID:             target.TeamID,
		SourceFilesystemID: snapshot.FilesystemID,
		HeadGenerationID:   snapshot.HeadGenerationID,
		BaseArtifactDigest: snapshot.BaseArtifactDigest,
		FormatGeneration:   snapshot.FormatGeneration,
		BaseImageDigest:    snapshot.SourceOCIDigest,
		CreatedAt:          time.Now().UTC(),
		UpdatedAt:          time.Now().UTC(),
	}
	if s.rootFSFilesystems == nil {
		s.rootFSFilesystems = make(map[string]*sandboxstore.RootFSFilesystem)
	}
	s.rootFSFilesystems[filesystem.ID] = cloneRootFSFilesystemForTest(filesystem)
	return cloneRootFSFilesystemForTest(filesystem), nil
}

func cloneRootFSSnapshotForTest(snapshot *sandboxstore.RootFSSnapshot) *sandboxstore.RootFSSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	return &clone
}
