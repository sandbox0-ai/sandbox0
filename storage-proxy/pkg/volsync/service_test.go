package volsync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sirupsen/logrus"
)

type fakeRepo struct {
	volumes   map[string]*db.SandboxVolume
	replicas  map[string]*db.SyncReplica
	journal   []*db.SyncJournalEntry
	conflicts []*db.SyncConflict
	requests  map[string]*db.SyncRequest
	retention map[string]*db.SyncRetentionState
	policies  map[string]*db.SyncNamespacePolicy
	nextSeq   int64
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		volumes:   make(map[string]*db.SandboxVolume),
		replicas:  make(map[string]*db.SyncReplica),
		requests:  make(map[string]*db.SyncRequest),
		retention: make(map[string]*db.SyncRetentionState),
		policies:  make(map[string]*db.SyncNamespacePolicy),
	}
}

func replicaKey(volumeID, replicaID string) string {
	return volumeID + ":" + replicaID
}

func requestKey(volumeID, replicaID, requestID string) string {
	return volumeID + ":" + replicaID + ":" + requestID
}

func (r *fakeRepo) GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error) {
	volume, ok := r.volumes[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return volume, nil
}

func (r *fakeRepo) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

func (r *fakeRepo) UpsertSyncReplica(ctx context.Context, replica *db.SyncReplica) error {
	return r.UpsertSyncReplicaTx(ctx, nil, replica)
}

func (r *fakeRepo) UpsertSyncReplicaTx(ctx context.Context, tx db.DB, replica *db.SyncReplica) error {
	replica.Capabilities = pathnorm.NormalizeFilesystemCapabilities(replica.Platform, replica.CaseSensitive, &replica.Capabilities)
	replica.CaseSensitive = replica.Capabilities.CaseSensitive
	key := replicaKey(replica.VolumeID, replica.ID)
	if existing, ok := r.replicas[key]; ok {
		existing.TeamID = replica.TeamID
		existing.DisplayName = replica.DisplayName
		existing.Platform = replica.Platform
		existing.RootPath = replica.RootPath
		existing.CaseSensitive = replica.CaseSensitive
		existing.Capabilities = replica.Capabilities
		existing.LastSeenAt = replica.LastSeenAt
		existing.UpdatedAt = replica.UpdatedAt
		return nil
	}
	clone := *replica
	r.replicas[key] = &clone
	return nil
}

func (r *fakeRepo) GetSyncNamespacePolicy(ctx context.Context, volumeID string) (*db.SyncNamespacePolicy, error) {
	policy, ok := r.policies[volumeID]
	if !ok {
		return nil, db.ErrNotFound
	}
	clone := *policy
	return &clone, nil
}

func (r *fakeRepo) GetSyncNamespacePolicyForUpdateTx(ctx context.Context, tx pgx.Tx, volumeID string) (*db.SyncNamespacePolicy, error) {
	return r.GetSyncNamespacePolicy(ctx, volumeID)
}

func (r *fakeRepo) UpsertSyncNamespacePolicyTx(ctx context.Context, tx pgx.Tx, policy *db.SyncNamespacePolicy) error {
	clone := *policy
	r.policies[policy.VolumeID] = &clone
	return nil
}

func (r *fakeRepo) GetSyncReplica(ctx context.Context, volumeID, replicaID string) (*db.SyncReplica, error) {
	replica, ok := r.replicas[replicaKey(volumeID, replicaID)]
	if !ok {
		return nil, db.ErrNotFound
	}
	clone := *replica
	clone.Capabilities = pathnorm.NormalizeFilesystemCapabilities(clone.Platform, clone.CaseSensitive, &clone.Capabilities)
	clone.CaseSensitive = clone.Capabilities.CaseSensitive
	return &clone, nil
}

func (r *fakeRepo) GetSyncReplicaForUpdate(ctx context.Context, tx pgx.Tx, volumeID, replicaID string) (*db.SyncReplica, error) {
	return r.GetSyncReplica(ctx, volumeID, replicaID)
}

func (r *fakeRepo) TouchSyncReplicaTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID string, lastSeenAt time.Time) error {
	replica, ok := r.replicas[replicaKey(volumeID, replicaID)]
	if !ok {
		return db.ErrNotFound
	}
	replica.LastSeenAt = lastSeenAt
	return nil
}

func (r *fakeRepo) UpdateSyncReplicaCursorTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID string, lastAppliedSeq int64, lastSeenAt time.Time) error {
	replica, ok := r.replicas[replicaKey(volumeID, replicaID)]
	if !ok {
		return db.ErrNotFound
	}
	replica.LastAppliedSeq = lastAppliedSeq
	replica.LastSeenAt = lastSeenAt
	return nil
}

func (r *fakeRepo) GetSyncHead(ctx context.Context, volumeID string) (int64, error) {
	var head int64
	for _, entry := range r.journal {
		if entry.VolumeID == volumeID && entry.Seq > head {
			head = entry.Seq
		}
	}
	return head, nil
}

func (r *fakeRepo) GetSyncRetentionState(ctx context.Context, volumeID string) (*db.SyncRetentionState, error) {
	state, ok := r.retention[volumeID]
	if !ok {
		return nil, db.ErrNotFound
	}
	clone := *state
	return &clone, nil
}

func (r *fakeRepo) GetSyncRetentionStateForUpdateTx(ctx context.Context, tx pgx.Tx, volumeID string) (*db.SyncRetentionState, error) {
	return r.GetSyncRetentionState(ctx, volumeID)
}

func (r *fakeRepo) ListSyncJournalEntries(ctx context.Context, volumeID string, afterSeq int64, limit int) ([]*db.SyncJournalEntry, error) {
	var entries []*db.SyncJournalEntry
	for _, entry := range r.journal {
		if entry.VolumeID == volumeID && entry.Seq > afterSeq {
			clone := *entry
			entries = append(entries, &clone)
		}
	}
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func (r *fakeRepo) DeleteSyncJournalEntriesUpToTx(ctx context.Context, tx pgx.Tx, volumeID string, maxSeq int64) (int64, error) {
	filtered := r.journal[:0]
	var deleted int64
	for _, entry := range r.journal {
		if entry.VolumeID == volumeID && entry.Seq <= maxSeq {
			deleted++
			continue
		}
		filtered = append(filtered, entry)
	}
	r.journal = filtered
	return deleted, nil
}

func (r *fakeRepo) CreateSyncJournalEntryTx(ctx context.Context, tx pgx.Tx, entry *db.SyncJournalEntry) error {
	r.nextSeq++
	clone := *entry
	clone.Seq = r.nextSeq
	if clone.CreatedAt.IsZero() {
		clone.CreatedAt = time.Now().UTC()
	}
	r.journal = append(r.journal, &clone)
	entry.Seq = clone.Seq
	entry.CreatedAt = clone.CreatedAt
	return nil
}

func (r *fakeRepo) CreateSyncConflictTx(ctx context.Context, tx pgx.Tx, conflict *db.SyncConflict) error {
	clone := *conflict
	r.conflicts = append(r.conflicts, &clone)
	return nil
}

func (r *fakeRepo) ListSyncConflicts(ctx context.Context, volumeID, status string, limit int) ([]*db.SyncConflict, error) {
	var conflicts []*db.SyncConflict
	for _, conflict := range r.conflicts {
		if conflict.VolumeID != volumeID {
			continue
		}
		if status != "" && conflict.Status != status {
			continue
		}
		clone := *conflict
		conflicts = append(conflicts, &clone)
	}
	if limit > 0 && len(conflicts) > limit {
		conflicts = conflicts[:limit]
	}
	return conflicts, nil
}

func (r *fakeRepo) GetSyncConflict(ctx context.Context, volumeID, conflictID string) (*db.SyncConflict, error) {
	for _, conflict := range r.conflicts {
		if conflict.VolumeID == volumeID && conflict.ID == conflictID {
			clone := *conflict
			return &clone, nil
		}
	}
	return nil, db.ErrNotFound
}

func (r *fakeRepo) UpdateSyncConflictTx(ctx context.Context, tx pgx.Tx, volumeID, conflictID, status string, metadata *json.RawMessage) error {
	for _, conflict := range r.conflicts {
		if conflict.VolumeID == volumeID && conflict.ID == conflictID {
			conflict.Status = status
			conflict.UpdatedAt = time.Now().UTC()
			conflict.Metadata = metadata
			return nil
		}
	}
	return db.ErrNotFound
}

func (r *fakeRepo) GetSyncRequestTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID, requestID string) (*db.SyncRequest, error) {
	request, ok := r.requests[requestKey(volumeID, replicaID, requestID)]
	if !ok {
		return nil, db.ErrNotFound
	}
	clone := *request
	return &clone, nil
}

func (r *fakeRepo) CreateSyncRequestTx(ctx context.Context, tx pgx.Tx, request *db.SyncRequest) error {
	clone := *request
	r.requests[requestKey(request.VolumeID, request.ReplicaID, request.RequestID)] = &clone
	return nil
}

func (r *fakeRepo) UpsertSyncRetentionStateTx(ctx context.Context, tx pgx.Tx, state *db.SyncRetentionState) error {
	existing, ok := r.retention[state.VolumeID]
	if ok && existing.CompactedThroughSeq > state.CompactedThroughSeq {
		clone := *existing
		r.retention[state.VolumeID] = &clone
		return nil
	}
	clone := *state
	r.retention[state.VolumeID] = &clone
	return nil
}

func (r *fakeRepo) GetLatestSyncJournalEntryByNormalizedPath(ctx context.Context, volumeID, normalizedPath string) (*db.SyncJournalEntry, error) {
	return r.getLatest(volumeID, normalizedPath)
}

func (r *fakeRepo) GetLatestSyncJournalEntryByNormalizedPathTx(ctx context.Context, tx pgx.Tx, volumeID, normalizedPath string) (*db.SyncJournalEntry, error) {
	return r.getLatest(volumeID, normalizedPath)
}

func (r *fakeRepo) getLatest(volumeID, normalizedPath string) (*db.SyncJournalEntry, error) {
	var latest *db.SyncJournalEntry
	for _, entry := range r.journal {
		if entry.VolumeID != volumeID {
			continue
		}
		match := entry.NormalizedPath == normalizedPath
		if !match && entry.NormalizedOldPath != nil {
			match = *entry.NormalizedOldPath == normalizedPath
		}
		if !match {
			continue
		}
		if latest == nil || entry.Seq > latest.Seq {
			clone := *entry
			latest = &clone
		}
	}
	if latest == nil {
		return nil, db.ErrNotFound
	}
	return latest, nil
}

type fakeArtifactWriter struct {
	calls []*db.SyncConflict
	size  int64
	err   error
}

func (f *fakeArtifactWriter) MaterializeConflict(ctx context.Context, volume *db.SandboxVolume, conflict *db.SyncConflict) (*ArtifactMaterialization, error) {
	if f.err != nil {
		return nil, f.err
	}
	clone := *conflict
	f.calls = append(f.calls, &clone)
	return &ArtifactMaterialization{SizeBytes: f.size}, nil
}

type fakeChangeApplier struct {
	calls []ChangeRequest
	err   error
}

func (f *fakeChangeApplier) ApplyChange(ctx context.Context, volume *db.SandboxVolume, change ChangeRequest) error {
	if f.err != nil {
		return f.err
	}
	f.calls = append(f.calls, change)
	return nil
}

type fakeVolumeMutationBarrier struct {
	calls        int
	lastVolumeID string
}

func (f *fakeVolumeMutationBarrier) WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.calls++
	f.lastVolumeID = volumeID
	return fn(ctx)
}

func (f *fakeVolumeMutationBarrier) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.calls++
	f.lastVolumeID = volumeID
	return fn(ctx)
}

func TestAppendReplicaChangesDetectsConflictAfterBaseSeq(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 1,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 1, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceReplica, NormalizedPath: "/app/readme.md", Path: "/app/README.md", ReplicaID: stringPtr("replica-1")},
		{Seq: 2, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, NormalizedPath: "/app/readme.md", Path: "/app/README.md"},
	}
	repo.nextSeq = 2

	svc := NewService(repo, logrus.New())
	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-conflict",
		BaseSeq:   1,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/README.md",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted changes = %d, want 0", len(resp.Accepted))
	}
	if len(resp.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(resp.Conflicts))
	}
	if resp.Conflicts[0].ExistingSeq == nil || *resp.Conflicts[0].ExistingSeq != 2 {
		t.Fatalf("existing seq = %v, want 2", resp.Conflicts[0].ExistingSeq)
	}
	if resp.Conflicts[0].ArtifactPath != "/app/README.sandbox0-conflict-replica-1-seq-2.md" {
		t.Fatalf("artifact path = %q", resp.Conflicts[0].ArtifactPath)
	}
	if resp.HeadSeq != 2 {
		t.Fatalf("head seq = %d, want 2", resp.HeadSeq)
	}
}

func TestAppendReplicaChangesMaterializesConflictArtifact(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 1,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 1, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceReplica, NormalizedPath: "/app/readme.md", Path: "/app/README.md", ReplicaID: stringPtr("replica-1")},
		{Seq: 2, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, NormalizedPath: "/app/readme.md", Path: "/app/README.md"},
	}
	repo.nextSeq = 2

	artifactWriter := &fakeArtifactWriter{size: 128}
	svc := NewService(repo, logrus.New())
	svc.SetConflictArtifactWriter(artifactWriter)

	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-artifact",
		BaseSeq:   1,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/README.md",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(artifactWriter.calls) != 1 {
		t.Fatalf("artifact writer calls = %d, want 1", len(artifactWriter.calls))
	}
	if resp.HeadSeq != 3 {
		t.Fatalf("head seq = %d, want 3", resp.HeadSeq)
	}
	if len(repo.journal) != 3 {
		t.Fatalf("journal entries = %d, want 3", len(repo.journal))
	}
	last := repo.journal[2]
	if last.Path != "/app/README.sandbox0-conflict-replica-1-seq-2.md" {
		t.Fatalf("artifact journal path = %q", last.Path)
	}
	if last.EventType != db.SyncEventWrite || last.Source != db.SyncSourceSandbox {
		t.Fatalf("unexpected artifact journal entry = %+v", last)
	}
	if resp.Conflicts[0].Metadata == nil {
		t.Fatal("expected conflict metadata to be populated")
	}
	var metadata map[string]any
	if err := json.Unmarshal(*resp.Conflicts[0].Metadata, &metadata); err != nil {
		t.Fatalf("Unmarshal conflict metadata: %v", err)
	}
	if metadata["artifact_materialized"] != true {
		t.Fatalf("artifact_materialized = %#v, want true", metadata["artifact_materialized"])
	}
}

func TestUpdateReplicaCursorRejectsAheadOfHead(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:        "replica-1",
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		CreatedAt: time.Now().UTC(),
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 3, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, NormalizedPath: "/main.go", Path: "/main.go"},
	}
	repo.nextSeq = 3

	svc := NewService(repo, logrus.New())
	_, err := svc.UpdateReplicaCursor(context.Background(), &UpdateCursorRequest{
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		ReplicaID:      "replica-1",
		LastAppliedSeq: 4,
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrCursorAhead) {
		t.Fatalf("error = %v, want ErrCursorAhead", err)
	}
}

func TestListChangesRejectsCompactedCursorWithReseedRequired(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.retention["vol-1"] = &db.SyncRetentionState{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 5,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 6, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/main.go", NormalizedPath: "/app/main.go"},
	}
	repo.nextSeq = 6

	svc := NewService(repo, logrus.New())
	_, err := svc.ListChanges(context.Background(), &ListChangesRequest{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		AfterSeq: 4,
	})
	if !errors.Is(err, ErrReseedRequired) {
		t.Fatalf("error = %v, want ErrReseedRequired", err)
	}
	var reseedErr *ReseedRequiredError
	if !errors.As(err, &reseedErr) {
		t.Fatalf("error = %v, want ReseedRequiredError", err)
	}
	if reseedErr.RetainedAfterSeq != 5 || reseedErr.HeadSeq != 6 {
		t.Fatalf("reseed error = %+v, want retained_after_seq=5 head_seq=6", reseedErr)
	}
}

func TestListChangesReseedRequiredIncrementsMetric(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.retention["vol-1"] = &db.SyncRetentionState{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 3,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 4, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/main.go", NormalizedPath: "/app/main.go"},
	}
	repo.nextSeq = 4

	registry := prometheus.NewRegistry()
	metrics := obsmetrics.NewStorageProxy(registry)
	svc := NewService(repo, logrus.New())
	svc.SetMetrics(metrics)

	_, err := svc.ListChanges(context.Background(), &ListChangesRequest{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		AfterSeq: 2,
	})
	if !errors.Is(err, ErrReseedRequired) {
		t.Fatalf("error = %v, want ErrReseedRequired", err)
	}

	if got := testutil.ToFloat64(metrics.VolumeSyncReseedTotal.WithLabelValues("list_changes")); got != 1 {
		t.Fatalf("reseed metric = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.VolumeSyncOperationsTotal.WithLabelValues("list_changes", "reseed_required")); got != 1 {
		t.Fatalf("operation metric = %v, want 1", got)
	}
}

func TestAppendReplicaChangesRejectsCompactedBaseSeqWithReseedRequired(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  true,
		LastAppliedSeq: 5,
	}
	repo.retention["vol-1"] = &db.SyncRetentionState{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 5,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 6, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/main.go", NormalizedPath: "/app/main.go"},
	}
	repo.nextSeq = 6

	svc := NewService(repo, logrus.New())
	_, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-stale",
		BaseSeq:   4,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/main.go",
		}},
	})
	if !errors.Is(err, ErrReseedRequired) {
		t.Fatalf("error = %v, want ErrReseedRequired", err)
	}
	if len(repo.journal) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(repo.journal))
	}
}

func TestAppendReplicaChangesRejectsExpiredReplicaLease(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastSeenAt:     time.Now().UTC().Add(-defaultReplicaLease - time.Minute),
		LastAppliedSeq: 0,
	}

	svc := NewService(repo, logrus.New())
	_, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-expired",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/main.go",
		}},
	})
	if !errors.Is(err, ErrReplicaLeaseExpired) {
		t.Fatalf("error = %v, want ErrReplicaLeaseExpired", err)
	}
	if len(repo.journal) != 0 {
		t.Fatalf("journal entries = %d, want 0", len(repo.journal))
	}
}

func TestAppendReplicaChangesRejectsMissingRequestID(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 0,
	}

	svc := NewService(repo, logrus.New())
	_, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/main.go",
		}},
	})
	if !errors.Is(err, ErrInvalidRequestID) {
		t.Fatalf("error = %v, want ErrInvalidRequestID", err)
	}
}

func TestUpdateReplicaCursorRejectsExpiredReplicaLease(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		LastSeenAt:     time.Now().UTC().Add(-defaultReplicaLease - time.Minute),
		LastAppliedSeq: 0,
		CreatedAt:      time.Now().UTC(),
	}

	svc := NewService(repo, logrus.New())
	_, err := svc.UpdateReplicaCursor(context.Background(), &UpdateCursorRequest{
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		ReplicaID:      "replica-1",
		LastAppliedSeq: 0,
	})
	if !errors.Is(err, ErrReplicaLeaseExpired) {
		t.Fatalf("error = %v, want ErrReplicaLeaseExpired", err)
	}
}

func TestUpdateReplicaCursorAllowsBootstrapRecoveryPastCompactionFloor(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		LastSeenAt:     time.Now().UTC(),
		LastAppliedSeq: 2,
		CreatedAt:      time.Now().UTC(),
	}
	repo.retention["vol-1"] = &db.SyncRetentionState{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 5,
	}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 6, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/main.go", NormalizedPath: "/app/main.go"},
	}
	repo.nextSeq = 6

	svc := NewService(repo, logrus.New())
	resp, err := svc.UpdateReplicaCursor(context.Background(), &UpdateCursorRequest{
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		ReplicaID:      "replica-1",
		LastAppliedSeq: 5,
	})
	if err != nil {
		t.Fatalf("UpdateReplicaCursor error = %v", err)
	}
	if resp.Replica.LastAppliedSeq != 5 {
		t.Fatalf("last_applied_seq = %d, want 5", resp.Replica.LastAppliedSeq)
	}
}

func TestCompactJournalDeletesCompactedHistoryAndAdvancesRetentionFloor(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.journal = []*db.SyncJournalEntry{
		{Seq: 1, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/1.go", NormalizedPath: "/app/1.go"},
		{Seq: 2, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/2.go", NormalizedPath: "/app/2.go"},
		{Seq: 3, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/3.go", NormalizedPath: "/app/3.go"},
		{Seq: 4, VolumeID: "vol-1", TeamID: "team-1", Source: db.SyncSourceSandbox, EventType: db.SyncEventWrite, Path: "/app/4.go", NormalizedPath: "/app/4.go"},
	}
	repo.nextSeq = 4
	barrier := &fakeVolumeMutationBarrier{}

	svc := NewService(repo, logrus.New())
	svc.SetVolumeMutationBarrier(barrier)

	resp, err := svc.CompactJournal(context.Background(), &CompactJournalRequest{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 2,
	})
	if err != nil {
		t.Fatalf("CompactJournal error = %v", err)
	}
	if resp.CompactedThroughSeq != 2 || resp.DeletedEntries != 2 || resp.HeadSeq != 4 {
		t.Fatalf("compact response = %+v, want compacted_through_seq=2 deleted_entries=2 head_seq=4", resp)
	}
	if barrier.calls != 1 || barrier.lastVolumeID != "vol-1" {
		t.Fatalf("barrier = %+v, want one exclusive call for vol-1", barrier)
	}
	if len(repo.journal) != 2 || repo.journal[0].Seq != 3 {
		t.Fatalf("journal after compact = %+v, want seqs [3 4]", repo.journal)
	}
	state := repo.retention["vol-1"]
	if state == nil || state.CompactedThroughSeq != 2 {
		t.Fatalf("retention state = %+v, want compacted_through_seq=2", state)
	}
}

func TestAppendReplicaChangesAppliesAcceptedChange(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 0,
	}

	content := "aGVsbG8K"
	applier := &fakeChangeApplier{}
	barrier := &fakeVolumeMutationBarrier{}
	svc := NewService(repo, logrus.New())
	svc.SetReplicaChangeApplier(applier)
	svc.SetVolumeMutationBarrier(barrier)

	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-accepted",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType:     db.SyncEventWrite,
			Path:          "/app/main.go",
			ContentBase64: &content,
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(applier.calls) != 1 {
		t.Fatalf("applier calls = %d, want 1", len(applier.calls))
	}
	if len(resp.Accepted) != 1 {
		t.Fatalf("accepted changes = %d, want 1", len(resp.Accepted))
	}
	if resp.HeadSeq != 1 {
		t.Fatalf("head seq = %d, want 1", resp.HeadSeq)
	}
	if barrier.calls != 1 || barrier.lastVolumeID != "vol-1" {
		t.Fatalf("barrier = %+v, want one shared call for vol-1", barrier)
	}
}

func TestAppendReplicaChangesReplaysStoredResponseForDuplicateRequestID(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 0,
	}

	content := "aGVsbG8K"
	applier := &fakeChangeApplier{}
	svc := NewService(repo, logrus.New())
	svc.SetReplicaChangeApplier(applier)

	req := &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-dup",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType:     db.SyncEventWrite,
			Path:          "/app/main.go",
			ContentBase64: &content,
		}},
	}

	first, err := svc.AppendReplicaChanges(context.Background(), req)
	if err != nil {
		t.Fatalf("first AppendReplicaChanges error = %v", err)
	}
	second, err := svc.AppendReplicaChanges(context.Background(), req)
	if err != nil {
		t.Fatalf("second AppendReplicaChanges error = %v", err)
	}

	if len(applier.calls) != 1 {
		t.Fatalf("applier calls = %d, want 1", len(applier.calls))
	}
	if len(repo.journal) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(repo.journal))
	}
	if len(repo.requests) != 1 {
		t.Fatalf("stored requests = %d, want 1", len(repo.requests))
	}
	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("Marshal first response: %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("Marshal second response: %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("responses differ: first=%s second=%s", firstJSON, secondJSON)
	}
}

func TestAppendReplicaChangesRejectsRequestIDReuseWithDifferentPayload(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 0,
	}

	content := "aGVsbG8K"
	svc := NewService(repo, logrus.New())
	svc.SetReplicaChangeApplier(&fakeChangeApplier{})

	_, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-collision",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType:     db.SyncEventWrite,
			Path:          "/app/main.go",
			ContentBase64: &content,
		}},
	})
	if err != nil {
		t.Fatalf("first AppendReplicaChanges error = %v", err)
	}

	_, err = svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-collision",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/other.go",
		}},
	})
	if !errors.Is(err, ErrRequestIDConflict) {
		t.Fatalf("error = %v, want ErrRequestIDConflict", err)
	}
}

func TestAppendReplicaChangesRejectsCasefoldMismatchForCaseInsensitiveReplica(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 3,
	}
	repo.journal = []*db.SyncJournalEntry{
		{
			Seq:            3,
			VolumeID:       "vol-1",
			TeamID:         "team-1",
			Source:         db.SyncSourceSandbox,
			EventType:      db.SyncEventWrite,
			Path:           "/app/main.go",
			NormalizedPath: "/app/main.go",
		},
	}
	repo.nextSeq = 3

	svc := NewService(repo, logrus.New())
	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-casefold",
		BaseSeq:   3,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/Main.go",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted changes = %d, want 0", len(resp.Accepted))
	}
	if len(resp.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(resp.Conflicts))
	}
	if resp.Conflicts[0].Reason != "casefold_collision" {
		t.Fatalf("conflict reason = %q, want casefold_collision", resp.Conflicts[0].Reason)
	}
	if resp.Conflicts[0].ExistingSeq == nil || *resp.Conflicts[0].ExistingSeq != 3 {
		t.Fatalf("existing seq = %v, want 3", resp.Conflicts[0].ExistingSeq)
	}
	if len(repo.journal) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(repo.journal))
	}
}

func TestAppendReplicaChangesAllowsCaseOnlyRenameForCaseInsensitiveReplica(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 4,
	}
	repo.journal = []*db.SyncJournalEntry{
		{
			Seq:               4,
			VolumeID:          "vol-1",
			TeamID:            "team-1",
			Source:            db.SyncSourceSandbox,
			EventType:         db.SyncEventRename,
			Path:              "/app/main.go",
			NormalizedPath:    "/app/main.go",
			OldPath:           stringPtr("/app/Main.go"),
			NormalizedOldPath: stringPtr("/app/main.go"),
		},
	}
	repo.nextSeq = 4

	svc := NewService(repo, logrus.New())
	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-rename",
		BaseSeq:   4,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventRename,
			OldPath:   "/app/main.go",
			Path:      "/app/Main.go",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(resp.Conflicts) != 0 {
		t.Fatalf("conflicts = %d, want 0", len(resp.Conflicts))
	}
	if len(resp.Accepted) != 1 {
		t.Fatalf("accepted changes = %d, want 1", len(resp.Accepted))
	}
	if resp.Accepted[0].EventType != db.SyncEventRename {
		t.Fatalf("accepted event type = %q, want rename", resp.Accepted[0].EventType)
	}
	if resp.Accepted[0].OldPath == nil || *resp.Accepted[0].OldPath != "/app/main.go" {
		t.Fatalf("accepted old path = %v, want /app/main.go", resp.Accepted[0].OldPath)
	}
	if resp.Accepted[0].Path != "/app/Main.go" {
		t.Fatalf("accepted path = %q, want /app/Main.go", resp.Accepted[0].Path)
	}
}

func TestAppendReplicaChangesRejectsUnicodeNormalizationCollisionForCaseInsensitiveReplica(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:             "replica-1",
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		CaseSensitive:  false,
		LastAppliedSeq: 6,
	}
	repo.journal = []*db.SyncJournalEntry{
		{
			Seq:            6,
			VolumeID:       "vol-1",
			TeamID:         "team-1",
			Source:         db.SyncSourceSandbox,
			EventType:      db.SyncEventWrite,
			Path:           "/app/Caf\u00e9.txt",
			NormalizedPath: "/app/cafe\u0301.txt",
		},
	}
	repo.nextSeq = 6

	svc := NewService(repo, logrus.New())
	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-unicode",
		BaseSeq:   6,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/Cafe\u0301.txt",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted changes = %d, want 0", len(resp.Accepted))
	}
	if len(resp.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(resp.Conflicts))
	}
	if resp.Conflicts[0].Reason != "casefold_collision" {
		t.Fatalf("conflict reason = %q, want casefold_collision", resp.Conflicts[0].Reason)
	}
	if resp.Conflicts[0].IncomingPath == nil || *resp.Conflicts[0].IncomingPath != "/app/Cafe\u0301.txt" {
		t.Fatalf("incoming path = %v, want decomposed cafe path", resp.Conflicts[0].IncomingPath)
	}
}

func TestAppendReplicaChangesRejectsWindowsIncompatiblePathForPortableReplica(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.replicas[replicaKey("vol-1", "replica-1")] = &db.SyncReplica{
		ID:       "replica-1",
		VolumeID: "vol-1",
		TeamID:   "team-1",
		Capabilities: pathnorm.FilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
			WindowsCompatiblePaths:          true,
		},
		LastAppliedSeq: 0,
	}

	svc := NewService(repo, logrus.New())
	resp, err := svc.AppendReplicaChanges(context.Background(), &AppendChangesRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		ReplicaID: "replica-1",
		RequestID: "req-win",
		BaseSeq:   0,
		Changes: []ChangeRequest{{
			EventType: db.SyncEventWrite,
			Path:      "/app/CON.txt",
		}},
	})
	if err != nil {
		t.Fatalf("AppendReplicaChanges error = %v", err)
	}
	if len(resp.Accepted) != 0 {
		t.Fatalf("accepted changes = %d, want 0", len(resp.Accepted))
	}
	if len(resp.Conflicts) != 1 {
		t.Fatalf("conflicts = %d, want 1", len(resp.Conflicts))
	}
	if resp.Conflicts[0].Reason != pathnorm.IssueCodeWindowsReservedName {
		t.Fatalf("conflict reason = %q, want %q", resp.Conflicts[0].Reason, pathnorm.IssueCodeWindowsReservedName)
	}
	if resp.Conflicts[0].ArtifactPath != "" {
		t.Fatalf("artifact path = %q, want empty for incompatible path conflict", resp.Conflicts[0].ArtifactPath)
	}
	if len(repo.journal) != 0 {
		t.Fatalf("journal entries = %d, want 0", len(repo.journal))
	}
}

func TestUpsertReplicaPersistsMergedNamespacePolicy(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.policies["vol-1"] = &db.SyncNamespacePolicy{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		Capabilities: pathnorm.FilesystemCapabilities{
			WindowsCompatiblePaths: true,
		},
	}

	svc := NewService(repo, logrus.New())
	resp, err := svc.UpsertReplica(context.Background(), &UpsertReplicaRequest{
		VolumeID:      "vol-1",
		TeamID:        "team-1",
		ReplicaID:     "replica-1",
		DisplayName:   "Laptop",
		Platform:      "darwin",
		RootPath:      "/tmp/work",
		CaseSensitive: false,
		Capabilities: &pathnorm.FilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
		},
	})
	if err != nil {
		t.Fatalf("UpsertReplica error = %v", err)
	}
	if resp.Replica == nil {
		t.Fatal("expected replica")
	}
	if !resp.Replica.Capabilities.WindowsCompatiblePaths {
		t.Fatalf("replica capabilities = %+v, want merged WindowsCompatiblePaths", resp.Replica.Capabilities)
	}
	if repo.policies["vol-1"] == nil || !repo.policies["vol-1"].Capabilities.WindowsCompatiblePaths {
		t.Fatalf("policy = %+v, want persisted windows-compatible policy", repo.policies["vol-1"])
	}
}

func TestValidateNamespaceMutationRejectsCasefoldCollisionUnderPolicy(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.policies["vol-1"] = &db.SyncNamespacePolicy{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		Capabilities: pathnorm.FilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
		},
	}
	repo.journal = []*db.SyncJournalEntry{{
		Seq:            1,
		VolumeID:       "vol-1",
		TeamID:         "team-1",
		Source:         db.SyncSourceSandbox,
		EventType:      db.SyncEventWrite,
		Path:           "/app/main.go",
		NormalizedPath: "/app/main.go",
	}}
	repo.nextSeq = 1

	svc := NewService(repo, logrus.New())
	err := svc.ValidateNamespaceMutation(context.Background(), &NamespaceMutationRequest{
		VolumeID:  "vol-1",
		TeamID:    "team-1",
		EventType: db.SyncEventCreate,
		Path:      "/app/Main.go",
	})
	if !errors.Is(err, ErrNamespaceIncompatible) {
		t.Fatalf("error = %v, want ErrNamespaceIncompatible", err)
	}
	var compatErr *NamespaceCompatibilityError
	if !errors.As(err, &compatErr) || len(compatErr.Issues) != 1 {
		t.Fatalf("compatErr = %+v, want one issue", compatErr)
	}
	if compatErr.Issues[0].Code != pathnorm.IssueCodeCasefoldCollision {
		t.Fatalf("issue code = %q, want %q", compatErr.Issues[0].Code, pathnorm.IssueCodeCasefoldCollision)
	}
}

func TestRecordRemoteChangeCoalescesHotWrites(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.journal = []*db.SyncJournalEntry{
		{
			Seq:            1,
			VolumeID:       "vol-1",
			TeamID:         "team-1",
			Source:         db.SyncSourceSandbox,
			EventType:      db.SyncEventWrite,
			Path:           "/src/main.go",
			NormalizedPath: "/src/main.go",
			CreatedAt:      time.Now().UTC().Add(-500 * time.Millisecond),
		},
	}
	repo.nextSeq = 1

	svc := NewService(repo, logrus.New())
	if err := svc.RecordRemoteChange(context.Background(), &RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/src/main.go",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordRemoteChange error = %v", err)
	}
	if len(repo.journal) != 1 {
		t.Fatalf("journal entries = %d, want 1", len(repo.journal))
	}
}

func TestNormalizePath(t *testing.T) {
	got := NormalizePath("src/../SRC/Main.go")
	if got != "/src/main.go" {
		t.Fatalf("NormalizePath = %q, want %q", got, "/src/main.go")
	}
}

func TestNormalizePathCanonicalizesUnicodeNormalization(t *testing.T) {
	got := NormalizePath("/app/Caf\u00e9.txt")
	want := "/app/cafe\u0301.txt"
	if got != want {
		t.Fatalf("NormalizePath = %q, want %q", got, want)
	}
	if other := NormalizePath("/app/Cafe\u0301.txt"); other != want {
		t.Fatalf("NormalizePath decomposed = %q, want %q", other, want)
	}
}

func TestListConflicts(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.conflicts = []*db.SyncConflict{
		{ID: "1", VolumeID: "vol-1", TeamID: "team-1", Status: db.SyncConflictStatusOpen, ArtifactPath: "/a"},
		{ID: "2", VolumeID: "vol-1", TeamID: "team-1", Status: "resolved", ArtifactPath: "/b"},
	}

	svc := NewService(repo, logrus.New())
	resp, err := svc.ListConflicts(context.Background(), &ListConflictsRequest{
		VolumeID: "vol-1",
		TeamID:   "team-1",
		Status:   db.SyncConflictStatusOpen,
	})
	if err != nil {
		t.Fatalf("ListConflicts error = %v", err)
	}
	if len(resp.Conflicts) != 1 || resp.Conflicts[0].ID != "1" {
		t.Fatalf("unexpected conflicts = %+v", resp.Conflicts)
	}
}

func TestMustMarshalMetadata(t *testing.T) {
	raw := mustMarshalMetadata(map[string]any{"a": 1})
	if raw == nil {
		t.Fatal("expected metadata")
	}
	var decoded map[string]int
	if err := json.Unmarshal(*raw, &decoded); err != nil {
		t.Fatalf("Unmarshal metadata: %v", err)
	}
	if decoded["a"] != 1 {
		t.Fatalf("metadata = %#v, want a=1", decoded)
	}
}

func TestResolveConflict(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}
	repo.conflicts = []*db.SyncConflict{
		{ID: "conflict-1", VolumeID: "vol-1", TeamID: "team-1", Status: db.SyncConflictStatusOpen},
	}

	svc := NewService(repo, logrus.New())
	conflict, err := svc.ResolveConflict(context.Background(), &ResolveConflictRequest{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		ConflictID: "conflict-1",
		Status:     db.SyncConflictStatusResolved,
		Resolution: "keep_remote",
		Note:       "accepted remote version",
	})
	if err != nil {
		t.Fatalf("ResolveConflict error = %v", err)
	}
	if conflict.Status != db.SyncConflictStatusResolved {
		t.Fatalf("status = %q, want %q", conflict.Status, db.SyncConflictStatusResolved)
	}
}

func TestResolveConflictRejectsInvalidStatus(t *testing.T) {
	repo := newFakeRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-1"}

	svc := NewService(repo, logrus.New())
	_, err := svc.ResolveConflict(context.Background(), &ResolveConflictRequest{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		ConflictID: "conflict-1",
		Status:     "open",
	})
	if !errors.Is(err, ErrInvalidConflictStatus) {
		t.Fatalf("error = %v, want ErrInvalidConflictStatus", err)
	}
}
