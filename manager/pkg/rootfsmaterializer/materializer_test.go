package rootfsmaterializer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestWorkerMaterializesCompositeGenerationAndPreservesContent(t *testing.T) {
	objects := newMaterializerObjects()
	basePayload := bytes.Repeat([]byte{0x11}, 3*rootfsblock.LogicalBlockSize)
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	changed := bytes.Repeat([]byte{0x42}, rootfsblock.LogicalBlockSize)
	generation := compositeMaterializerGeneration(t, base.Descriptor, "generation-a", "team-a", 7, time.Time{}, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 1, Data: changed,
	}})
	store := newMaterializerStore(generation)
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects, ScanLimit: 10})
	require.NoError(t, err)

	result, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 1, Materialized: 1, Batches: 1}, result)
	require.Len(t, store.publications, 1)
	published := store.publications[0].Members[0]
	require.Equal(t, int64(7), published.ExpectedLocatorVersion)
	next, err := rootfsblock.DecodeDescriptor(published.MaterializedDescriptor)
	require.NoError(t, err)
	require.Nil(t, next.CompositeTail)

	reader, err := rootfsblock.NewReader(objects, next, rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	expected := append([]byte(nil), basePayload...)
	copy(expected[rootfsblock.LogicalBlockSize:2*rootfsblock.LogicalBlockSize], changed)
	require.Equal(t, expected, actual)
	require.Equal(t, []string{"register", "mark", "register", "mark", "publish"}, store.events)
}

func TestWorkerSkipsBadCandidateWithoutPoisoningValidBatch(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	good := compositeMaterializerGeneration(t, base.Descriptor, "good", "team-a", 1, time.Time{}, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize),
	}})
	bad := good
	bad.ID = "bad"
	bad.Descriptor = []byte("bad")
	store := newMaterializerStore(bad, good)
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.ErrorContains(t, err, "generation bad")
	require.Equal(t, Result{Scanned: 2, Materialized: 1, Batches: 1, Failed: 1}, result)
	require.Len(t, store.publications, 1)
	require.Equal(t, "good", store.publications[0].Members[0].GenerationID)
}

func TestWorkerResumesExactBatchAcrossObjectStoreOutage(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	first := compositeMaterializerGeneration(t, base.Descriptor, "outage-a", "team-a", 3, time.Time{}, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{2}, rootfsblock.LogicalBlockSize),
	}})
	second := compositeMaterializerGeneration(t, base.Descriptor, "outage-b", "team-a", 4, time.Time{}, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{3}, rootfsblock.LogicalBlockSize),
	}})
	store := newMaterializerStore(first, second)
	failing := &failingMaterializerPublisher{}
	worker, err := New(Config{Store: store, Source: objects, Publisher: failing})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.ErrorContains(t, err, "object store unavailable")
	require.Equal(t, Result{Scanned: 2, Batches: 1, Failed: 2}, result)
	require.Equal(t, 1, failing.calls, "one failed object request must stop the entire atomic batch")
	require.Empty(t, store.publications)
	require.NotNil(t, store.pending)
	originalBatchID := store.pending.BatchID

	recovered, err := New(Config{Store: store, Source: objects, Publisher: objects})
	require.NoError(t, err)
	result, err = recovered.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 2, Materialized: 2, Batches: 1}, result)
	require.Len(t, store.publications, 1)
	require.Equal(t, originalBatchID, store.publications[0].BatchID)
	require.Len(t, store.beginRequests, 1, "recovery must reuse journaled membership instead of creating a new batch")
}

func TestWorkerDefersYoungTailAndBoundsForcedFlushesByLane(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	young := compositeMaterializerGeneration(t, base.Descriptor, "young", "team-young", 1, time.Now(), []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize),
	}})
	store := newMaterializerStore(young)
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 1, Deferred: 1}, result)
	require.Empty(t, store.beginRequests)

	old := time.Now().Add(-time.Hour)
	first := compositeMaterializerGeneration(t, base.Descriptor, "old-a", "team-a", 1, old, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{2}, rootfsblock.LogicalBlockSize),
	}})
	second := compositeMaterializerGeneration(t, base.Descriptor, "old-b", "team-b", 1, old, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{3}, rootfsblock.LogicalBlockSize),
	}})
	store = newMaterializerStore(first, second)
	worker, err = New(Config{Store: store, Source: objects, Publisher: objects, ForcedFlushesPerRun: 1})
	require.NoError(t, err)
	result, err = worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 2, Materialized: 1, Deferred: 1, Batches: 1}, result)
	require.Len(t, store.publications, 1)
	require.NotEqual(t,
		sandboxstore.RootFSMaterializationPackLane("team-a", 1),
		sandboxstore.RootFSMaterializationPackLane("team-b", 1),
	)
}

func TestWorkerPacksTenThousandTinyGenerationsIntoTwoObjects(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	generations := make([]sandboxstore.RootFSGeneration, 10_000)
	for index := range generations {
		generations[index] = compositeMaterializerGeneration(
			t, base.Descriptor, fmt.Sprintf("generation-%04d", index), "team-a", 1, time.Time{},
			[]rootfsblock.BlockUpdate{{
				Sequence: 1, Block: 0,
				Data: bytes.Repeat([]byte{byte(index%251 + 1)}, rootfsblock.LogicalBlockSize),
			}},
		)
	}
	store := newMaterializerStore(generations...)
	before := objects.count()
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects, ScanLimit: 10_000})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 10_000, Materialized: 10_000, Batches: 1}, result)
	require.Equal(t, before+2, objects.count())
	require.Len(t, store.publications, 1)
	require.Len(t, store.publications[0].Members, 10_000)
}

func compositeMaterializerGeneration(
	t *testing.T,
	base rootfsblock.Descriptor,
	id, teamID string,
	locatorVersion int64,
	createdAt time.Time,
	updates []rootfsblock.BlockUpdate,
) sandboxstore.RootFSGeneration {
	t.Helper()
	_, payload, err := rootfsblock.BuildCompositeGeneration(base, updates)
	require.NoError(t, err)
	return sandboxstore.RootFSGeneration{
		ID: id, DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable,
		LocatorVersion: locatorVersion, Descriptor: payload, CreatedAt: createdAt,
		FormatGeneration: 1, MaterializationTeamID: teamID,
		MaterializationPackLane: sandboxstore.RootFSMaterializationPackLane(teamID, 1),
	}
}

type materializerStore struct {
	mu            sync.Mutex
	generations   []sandboxstore.RootFSGeneration
	pending       *sandboxstore.RootFSGenerationMaterializationBatch
	beginRequests []sandboxstore.BeginRootFSGenerationMaterializationBatchRequest
	publications  []sandboxstore.PublishRootFSGenerationMaterializationBatchRequest
	published     map[string]struct{}
	registered    map[string]rootfsblock.ObjectReference
	marked        map[string]struct{}
	events        []string
}

func newMaterializerStore(generations ...sandboxstore.RootFSGeneration) *materializerStore {
	return &materializerStore{
		generations: append([]sandboxstore.RootFSGeneration(nil), generations...),
		published:   make(map[string]struct{}), registered: make(map[string]rootfsblock.ObjectReference),
		marked: make(map[string]struct{}),
	}
}

func (s *materializerStore) ListCompositeRootFSGenerations(_ context.Context, limit int) ([]sandboxstore.RootFSGeneration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]sandboxstore.RootFSGeneration, 0, min(limit, len(s.generations)))
	for _, generation := range s.generations {
		if _, found := s.published[generation.ID]; found {
			continue
		}
		result = append(result, generation)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *materializerStore) GetOldestUploadingRootFSGenerationMaterializationBatch(context.Context) (*sandboxstore.RootFSGenerationMaterializationBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.State != "uploading" {
		return nil, nil
	}
	copy := *s.pending
	copy.Members = cloneMaterializerMembers(s.pending.Members)
	return &copy, nil
}

func (s *materializerStore) BeginRootFSGenerationMaterializationBatch(
	_ context.Context,
	req *sandboxstore.BeginRootFSGenerationMaterializationBatchRequest,
) (*sandboxstore.RootFSGenerationMaterializationBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyRequest := *req
	copyRequest.Members = cloneMaterializerMembers(req.Members)
	s.beginRequests = append(s.beginRequests, copyRequest)
	batch := &sandboxstore.RootFSGenerationMaterializationBatch{
		BatchID: req.BatchID, PackLane: req.PackLane, TeamID: req.TeamID,
		FormatGeneration: req.FormatGeneration, State: "uploading",
		Members: cloneMaterializerMembers(req.Members),
	}
	s.pending = batch
	copy := *batch
	copy.Members = cloneMaterializerMembers(batch.Members)
	return &copy, nil
}

func (s *materializerStore) RegisterRootFSGenerationMaterializationBatchObject(
	_ context.Context,
	batchID string,
	reference rootfsblock.ObjectReference,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.BatchID != batchID || s.pending.State != "uploading" {
		return fmt.Errorf("batch is not uploading")
	}
	s.registered[reference.Key] = reference
	s.events = append(s.events, "register")
	return nil
}

func (s *materializerStore) MarkRootFSGenerationMaterializationBatchObjectUploaded(
	_ context.Context,
	batchID, objectKey string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.BatchID != batchID {
		return fmt.Errorf("batch is missing")
	}
	if _, found := s.registered[objectKey]; !found {
		return fmt.Errorf("object was not registered")
	}
	s.marked[objectKey] = struct{}{}
	s.events = append(s.events, "mark")
	return nil
}

func (s *materializerStore) PublishRootFSGenerationMaterializationBatch(
	_ context.Context,
	req *sandboxstore.PublishRootFSGenerationMaterializationBatchRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.BatchID != req.BatchID {
		return fmt.Errorf("batch is missing")
	}
	copyRequest := *req
	copyRequest.Members = make([]sandboxstore.RootFSGenerationMaterializationPublication, len(req.Members))
	for index, member := range req.Members {
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		member.MaterializedDescriptor = append([]byte(nil), member.MaterializedDescriptor...)
		member.References = append([]rootfsblock.ObjectReference(nil), member.References...)
		copyRequest.Members[index] = member
		for _, reference := range member.References {
			if _, found := s.marked[reference.Key]; !found {
				return fmt.Errorf("object %s was not uploaded", reference.Key)
			}
		}
		s.published[member.GenerationID] = struct{}{}
	}
	s.publications = append(s.publications, copyRequest)
	s.pending.State = "published"
	s.events = append(s.events, "publish")
	return nil
}

func (s *materializerStore) ReconcileRootFSGenerationMaterializationGarbage(
	context.Context, time.Duration, time.Duration, int,
) (*sandboxstore.RootFSGenerationMaterializationGarbageResult, error) {
	return &sandboxstore.RootFSGenerationMaterializationGarbageResult{}, nil
}

func cloneMaterializerMembers(
	members []sandboxstore.RootFSGenerationMaterializationIdentity,
) []sandboxstore.RootFSGenerationMaterializationIdentity {
	result := make([]sandboxstore.RootFSGenerationMaterializationIdentity, len(members))
	for index, member := range members {
		member.ExpectedDescriptor = append([]byte(nil), member.ExpectedDescriptor...)
		result[index] = member
	}
	return result
}

type materializerObjects struct {
	mu      sync.Mutex
	objects map[string][]byte
}

type failingMaterializerPublisher struct{ calls int }

func (p *failingMaterializerPublisher) PutImmutable(context.Context, string, []byte) error {
	p.calls++
	return fmt.Errorf("object store unavailable")
}

func newMaterializerObjects() *materializerObjects {
	return &materializerObjects{objects: make(map[string][]byte)}
}

func (s *materializerObjects) PutImmutable(_ context.Context, key string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, ok := s.objects[key]; ok && !bytes.Equal(current, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}

func (s *materializerObjects) Get(key string, offset, length int64) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.objects[key]
	if !ok || offset < 0 || length < 0 || offset > int64(len(payload))-length {
		return nil, fmt.Errorf("object range not found: %s", digest.FromString(key))
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}

func (s *materializerObjects) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}
