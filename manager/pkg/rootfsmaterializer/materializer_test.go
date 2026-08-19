package rootfsmaterializer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

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
	composite, compositePayload, err := rootfsblock.BuildCompositeGeneration(base.Descriptor, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 1, Data: changed,
	}})
	require.NoError(t, err)
	store := &materializerStore{generations: []sandboxstore.RootFSGeneration{{
		ID: "generation-a", DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable,
		LocatorVersion: 7, Descriptor: compositePayload,
	}}}
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects, ScanLimit: 10})
	require.NoError(t, err)

	result, err := worker.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 1, Materialized: 1}, result)
	require.Len(t, store.published, 1)
	published := store.published[0]
	require.Equal(t, int64(7), published.ExpectedLocatorVersion)
	next, err := rootfsblock.DecodeDescriptor(published.MaterializedDescriptor)
	require.NoError(t, err)
	require.Nil(t, next.CompositeTail)
	require.NotEqual(t, composite.MappingRoot.RootDigest, next.MappingRoot.RootDigest)

	reader, err := rootfsblock.NewReader(objects, next, rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	expected := append([]byte(nil), basePayload...)
	copy(expected[rootfsblock.LogicalBlockSize:2*rootfsblock.LogicalBlockSize], changed)
	require.Equal(t, expected, actual)
}

func TestWorkerContinuesAfterBadCandidate(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	_, payload, err := rootfsblock.BuildCompositeGeneration(base.Descriptor, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize),
	}})
	require.NoError(t, err)
	store := &materializerStore{generations: []sandboxstore.RootFSGeneration{
		{ID: "bad", DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable, LocatorVersion: 1, Descriptor: []byte("bad")},
		{ID: "good", DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable, LocatorVersion: 1, Descriptor: payload},
	}}
	worker, err := New(Config{Store: store, Source: objects, Publisher: objects})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.ErrorContains(t, err, "generation bad")
	require.Equal(t, Result{Scanned: 2, Materialized: 1, Failed: 1}, result)
	require.Len(t, store.published, 1)
	require.Equal(t, "good", store.published[0].GenerationID)
}

func TestWorkerRetainsCompositeLocatorAcrossObjectStoreOutage(t *testing.T) {
	objects := newMaterializerObjects()
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	_, payload, err := rootfsblock.BuildCompositeGeneration(base.Descriptor, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{2}, rootfsblock.LogicalBlockSize),
	}})
	require.NoError(t, err)
	store := &materializerStore{generations: []sandboxstore.RootFSGeneration{
		{ID: "outage-a", DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable, LocatorVersion: 3, Descriptor: payload},
		{ID: "outage-b", DurabilityState: sandboxstore.RootFSGenerationStateCompositeDurable, LocatorVersion: 4, Descriptor: payload},
	}}
	failing := &failingMaterializerPublisher{}
	worker, err := New(Config{
		Store: store, Source: objects, Publisher: failing,
	})
	require.NoError(t, err)
	result, err := worker.RunOnce(t.Context())
	require.ErrorContains(t, err, "object store unavailable")
	require.Equal(t, Result{Scanned: 2, Failed: 1}, result)
	require.Equal(t, 1, failing.calls, "one outage must stop the pass instead of issuing a request per candidate")
	require.Empty(t, store.published, "PG locator must not advance before every immutable object is durable")

	recovered, err := New(Config{Store: store, Source: objects, Publisher: objects})
	require.NoError(t, err)
	result, err = recovered.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Scanned: 2, Materialized: 2}, result)
	require.Len(t, store.published, 2)
}

type materializerStore struct {
	generations []sandboxstore.RootFSGeneration
	published   []sandboxstore.RootFSGenerationMaterialization
}

func (s *materializerStore) ListCompositeRootFSGenerations(_ context.Context, limit int) ([]sandboxstore.RootFSGeneration, error) {
	limit = min(limit, len(s.generations))
	return append([]sandboxstore.RootFSGeneration(nil), s.generations[:limit]...), nil
}

func (s *materializerStore) PublishRootFSGenerationMaterialization(
	_ context.Context,
	request *sandboxstore.RootFSGenerationMaterialization,
) error {
	copy := *request
	copy.ExpectedDescriptor = append([]byte(nil), request.ExpectedDescriptor...)
	copy.MaterializedDescriptor = append([]byte(nil), request.MaterializedDescriptor...)
	s.published = append(s.published, copy)
	return nil
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
	if !ok || offset < 0 || length < 0 || offset+length > int64(len(payload)) {
		return nil, fmt.Errorf("object range not found: %s", digest.FromString(key))
	}
	return io.NopCloser(bytes.NewReader(payload[offset : offset+length])), nil
}
