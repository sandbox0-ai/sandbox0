package runtimecheckpoint

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type gatedPublicationStore struct {
	objectstore.ContextConditionalStore
	entered  chan struct{}
	release  chan struct{}
	active   atomic.Int32
	overflow atomic.Bool
}

func (s *gatedPublicationStore) PutIfAbsentContext(ctx context.Context, key string, data io.Reader) (bool, error) {
	if strings.Contains(key, "/chunks/") {
		if s.active.Add(1) > publicationConcurrency {
			s.overflow.Store(true)
		}
		defer s.active.Add(-1)
		select {
		case s.entered <- struct{}{}:
		default:
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return s.ContextConditionalStore.PutIfAbsentContext(ctx, key, data)
}

func TestPublicationUploadsBoundedConcurrentChunksBeforeManifest(t *testing.T) {
	objects := objectstore.NewMemoryStore("")
	gate := &gatedPublicationStore{ContextConditionalStore: objects.(objectstore.ContextConditionalStore),
		entered: make(chan struct{}, publicationConcurrency), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(gate.release) })
	store, err := New(gate, 5*ChunkBytes)
	require.NoError(t, err)
	source := privateImage(t, map[string][]byte{"pages.img": bytes.Repeat([]byte{42}, 4*ChunkBytes+11)})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := store.Publish(ctx, testBinding(), source); done <- err }()
	for range publicationConcurrency {
		select {
		case <-gate.entered:
		case <-ctx.Done():
			t.Fatal("publication did not issue concurrent chunk uploads")
		}
	}
	require.Equal(t, int32(publicationConcurrency), gate.active.Load())
	binding, err := testBinding().Digest()
	require.NoError(t, err)
	_, err = objects.Head(manifestKey(binding))
	require.Error(t, err, "an in-flight chunk must prevent manifest publication")
	release.Do(func() { close(gate.release) })
	require.NoError(t, <-done)
	require.False(t, gate.overflow.Load())
	_, err = objects.Head(manifestKey(binding))
	require.NoError(t, err)
}
