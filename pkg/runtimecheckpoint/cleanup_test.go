package runtimecheckpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type cleanupFaultStore struct {
	objectstore.ContextCleanupStore
	list      func(context.Context, string) ([]objectstore.Info, bool, string, error)
	failAfter int
	deletes   int
}

func (s *cleanupFaultStore) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	return s.ContextCleanupStore.(checkpointObjectReader).GetContext(ctx, key, off, limit)
}

func (s *cleanupFaultStore) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]objectstore.Info, bool, string, error) {
	if s.list != nil {
		return s.list(ctx, prefix)
	}
	return s.ContextCleanupStore.ListContext(ctx, prefix, after, token, delimiter, limit)
}
func (s *cleanupFaultStore) DeleteContext(ctx context.Context, key string) error {
	if s.failAfter > 0 && s.deletes == s.failAfter {
		return errors.New("injected deletion failure")
	}
	s.deletes++
	return s.ContextCleanupStore.DeleteContext(ctx, key)
}

func TestCheckpointCollectorBoundedPartialUploadAndLostReply(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	binding := testBinding()
	bd, err := binding.Digest()
	require.NoError(t, err)
	for i := 0; i < cleanupBatchSize+7; i++ {
		require.NoError(t, raw.Put(chunkKey(bd, digest.FromString(fmt.Sprint(i)).String()), strings.NewReader("orphan upload")))
	}
	other := testBinding()
	other.OperationID = "another-operation"
	otherDigest, err := other.Digest()
	require.NoError(t, err)
	require.NoError(t, raw.Put(manifestKey(otherDigest), strings.NewReader("must survive")))
	fault := &cleanupFaultStore{ContextCleanupStore: raw.(objectstore.ContextCleanupStore), failAfter: 3}
	c, err := NewCollector(fault)
	require.NoError(t, err)
	done, err := c.Collect(t.Context(), binding)
	require.ErrorContains(t, err, "injected")
	require.False(t, done)
	fault.failAfter = 0
	fault.deletes = 0
	done, err = c.Collect(t.Context(), binding)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, cleanupBatchSize, fault.deletes)
	c, err = NewCollector(raw)
	require.NoError(t, err)
	done, err = c.Collect(t.Context(), binding)
	require.NoError(t, err)
	require.False(t, done)
	for range 2 {
		done, err = c.Collect(t.Context(), binding)
		require.NoError(t, err)
		require.True(t, done)
	}
	_, err = raw.Head(manifestKey(otherDigest))
	require.NoError(t, err)
}

func TestCheckpointCollectorValidatesWholePageBeforeDeletion(t *testing.T) {
	for _, bad := range []string{"outside/manifest.json", "extra", "chunks/../escape", "chunks/" + strings.Repeat("a", 63)} {
		t.Run(bad, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			bd, err := testBinding().Digest()
			require.NoError(t, err)
			fault := &cleanupFaultStore{ContextCleanupStore: raw.(objectstore.ContextCleanupStore)}
			fault.list = func(_ context.Context, prefix string) ([]objectstore.Info, bool, string, error) {
				key := prefix + bad
				if strings.HasPrefix(bad, "outside/") {
					key = bad
				}
				return []objectstore.Info{{Key: chunkKey(bd, digest.FromString("safe").String())}, {Key: key}}, false, "", nil
			}
			c, err := NewCollector(fault)
			require.NoError(t, err)
			done, err := c.Collect(t.Context(), testBinding())
			require.Error(t, err)
			require.False(t, done)
			require.Zero(t, fault.deletes)
		})
	}
	raw := objectstore.NewMemoryStore("")
	c, err := NewCollector(raw)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Collect(ctx, testBinding())
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Collect(t.Context(), Binding{})
	require.Error(t, err)
}
