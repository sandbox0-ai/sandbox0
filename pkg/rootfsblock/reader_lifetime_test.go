package rootfsblock

import (
	"context"
	"io"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

type lifetimeRangeSource struct {
	RangeSource
	get func(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func (s lifetimeRangeSource) Get(string, int64, int64) (io.ReadCloser, error) {
	panic("context-aware source must not fall back to Get")
}

func (s lifetimeRangeSource) GetContext(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return s.get(ctx, key, offset, length)
}

func TestReaderLifetimeRejectsMissingOrCanceledContext(t *testing.T) {
	base, _, descriptor, _ := adaptiveFixture(t, bulkReadThreshold, 1024)
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)
	_, err = NewReaderWithCacheContext(nil, base, descriptor, cache) //nolint:staticcheck // SA1012: verify the explicit lifetime API rejects an invalid nil context.
	require.ErrorContains(t, err, "lifetime is required")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = NewReaderWithCacheContext(ctx, base, descriptor, cache)
	require.ErrorIs(t, err, context.Canceled)
	requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
}

func TestReaderLifetimeCancelsQueuedMappingWithoutSourceRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, _, descriptor, _ := adaptiveFixture(t, bulkReadThreshold, 1024)
		cache, err := NewReadCache(DefaultReadCacheBytes)
		require.NoError(t, err)
		holdReadAdmission(t, cache)
		calls := make(chan struct{}, 1)
		source := lifetimeRangeSource{get: func(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
			calls <- struct{}{}
			return base.Get(key, offset, length)
		}}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := NewReaderWithCacheContext(ctx, source, descriptor, cache)
			done <- err
		}()
		synctest.Wait()
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 1, 1)
		cancel()
		synctest.Wait()
		require.Len(t, done, 1)
		require.ErrorIs(t, <-done, context.Canceled)
		require.Empty(t, calls)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 8, 0, 0)
	})
}

type lifetimeReadBody struct {
	ctx     context.Context
	started chan struct{}
	closed  bool
}

func (b *lifetimeReadBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *lifetimeReadBody) Close() error { b.closed = true; return nil }

func TestReaderLifetimeCancelsTransportAndReleasesSharedSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, _, descriptor, expected := adaptiveFixture(t, bulkReadThreshold, 1024)
		cache, err := NewReadCache(DefaultReadCacheBytes)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		body := &lifetimeReadBody{started: make(chan struct{})}
		source := lifetimeRangeSource{get: func(got context.Context, key string, offset, length int64) (io.ReadCloser, error) {
			if got != ctx {
				t.Error("source did not receive Reader lifetime")
			}
			if key != base.pack {
				return base.Get(key, offset, length)
			}
			body.ctx = got
			return body, nil
		}}
		reader, err := NewReaderWithCacheContext(ctx, source, descriptor, cache)
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { _, err := reader.ReadAt(make([]byte, 1), 0); done <- err }()
		<-body.started
		requireReadAdmissionUsage(t, &cache.sourceSlots, 1, 0, 0)
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		require.True(t, body.closed)
		requireReadAdmissionUsage(t, &cache.sourceSlots, 0, 0, 0)
		other, err := NewReaderWithCache(base, descriptor, cache)
		require.NoError(t, err)
		actual := make([]byte, 1)
		_, err = other.ReadAt(actual, 0)
		require.NoError(t, err)
		require.Equal(t, expected[:1], actual, "a canceled source read must not poison shared verified content")
	})
}
