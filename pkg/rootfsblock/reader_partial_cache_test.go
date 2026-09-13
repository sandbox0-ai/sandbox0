package rootfsblock

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestAdaptiveReadTrimsVerifiedCachedWindowEdges(t *testing.T) {
	const ranges = adaptiveWindowRanges
	singleMiss := make([]int, 0, ranges-1)
	for index := range ranges {
		if index != 7 {
			singleMiss = append(singleMiss, index)
		}
	}
	for _, test := range []struct {
		name        string
		cached      []int
		first, last int
	}{
		{"prefix", []int{0, 1}, 2, ranges},
		{"suffix", []int{ranges - 2, ranges - 1}, 0, ranges - 2},
		{"both-edges", []int{0, 1, ranges - 2, ranges - 1}, 2, ranges - 2},
		{"interior-keeps-one-request", []int{7}, 0, ranges},
		{"edges-and-interior", []int{0, 7, ranges - 1}, 1, ranges - 1},
		{"single-miss", singleMiss, 7, 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
			reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			var want []adaptiveGet
			for _, index := range test.cached {
				actual := make([]byte, 512)
				offset := index * adaptiveRangeBytes
				_, err := reader.ReadAt(actual, int64(offset))
				require.NoError(t, err)
				require.Equal(t, expected[offset:offset+len(actual)], actual)
				want = append(want, adaptiveGet{int64(offset), adaptiveRangeBytes})
			}
			actual := make([]byte, len(expected))
			n, err := reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, len(actual), n)
			require.Equal(t, expected, actual)
			want = append(want, adaptiveGet{int64(test.first * adaptiveRangeBytes), int64((test.last - test.first) * adaptiveRangeBytes)})
			require.Equal(t, want, source.requests(), "trim cache hits without splitting a bulk window into serial GETs")
			clear(actual)
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
			require.Equal(t, want, source.requests(), "fully cached replay must issue no GET")
			require.LessOrEqual(t, reader.cache.bytes, reader.cache.maxBytes)
		})
	}
}

func TestAdaptiveReadPartialCacheConcurrentMissesShareTrimmedFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
		reader, err := NewReader(base, descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		for _, index := range []int{0, adaptiveWindowRanges - 1} {
			_, err := reader.ReadAt(make([]byte, 512), int64(index*adaptiveRangeBytes))
			require.NoError(t, err)
		}
		source := &readerAdmissionSource{RangeSource: base, pack: base.pack, started: make(chan int64, 64), finish: make(chan struct{})}
		reader.source = source
		var readers sync.WaitGroup
		t.Cleanup(func() { close(source.finish); readers.Wait() })
		for index := range 64 {
			readers.Add(1)
			go func() {
				defer readers.Done()
				offset := (index % (coalescedReadBytes / bulkReadThreshold)) * bulkReadThreshold
				actual := make([]byte, bulkReadThreshold)
				_, err := reader.ReadAt(actual, int64(offset))
				require.NoError(t, err)
				require.Equal(t, expected[offset:offset+len(actual)], actual)
			}()
		}
		synctest.Wait()
		require.Len(t, source.started, 1)
		source.finish <- struct{}{}
		readers.Wait()
		require.Equal(t, []adaptiveGet{{0, adaptiveRangeBytes}, {(adaptiveWindowRanges - 1) * adaptiveRangeBytes, adaptiveRangeBytes},
			{adaptiveRangeBytes, (adaptiveWindowRanges - 2) * adaptiveRangeBytes}}, base.requests())
	})
}

func TestAdaptiveReadRechecksCacheAfterSourceAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
		reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
		require.NoError(t, err)
		var readers sync.WaitGroup
		releases := make([]func(), maxConcurrentSourceReads)
		for index := range releases {
			releases[index], err = reader.acquireSourceSlot()
			require.NoError(t, err)
		}
		t.Cleanup(func() {
			for _, release := range releases {
				if release != nil {
					release()
				}
			}
			readers.Wait()
		})
		readers.Add(1)
		go func() {
			defer readers.Done()
			actual := make([]byte, len(expected))
			_, err := reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}()
		synctest.Wait()
		require.Empty(t, source.requests(), "window is waiting for source admission")
		// Simulate an independent reader publishing the same verified content
		// while this reader has no source slot; no provider GET is then needed.
		for index, entry := range reader.root.Entries {
			payload := bytes.Clone(expected[index*adaptiveRangeBytes : (index+1)*adaptiveRangeBytes])
			require.Equal(t, entry.Object.Checksum, digest.FromBytes(payload).String())
			reader.cache.addVerified(rangeCacheKey(entry.Object), payload)
		}
		releases[0]()
		releases[0] = nil
		readers.Wait()
		require.Empty(t, source.requests())
	})
}

func TestAdaptiveReadTrimmedTransportFailuresDoNotPopulateCache(t *testing.T) {
	for _, kind := range []string{"short", "excess", "terminal-error", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			source, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
			reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
			require.NoError(t, err)
			for index, entry := range reader.root.Entries {
				if index != 7 {
					reader.cache.addVerified(rangeCacheKey(entry.Object), bytes.Clone(expected[index*adaptiveRangeBytes:(index+1)*adaptiveRangeBytes]))
				}
			}
			missing := reader.root.Entries[7]
			payload := bytes.Clone(expected[7*adaptiveRangeBytes : 8*adaptiveRangeBytes])
			var stream io.Reader
			switch kind {
			case "short":
				stream = bytes.NewReader(payload[:len(payload)-1])
			case "excess":
				stream = bytes.NewReader(append(payload, 0))
			case "terminal-error":
				stream = &terminalRangeReader{payload: payload, err: errors.New("terminal trimmed read error")}
			case "corrupt":
				payload[0] ^= 1
				stream = bytes.NewReader(payload)
			}
			body := &observedRangeBody{Reader: stream}
			provider := &partialCacheContractSource{body: body}
			reader.source = provider
			result, err := reader.readCoalesced(reader.root.Entries)
			if kind == "corrupt" {
				require.NoError(t, err, "independent fragments retain their verification status")
				require.False(t, result.valid[7])
			} else {
				require.Error(t, err)
				require.Nil(t, result.payload, "transport failure cannot expose a partial merged buffer")
			}
			require.True(t, body.closed)
			require.LessOrEqual(t, body.bytesRead, adaptiveRangeBytes+1)
			require.Equal(t, adaptiveGet{7 * adaptiveRangeBytes, adaptiveRangeBytes}, provider.request)
			_, cached := reader.cache.get(rangeCacheKey(missing.Object))
			require.False(t, cached)
			for index, entry := range reader.root.Entries {
				if index != 7 {
					payload, ok := reader.cache.get(rangeCacheKey(entry.Object))
					require.True(t, ok)
					require.Equal(t, entry.Object.Checksum, digest.FromBytes(payload).String())
				}
			}
		})
	}
}

type partialCacheContractSource struct {
	body    io.ReadCloser
	request adaptiveGet
}

func TestAdaptiveReadTrimmedResultSurvivesCachedEdgeEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		base, _, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
		reader, err := NewReader(base, descriptor, 2*coalescedReadBytes)
		require.NoError(t, err)
		for _, index := range []int{0, adaptiveWindowRanges - 1} {
			_, err := reader.ReadAt(make([]byte, 512), int64(index*adaptiveRangeBytes))
			require.NoError(t, err)
		}
		source := &readerAdmissionSource{RangeSource: base, pack: base.pack, started: make(chan int64, 1), finish: make(chan struct{})}
		reader.source = source
		var readers sync.WaitGroup
		t.Cleanup(func() { close(source.finish); readers.Wait() })
		readers.Add(1)
		go func() {
			defer readers.Done()
			actual := make([]byte, len(expected))
			_, err := reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		}()
		synctest.Wait()
		require.Len(t, source.started, 1)
		for index := range 3 * adaptiveWindowRanges {
			payload := bytes.Repeat([]byte{byte(index + 64)}, adaptiveRangeBytes)
			reader.cache.addVerified(readCacheKey{checksum: digest.FromBytes(payload).String(), length: int64(len(payload))}, payload)
		}
		for _, index := range []int{0, adaptiveWindowRanges - 1} {
			_, found := reader.cache.get(rangeCacheKey(reader.root.Entries[index].Object))
			require.False(t, found, "cached edge must actually have been evicted")
		}
		source.finish <- struct{}{}
		readers.Wait()
		require.Equal(t, []adaptiveGet{{0, adaptiveRangeBytes}, {(adaptiveWindowRanges - 1) * adaptiveRangeBytes, adaptiveRangeBytes},
			{adaptiveRangeBytes, (adaptiveWindowRanges - 2) * adaptiveRangeBytes}}, base.requests(), "retained edges must not require another GET")
		require.LessOrEqual(t, reader.cache.bytes, reader.cache.maxBytes)
	})
}

func TestReadSourceRangeIntoRejectsMissingPayloadBuffer(t *testing.T) {
	reader := &Reader{}
	for _, size := range []int{0, 1} {
		require.ErrorContains(t, reader.readSourceRangeInto("unused", 0, make([]byte, size)), "payload and excess byte")
	}
}

func (s *partialCacheContractSource) Get(_ string, offset, length int64) (io.ReadCloser, error) {
	s.request = adaptiveGet{offset, length}
	return s.body, nil
}

func TestAdaptiveReadPartialCacheRetainsVerifiedBytes(t *testing.T) {
	source, store, descriptor, expected := adaptiveFixture(t, coalescedReadBytes, 1024)
	reader, err := NewReader(source, descriptor, DefaultReadCacheBytes)
	require.NoError(t, err)
	for _, index := range []int{0, 7, adaptiveWindowRanges - 1} {
		actual := make([]byte, 512)
		_, err := reader.ReadAt(actual, int64(index*adaptiveRangeBytes))
		require.NoError(t, err)
		store.objects[source.pack][index*adaptiveRangeBytes] ^= 1
	}
	actual := bytes.Repeat([]byte{0xee}, len(expected))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual, "cached immutable bytes remain authoritative despite corrupt redundant source bytes")
	require.Equal(t, adaptiveGet{adaptiveRangeBytes, (adaptiveWindowRanges - 2) * adaptiveRangeBytes}, source.requests()[3])
	for _, entry := range reader.root.Entries {
		payload, ok := reader.cache.get(rangeCacheKey(entry.Object))
		require.True(t, ok)
		require.Equal(t, entry.Object.Checksum, digest.FromBytes(payload).String())
		require.Less(t, cap(payload), 2*adaptiveRangeBytes, "cache entries must not retain a full bulk allocation")
	}
}
