package rootfsblock

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestReadRangePreservesLengthAndTerminalErrorChecks(t *testing.T) {
	payload := bytes.Repeat([]byte{0x6d}, LogicalBlockSize)
	object := ObjectRange{Key: "packs/contract", Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	terminalErr := errors.New("terminal source error")
	tests := []struct {
		name        string
		reader      func() io.Reader
		wantErr     error
		wantMessage string
	}{
		{name: "exact", reader: func() io.Reader { return bytes.NewReader(payload) }},
		{name: "fragmented", reader: func() io.Reader { return iotest.OneByteReader(bytes.NewReader(payload)) }},
		{name: "data-with-eof", reader: func() io.Reader { return &terminalRangeReader{payload: payload, err: io.EOF} }},
		{name: "empty", reader: func() io.Reader { return bytes.NewReader(nil) }, wantMessage: "expected 4096"},
		{name: "short", reader: func() io.Reader { return bytes.NewReader(payload[:len(payload)-1]) }, wantMessage: "expected 4096"},
		{name: "excess", reader: func() io.Reader { return bytes.NewReader(append(bytes.Clone(payload), 0)) }, wantMessage: "expected 4096"},
		{name: "large-excess", reader: func() io.Reader { return bytes.NewReader(bytes.Repeat(payload, 2)) }, wantMessage: "expected 4096"},
		{name: "corrupt", reader: func() io.Reader { return bytes.NewReader(make([]byte, len(payload))) }, wantMessage: "checksum mismatch"},
		{name: "error-with-last-byte", reader: func() io.Reader { return &terminalRangeReader{payload: payload, err: terminalErr} }, wantErr: terminalErr},
		{name: "unexpected-eof-with-last-byte", reader: func() io.Reader { return &terminalRangeReader{payload: payload, err: io.ErrUnexpectedEOF} }, wantErr: io.ErrUnexpectedEOF},
		{name: "error-after-last-byte", reader: func() io.Reader {
			return io.MultiReader(bytes.NewReader(payload), &terminalRangeReader{err: terminalErr})
		}, wantErr: terminalErr},
		{name: "error-with-excess-byte", reader: func() io.Reader {
			return &terminalRangeReader{payload: append(bytes.Clone(payload), 0), err: terminalErr}
		}, wantErr: terminalErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &observedRangeBody{Reader: test.reader()}
			store := &contractRangeSource{body: body}
			cache, err := NewReadCache(DefaultReadCacheBytes)
			require.NoError(t, err)
			reader := &Reader{source: store, cache: cache}
			actual, err := reader.readRange(object)
			require.True(t, body.closed, "every obtained body must be closed")
			require.LessOrEqual(t, body.bytesRead, len(payload)+1, "excess detection must stay bounded")
			cached, found := cache.get(rangeCacheKey(object))
			if test.wantErr != nil || test.wantMessage != "" {
				require.Error(t, err)
				if test.wantErr != nil {
					require.ErrorIs(t, err, test.wantErr)
				}
				if test.wantMessage != "" {
					require.ErrorContains(t, err, test.wantMessage)
				}
				require.Nil(t, actual)
				require.False(t, found, "failed or incomplete reads must not populate the verified cache")
				return
			}
			require.NoError(t, err)
			require.Equal(t, payload, actual)
			require.True(t, found)
			require.Equal(t, payload, cached)
		})
	}
}

func TestReaderKeepsVerifiedBytesPrivateFromCallerBuffers(t *testing.T) {
	store, descriptor, _, pack := coalescingFixture(t)
	cache, err := NewReadCache(DefaultReadCacheBytes)
	require.NoError(t, err)
	reader, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	entry, _, found, err := reader.resolve(reader.root, 0)
	require.NoError(t, err)
	require.True(t, found)
	verified, err := reader.readRange(entry.Object)
	require.NoError(t, err)
	cached, found := cache.get(rangeCacheKey(entry.Object))
	require.True(t, found)
	require.Same(t, &verified[0], &cached[0], "cache admission must not copy an owned verified range")

	caller := make([]byte, len(verified))
	_, err = reader.ReadAt(caller, 0)
	require.NoError(t, err)
	expected := bytes.Clone(caller)
	clear(caller)
	other, err := NewReaderWithCache(store, descriptor, cache)
	require.NoError(t, err)
	_, err = other.ReadAt(caller, 0)
	require.NoError(t, err)
	require.Equal(t, expected, caller, "caller buffers must not alias shared verified bytes")
	require.Equal(t, 1, store.count(pack))
}

func TestReadCacheRetainedPayloadSurvivesConcurrentEviction(t *testing.T) {
	store := newRangeTestStore()
	expected := bytes.Repeat([]byte{0x6d}, LogicalBlockSize)
	object := store.put("packs/retained", expected)
	cache, err := NewReadCache(2 * LogicalBlockSize)
	require.NoError(t, err)
	reader := &Reader{source: store, cache: cache}
	retained, err := reader.readRange(object)
	require.NoError(t, err)

	done := make(chan struct{})
	t.Cleanup(func() { <-done })
	go func() {
		defer close(done)
		for index := range 64 {
			payload := bytes.Repeat([]byte{byte(index)}, LogicalBlockSize)
			key := readCacheKey{checksum: digest.FromBytes(payload).String(), length: int64(len(payload))}
			cache.addVerified(key, payload)
		}
	}()
	for range 64 {
		require.Equal(t, object.Checksum, digest.FromBytes(retained).String())
	}
	<-done
	_, found := cache.get(rangeCacheKey(object))
	require.False(t, found, "the small cache must have evicted the retained range")
	require.Equal(t, expected, retained, "eviction must not clear or recycle a buffer still held by a reader")
	require.LessOrEqual(t, cache.bytes, cache.maxBytes)
}

type terminalRangeReader struct {
	payload []byte
	err     error
}

func (r *terminalRangeReader) Read(target []byte) (int, error) {
	n := copy(target, r.payload)
	r.payload = r.payload[n:]
	if len(r.payload) == 0 {
		err := r.err
		r.err = io.EOF
		return n, err
	}
	return n, nil
}

type observedRangeBody struct {
	io.Reader
	closed    bool
	bytesRead int
}

func (r *observedRangeBody) Read(target []byte) (int, error) {
	n, err := r.Reader.Read(target)
	r.bytesRead += n
	return n, err
}

func (r *observedRangeBody) Close() error { r.closed = true; return nil }

type contractRangeSource struct{ body io.ReadCloser }

func (s *contractRangeSource) Get(string, int64, int64) (io.ReadCloser, error) { return s.body, nil }
