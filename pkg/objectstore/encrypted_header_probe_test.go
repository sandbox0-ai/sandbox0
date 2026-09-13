package objectstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// Extend only JSON whitespace, preserving the original envelope fields, frame
// ciphertext, and AAD. This exercises existing v1 objects with large headers.
func paddedHeaderProbeObject(t *testing.T, raw []byte, headerEnd int) []byte {
	t.Helper()
	prefixEnd := len(encryptedObjectMagic) + 4
	oldEnd := prefixEnd + int(binary.BigEndian.Uint32(raw[len(encryptedObjectMagic):prefixEnd]))
	if headerEnd < oldEnd {
		t.Fatal("test header is shorter than the original")
	}
	out := append([]byte(nil), raw[:oldEnd]...)
	out = append(out, bytes.Repeat([]byte(" "), headerEnd-oldEnd)...)
	out = append(out, raw[oldEnd:]...)
	binary.BigEndian.PutUint32(out[len(encryptedObjectMagic):prefixEnd], uint32(headerEnd-prefixEnd))
	return out
}

func TestEncryptedHeaderProbeBoundsPrefixAndRemainder(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		for _, end := range []int{encryptedObjectHeaderProbeBytes - 1, encryptedObjectHeaderProbeBytes,
			encryptedObjectHeaderProbeBytes + 1, maxEncryptedObjectHeaderBytes + len(encryptedObjectMagic) + 4} {
			t.Run(fmt.Sprintf("%s/headerEnd=%d", algorithm, end), func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, algorithm, testEncryptedHeaderCacheConfig)
				store.cfg.ChunkSize = 4096
				plaintext := bytes.Repeat([]byte("0123456789abcdef"), 2048)
				putCacheTestObject(t, store, "pack", string(plaintext))
				raw := paddedHeaderProbeObject(t, rawCacheTestObject(t, base, "pack"), end)
				putCacheTestObject(t, base.ContextConditionalStore, "pack", string(raw))
				const offset, length = int64(16385), int64(8192)
				requireCacheTestRange(t, store, "pack", offset, length, string(plaintext[offset:offset+length]))
				want := []rangeReadCall{{off: 0, limit: encryptedObjectHeaderProbeBytes}}
				if end > encryptedObjectHeaderProbeBytes {
					want = append(want, rangeReadCall{off: encryptedObjectHeaderProbeBytes, limit: int64(end - encryptedObjectHeaderProbeBytes)})
				}
				want = append(want, rangeReadCall{off: int64(end) + 4*(4096+20), limit: 3 * (4096 + 20)})
				if got := base.calls(); !reflect.DeepEqual(got, want) || encryptor.unwraps.Load() != 1 {
					t.Fatalf("calls=%+v unwraps=%d, want %+v and 1", got, encryptor.unwraps.Load(), want)
				}
			})
		}
	}
}

type headerProbeFaultStore struct {
	ContextConditionalStore
	get func(context.Context, int64, int64) (io.ReadCloser, error)
}

func (s headerProbeFaultStore) GetContext(ctx context.Context, _ string, off, limit int64) (io.ReadCloser, error) {
	return s.get(ctx, off, limit)
}

type headerProbeErrorReader struct {
	io.Reader
	readErr, closeErr error
	closed            *int
}

func (r headerProbeErrorReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return r.Reader.Read(p)
}

func (r headerProbeErrorReader) Close() error {
	*r.closed++
	return r.closeErr
}

func TestEncryptedHeaderProbeFailsClosedOnMalformedRanges(t *testing.T) {
	for _, kind := range []string{"short-prefix", "zero-size", "oversized-header", "short-header",
		"short-remainder", "long-probe", "long-remainder", "invalid-json"} {
		t.Run(kind, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
			putCacheTestObject(t, store, "pack", "data")
			const headerEnd = encryptedObjectHeaderProbeBytes + 32
			raw := paddedHeaderProbeObject(t, rawCacheTestObject(t, base, "pack"), headerEnd)
			prefixEnd := len(encryptedObjectMagic) + 4
			wantCalls := 1
			switch kind {
			case "short-prefix":
				raw = raw[:prefixEnd-1]
			case "zero-size":
				binary.BigEndian.PutUint32(raw[len(encryptedObjectMagic):prefixEnd], 0)
			case "oversized-header":
				binary.BigEndian.PutUint32(raw[len(encryptedObjectMagic):prefixEnd], maxEncryptedObjectHeaderBytes+1)
			case "short-header":
				raw = raw[:encryptedObjectHeaderProbeBytes-1]
			case "short-remainder":
				raw, wantCalls = raw[:headerEnd-1], 2
			case "long-remainder":
				wantCalls = 2
			case "invalid-json":
				raw[prefixEnd], wantCalls = '!', 2
			}
			calls, closes := 0, 0
			store.store = headerProbeFaultStore{ContextConditionalStore: base.ContextConditionalStore,
				get: func(_ context.Context, off, limit int64) (io.ReadCloser, error) {
					calls++
					if kind == "long-probe" && off == 0 || kind == "long-remainder" && off != 0 {
						limit++ // Simulate a provider violating the requested range.
					}
					end := min(off+limit, int64(len(raw)))
					return headerProbeErrorReader{Reader: bytes.NewReader(raw[off:end]), closed: &closes}, nil
				}}
			_, _, _, err := store.readEncryptedObjectHeader(t.Context(), "pack", true)
			if err == nil || calls != wantCalls || closes != calls {
				t.Fatalf("err=%v calls=%d closes=%d, want failure and %d closed ranges", err, calls, closes, wantCalls)
			}
		})
	}
}

func TestEncryptedHeaderProbePreservesErrorsAndCancellation(t *testing.T) {
	for _, stage := range []int{1, 2} {
		for _, kind := range []string{"get", "read", "close", "cancel"} {
			t.Run(fmt.Sprintf("stage=%d/%s", stage, kind), func(t *testing.T) {
				store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
				putCacheTestObject(t, store, "pack", "data")
				raw := paddedHeaderProbeObject(t, rawCacheTestObject(t, base, "pack"), encryptedObjectHeaderProbeBytes+32)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				sentinel := errors.New("test provider failure")
				wantErr := sentinel
				if kind == "cancel" {
					wantErr = context.Canceled
				}
				calls, closes := 0, 0
				store.store = headerProbeFaultStore{ContextConditionalStore: base.ContextConditionalStore,
					get: func(gotCtx context.Context, off, limit int64) (io.ReadCloser, error) {
						calls++
						if gotCtx != ctx {
							t.Fatal("provider did not receive the caller context")
						}
						reader := headerProbeErrorReader{Reader: bytes.NewReader(raw[off:min(off+limit, int64(len(raw)))]), closed: &closes}
						if calls == stage {
							switch kind {
							case "get":
								return nil, sentinel
							case "read":
								reader.readErr = sentinel
							case "close":
								reader.closeErr = sentinel
							case "cancel":
								cancel()
							}
						}
						return reader, nil
					}}
				_, _, _, err := store.readEncryptedObjectHeader(ctx, "pack", true)
				wantCloses := stage
				if kind == "get" {
					wantCloses--
				}
				if !errors.Is(err, wantErr) || calls != stage || closes != wantCloses {
					t.Fatalf("err=%v calls=%d closes=%d, want %v, %d and %d", err, calls, closes, wantErr, stage, wantCloses)
				}
			})
		}
	}
}
