package objectstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

var prefixReadTestCache = EncryptedHeaderCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20, MaxPrefixBytes: 256 << 10}

func TestEncryptedPrefixReadColdMappingUsesOneGETWithoutCachingCiphertext(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		for _, length := range []int{1, 14132, 64 << 10, 193568} {
			t.Run(fmt.Sprintf("%s/bytes=%d", algorithm, length), func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, algorithm, prefixReadTestCache)
				store.cfg.ChunkSize = 64 << 10
				payload := bytes.Repeat([]byte{0xa5}, length)
				putCacheTestObject(t, store, "mapping", string(payload))
				requireCacheTestRange(t, store, "mapping", 0, int64(length), string(payload))
				calls := base.calls()
				if len(calls) != 1 || calls[0].off != 0 || calls[0].limit > prefixReadTestCache.MaxPrefixBytes || encryptor.unwraps.Load() != 1 {
					t.Fatalf("cold calls=%+v unwraps=%d", calls, encryptor.unwraps.Load())
				}
				// Header-cache hits still read data. Verified plaintext/mapping
				// caching belongs to the caller, not the encryption wrapper.
				requireCacheTestRange(t, store, "mapping", 0, int64(length), string(payload))
				calls = base.calls()
				if len(calls) != 2 || calls[1].off <= 0 || encryptor.unwraps.Load() != 1 {
					t.Fatalf("warm calls=%+v unwraps=%d", calls, encryptor.unwraps.Load())
				}
			})
		}
	}
}

func TestEncryptedPrefixReadContinuesStoredGeometryWithoutOverlap(t *testing.T) {
	for _, geometry := range []struct {
		name       string
		chunk      int64
		headerEnd  int
		length     int64
		objectSize int
	}{
		{"old-1m-frame", 1 << 20, 0, 193568, 2 << 20},
		{"small-frames", 8, 0, 40000, 80000},
		{"prefix-cap", 64 << 10, 0, 256 << 10, 512 << 10},
		{"header-shifts-frames", 64 << 10, 2000, 64 << 10, 512 << 10},
		{"header-larger-than-probe", 64 << 10, 300000, 64 << 10, 512 << 10},
		{"short-final-frame", 1 << 20, 0, 193568, 193568},
		{"exact-probe-eof", 64 << 10, 2560, 64000, 64000},
	} {
		t.Run(geometry.name, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", prefixReadTestCache)
			store.cfg.ChunkSize = 64 << 10
			writerConfig := store.cfg
			writerConfig.ChunkSize = geometry.chunk
			payload := bytes.Repeat([]byte("0123456789abcdef"), (geometry.objectSize+15)/16)[:geometry.objectSize]
			putCacheTestObject(t, Encrypting(base, writerConfig), "pack", string(payload))
			if geometry.headerEnd > 0 {
				raw := paddedHeaderProbeObject(t, rawCacheTestObject(t, base, "pack"), geometry.headerEnd)
				putCacheTestObject(t, base.ContextConditionalStore, "pack", string(raw))
			}
			requireCacheTestRange(t, store, "pack", 0, geometry.length, string(payload[:geometry.length]))
			calls := base.calls()
			if calls[0].off != 0 || calls[0].limit > prefixReadTestCache.MaxPrefixBytes {
				t.Fatalf("unbounded first range: %+v", calls)
			}
			if geometry.name == "short-final-frame" || geometry.name == "exact-probe-eof" {
				if len(calls) != 1 {
					t.Fatalf("complete object re-fetched: %+v", calls)
				}
			} else {
				for i := 1; i < len(calls); i++ {
					if calls[i].off != calls[i-1].off+calls[i-1].limit || calls[i].limit <= 0 {
						t.Fatalf("noncontiguous/overlapping continuation: %+v", calls)
					}
				}
				if len(calls) < 2 || len(calls) > 3 {
					t.Fatalf("unexpected continuation count: %+v", calls)
				}
			}
		})
	}
}

func TestEncryptedPrefixReadBoundsAndNonzeroOffsets(t *testing.T) {
	store, base, _ := newCacheTestStore(t, "", prefixReadTestCache)
	store.cfg.ChunkSize = 64 << 10
	for _, test := range []struct{ off, limit, want int64 }{
		{0, 0, 1024}, {0, -1, 1024}, {1, 1, 1024}, {0, (256 << 10) + 1, 1024},
		{0, 1, 1024 + (64 << 10) + 20}, {0, 256 << 10, 256 << 10},
	} {
		if got := store.prefixProbeBytes(test.off, test.limit); got != test.want {
			t.Fatalf("probe(%d,%d)=%d, want %d", test.off, test.limit, got, test.want)
		}
	}
	store.headerCache.cfg.MaxPrefixBytes = maxInt64
	store.cfg.ChunkSize = 1
	if got := store.prefixProbeBytes(0, 1<<20); got != 1<<20 {
		t.Fatalf("unbounded configured prefix: %d", got)
	}
	store.cfg.ChunkSize = maxInt64
	if got := store.prefixProbeBytes(0, 1); got != 1024 {
		t.Fatalf("invalid writer geometry used: %d", got)
	}
	store.cfg.ChunkSize = 64 << 10
	putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 2<<20)))
	requireCacheTestRange(t, store, "pack", (1<<20)+1, 1, string([]byte{7}))
	if calls := base.calls(); len(calls) != 2 || calls[0].limit != 1024 || calls[1].off < 1<<20 {
		t.Fatalf("nonzero demand prefetched preceding data: %+v", calls)
	}
}

func TestEncryptedPrefixReadWaitersHaveIndependentCancellation(t *testing.T) {
	for _, cancelLeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelLeader=%t", cancelLeader), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, "", prefixReadTestCache)
				store.cfg.ChunkSize = 64 << 10
				putCacheTestObject(t, store, "mapping", "contents")
				release := make(chan struct{})
				base.beforeGet = func(ctx context.Context, _ string, off int64) error {
					if off == 0 {
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}
				canceled, cancel := context.WithCancel(t.Context())
				defer cancel()
				leaderCtx, followerCtx := t.Context(), canceled
				if cancelLeader {
					leaderCtx, followerCtx = canceled, t.Context()
				}
				leader, follower := make(chan error, 1), make(chan error, 1)
				read := func(ctx context.Context, result chan error) {
					got, err := readCacheTestRange(ctx, store, "mapping", 0, 8)
					if err == nil && got != "contents" {
						err = fmt.Errorf("wrong content %q", got)
					}
					result <- err
				}
				go read(leaderCtx, leader)
				synctest.Wait()
				go read(followerCtx, follower)
				synctest.Wait()
				cancel()
				failed, surviving, wantGets := follower, leader, 1
				if cancelLeader {
					failed, surviving, wantGets = leader, follower, 2
				}
				if err := <-failed; !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled waiter: %v", err)
				}
				close(release)
				if err := <-surviving; err != nil {
					t.Fatal(err)
				}
				if len(base.calls()) != wantGets || encryptor.unwraps.Load() != 1 {
					t.Fatalf("gets=%d unwraps=%d, want %d/1", len(base.calls()), encryptor.unwraps.Load(), wantGets)
				}
			})
		})
	}
}

func TestEncryptedPrefixReadMutationFencesCapturedCiphertext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", prefixReadTestCache)
		store.cfg.ChunkSize = 64 << 10
		putCacheTestObject(t, store, "mapping", "contents")
		var gets atomic.Int64
		release := make(chan struct{})
		store.store = headerProbeFaultStore{ContextConditionalStore: base,
			get: func(ctx context.Context, off, limit int64) (io.ReadCloser, error) {
				reader, err := base.GetContext(ctx, "mapping", off, limit)
				if gets.Add(1) == 1 {
					// memoryStore has already captured the original ciphertext.
					<-release
				}
				return reader, err
			}}
		result := make(chan error, 1)
		go func() {
			got, err := readCacheTestRange(t.Context(), store, "mapping", 0, 8)
			if err == nil && got != "contents" {
				err = fmt.Errorf("wrong content %q", got)
			}
			result <- err
		}()
		synctest.Wait()
		putCacheTestObject(t, store, "mapping", "contents")
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if gets.Load() != 2 {
			t.Fatalf("invalidated load was reused or prefix was mixed: gets=%d", gets.Load())
		}
	})
}

func TestEncryptedPrefixReadAuthenticatesAndDoesNotRetryAfterAuthenticatedFrame(t *testing.T) {
	for _, corruption := range []string{"first-frame", "later-frame", "wrong-key", "truncated-frame"} {
		t.Run(corruption, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", prefixReadTestCache)
			store.cfg.ChunkSize = 64 << 10
			payload := bytes.Repeat([]byte{9}, 128<<10)
			putCacheTestObject(t, store, "pack", string(payload))
			raw := rawCacheTestObject(t, base, "pack")
			prefixEnd := len(encryptedObjectMagic) + 4
			headerEnd := prefixEnd + int(binary.BigEndian.Uint32(raw[len(encryptedObjectMagic):prefixEnd]))
			key, wantGets := "pack", 2
			switch corruption {
			case "first-frame":
				raw[headerEnd+20] ^= 1
			case "later-frame":
				raw[headerEnd+(64<<10)+20+20] ^= 1
				wantGets = 1
			case "wrong-key":
				key = "different-object"
			case "truncated-frame":
				raw = raw[:headerEnd+30]
			}
			putCacheTestObject(t, base.ContextConditionalStore, key, string(raw))
			got, err := readCacheTestRange(t.Context(), store, key, 0, int64(len(payload)))
			if err == nil {
				t.Fatal("corrupt prefix accepted")
			}
			if corruption != "later-frame" && got != "" {
				t.Fatal("unauthenticated plaintext escaped")
			}
			if corruption == "later-frame" && !bytes.Equal([]byte(got), payload[:64<<10]) {
				t.Fatal("unexpected authenticated prefix")
			}
			if len(base.calls()) != wantGets {
				t.Fatalf("gets=%+v, want %d (no unchanged-envelope data retry)", base.calls(), wantGets)
			}
		})
	}
}
