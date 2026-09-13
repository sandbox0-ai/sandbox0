package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

var testEncryptedHeaderCacheConfig = EncryptedHeaderCacheConfig{MaxEntries: 4, MaxBytes: 1 << 20}

type cacheTestEncryptor struct {
	Encryptor
	unwraps atomic.Int64
}

func (e *cacheTestEncryptor) Decrypt(in []byte) ([]byte, error) {
	e.unwraps.Add(1)
	return e.Encryptor.Decrypt(in)
}

type cacheTestStore struct {
	ContextConditionalStore
	mu        sync.Mutex
	gets      []rangeReadCall
	beforeGet func(context.Context, string, int64) error
}

func (s *cacheTestStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), key, off, limit)
}

func (s *cacheTestStore) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.gets = append(s.gets, rangeReadCall{off: off, limit: limit})
	s.mu.Unlock()
	if s.beforeGet != nil {
		if err := s.beforeGet(ctx, key, off); err != nil {
			return nil, err
		}
	}
	return s.ContextConditionalStore.GetContext(ctx, key, off, limit)
}

func (s *cacheTestStore) calls() []rangeReadCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rangeReadCall(nil), s.gets...)
}

func newCacheTestStore(t *testing.T, algorithm string, cache EncryptedHeaderCacheConfig) (*encryptedStore, *cacheTestStore, *cacheTestEncryptor) {
	t.Helper()
	base := &cacheTestStore{ContextConditionalStore: NewMemoryStore(t.Name()).(ContextConditionalStore)}
	encryptor := &cacheTestEncryptor{Encryptor: reversibleTestEncryptor{}}
	store := EncryptingImmutable(base, EncryptionConfig{
		Enabled: true, Algorithm: algorithm, KeyEncryptor: encryptor, ChunkSize: 8,
	}, cache).(*encryptedStore)
	return store, base, encryptor
}

func readCacheTestRange(ctx context.Context, store ContextConditionalStore, key string, off, limit int64) (string, error) {
	reader, err := store.GetContext(ctx, key, off, limit)
	if err != nil {
		return "", err
	}
	payload, err := io.ReadAll(reader)
	return string(payload), errors.Join(err, reader.Close())
}

func requireCacheTestRange(t *testing.T, store ContextConditionalStore, key string, off, limit int64, want string) {
	t.Helper()
	got, err := readCacheTestRange(t.Context(), store, key, off, limit)
	if err != nil || got != want {
		t.Fatalf("GetContext(%q, %d, %d) = %q, %v, want %q", key, off, limit, got, err, want)
	}
}

func putCacheTestObject(t *testing.T, store Store, key, payload string) {
	t.Helper()
	if err := store.Put(key, strings.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
}

func TestImmutableEncryptedHeaderCacheReusesHeadersAndAEADAcrossRanges(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			store, base, encryptor := newCacheTestStore(t, algorithm, testEncryptedHeaderCacheConfig)
			privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			encryptor.Encryptor = newRSAEncryptor(privateKey)
			const payload = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
			// The original, uncached writer creates the existing v1 object.
			putCacheTestObject(t, Encrypting(base, store.cfg), "pack", payload)
			requireCacheTestRange(t, store, "pack", 33, 5, payload[33:38])
			calls := base.calls()
			if len(calls) != 2 || calls[0] != (rangeReadCall{off: 0, limit: encryptedObjectHeaderProbeBytes}) {
				t.Fatalf("first read = %+v, want one bounded header probe and one data GET", calls)
			}
			raw := rawCacheTestObject(t, base, "pack")
			prefixEnd := len(encryptedObjectMagic) + 4
			headerEnd := int64(prefixEnd) + int64(binary.BigEndian.Uint32(raw[len(encryptedObjectMagic):prefixEnd]))
			requireCacheTestRange(t, store, "pack", 9, 13, payload[9:22])
			calls = base.calls()
			want := rangeReadCall{off: headerEnd + 28, limit: 56}
			if len(calls) != 3 || calls[2] != want || encryptor.unwraps.Load() != 1 {
				t.Fatalf("later read = %+v, unwraps = %d; want one data GET %+v and one total unwrap", calls, encryptor.unwraps.Load(), want)
			}
			requireCacheTestRange(t, store, "pack", 0, -1, payload)
			if len(base.calls()) != 4 || encryptor.unwraps.Load() != 1 {
				t.Fatal("full read did not reuse cached crypto state")
			}
		})
	}
}

func TestImmutableEncryptedHeaderCacheBoundedLRUEviction(t *testing.T) {
	for _, test := range []struct {
		name string
		cfg  EncryptedHeaderCacheConfig
	}{
		{"entries", EncryptedHeaderCacheConfig{MaxEntries: 2, MaxBytes: 1 << 20}},
		{"bytes", EncryptedHeaderCacheConfig{MaxEntries: 10, MaxBytes: 5000}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, base, encryptor := newCacheTestStore(t, "", test.cfg)
			for _, key := range []string{"a", "b", "c"} {
				putCacheTestObject(t, store, key, "0123456789")
			}
			for _, key := range []string{"a", "b", "a", "c", "a"} {
				requireCacheTestRange(t, store, key, 0, 1, "0")
			}
			if len(base.calls()) != 8 || encryptor.unwraps.Load() != 3 {
				t.Fatalf("unexpected LRU hits: calls=%d unwraps=%d", len(base.calls()), encryptor.unwraps.Load())
			}
			requireCacheTestRange(t, store, "b", 0, 1, "0")
			if len(base.calls()) != 10 || encryptor.unwraps.Load() != 4 {
				t.Fatal("least recently used header was not evicted")
			}
			cache := store.headerCache
			cache.mu.Lock()
			defer cache.mu.Unlock()
			if len(cache.entries) > 2 || cache.bytes > test.cfg.MaxBytes {
				t.Fatalf("cache exceeds bounds: entries=%d bytes=%d", len(cache.entries), cache.bytes)
			}
		})
	}
}

func TestImmutableEncryptedHeaderCacheDisabledOrOversized(t *testing.T) {
	for _, cfg := range []EncryptedHeaderCacheConfig{
		{}, {MaxEntries: -1, MaxBytes: 4096}, {MaxEntries: 2}, {MaxEntries: 2, MaxBytes: 1},
	} {
		t.Run(fmt.Sprint(cfg), func(t *testing.T) {
			store, base, encryptor := newCacheTestStore(t, "", cfg)
			putCacheTestObject(t, store, "pack", "0123456789")
			for range 2 {
				requireCacheTestRange(t, store, "pack", 0, 1, "0")
			}
			if len(base.calls()) != 4 || encryptor.unwraps.Load() != 2 {
				t.Fatal("disabled or oversized cache retained metadata")
			}
		})
	}
}

func TestImmutableEncryptedHeaderCacheCoalescesConcurrentMisses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		const payload = "0123456789abcdefghijklmnopqrstuvwxyz"
		putCacheTestObject(t, store, "pack", payload)
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
		const callers = 16
		results := make(chan error, callers)
		for i := range callers {
			go func() {
				got, err := readCacheTestRange(t.Context(), store, "pack", int64(i), 9)
				if err == nil && got != payload[i:i+9] {
					err = fmt.Errorf("range %d = %q", i, got)
				}
				results <- err
			}()
		}
		synctest.Wait()
		if len(base.calls()) != 1 {
			t.Fatalf("concurrent misses issued %d prefix GETs", len(base.calls()))
		}
		close(release)
		for range callers {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if len(base.calls()) != callers+1 || encryptor.unwraps.Load() != 1 {
			t.Fatalf("calls=%d unwraps=%d, want %d and 1", len(base.calls()), encryptor.unwraps.Load(), callers+1)
		}
	})
}

func TestImmutableEncryptedHeaderCacheCancellationIsPerCaller(t *testing.T) {
	for _, cancelLeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelLeader=%v", cancelLeader), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
				putCacheTestObject(t, store, "pack", "0123456789")
				release := make(chan struct{})
				base.beforeGet = func(ctx context.Context, _ string, off int64) error {
					if off == 0 {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-release:
						}
					}
					return nil
				}
				canceledCtx, cancel := context.WithCancel(t.Context())
				defer cancel()
				leaderCtx, followerCtx := t.Context(), canceledCtx
				if cancelLeader {
					leaderCtx, followerCtx = canceledCtx, t.Context()
				}
				leader, follower := make(chan error, 1), make(chan error, 1)
				go func() { _, err := readCacheTestRange(leaderCtx, store, "pack", 0, 1); leader <- err }()
				synctest.Wait()
				go func() { _, err := readCacheTestRange(followerCtx, store, "pack", 8, 1); follower <- err }()
				synctest.Wait()
				cancel()
				canceled, surviving := follower, leader
				if cancelLeader {
					canceled, surviving = leader, follower
				}
				if err := <-canceled; !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled caller = %v", err)
				}
				close(release)
				if err := <-surviving; err != nil {
					t.Fatalf("surviving caller = %v", err)
				}
				if len(base.calls()) != 2 || encryptor.unwraps.Load() != 1 {
					t.Fatal("one canceled waiter canceled or duplicated the shared load")
				}
				// Cancellation also applies to an already populated cache.
				if _, err := store.GetContext(canceledCtx, "pack", 0, 1); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled cache hit = %v", err)
				}
				if len(base.calls()) != 2 {
					t.Fatal("canceled cache hit performed I/O")
				}
			})
		})
	}
}

func TestImmutableEncryptedHeaderCacheAllCanceledStopsLoadAndAllowsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		putCacheTestObject(t, store, "pack", "0123456789")
		var attempts atomic.Int64
		stopped := make(chan struct{})
		base.beforeGet = func(ctx context.Context, _ string, off int64) error {
			if off == 0 && attempts.Add(1) == 1 {
				<-ctx.Done()
				close(stopped)
				return ctx.Err()
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := readCacheTestRange(ctx, store, "pack", 0, 1); result <- err }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled load = %v", err)
		}
		<-stopped
		requireCacheTestRange(t, store, "pack", 8, 1, "8")
		if len(base.calls()) != 3 || encryptor.unwraps.Load() != 1 {
			t.Fatal("canceled load poisoned retry")
		}
	})
}

func TestImmutableEncryptedHeaderCacheCanceledRangeReleasesDecryptor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		putCacheTestObject(t, store, "pack", "0123456789abcdef")
		requireCacheTestRange(t, store, "pack", 0, 1, "0")
		ctx, cancel := context.WithCancel(t.Context())
		reader, err := store.GetContext(ctx, "pack", 8, 8)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		// The decryptor is blocked writing to its consumer, not loading headers.
		synctest.Wait()
		cancel()
		synctest.Wait()
		if _, err := io.ReadAll(reader); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled range read = %v", err)
		}
		requireCacheTestRange(t, store, "pack", 8, 8, "89abcdef")
		if len(base.calls()) != 4 || encryptor.unwraps.Load() != 1 {
			t.Fatal("range cancellation evicted valid immutable metadata")
		}
	})
}

func TestImmutableEncryptedHeaderCacheBoundsActiveLoads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", EncryptedHeaderCacheConfig{MaxEntries: 1, MaxBytes: 1 << 20})
		for _, key := range []string{"a", "b"} {
			putCacheTestObject(t, store, key, "0123456789")
		}
		release := make(chan struct{})
		base.beforeGet = func(_ context.Context, _ string, off int64) error {
			if off == 0 {
				// An uncooperative provider must still occupy its loader slot.
				<-release
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		first := make(chan error, 1)
		go func() { _, err := readCacheTestRange(ctx, store, "a", 0, 1); first <- err }()
		synctest.Wait()
		cancel()
		if err := <-first; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		waitingCtx, cancelWaiting := context.WithCancel(t.Context())
		waiting := make(chan error, 1)
		go func() { _, err := readCacheTestRange(waitingCtx, store, "b", 0, 1); waiting <- err }()
		synctest.Wait()
		if len(base.calls()) != 1 {
			t.Fatal("canceled but active loader no longer counted toward bound")
		}
		cancelWaiting()
		if err := <-waiting; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		close(release)
		requireCacheTestRange(t, store, "b", 0, 1, "0")
	})
}

func TestEncryptedHeaderCacheGenericMutableStoreRemainsUncached(t *testing.T) {
	immutable, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
	store := Encrypting(base, immutable.cfg).(ContextConditionalStore)
	putCacheTestObject(t, store, "mutable", "first value")
	for range 2 {
		requireCacheTestRange(t, store, "mutable", 0, 5, "first")
	}
	if len(base.calls()) != 4 || encryptor.unwraps.Load() != 2 {
		t.Fatal("generic mutable store unexpectedly cached metadata")
	}
	putCacheTestObject(t, Encrypting(base, immutable.cfg), "mutable", "new payload")
	// A separate writer changes the encrypted bytes, including key and nonce.
	requireCacheTestRange(t, store, "mutable", 0, -1, "new payload")
}

func TestImmutableEncryptedHeaderCacheInvalidatesMutations(t *testing.T) {
	store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
	putCacheTestObject(t, store, "pack", "first value")
	requireCacheTestRange(t, store, "pack", 0, 5, "first")
	putCacheTestObject(t, store, "pack", "second value")
	requireCacheTestRange(t, store, "pack", 0, 6, "second")
	if len(base.calls()) != 4 {
		t.Fatal("overwrite did not invalidate cached header")
	}
	// The underlying memory store normalizes a leading slash on deletion.
	if err := store.Delete("/pack"); err != nil {
		t.Fatal(err)
	}
	created, err := store.PutIfAbsentContext(t.Context(), "pack", strings.NewReader("third value"))
	if err != nil || !created {
		t.Fatalf("recreate = %v, %v", created, err)
	}
	requireCacheTestRange(t, store, "pack", 0, 5, "third")
	created, err = store.PutIfAbsent("pack", strings.NewReader("conflict"))
	if err != nil || created {
		t.Fatalf("conditional collision = %v, %v", created, err)
	}
	requireCacheTestRange(t, store, "pack", 0, 5, "third")
}

func TestImmutableEncryptedHeaderCacheMutationsPreserveUnrelatedHeaders(t *testing.T) {
	store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
	putCacheTestObject(t, store, "hot-pack", "0123456789")
	requireCacheTestRange(t, store, "hot-pack", 0, 1, "0")
	for i := range 20 {
		key := fmt.Sprintf("materialized-%d", i)
		putCacheTestObject(t, store, key, "new data")
		requireCacheTestRange(t, store, "hot-pack", 8, 1, "8")
		if err := store.Delete(key); err != nil {
			t.Fatal(err)
		}
		requireCacheTestRange(t, store, "hot-pack", 0, 1, "0")
	}
	if len(base.calls()) != 42 || encryptor.unwraps.Load() != 1 {
		t.Fatalf("unrelated writes evicted a hot header: gets=%d unwraps=%d", len(base.calls()), encryptor.unwraps.Load())
	}
	// Even hash collisions must retain completed entries. Mutation state has
	// fixed size rather than retaining one entry for every historical object key.
	var collision string
	for i := 0; collision == ""; i++ {
		candidate := fmt.Sprintf("collision-%d", i)
		if store.headerCache.fence(candidate) == store.headerCache.fence("hot-pack") {
			collision = candidate
		}
	}
	finish := store.headerCache.beginMutation(collision)
	requireCacheTestRange(t, store, "hot-pack", 8, 1, "8")
	finish()
	requireCacheTestRange(t, store, "hot-pack", 0, 1, "0")
	if len(base.calls()) != 44 || encryptor.unwraps.Load() != 1 {
		t.Fatal("a mutation stripe collision evicted an unrelated cached header")
	}
}

func TestImmutableEncryptedHeaderCacheCrossWrapperGCRecreation(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		for _, chunkSize := range []int64{8, 64} {
			t.Run(fmt.Sprintf("%s/chunk=%d", algorithm, chunkSize), func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
				const payload = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
				key := fmt.Sprintf("rootfs/packs/sha256/%x", sha256.Sum256([]byte(payload)))
				writer := Encrypting(base, store.cfg).(ContextConditionalStore)
				if created, err := writer.PutIfAbsentContext(t.Context(), key, strings.NewReader(payload)); err != nil || !created {
					t.Fatalf("first publication = %v, %v", created, err)
				}
				requireCacheTestRange(t, store, key, 9, 5, payload[9:14])
				original := rawCacheTestObject(t, base, key)
				// Match GC and ObjectStorePublisher: another wrapper deletes the
				// content key, then conditionally recreates the same plaintext.
				if err := writer.Delete(key); err != nil {
					t.Fatal(err)
				}
				cfg := store.cfg
				cfg.Algorithm, cfg.ChunkSize = algorithm, chunkSize
				writer = Encrypting(base, cfg).(ContextConditionalStore)
				if created, err := writer.PutIfAbsentContext(t.Context(), key, strings.NewReader(payload)); err != nil || !created {
					t.Fatalf("republication = %v, %v", created, err)
				}
				if bytes.Equal(original, rawCacheTestObject(t, base, key)) {
					t.Fatal("test did not produce a fresh encryption envelope")
				}
				requireCacheTestRange(t, store, key, 41, 11, payload[41:52])
				if len(base.calls()) != 5 || encryptor.unwraps.Load() != 2 {
					t.Fatalf("recreated object: gets=%d unwraps=%d, want 5 and 2", len(base.calls()), encryptor.unwraps.Load())
				}
				requireCacheTestRange(t, store, key, 0, -1, payload)
				if len(base.calls()) != 6 || encryptor.unwraps.Load() != 2 {
					t.Fatal("replacement envelope was not reused")
				}
			})
		}
	}
}

func rawCacheTestObject(t *testing.T, store *cacheTestStore, key string) []byte {
	t.Helper()
	reader, err := store.ContextConditionalStore.Get(key, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestImmutableEncryptedHeaderCacheCoalescesStaleHeaderRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		putCacheTestObject(t, store, "pack", "0123456789abcdef")
		requireCacheTestRange(t, store, "pack", 0, 1, "0")
		writer := Encrypting(base, store.cfg)
		if err := writer.Delete("pack"); err != nil {
			t.Fatal(err)
		}
		putCacheTestObject(t, writer, "pack", "0123456789abcdef")
		dataRelease, headerRelease := make(chan struct{}), make(chan struct{})
		base.beforeGet = func(ctx context.Context, _ string, off int64) error {
			var release <-chan struct{}
			if off == 0 {
				release = headerRelease
			} else if off > int64(len(encryptedObjectMagic)+4) {
				release = dataRelease
			} else {
				return nil
			}
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		const callers = 12
		results := make(chan error, callers)
		for range callers {
			go func() {
				got, err := readCacheTestRange(t.Context(), store, "pack", 8, 8)
				if err == nil && got != "89abcdef" {
					err = fmt.Errorf("recreated range = %q", got)
				}
				results <- err
			}()
		}
		synctest.Wait()
		close(dataRelease)
		synctest.Wait()
		if len(base.calls()) != 2+callers+1 {
			t.Fatalf("refreshes did not coalesce: gets=%d", len(base.calls()))
		}
		close(headerRelease)
		for range callers {
			if err := <-results; err != nil {
				t.Fatal(err)
			}
		}
		if len(base.calls()) != 2+2*callers+1 || encryptor.unwraps.Load() != 2 {
			t.Fatalf("stale reads: gets=%d unwraps=%d", len(base.calls()), encryptor.unwraps.Load())
		}
	})
}

func TestImmutableEncryptedHeaderCacheRefreshDoesNotHideCorruption(t *testing.T) {
	for _, kind := range []string{"same-header", "new-header", "later-frame", "plaintext"} {
		t.Run(kind, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
			putCacheTestObject(t, store, "pack", "0123456789abcdef")
			requireCacheTestRange(t, store, "pack", 0, 1, "0")
			if kind == "new-header" {
				putCacheTestObject(t, Encrypting(base, store.cfg), "pack", "0123456789abcdef")
			}
			payload := rawCacheTestObject(t, base, "pack")
			prefixEnd := len(encryptedObjectMagic) + 4
			headerEnd := prefixEnd + int(binary.BigEndian.Uint32(payload[len(encryptedObjectMagic):prefixEnd]))
			frameOffset := headerEnd
			if kind == "later-frame" {
				frameOffset += 28
			}
			payload[frameOffset+4] ^= 1
			if kind == "plaintext" {
				payload = []byte("replacement plaintext")
			}
			putCacheTestObject(t, base.ContextConditionalStore, "pack", string(payload))
			got, err := readCacheTestRange(t.Context(), store, "pack", 0, -1)
			if err == nil {
				t.Fatal("corruption was hidden by a header refresh")
			}
			wantPlaintext, wantGets := "", 4
			switch kind {
			case "new-header":
				wantGets = 5 // One stale range, one header refresh, one failing retry.
			case "later-frame":
				wantPlaintext, wantGets = "01234567", 3 // No retry after authenticated data.
			case "plaintext":
				wantGets = 4 // Strict prefix rejection on refresh.
			}
			if got != wantPlaintext || len(base.calls()) != wantGets {
				t.Fatalf("corrupt read = %q, gets=%d; want %q and %d", got, len(base.calls()), wantPlaintext, wantGets)
			}
		})
	}
}

func TestImmutableEncryptedHeaderCacheCanceledRefreshCanRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		putCacheTestObject(t, store, "pack", "0123456789abcdef")
		requireCacheTestRange(t, store, "pack", 0, 1, "0")
		putCacheTestObject(t, Encrypting(base, store.cfg), "pack", "0123456789abcdef")
		var refreshes atomic.Int64
		base.beforeGet = func(ctx context.Context, _ string, off int64) error {
			if off == 0 && refreshes.Add(1) == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := readCacheTestRange(ctx, store, "pack", 8, 8); result <- err }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled refresh = %v", err)
		}
		synctest.Wait()
		requireCacheTestRange(t, store, "pack", 8, 8, "89abcdef")
		if len(base.calls()) != 6 || encryptor.unwraps.Load() != 2 {
			t.Fatalf("refresh cancellation poisoned retry: gets=%d unwraps=%d", len(base.calls()), encryptor.unwraps.Load())
		}
	})
}

func TestImmutableEncryptedHeaderCacheFencesLoadDuringMutation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
		putCacheTestObject(t, store, "pack", "old payload")
		release := make(chan struct{})
		var blocked atomic.Bool
		base.beforeGet = func(_ context.Context, _ string, off int64) error {
			if off == 0 && !blocked.Swap(true) {
				<-release
			}
			return nil
		}
		result := make(chan error, 1)
		go func() {
			got, err := readCacheTestRange(t.Context(), store, "pack", 0, 3)
			if err == nil && got != "new" {
				err = fmt.Errorf("read after mutation = %q", got)
			}
			result <- err
		}()
		synctest.Wait()
		putCacheTestObject(t, store, "pack", "new payload")
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		// The invalidated header probe must be retried, never published.
		if len(base.calls()) != 3 {
			t.Fatalf("calls = %+v, want fenced load, fresh load and data", base.calls())
		}
		requireCacheTestRange(t, store, "pack", 4, 7, "payload")
		if len(base.calls()) != 4 {
			t.Fatal("fresh header was not cached")
		}
	})
}

func TestImmutableEncryptedHeaderCacheRejectsCorruptionWithoutPoisoning(t *testing.T) {
	for _, corruption := range []string{"plaintext", "prefix", "header-size", "json", "version", "nonce", "key", "algorithm", "chunk-size", "ciphertext", "truncated-frame"} {
		t.Run(corruption, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", testEncryptedHeaderCacheConfig)
			putCacheTestObject(t, store, "pack", "0123456789abcdef")
			rawReader, err := base.ContextConditionalStore.Get("pack", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			original, err := io.ReadAll(rawReader)
			_ = rawReader.Close()
			if err != nil {
				t.Fatal(err)
			}
			broken := append([]byte(nil), original...)
			prefixEnd := len(encryptedObjectMagic) + 4
			headerEnd := prefixEnd + int(binary.BigEndian.Uint32(original[len(encryptedObjectMagic):prefixEnd]))
			switch corruption {
			case "plaintext":
				broken = []byte("plain rootfs content must be rejected")
			case "prefix":
				broken = broken[:len(encryptedObjectMagic)+1]
			case "header-size":
				binary.BigEndian.PutUint32(broken[len(encryptedObjectMagic):], maxEncryptedObjectHeaderBytes+1)
			case "json":
				broken[prefixEnd] = '!'
			case "ciphertext":
				broken[headerEnd+4] ^= 1
			case "truncated-frame":
				broken = broken[:headerEnd+6]
			default:
				var header encryptedObjectHeader
				if err := json.Unmarshal(original[prefixEnd:headerEnd], &header); err != nil {
					t.Fatal(err)
				}
				switch corruption {
				case "version":
					header.Version++
				case "nonce":
					header.NoncePrefix = []byte{1}
				case "key":
					header.WrappedKey = []byte{1}
				case "algorithm":
					header.Algorithm = "unsupported"
				case "chunk-size":
					header.ChunkSize = 0
				}
				var buffer bytes.Buffer
				if err := writeEncryptedObjectHeader(&buffer, header); err != nil {
					t.Fatal(err)
				}
				broken = append(buffer.Bytes(), original[headerEnd:]...)
			}
			putCacheTestObject(t, base.ContextConditionalStore, "pack", string(broken))
			for range 2 {
				if _, err := readCacheTestRange(t.Context(), store, "pack", 0, 5); err == nil {
					t.Fatal("corruption accepted")
				}
			}
			// Repair outside the wrapper only for this fault-injection test: errors
			// must leave no cached metadata that could prevent a healthy retry.
			putCacheTestObject(t, base.ContextConditionalStore, "pack", string(original))
			before := len(base.calls())
			requireCacheTestRange(t, store, "pack", 0, 5, "01234")
			if len(base.calls())-before != 2 {
				t.Fatal("corrupt header or data retained cached state")
			}
			requireCacheTestRange(t, store, "pack", 8, 5, "89abc")
			if len(base.calls())-before != 3 {
				t.Fatal("healthy retry did not populate cache")
			}
		})
	}
}

func TestImmutableEncryptedHeaderCachePreservesObjectAndChunkAAD(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, algorithm, testEncryptedHeaderCacheConfig)
			for _, key := range []string{"a", "b"} {
				putCacheTestObject(t, store, key, "0123456789abcdef")
			}
			requireCacheTestRange(t, store, "a", 0, 1, "0")
			requireCacheTestRange(t, store, "b", 8, 1, "8")
			raw, err := base.ContextConditionalStore.Get("a", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(raw)
			_ = raw.Close()
			if err != nil {
				t.Fatal(err)
			}
			// Replacing b with a fails both with b's cached crypto and after eviction
			// with a's parsed header, because the exact key remains in frame AAD.
			putCacheTestObject(t, base.ContextConditionalStore, "b", string(payload))
			for range 2 {
				if _, err := readCacheTestRange(t.Context(), store, "b", 0, 1); err == nil {
					t.Fatal("cross-object ciphertext substitution accepted")
				}
			}
			prefixEnd := len(encryptedObjectMagic) + 4
			headerEnd := prefixEnd + int(binary.BigEndian.Uint32(payload[len(encryptedObjectMagic):prefixEnd]))
			copy(payload[headerEnd:headerEnd+28], payload[headerEnd+28:])
			putCacheTestObject(t, base.ContextConditionalStore, "a", string(payload))
			if _, err := readCacheTestRange(t.Context(), store, "a", 0, 1); err == nil {
				t.Fatal("cross-chunk ciphertext substitution accepted")
			}
		})
	}
}

func TestImmutableEncryptedHeaderCacheDoesNotInventCapabilities(t *testing.T) {
	base := NewMemoryStore(t.Name())
	cfg := EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}}
	for _, backing := range []Store{objectStoreWithoutConditionalCreate{Store: base}, Prefix(objectStoreWithoutConditionalCreate{Store: base}, "rootfs")} {
		store := EncryptingImmutable(backing, cfg, testEncryptedHeaderCacheConfig).(*encryptedStore)
		if SupportsConditionalCreate(store) || SupportsContextConditionalCreate(store) {
			t.Fatal("immutable encryption invented conditional capability")
		}
		putCacheTestObject(t, store, "pack", "data")
		reader, err := store.Get("pack", 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.GetContext(t.Context(), "pack", 0, 1); err == nil {
			t.Fatal("cached metadata bypassed contextual capability check")
		}
		if _, err := store.PutIfAbsent("pack", strings.NewReader("data")); err == nil {
			t.Fatal("unsupported conditional create succeeded")
		}
	}
}
