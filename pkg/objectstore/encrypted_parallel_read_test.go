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

var parallelReadTestCache = EncryptedHeaderCacheConfig{MaxEntries: 16, MaxBytes: 1 << 20, MaxParallelReadBytes: 256 << 10}

func TestEncryptedParallelReadOverlapsColdHeaderAndDemand(t *testing.T) {
	for _, algorithm := range []string{EncryptionAlgoAES256GCMRSA, EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, base, encryptor := newCacheTestStore(t, algorithm, parallelReadTestCache)
				store.cfg.ChunkSize = 64 << 10
				payload := bytes.Repeat([]byte("0123456789abcdef"), 16384)
				putCacheTestObject(t, store, "pack", string(payload))
				off, limit := int64(65543), int64(4096)
				hintOffset, hintLength, ok := store.parallelEncryptedReadHint(off, limit)
				if !ok {
					t.Fatal("no bounded hint")
				}
				release := make(chan struct{})
				base.beforeGet = func(ctx context.Context, _ string, _ int64) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				done := make(chan error, 1)
				go func() {
					got, err := readCacheTestRange(t.Context(), store, "pack", off, limit)
					if err == nil && got != string(payload[off:off+limit]) {
						err = errors.New("wrong data")
					}
					done <- err
				}()
				synctest.Wait()
				calls := base.calls()
				if len(calls) != 2 {
					t.Fatalf("header and data did not overlap: %+v", calls)
				}
				seen := map[rangeReadCall]bool{}
				for _, call := range calls {
					seen[call] = true
				}
				if !seen[rangeReadCall{off: 0, limit: 1024}] || !seen[rangeReadCall{off: hintOffset, limit: hintLength}] {
					t.Fatalf("unexpected calls: %+v", calls)
				}
				close(release)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				requireCacheTestRange(t, store, "pack", off, limit, string(payload[off:off+limit]))
				if len(base.calls()) != 3 || encryptor.unwraps.Load() != 1 {
					t.Fatal("warm header did not use one exact data read")
				}
			})
		})
	}
}

func TestEncryptedParallelReadSlowDataDoesNotDelayHeaderWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 64 << 10
		putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 4<<16)))
		hintOffset, _, _ := store.parallelEncryptedReadHint(65537, 1)
		base.beforeGet = func(ctx context.Context, _ string, off int64) error {
			if off == hintOffset {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { _, err := readCacheTestRange(ctx, store, "pack", 65537, 1); done <- err }()
		synctest.Wait()
		if !store.headerCache.contains("pack") {
			t.Fatal("header publication waits for private data")
		}
		requireCacheTestRange(t, store, "pack", 131073, 1, string([]byte{7}))
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
		synctest.Wait()
		if len(store.parallelReads) != 0 {
			t.Fatal("probe admission leaked")
		}
	})
}

func TestEncryptedParallelReadCanceledLeaderPreservesSharedHeaderForWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, encryptor := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 65536
		putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 4<<16)))
		leaderOffset, _, _ := store.parallelEncryptedReadHint(65537, 1)
		releaseHeader := make(chan struct{})
		base.beforeGet = func(ctx context.Context, _ string, off int64) error {
			if off == 0 {
				select {
				case <-releaseHeader:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if off == leaderOffset {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		leader, follower := make(chan error, 1), make(chan error, 1)
		go func() { _, err := readCacheTestRange(ctx, store, "pack", 65537, 1); leader <- err }()
		synctest.Wait()
		go func() {
			got, err := readCacheTestRange(t.Context(), store, "pack", 131073, 1)
			if err == nil && got != string([]byte{7}) {
				err = errors.New("wrong follower data")
			}
			follower <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-leader; !errors.Is(err, context.Canceled) {
			t.Fatalf("leader cancellation: %v", err)
		}
		close(releaseHeader)
		if err := <-follower; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		var headers int
		for _, call := range base.calls() {
			if call.off == 0 {
				headers++
			}
		}
		if headers != 1 || encryptor.unwraps.Load() != 1 || len(store.parallelReads) != 0 {
			t.Fatalf("shared load disrupted: headers=%d unwraps=%d slots=%d", headers, encryptor.unwraps.Load(), len(store.parallelReads))
		}
	})
}

func TestEncryptedParallelReadAdmissionRemainsChargedUntilProviderExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 64 << 10
		putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 4<<16)))
		release := make(chan struct{})
		base.beforeGet = func(context.Context, string, int64) error { <-release; return nil }
		var probes []*parallelEncryptedRead
		for range maxParallelEncryptedReads {
			p := store.startParallelEncryptedRead(t.Context(), "pack", 65537, 1)
			if p == nil {
				t.Fatal("premature saturation")
			}
			probes = append(probes, p)
		}
		synctest.Wait()
		for _, p := range probes {
			p.cancel()
		}
		synctest.Wait()
		if len(store.parallelReads) != maxParallelEncryptedReads {
			t.Fatal("cancellation released a still-running provider")
		}
		if store.startParallelEncryptedRead(t.Context(), "pack", 65537, 1) != nil {
			t.Fatal("unbounded probe admission")
		}
		if len(base.calls()) != maxParallelEncryptedReads {
			t.Fatal("unexpected provider calls")
		}
		close(release)
		synctest.Wait()
		if len(store.parallelReads) != 0 {
			t.Fatal("completed provider retained slot")
		}
	})
}

func TestEncryptedParallelReadStoredGeometryAndShortFinalFrames(t *testing.T) {
	for _, test := range []struct {
		name            string
		chunk           int64
		headerEnd, size int
		off, length     int64
	}{
		{"old-1m", 1 << 20, 0, 2 << 20, 65543, 1000},
		{"old-small", 8, 0, 1 << 17, 65543, 1000},
		{"large-header", 65536, 2000, 1 << 18, 65543, 1000},
		{"short-final", 65536, 0, 65536 + 90, 65543, 80},
		{"cross-frame", 65536, 0, 1 << 18, 131070, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
			store.cfg.ChunkSize = 65536
			writer := store.cfg
			writer.ChunkSize = test.chunk
			payload := bytes.Repeat([]byte("0123456789abcdef"), (test.size+15)/16)[:test.size]
			putCacheTestObject(t, Encrypting(base, writer), "pack", string(payload))
			if test.headerEnd > 0 {
				putCacheTestObject(t, base.ContextConditionalStore, "pack", string(paddedHeaderProbeObject(t, rawCacheTestObject(t, base, "pack"), test.headerEnd)))
			}
			requireCacheTestRange(t, store, "pack", test.off, test.length, string(payload[test.off:test.off+test.length]))
			if len(base.calls()) > 4 {
				t.Fatalf("unbounded fallback: %+v", base.calls())
			}
		})
	}
}

func TestEncryptedParallelReadCompletedBodiesRemainBoundedWhileHeaderPending(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 65536
		putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 2<<20)))
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
		const callers = 20
		done := make(chan error, callers)
		for i := range callers {
			go func() {
				got, err := readCacheTestRange(t.Context(), store, "pack", int64(i+1)*65536+1, 1)
				if err == nil && got != string([]byte{7}) {
					err = errors.New("wrong data")
				}
				done <- err
			}()
		}
		synctest.Wait()
		if len(store.parallelReads) != maxParallelEncryptedReads || len(base.calls()) != maxParallelEncryptedReads+1 {
			t.Fatalf("completed buffers escaped admission: slots=%d calls=%d", len(store.parallelReads), len(base.calls()))
		}
		close(release)
		for range callers {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if len(store.parallelReads) != 0 {
			t.Fatal("completed reads retained admission")
		}
	})
}

func TestEncryptedParallelReadHintBounds(t *testing.T) {
	store, _, _ := newCacheTestStore(t, "", parallelReadTestCache)
	store.cfg.ChunkSize = 65536
	for _, test := range []struct {
		off, limit int64
		valid      bool
	}{
		{0, 1, false}, {1, 1, false}, {65536, 0, false}, {65536, -1, false},
		{65536, 1, true}, {65536, 128 << 10, true}, {65536, 256 << 10, false},
		{maxInt64, 1, false}, {maxInt64 - 65536, 1, false},
	} {
		off, length, ok := store.parallelEncryptedReadHint(test.off, test.limit)
		if ok != test.valid {
			t.Fatalf("hint(%d,%d): %d %d %t", test.off, test.limit, off, length, ok)
		}
		if ok && (off <= 0 || length > parallelReadTestCache.MaxParallelReadBytes || off > maxInt64-length) {
			t.Fatal("hint exceeds bounds")
		}
	}
	store.headerCache.cfg.MaxParallelReadBytes = maxInt64
	store.cfg.ChunkSize = 1
	if _, length, ok := store.parallelEncryptedReadHint(100, 40000); ok && length > 1<<20 {
		t.Fatal("uncapped setting")
	}
	store.headerCache.cfg.MaxParallelReadBytes = 0
	if _, _, ok := store.parallelEncryptedReadHint(100, 1); ok {
		t.Fatal("disabled probe")
	}
}

func TestEncryptedParallelReadAuthenticatesBeforeDelivery(t *testing.T) {
	for _, failure := range []string{"first-frame", "later-frame", "wrong-key", "cross-chunk", "truncated"} {
		t.Run(failure, func(t *testing.T) {
			store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
			store.cfg.ChunkSize = 65536
			payload := bytes.Repeat([]byte{7}, 4<<16)
			putCacheTestObject(t, store, "pack", string(payload))
			raw := rawCacheTestObject(t, base, "pack")
			prefix := len(encryptedObjectMagic) + 4
			headerEnd := prefix + int(binary.BigEndian.Uint32(raw[len(encryptedObjectMagic):prefix]))
			frame := 65536 + 20
			key := "pack"
			switch failure {
			case "first-frame":
				raw[headerEnd+frame+30] ^= 1
			case "later-frame":
				raw[headerEnd+2*frame+30] ^= 1
			case "wrong-key":
				key = "different-pack"
			case "cross-chunk":
				copy(raw[headerEnd+frame:headerEnd+2*frame], raw[headerEnd:headerEnd+frame])
			case "truncated":
				raw = raw[:headerEnd+frame+30]
			}
			putCacheTestObject(t, base.ContextConditionalStore, key, string(raw))
			got, err := readCacheTestRange(t.Context(), store, key, 65536, 128<<10)
			if err == nil {
				t.Fatal("corrupt data accepted")
			}
			if failure == "later-frame" {
				if len(got) != 65536 || len(base.calls()) != 2 {
					t.Fatalf("retried after authenticated frame: got=%d calls=%+v", len(got), base.calls())
				}
			} else if got != "" {
				t.Fatal("unauthenticated data escaped")
			}
		})
	}
}

func TestEncryptedParallelReadProviderFailureFallsBackToExactDemand(t *testing.T) {
	store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
	store.cfg.ChunkSize = 65536
	putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 4<<16)))
	hint, _, _ := store.parallelEncryptedReadHint(65537, 1)
	base.beforeGet = func(_ context.Context, _ string, off int64) error {
		if off == hint {
			return fmt.Errorf("hint rejected")
		}
		return nil
	}
	requireCacheTestRange(t, store, "pack", 65537, 1, string([]byte{7}))
	if len(base.calls()) != 3 {
		t.Fatalf("unexpected fallback: %+v", base.calls())
	}
}

func TestEncryptedParallelReadRejectsOversizedProviderBody(t *testing.T) {
	store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
	store.cfg.ChunkSize = 65536
	putCacheTestObject(t, store, "pack", string(bytes.Repeat([]byte{7}, 4<<16)))
	hint, length, _ := store.parallelEncryptedReadHint(65537, 1)
	store.store = headerProbeFaultStore{ContextConditionalStore: base, get: func(ctx context.Context, off, limit int64) (io.ReadCloser, error) {
		if off == hint {
			return io.NopCloser(bytes.NewReader(make([]byte, length+1))), nil
		}
		return base.GetContext(ctx, "pack", off, limit)
	}}
	requireCacheTestRange(t, store, "pack", 65537, 1, string([]byte{7}))
}

func TestEncryptedParallelReadRecreatedEnvelopeCannotUseOldCiphertext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 65536
		payload := string(bytes.Repeat([]byte{7}, 4<<16))
		putCacheTestObject(t, store, "pack", payload)
		hint, _, _ := store.parallelEncryptedReadHint(65537, 1)
		release := make(chan struct{})
		var hinted atomic.Int64
		store.store = headerProbeFaultStore{ContextConditionalStore: base, get: func(ctx context.Context, off, limit int64) (io.ReadCloser, error) {
			body, err := base.GetContext(ctx, "pack", off, limit)
			if off == hint && hinted.Add(1) == 1 {
				<-release
			}
			return body, err
		}}
		done := make(chan error, 1)
		go func() { _, err := readCacheTestRange(t.Context(), store, "pack", 65537, 1); done <- err }()
		synctest.Wait()
		// Invalidate while the first reader still owns an old encrypted body.
		putCacheTestObject(t, store, "pack", payload)
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		// The old in-flight read can validly finish against its old envelope,
		// but cannot repopulate the invalidated LRU for the recreated object.
		if store.headerCache.contains("pack") {
			t.Fatal("old envelope resurrected")
		}
		requireCacheTestRange(t, store, "pack", 65537, 1, payload[65537:65538])
		if !store.headerCache.contains("pack") {
			t.Fatal("new envelope not cached")
		}
	})
}

func TestEncryptedParallelReadNewHeaderRejectsPreviouslyCapturedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, base, _ := newCacheTestStore(t, "", parallelReadTestCache)
		store.cfg.ChunkSize = 65536
		payload := string(bytes.Repeat([]byte{7}, 4<<16))
		putCacheTestObject(t, store, "pack", payload)
		hint, _, _ := store.parallelEncryptedReadHint(65537, 1)
		releaseHeader := make(chan struct{})
		releaseData := make(chan struct{})
		var headers atomic.Int64
		store.store = headerProbeFaultStore{ContextConditionalStore: base, get: func(ctx context.Context, off, limit int64) (io.ReadCloser, error) {
			if off == 0 && headers.Add(1) == 1 {
				<-releaseHeader
			}
			body, err := base.GetContext(ctx, "pack", off, limit)
			if off == hint {
				<-releaseData
			}
			return body, err
		}}
		done := make(chan error, 1)
		go func() {
			got, err := readCacheTestRange(t.Context(), store, "pack", 65537, 1)
			if got != "" {
				err = errors.New("mixed envelope returned plaintext")
			}
			done <- err
		}()
		synctest.Wait()
		// External re-encryption has no wrapper invalidation; first header is
		// now new but the private data response still belongs to the old key.
		putCacheTestObject(t, base.ContextConditionalStore, "pack", "")
		putCacheTestObject(t, Encrypting(base, store.cfg), "pack", payload)
		close(releaseHeader)
		close(releaseData)
		if err := <-done; err == nil {
			t.Fatal("mixed envelope was accepted")
		}
		if headers.Load() != 2 {
			t.Fatalf("expected one unchanged-header check, got %d", headers.Load())
		}
	})
}
