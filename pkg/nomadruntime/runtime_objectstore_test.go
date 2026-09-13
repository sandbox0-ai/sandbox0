package nomadruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

type runtimeObjectStoreContextKey struct{}

type runtimeObjectStoreRead struct {
	offset  int64
	limit   int64
	context any
}

type runtimeObjectStoreRecorder struct {
	objectstore.ContextConditionalStore
	mu    sync.Mutex
	reads []runtimeObjectStoreRead
}

func (s *runtimeObjectStoreRecorder) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.GetContext(context.Background(), key, off, limit)
}

func (s *runtimeObjectStoreRecorder) GetContext(ctx context.Context, key string, off, limit int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.reads = append(s.reads, runtimeObjectStoreRead{offset: off, limit: limit, context: ctx.Value(runtimeObjectStoreContextKey{})})
	s.mu.Unlock()
	return s.ContextConditionalStore.GetContext(ctx, key, off, limit)
}

func (s *runtimeObjectStoreRecorder) calls() []runtimeObjectStoreRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]runtimeObjectStoreRead(nil), s.reads...)
}

func TestRuntimeObjectStoreUsesSharedImmutableEncryption(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(t.TempDir(), "rootfs-key.pem")
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, algorithm := range []string{objectstore.EncryptionAlgoAES256GCMRSA, objectstore.EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			cfg := Config{
				RootFSObjectType: "s3", RootFSObjectBucket: "runtime-rootfs", RootFSObjectRegion: "us-east-1",
				RootFSObjectEndpoint: "https://objects.invalid", RootFSObjectAccessKey: "test-access-key",
				RootFSObjectSecretKey: "test-secret-key", RootFSObjectSessionToken: "test-session-token",
				RootFSObjectEncryptionEnabled: true, RootFSObjectEncryptionKeyPath: keyPath,
				RootFSObjectEncryptionAlgorithm: algorithm,
			}
			base := &runtimeObjectStoreRecorder{ContextConditionalStore: objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)}
			store, err := newRuntimeObjectStore(cfg, func(got objectstore.Config) (objectstore.Store, error) {
				want := objectstore.Config{
					Type: cfg.RootFSObjectType, Bucket: cfg.RootFSObjectBucket, Region: cfg.RootFSObjectRegion,
					Endpoint: cfg.RootFSObjectEndpoint, AccessKey: cfg.RootFSObjectAccessKey,
					SecretKey: cfg.RootFSObjectSecretKey, SessionToken: cfg.RootFSObjectSessionToken,
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("runtime provider configuration changed")
				}
				return base, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			conditional, ok := store.(objectstore.ContextConditionalStore)
			if !ok || !objectstore.SupportsContextConditionalCreate(store) {
				t.Fatal("runtime store lost contextual conditional access")
			}
			const key = "rootfs/packs/sha256/runtime-pack"
			payload := bytes.Repeat([]byte("rootfs block pack"), 1<<17)
			ctx := context.WithValue(t.Context(), runtimeObjectStoreContextKey{}, "runtime-read-attribution")
			if created, err := conditional.PutIfAbsentContext(ctx, key, bytes.NewReader(payload)); err != nil || !created {
				t.Fatalf("publish runtime pack = %v, %v", created, err)
			}
			raw, err := base.ContextConditionalStore.Get(key, 0, 1024)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := io.ReadAll(raw)
			_ = raw.Close()
			if err != nil {
				t.Fatal(err)
			}
			if encrypted, err := objectstore.HasEncryptedObjectHeader(bytes.NewReader(encoded)); err != nil || !encrypted {
				t.Fatalf("configured runtime encryption missing: %v", err)
			}
			if !bytes.Contains(encoded, []byte(`"algorithm":"`+algorithm+`"`)) {
				t.Fatal("runtime ignored the configured encryption algorithm")
			}
			for i, offset := range []int64{17, (1 << 20) + 17} {
				reader, err := conditional.GetContext(ctx, key, offset, 13)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(reader)
				_ = reader.Close()
				if err != nil || !bytes.Equal(got, payload[offset:offset+13]) {
					t.Fatalf("runtime range %d: content matches=%v error=%v", i, bytes.Equal(got, payload[offset:offset+13]), err)
				}
				calls := base.calls()
				if len(calls) != 2+i {
					t.Fatalf("runtime range %d used %d total GETs, want %d", i, len(calls), 2+i)
				}
				if limit := calls[len(calls)-1].limit; limit != (16<<10)+4+16 {
					t.Fatalf("runtime encryption frame = %d, want 16 KiB plus framing and tag", limit)
				}
				for _, call := range calls {
					if call.context != "runtime-read-attribution" {
						t.Fatal("runtime request context was lost through shared encryption")
					}
				}
			}
			calls := base.calls()
			if calls[0].offset != 0 || calls[0].limit != 1024 || calls[1].offset <= 0 || calls[2].offset <= calls[1].offset {
				t.Fatalf("runtime GETs did not fetch a bounded header probe followed by distinct data frames: %+v", calls)
			}
			cancelCtx, cancel := context.WithCancel(ctx)
			cancel()
			if _, err := conditional.GetContext(cancelCtx, key, 0, 13); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled runtime cache hit = %v", err)
			}
			if len(base.calls()) != 3 {
				t.Fatal("canceled runtime cache hit issued a provider request")
			}
		})
	}
}

func TestRuntimeObjectStoreWithoutEncryptionIsUnchanged(t *testing.T) {
	base := &runtimeObjectStoreRecorder{ContextConditionalStore: objectstore.NewMemoryStore(t.Name()).(objectstore.ContextConditionalStore)}
	store, err := newRuntimeObjectStore(Config{
		RootFSObjectEncryptionKeyPath:   "/missing-disabled-encryption-key.pem",
		RootFSObjectEncryptionAlgorithm: "unused-disabled-algorithm",
	}, func(objectstore.Config) (objectstore.Store, error) { return base, nil })
	if err != nil {
		t.Fatal(err)
	}
	if store != base {
		t.Fatal("disabled runtime encryption changed the provider store")
	}
	if err := store.Put("pack", strings.NewReader("0123456789")); err != nil {
		t.Fatal(err)
	}
	for i, offset := range []int64{0, 5} {
		reader, err := store.Get("pack", offset, 5)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil || string(got) != "0123456789"[offset:offset+5] {
			t.Fatalf("unencrypted runtime range = %q, %v", got, err)
		}
		if len(base.calls()) != i+1 {
			t.Fatal("unencrypted runtime read issued extra GETs")
		}
	}
}

func TestRuntimeObjectStorePropagatesConstructionErrors(t *testing.T) {
	providerErr := errors.New("provider construction failed")
	if _, err := newRuntimeObjectStore(Config{}, func(objectstore.Config) (objectstore.Store, error) {
		return nil, providerErr
	}); !errors.Is(err, providerErr) {
		t.Fatalf("provider construction error = %v", err)
	}
	if _, err := newRuntimeObjectStore(Config{
		RootFSObjectEncryptionEnabled: true,
		RootFSObjectEncryptionKeyPath: filepath.Join(t.TempDir(), "missing-key.pem"),
	}, func(objectstore.Config) (objectstore.Store, error) {
		return objectstore.NewMemoryStore(t.Name()), nil
	}); err == nil {
		t.Fatal("runtime encryption accepted a missing configured key")
	}
}
