package runtimecheckpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsobjectstore"
	"github.com/stretchr/testify/require"
)

type captureObjectStore struct {
	objectstore.ContextConditionalStore
	chunkPuts      atomic.Int64
	failManifest   bool
	loseChunkReply bool
}

func (s *captureObjectStore) PutIfAbsentContext(ctx context.Context, key string, reader io.Reader) (bool, error) {
	if strings.Contains(key, "/chunks/") {
		s.chunkPuts.Add(1)
	}
	if s.failManifest && strings.HasSuffix(key, "/manifest.json") {
		return false, errors.New("manifest publication unavailable")
	}
	created, err := s.ContextConditionalStore.PutIfAbsentContext(ctx, key, reader)
	if err == nil && s.loseChunkReply && strings.Contains(key, "/chunks/") {
		return false, io.ErrUnexpectedEOF
	}
	return created, err
}
func (s *captureObjectStore) ListContext(ctx context.Context, prefix, after, token, delimiter string, limit int64) ([]objectstore.Info, bool, string, error) {
	return s.ContextConditionalStore.(objectstore.ContextCleanupStore).ListContext(ctx, prefix, after, token, delimiter, limit)
}
func (s *captureObjectStore) DeleteContext(ctx context.Context, key string) error {
	return s.ContextConditionalStore.(objectstore.ContextCleanupStore).DeleteContext(ctx, key)
}

func TestCaptureScopeUsesExactSourceBeforeRootFSCut(t *testing.T) {
	source, assignment, cut := bindingFixture(t)
	d := digest.FromString("compatibility").String()
	scope, err := BindCapture("migration", source, assignment, d, d)
	require.NoError(t, err)
	_, err = scope.Digest()
	require.NoError(t, err)
	binding, err := Bind("migration", source, assignment, d, d, cut)
	require.NoError(t, err)
	require.True(t, scope.matches(binding))
	require.Error(t, scope.source.Validate(), "a capture scope is not a final binding")
	source.Identity.BootID = "another-boot"
	other, err := BindCapture("migration", source, assignment, d, d)
	require.NoError(t, err)
	require.NotEqual(t, scope, other)
	require.False(t, other.matches(binding))
	source.Identity.WriterGrantToken = "must-never-be-persisted"
	_, err = BindCapture("migration", source, assignment, d, d)
	require.Error(t, err)
	_, err = (CaptureScope{}).Digest()
	require.Error(t, err)
}

func TestCaptureStagingPublishesOnlyFinalVerifiedChunks(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	counted := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore)}
	store, err := New(counted, 2*ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, err := captureScopeForBinding(binding)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
	require.NoError(t, err)
	unchanged := bytes.Repeat([]byte{0x43}, ChunkBytes)
	_, err = stage.StageChunk(t.Context(), unchanged)
	require.NoError(t, err)
	_, err = stage.StageChunk(t.Context(), []byte("tentative tail"))
	require.NoError(t, err)
	bd, _ := binding.Digest()
	_, err = raw.Head(manifestKey(bd))
	require.True(t, objectstore.IsNotFound(err), "tentative uploads cannot be restored")
	final := append(bytes.Clone(unchanged), []byte("final tail")...)
	directory := privateImage(t, map[string][]byte{"pages.img": final})
	ref, err := stage.Publish(t.Context(), binding, directory)
	require.NoError(t, err)
	require.EqualValues(t, 3, counted.chunkPuts.Load(), "the unchanged completed chunk is not re-uploaded")
	destination := filepath.Join(t.TempDir(), "restored")
	manifest, err := store.Download(t.Context(), binding, ref, destination)
	require.NoError(t, err)
	require.Equal(t, StagedManifestVersion, manifest.Version)
	_, err = store.PublishPlanned(t.Context(), binding, LocalImagePlan{Manifest: manifest, Reference: ref}, directory)
	require.ErrorContains(t, err, "capture custodian", "legacy publication must not write staged chunks under different keys")
	actual, err := os.ReadFile(filepath.Join(destination, "pages.img"))
	require.NoError(t, err)
	require.Equal(t, final, actual)
	_, err = store.VerifyLocal(t.Context(), binding, ref, destination)
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, store.WritePeerImage(t.Context(), binding, ref, directory, &wire))
	peerDirectory := filepath.Join(t.TempDir(), "peer")
	peerManifest, err := ReceivePeerImage(t.Context(), binding, ref, peerDirectory, &wire, 2*ChunkBytes,
		func(size int64, inodes uint64) error {
			wantSize, wantInodes, err := manifest.StagingFootprint()
			require.NoError(t, err)
			require.Equal(t, wantSize, size)
			require.Equal(t, wantInodes, inodes)
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, manifest, peerManifest)
	_, err = store.VerifyLocal(t.Context(), binding, ref, peerDirectory)
	require.NoError(t, err)
	_, err = stage.StageChunk(t.Context(), []byte("late"))
	require.ErrorContains(t, err, "already bound")
	wrong := binding
	wrong.RootFSGenerationID = "different-cut"
	_, err = stage.Publish(t.Context(), wrong, directory)
	require.ErrorContains(t, err, "another filesystem cut")

	// A restarted publication recovers its existing scope, rechecks immutable
	// chunk collisions, and returns the same full reference after a lost reply.
	recovered, err := store.OpenCaptureStaging(t.Context(), scope, 4*ChunkBytes)
	require.NoError(t, err)
	retry, err := recovered.Publish(t.Context(), binding, directory)
	require.NoError(t, err)
	require.Equal(t, ref, retry)
	c, err := NewCollector(raw)
	require.NoError(t, err)
	_, err = c.Collect(t.Context(), wrong)
	require.ErrorContains(t, err, "another filesystem cut")
	for i := 0; ; i++ {
		require.Less(t, i, 8)
		done, err := c.Collect(t.Context(), binding)
		require.NoError(t, err)
		if done {
			break
		}
	}
	objects, more, _, err := raw.List("runtime-checkpoints/", "", "", "", 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, objects, "discarded tentative chunks must also be collected")
}

func TestCaptureStagingBudgetSurvivesRestartAndCountsShortChunks(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	store, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, 2*ChunkBytes)
	require.NoError(t, err)
	for _, value := range []string{"one", "one", "two"} {
		_, err = stage.StageChunk(t.Context(), []byte(value))
		require.NoError(t, err)
	}
	_, err = stage.StageChunk(t.Context(), []byte("three"))
	require.ErrorContains(t, err, "budget exhausted")
	recovered, err := store.OpenCaptureStaging(t.Context(), scope, 2*ChunkBytes)
	require.NoError(t, err)
	_, err = recovered.StageChunk(t.Context(), []byte("one"))
	require.NoError(t, err)
	_, err = recovered.StageChunk(t.Context(), []byte("three"))
	require.ErrorContains(t, err, "budget exhausted")
	_, err = store.OpenCaptureStaging(t.Context(), scope, 3*ChunkBytes)
	require.ErrorContains(t, err, "collision")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = recovered.StageChunk(ctx, []byte("one"))
	require.ErrorIs(t, err, context.Canceled)
	c, err := NewCollector(raw)
	require.NoError(t, err)
	_, err = c.Collect(t.Context(), testBinding())
	require.ErrorContains(t, err, "terminal capture authority")
	done, err := c.CollectCapture(t.Context(), scope)
	require.NoError(t, err)
	require.False(t, done)
	done, err = c.CollectCapture(t.Context(), scope)
	require.NoError(t, err)
	require.True(t, done)
}

func TestCaptureStagingLostChunkReplyDoesNotResetBudget(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	fault := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), loseChunkReply: true}
	store, err := New(fault, ChunkBytes)
	require.NoError(t, err)
	scope, _ := captureScopeForBinding(testBinding())
	stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	_, err = stage.StageChunk(t.Context(), []byte("stored despite lost reply"))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	fault.loseChunkReply = false
	recovered, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	_, err = recovered.StageChunk(t.Context(), []byte("another candidate"))
	require.ErrorContains(t, err, "budget exhausted")
	_, err = recovered.StageChunk(t.Context(), []byte("stored despite lost reply"))
	require.NoError(t, err)
}

func TestCaptureCleanupRetainsBindingAcrossPartialDeletion(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	store, err := New(raw, ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, _ := captureScopeForBinding(binding)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, (cleanupBatchSize+10)*ChunkBytes)
	require.NoError(t, err)
	for i := 0; i < cleanupBatchSize+2; i++ {
		_, err = stage.StageChunk(t.Context(), []byte(fmt.Sprint(i)))
		require.NoError(t, err)
	}
	_, err = stage.Publish(t.Context(), binding, privateImage(t, map[string][]byte{"checkpoint.img": []byte("final image")}))
	require.NoError(t, err)
	other := binding
	other.OperationID = "unrelated"
	otherScope, _ := captureScopeForBinding(other)
	otherStage, err := store.OpenCaptureStaging(t.Context(), otherScope, ChunkBytes)
	require.NoError(t, err)
	_, err = otherStage.StageChunk(t.Context(), []byte("unrelated"))
	require.NoError(t, err)
	fault := &cleanupFaultStore{ContextCleanupStore: raw.(objectstore.ContextCleanupStore), failAfter: 3}
	c, err := NewCollector(fault)
	require.NoError(t, err)
	done, err := c.CollectCapture(t.Context(), scope) // Removes only the final manifest.
	require.NoError(t, err)
	require.False(t, done)
	done, err = c.CollectCapture(t.Context(), scope)
	require.ErrorContains(t, err, "injected")
	require.False(t, done)
	_, err = raw.Head(stage.prefix + "publication.json")
	require.NoError(t, err, "partial deletion must retain binding custody")
	fault.failAfter = 0
	for i := 0; ; i++ {
		require.Less(t, i, 10)
		done, err = c.CollectCapture(t.Context(), scope)
		require.NoError(t, err)
		if done {
			break
		}
	}
	objects, more, _, err := raw.List(stage.prefix, "", "", "", 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, objects)
	_, err = raw.Head(otherStage.prefix + "reservation.json")
	require.NoError(t, err)
}

func TestCaptureStagingFailedManifestCannotPublishOrChangeSource(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	fault := &captureObjectStore{ContextConditionalStore: raw.(objectstore.ContextConditionalStore), failManifest: true}
	store, err := New(fault, ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, _ := captureScopeForBinding(binding)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	directory := privateImage(t, map[string][]byte{"pages.img": []byte("final state")})
	other := binding
	other.OperationID = "another-operation"
	_, err = stage.Publish(t.Context(), other, directory)
	require.ErrorContains(t, err, "changed its capture source")
	_, err = stage.Publish(t.Context(), binding, directory)
	require.ErrorContains(t, err, "manifest publication unavailable")
	bd, _ := binding.Digest()
	_, err = raw.Head(manifestKey(bd))
	require.True(t, objectstore.IsNotFound(err))
	fault.failManifest = false
	recovered, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	ref, err := recovered.Publish(t.Context(), binding, directory)
	require.NoError(t, err)
	_, err = store.Download(t.Context(), binding, ref, filepath.Join(t.TempDir(), "restored"))
	require.NoError(t, err)
	// The region may have lost the final reply. Terminal capture authority
	// alone must clean both the staged data and its already-written manifest.
	c, err := NewCollector(raw)
	require.NoError(t, err)
	for i := 0; ; i++ {
		require.Less(t, i, 8)
		done, err := c.CollectCapture(t.Context(), scope)
		require.NoError(t, err)
		if done {
			break
		}
	}
	objects, _, _, err := raw.List("runtime-checkpoints/", "", "", "", 100)
	require.NoError(t, err)
	require.Empty(t, objects)
}

func TestCaptureStagingUsesRegionalEncryptionWithoutRelocatingObjects(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600))
	raw := objectstore.NewMemoryStore("")
	encrypted, err := rootfsobjectstore.WrapEncryption(raw, config.RootFSObjectStorageConfig{
		ObjectEncryptionEnabled: true, ObjectEncryptionKeyPath: path, ObjectEncryptionAlgo: "aes256gcm-rsa",
	})
	require.NoError(t, err)
	store, err := New(encrypted, ChunkBytes)
	require.NoError(t, err)
	binding := testBinding()
	scope, _ := captureScopeForBinding(binding)
	stage, err := store.OpenCaptureStaging(t.Context(), scope, ChunkBytes)
	require.NoError(t, err)
	secret := []byte("synthetic process memory encrypted before final binding")
	chunk, err := stage.StageChunk(t.Context(), secret)
	require.NoError(t, err)
	objectKey := stage.prefix + "chunks/" + strings.TrimPrefix(chunk.Digest, "sha256:")
	reader, err := raw.Get(objectKey, 0, -1)
	require.NoError(t, err)
	ciphertext, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.NotContains(t, string(ciphertext), string(secret))
	ref, err := stage.Publish(t.Context(), binding, privateImage(t, map[string][]byte{"pages.img": secret}))
	require.NoError(t, err)
	_, err = store.Download(t.Context(), binding, ref, filepath.Join(t.TempDir(), "restored"))
	require.NoError(t, err)
	c, err := NewCollector(encrypted)
	require.NoError(t, err)
	for i := 0; ; i++ {
		require.Less(t, i, 8)
		done, err := c.Collect(t.Context(), binding)
		require.NoError(t, err)
		if done {
			break
		}
	}
}
