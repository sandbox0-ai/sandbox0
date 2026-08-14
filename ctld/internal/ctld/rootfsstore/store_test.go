package rootfsstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterSharesObjectsAcrossTeamFilesystems(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	first, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	second, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)

	firstObject, err := first.Put(context.Background(), rootfshead.ChunkMediaType, []byte("shared"))
	require.NoError(t, err)
	secondObject, err := second.Put(context.Background(), rootfshead.ChunkMediaType, []byte("shared"))
	require.NoError(t, err)
	assert.Equal(t, firstObject, secondObject)
	createdBytes, createdObjects := second.CreatedMetrics()
	assert.Zero(t, createdBytes)
	assert.Zero(t, createdObjects)
}

func TestWriterDeduplicatesThroughEncryptedStore(t *testing.T) {
	base := objectstore.NewMemoryStore(t.Name())
	store := objectstore.Encrypting(base, objectstore.EncryptionConfig{
		Enabled:      true,
		KeyEncryptor: copyEncryptor{},
		ChunkSize:    8,
	})
	first, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	second, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)

	firstObject, err := first.Put(context.Background(), rootfshead.ChunkMediaType, []byte("shared plaintext"))
	require.NoError(t, err)
	physical, err := base.Head(firstObject.Key)
	require.NoError(t, err)
	assert.NotEqual(t, firstObject.Size, physical.Size)

	secondObject, err := second.Put(context.Background(), rootfshead.ChunkMediaType, []byte("shared plaintext"))
	require.NoError(t, err)
	assert.Equal(t, firstObject, secondObject)
	createdBytes, createdObjects := second.CreatedMetrics()
	assert.Zero(t, createdBytes)
	assert.Zero(t, createdObjects)
	payload, err := Read(context.Background(), store, second.Prefix(), secondObject)
	require.NoError(t, err)
	assert.Equal(t, []byte("shared plaintext"), payload)
}

func TestWriterSeparatesTeams(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	first, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	second, err := NewTeamWriter(store, "team-2")
	require.NoError(t, err)

	firstObject, err := first.Put(context.Background(), rootfshead.ChunkMediaType, []byte("private"))
	require.NoError(t, err)
	secondObject, err := second.Put(context.Background(), rootfshead.ChunkMediaType, []byte("private"))
	require.NoError(t, err)
	assert.NotEqual(t, firstObject.Key, secondObject.Key)
}

func TestWriterSingleflightsConcurrentPuts(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)

	const workers = 32
	objects := make([]rootfshead.Object, workers)
	errs := make([]error, workers)
	var group sync.WaitGroup
	for i := range workers {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			objects[i], errs[i] = writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("payload"))
		}(i)
	}
	group.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		assert.Equal(t, objects[0], objects[i])
	}
	createdBytes, createdObjects := writer.CreatedMetrics()
	assert.Equal(t, int64(len("payload")), createdBytes)
	assert.Equal(t, int64(1), createdObjects)
	pending := writer.PendingProtection()
	require.Len(t, pending, 1)
	writer.MarkProtected(pending)
	assert.Empty(t, writer.PendingProtection())
	_, err = writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("payload"))
	require.NoError(t, err)
	assert.Empty(t, writer.PendingProtection(), "same-generation exact protection should be reused")
	writer.RotateGeneration()
	assert.Empty(t, writer.PendingProtection())
	_, err = writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("payload"))
	require.NoError(t, err)
	assert.Len(t, writer.PendingProtection(), 1, "a new generation must protect reused CAS objects again")
}

func TestReadRejectsCorruptPayload(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	object, err := writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("valid"))
	require.NoError(t, err)
	require.NoError(t, store.Put(object.Key, bytes.NewReader([]byte("bad!!"))))

	_, err = Read(context.Background(), store, writer.Prefix(), object)
	assert.Error(t, err)
}

func TestPutImmutableMarksBackendFailuresUnavailable(t *testing.T) {
	store := &failingStore{Store: objectstore.NewMemoryStore(t.Name()), headErr: errors.New("head failed")}
	writer, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)

	_, err = writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("payload"))
	assert.ErrorIs(t, err, ErrBackendUnavailable)
	assert.ErrorContains(t, err, "head failed")

	store.headErr = nil
	store.putErr = errors.New("put failed")
	_, err = writer.Put(context.Background(), rootfshead.ChunkMediaType, []byte("different payload"))
	assert.ErrorIs(t, err, ErrBackendUnavailable)
	assert.ErrorContains(t, err, "put failed")
}

func TestPrefixFromObjectRoundTrip(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := NewTeamWriter(store, "team-1")
	require.NoError(t, err)
	object, err := writer.Put(context.Background(), rootfshead.HeadMediaType, []byte("head"))
	require.NoError(t, err)
	prefix, err := PrefixFromObject(object)
	require.NoError(t, err)
	assert.Equal(t, writer.Prefix(), prefix)
}

type copyEncryptor struct{}

type failingStore struct {
	objectstore.Store
	headErr error
	putErr  error
}

func (s *failingStore) Head(key string) (objectstore.Info, error) {
	if s.headErr != nil {
		return objectstore.Info{}, s.headErr
	}
	return s.Store.Head(key)
}

func (s *failingStore) Put(key string, reader io.Reader) error {
	if s.putErr != nil {
		return s.putErr
	}
	return s.Store.Put(key, reader)
}

func (copyEncryptor) Encrypt(payload []byte) ([]byte, error) {
	return append([]byte(nil), payload...), nil
}

func (copyEncryptor) Decrypt(payload []byte) ([]byte, error) {
	return append([]byte(nil), payload...), nil
}
