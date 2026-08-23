package rootfsblock

import (
	"bytes"
	"io"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func TestObjectStorePublisherIsIdempotentAndRejectsConflict(t *testing.T) {
	store := objectstore.NewMemoryStore("").(objectstore.ConditionalStore)
	publisher := ObjectStorePublisher{Store: store}
	require.NoError(t, publisher.PutImmutable(t.Context(), "rootfs/object", []byte("first")))
	require.NoError(t, publisher.PutImmutable(t.Context(), "rootfs/object", []byte("first")))
	require.ErrorContains(t, publisher.PutImmutable(t.Context(), "rootfs/object", []byte("second")), "different content")

	body, err := store.Get("rootfs/object", 0, -1)
	require.NoError(t, err)
	defer body.Close()
	actual := new(bytes.Buffer)
	_, err = actual.ReadFrom(body)
	require.NoError(t, err)
	require.Equal(t, "first", actual.String())
}

func TestObjectStorePublisherRejectsNonContextualStore(t *testing.T) {
	store := nonContextConditionalStore{Store: objectstore.NewMemoryStore(t.Name())}
	err := (ObjectStorePublisher{Store: store}).PutImmutable(t.Context(), "rootfs/object", []byte("payload"))
	require.ErrorContains(t, err, "contextual conditional object store")
}

type nonContextConditionalStore struct {
	objectstore.Store
}

func (s nonContextConditionalStore) PutIfAbsent(key string, reader io.Reader) (bool, error) {
	if conditional, ok := s.Store.(objectstore.ConditionalStore); ok {
		return conditional.PutIfAbsent(key, reader)
	}
	return false, nil
}
