package rootfsblock

import (
	"bytes"
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
