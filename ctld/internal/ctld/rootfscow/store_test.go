package rootfscow

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectWriterReferencesPreexistingCASObject(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	first, err := NewObjectWriter(store, "sandbox-rootfs/test")
	require.NoError(t, err)
	payload := []byte("same content on a restored branch")
	created, err := first.Put(context.Background(), rootfshead.ChunkMediaType, payload)
	require.NoError(t, err)

	second, err := NewObjectWriter(store, "sandbox-rootfs/test")
	require.NoError(t, err)
	reused, err := second.Put(context.Background(), rootfshead.ChunkMediaType, payload)
	require.NoError(t, err)
	assert.Equal(t, created, reused)
	createdBytes, createdCount := second.CreatedMetrics()
	assert.Zero(t, createdBytes)
	assert.Zero(t, createdCount)
	assert.Equal(t, []rootfshead.Object{reused}, second.Referenced())
}
