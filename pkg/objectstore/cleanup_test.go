package objectstore

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextCleanupPreservesEncryptedPrefixAndCancellation(t *testing.T) {
	raw := NewMemoryStore("")
	store := Encrypting(Prefix(raw, "region"), EncryptionConfig{Enabled: true, KeyEncryptor: reversibleTestEncryptor{}, ChunkSize: 8})
	require.True(t, SupportsContextCleanup(store))
	require.NoError(t, store.Put("images/a", strings.NewReader("secret")))
	require.NoError(t, raw.Put("images/a", strings.NewReader("another namespace")))
	c := store.(ContextCleanupStore)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.DeleteContext(ctx, "images/a"), context.Canceled)
	_, _, _, err := c.ListContext(ctx, "images/", "", "", "", 10)
	require.ErrorIs(t, err, context.Canceled)
	items, more, _, err := c.ListContext(t.Context(), "images/", "", "", "", 10)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, items, 1)
	require.Equal(t, "images/a", items[0].Key)
	require.NoError(t, c.DeleteContext(t.Context(), items[0].Key))
	_, err = raw.Head("region/images/a")
	require.True(t, IsNotFound(err))
	_, err = raw.Head("images/a")
	require.NoError(t, err)
	unsupported := Prefix(struct{ Store }{raw}, "other")
	require.False(t, SupportsContextCleanup(unsupported))
	require.Error(t, unsupported.(ContextCleanupStore).DeleteContext(t.Context(), "a"))
}
