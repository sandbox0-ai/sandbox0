package sandboxstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type rootFSAuditTestEncryptor struct{}

func (rootFSAuditTestEncryptor) Encrypt(in []byte) ([]byte, error) {
	return append([]byte(nil), in...), nil
}

func (rootFSAuditTestEncryptor) Decrypt(in []byte) ([]byte, error) {
	return append([]byte(nil), in...), nil
}

type rootFSAuditTestInspector struct {
	store    objectstore.Store
	physical bool
}

func (i rootFSAuditTestInspector) StatRootFSObject(key string) (RootFSObjectInfo, error) {
	var info objectstore.Info
	var err error
	if i.physical {
		info, err = i.store.Head(key)
	} else {
		info, err = objectstore.HeadContent(i.store, key)
	}
	return RootFSObjectInfo{Key: info.Key, Size: info.Size, Modified: info.Modified, SizeIsLogical: !i.physical}, err
}

func TestRootFSObjectAuditEncryptedContentIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	base := objectstore.NewMemoryStore(t.Name())
	objects := objectstore.EncryptingImmutable(base, objectstore.EncryptionConfig{
		Enabled: true, KeyEncryptor: rootFSAuditTestEncryptor{}, ChunkSize: 8,
	}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 16, MaxBytes: 1 << 20})
	register := func(name string, payload []byte, catalogSize int64) string {
		key := "rootfs/" + name + "/sha256/" + digest.FromBytes(payload).Encoded()
		_, err := pool.Exec(ctx, `
			INSERT INTO manager.rootfs_materialization_objects
				(object_key, object_kind, object_size, checksum, uploaded_at)
			VALUES ($1, $2, $3, $4, NOW())
		`, key, rootfsblock.ObjectKindMappingPage, catalogSize, digest.FromBytes(payload).String())
		require.NoError(t, err)
		return key
	}
	payload := []byte("immutable mapping payload")
	key := register("valid", payload, int64(len(payload)))
	require.NoError(t, objects.Put(key, bytes.NewReader(payload)))

	// Reproduce the previous physical-size comparison using the real audit
	// transaction, then verify the logical inspector clears its false error.
	result, err := store.AuditRootFSObjects(ctx, rootFSAuditTestInspector{store: objects, physical: true}, "", 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSObjectAuditResult{Checked: 1, SizeMismatched: 1}, result)
	var message string
	require.NoError(t, pool.QueryRow(ctx, "SELECT last_error FROM manager.rootfs_materialization_objects WHERE object_key=$1", key).Scan(&message))
	require.Contains(t, message, "does not match catalog size")
	result, err = store.AuditRootFSObjects(ctx, rootFSAuditTestInspector{store: objects}, "", 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSObjectAuditResult{Checked: 1}, result)
	var healthy bool
	var catalogSize int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT missing_at IS NULL AND last_error='' AND last_audited_at IS NOT NULL, object_size
		FROM manager.rootfs_materialization_objects WHERE object_key=$1
	`, key).Scan(&healthy, &catalogSize))
	require.True(t, healthy)
	require.Equal(t, int64(len(payload)), catalogSize, "never rewrite catalog truth to match ciphertext")

	wrong := register("wrong-size", payload, int64(len(payload)+1))
	require.NoError(t, objects.Put(wrong, bytes.NewReader(payload)))
	missing := register("missing", payload, int64(len(payload)))
	plain := register("unencrypted", payload, int64(len(payload)))
	require.NoError(t, base.Put(plain, bytes.NewReader(payload)))
	result, err = store.AuditRootFSObjects(ctx, rootFSAuditTestInspector{store: objects}, "", 100)
	require.NoError(t, err)
	require.Equal(t, &RootFSObjectAuditResult{Checked: 4, Missing: 2, SizeMismatched: 1}, result)
	for _, bad := range []string{wrong, missing, plain} {
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT last_error FROM manager.rootfs_materialization_objects WHERE object_key=$1
		`, bad).Scan(&message))
		require.NotEmpty(t, message, "real audit failures must remain visible")
	}
}
