package main

import (
	"bytes"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

// The reversible wrapper is only a deterministic fixture; object payloads
// still use the real AEAD implementation exercised by production stores.
type rootFSInspectorTestEncryptor struct{}

func (rootFSInspectorTestEncryptor) Encrypt(in []byte) ([]byte, error) {
	return append([]byte(nil), in...), nil
}

func (rootFSInspectorTestEncryptor) Decrypt(in []byte) ([]byte, error) {
	return append([]byte(nil), in...), nil
}

func TestRootFSObjectInspectorReportsCatalogPayloadSize(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			base := objectstore.NewMemoryStore(t.Name())
			store := objectstore.Encrypting(base, objectstore.EncryptionConfig{
				Enabled: encrypted, KeyEncryptor: rootFSInspectorTestEncryptor{}, ChunkSize: 8,
			})
			payload := []byte("immutable rootfs mapping payload")
			require.NoError(t, store.Put("maps/object", bytes.NewReader(payload)))
			info, err := (rootFSObjectStoreInspector{store: store}).StatRootFSObject("maps/object")
			require.NoError(t, err)
			require.Equal(t, int64(len(payload)), info.Size, "catalog sizes exclude the encryption envelope")
			physical, err := base.Head("maps/object")
			require.NoError(t, err)
			if encrypted {
				require.Greater(t, physical.Size, info.Size)
			} else {
				require.Equal(t, physical.Size, info.Size)
			}
			require.Equal(t, "maps/object", info.Key)
			require.Equal(t, physical.Modified, info.Modified)
		})
	}
}
