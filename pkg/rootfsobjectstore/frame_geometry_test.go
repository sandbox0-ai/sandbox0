package rootfsobjectstore

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

func TestRootFSFrameGeometryReadsOldAndNewObjectsWithSameEncryptionKey(t *testing.T) {
	cfg := rootFSObjectEncryptionTestConfig(t)
	key, err := objectstore.LoadEncryptionKey(cfg.ObjectEncryptionKeyPath)
	require.NoError(t, err)
	encryptor, err := objectstore.NewKeyEncryptor(key, "")
	require.NoError(t, err)
	expected := make([]byte, 2<<20)
	for i := range expected {
		expected[i] = byte(i/4096 + i%251)
	}
	for _, algorithm := range []string{objectstore.EncryptionAlgoAES256GCMRSA, objectstore.EncryptionAlgoCHACHA20RSA} {
		t.Run(algorithm, func(t *testing.T) {
			cfg := cfg
			cfg.ObjectEncryptionAlgo = algorithm
			raw := &countingRootFSStore{ContextConditionalStore: objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)}
			wrap := func(frame int) objectstore.Store {
				if frame == 16<<10 {
					store, err := WrapEncryption(raw, cfg)
					require.NoError(t, err)
					return store
				}
				return objectstore.Encrypting(raw, objectstore.EncryptionConfig{
					Enabled: true, Algorithm: algorithm, KeyEncryptor: encryptor, ChunkSize: int64(frame),
				})
			}
			for _, storedFrame := range []int{1 << 20, 64 << 10, 16 << 10} {
				key := fmt.Sprintf("pack-%d", storedFrame)
				require.NoError(t, wrap(storedFrame).Put(key, bytes.NewReader(expected)))
				for _, readerFrame := range []int{1 << 20, 64 << 10, 16 << 10} {
					t.Run(fmt.Sprintf("stored-%d-reader-%d", storedFrame, readerFrame), func(t *testing.T) {
						reader := wrap(readerFrame)
						for _, offset := range []int64{2, int64(storedFrame) - 2} {
							body, err := reader.Get(key, offset, 5)
							require.NoError(t, err)
							actual, err := io.ReadAll(body)
							require.NoError(t, err)
							require.NoError(t, body.Close())
							require.Equal(t, expected[offset:offset+5], actual)
							frames := int64(1)
							if offset != 2 {
								frames = 2
							}
							require.Equal(t, frames*(int64(storedFrame)+20), raw.lastLimit.Load(),
								"wire range follows stored geometry, not reader defaults")
						}
					})
				}
			}
			// GC may recreate an immutable plaintext key with a different envelope
			// and frame size while an independent ctld reader retains the old header.
			reader := wrap(16 << 10)
			const key = "recreated-pack"
			for _, frame := range []int{1 << 20, 64 << 10, 16 << 10, 1 << 20} {
				require.NoError(t, raw.Delete(key))
				require.NoError(t, wrap(frame).Put(key, bytes.NewReader(expected)))
				body, err := reader.Get(key, 2, 5)
				require.NoError(t, err)
				actual, err := io.ReadAll(body)
				require.NoError(t, err)
				require.NoError(t, body.Close())
				require.Equal(t, expected[2:7], actual)
				require.Equal(t, int64(frame)+20, raw.lastLimit.Load())
			}
			const corruptKey = "corrupted-pack"
			require.NoError(t, wrap(16<<10).Put(corruptKey, bytes.NewReader(expected)))
			body, err := raw.Get(corruptKey, 0, -1)
			require.NoError(t, err)
			ciphertext, err := io.ReadAll(body)
			require.NoError(t, err)
			require.NoError(t, body.Close())
			ciphertext[len(ciphertext)-1] ^= 1
			require.NoError(t, raw.Put(corruptKey, bytes.NewReader(ciphertext)))
			body, err = reader.Get(corruptKey, int64(len(expected))-5, 5)
			if err == nil {
				var actual []byte
				actual, err = io.ReadAll(body)
				require.Empty(t, actual, "a failed demanded frame must expose no plaintext")
				_ = body.Close()
			}
			require.Error(t, err, "smaller frames must retain authentication")
		})
	}
}
