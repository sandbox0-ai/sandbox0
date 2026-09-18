// Package rootfsobjectstore constructs the regional RootFS object store from
// the shared manager/ctld configuration, including optional envelope
// encryption. Runtime services share this constructor so credentials and
// encryption cannot drift.
package rootfsobjectstore

import (
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

// New objects use small independently authenticated frames so a small block
// mapping range need not download/decrypt a 1MiB frame. Readers always honor the
// stored header; existing objects and keys retain their original geometry.
const rootFSObjectChunkSize = 16 << 10

func Create(
	cfg config.RootFSObjectStorageConfig,
	observer objectstore.RequestObserver,
) (objectstore.Store, error) {
	return create(cfg, observer)
}

func create(
	cfg config.RootFSObjectStorageConfig,
	observer objectstore.RequestObserver,
) (objectstore.Store, error) {
	store, err := objectstore.Create(objectstore.Config{
		Type: cfg.Type, Bucket: cfg.Bucket, Region: cfg.Region, Endpoint: cfg.Endpoint,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, SessionToken: cfg.SessionToken,
		RequestObserver: observer,
	})
	if err != nil {
		return nil, err
	}
	return WrapEncryption(store, cfg)
}

func WrapEncryption(
	store objectstore.Store,
	cfg config.RootFSObjectStorageConfig,
) (objectstore.Store, error) {
	return wrapEncryption(store, cfg)
}

func wrapEncryption(
	store objectstore.Store,
	cfg config.RootFSObjectStorageConfig,
) (objectstore.Store, error) {
	if store == nil || !cfg.ObjectEncryptionEnabled {
		return store, nil
	}
	keyPEM, err := objectstore.LoadEncryptionKey(cfg.ObjectEncryptionKeyPath)
	if err != nil {
		return nil, err
	}
	keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, cfg.ObjectEncryptionPassphrase)
	if err != nil {
		return nil, err
	}
	encryption := objectstore.EncryptionConfig{
		Enabled: true, Algorithm: cfg.ObjectEncryptionAlgo, KeyEncryptor: keyEncryptor,
		ChunkSize: rootFSObjectChunkSize,
	}
	// RootFS keys identify immutable content. Conditional publishers verify
	// collisions and readers independently verify block/descriptor digests. GC can
	// delete/recreate a key with a new envelope; the wrapper refreshes stale crypto
	// once before plaintext delivery and authenticates the replacement frames.
	return objectstore.EncryptingImmutable(store, encryption, objectstore.EncryptedHeaderCacheConfig{
		MaxEntries: 1024,
		MaxBytes:   8 << 20,
		// Cold mapping objects begin at offset zero. Co-read their envelope and
		// demanded frames without retaining ciphertext in the header cache.
		MaxPrefixBytes: 256 << 10,
		// Cold nonzero pack reads can overlap header and bounded ciphertext
		// acquisition; stored geometry and AEAD remain mandatory before use.
		MaxParallelReadBytes: 256 << 10,
	}), nil
}
