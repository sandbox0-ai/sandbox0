// Package rootfsobjectstore constructs the regional RootFS object store from
// the shared manager/ctld configuration, including optional envelope
// encryption. Runtime services and one-time migration tools must use the same
// constructor so credentials and encryption cannot drift.
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
	return create(cfg, observer, false)
}

// CreateLegacyReadCompatible constructs the one-time migration source reader
// for regions that enabled object encryption after some legacy layers already
// existed. Callers must independently verify every plaintext object's size and
// digest; target and active runtime stores must continue to use Create.
func CreateLegacyReadCompatible(
	cfg config.RootFSObjectStorageConfig,
	observer objectstore.RequestObserver,
) (objectstore.Store, error) {
	return create(cfg, observer, true)
}

func create(
	cfg config.RootFSObjectStorageConfig,
	observer objectstore.RequestObserver,
	allowLegacyPlaintext bool,
) (objectstore.Store, error) {
	store, err := objectstore.Create(objectstore.Config{
		Type: cfg.Type, Bucket: cfg.Bucket, Region: cfg.Region, Endpoint: cfg.Endpoint,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, SessionToken: cfg.SessionToken,
		RequestObserver: observer,
	})
	if err != nil {
		return nil, err
	}
	return wrapEncryption(store, cfg, allowLegacyPlaintext)
}

func WrapEncryption(
	store objectstore.Store,
	cfg config.RootFSObjectStorageConfig,
) (objectstore.Store, error) {
	return wrapEncryption(store, cfg, false)
}

func wrapEncryption(
	store objectstore.Store,
	cfg config.RootFSObjectStorageConfig,
	allowLegacyPlaintext bool,
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
	if allowLegacyPlaintext {
		return objectstore.EncryptingLegacyReadCompatible(store, encryption), nil
	}
	// RootFS keys identify immutable content. Conditional publishers verify
	// collisions and readers independently verify block/descriptor digests. GC can
	// delete/recreate a key with a new envelope; the wrapper refreshes stale crypto
	// once before plaintext delivery and authenticates the replacement frames.
	// Keep migration readers above uncached because their source can be rewritten.
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
