// Package rootfsobjectstore constructs the regional RootFS object store from
// the shared manager/ctld configuration, including optional envelope
// encryption. Runtime services and one-time migration tools must use the same
// constructor so credentials and encryption cannot drift.
package rootfsobjectstore

import (
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

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
	}
	if allowLegacyPlaintext {
		return objectstore.EncryptingLegacyReadCompatible(store, encryption), nil
	}
	return objectstore.Encrypting(store, encryption), nil
}
