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
	return objectstore.Encrypting(store, objectstore.EncryptionConfig{
		Enabled: true, Algorithm: cfg.ObjectEncryptionAlgo, KeyEncryptor: keyEncryptor,
	}), nil
}
