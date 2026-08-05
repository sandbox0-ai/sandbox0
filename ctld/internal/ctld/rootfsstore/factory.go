package rootfsstore

import (
	"fmt"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

// NewObjectStore builds the shared encrypted rootfs object-store boundary used
// by ctld capture and the external snapshotter reader.
func NewObjectStore(cfg *apiconfig.StorageProxyConfig, observer objectstore.RequestObserver) (objectstore.Store, error) {
	if cfg == nil {
		return nil, fmt.Errorf("storage config is not configured")
	}
	store, err := objectstore.Create(objectstore.Config{
		Type:            cfg.ObjectStorageType,
		Bucket:          cfg.S3Bucket,
		Region:          cfg.S3Region,
		Endpoint:        cfg.S3Endpoint,
		AccessKey:       cfg.S3AccessKey,
		SecretKey:       cfg.S3SecretKey,
		SessionToken:    cfg.S3SessionToken,
		RequestObserver: observer,
	})
	if err != nil {
		return nil, err
	}
	if !cfg.ObjectEncryptionEnabled {
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
		Enabled:      true,
		Algorithm:    cfg.ObjectEncryptionAlgo,
		KeyEncryptor: keyEncryptor,
	}), nil
}
