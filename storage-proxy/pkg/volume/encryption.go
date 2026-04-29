package volume

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func S0FSEncryptionConfig(cfg *config.StorageProxyConfig) (*s0fs.EncryptionConfig, error) {
	if cfg == nil || !cfg.ObjectEncryptionEnabled {
		return nil, nil
	}
	keyPEM, err := objectstore.LoadEncryptionKey(cfg.ObjectEncryptionKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load object encryption key: %w", err)
	}
	keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, cfg.ObjectEncryptionPassphrase)
	if err != nil {
		return nil, fmt.Errorf("create object encryption key encryptor: %w", err)
	}
	return &s0fs.EncryptionConfig{
		Enabled:      true,
		Algorithm:    cfg.ObjectEncryptionAlgo,
		KeyEncryptor: keyEncryptor,
	}, nil
}
