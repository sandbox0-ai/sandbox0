package volsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
)

const replayPayloadRefPrefix = "sha256:"

type replayPayloadStore interface {
	PutPayload(context.Context, *db.SandboxVolume, string, []byte) (string, error)
	GetPayload(context.Context, *db.SandboxVolume, string) (io.ReadCloser, error)
}

type objectReplayPayloadStore struct {
	store object.ObjectStorage
}

func NewObjectReplayPayloadStore(store object.ObjectStorage) replayPayloadStore {
	if store == nil {
		return nil
	}
	return &objectReplayPayloadStore{store: store}
}

func NewReplayPayloadStore(cfg *config.StorageProxyConfig) (replayPayloadStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("storage proxy config is nil")
	}

	store, err := juicefs.CreateObjectStorage(juicefs.ObjectStorageConfig{
		Type:         cfg.ObjectStorageType,
		Bucket:       cfg.S3Bucket,
		Region:       cfg.S3Region,
		Endpoint:     cfg.S3Endpoint,
		AccessKey:    cfg.S3AccessKey,
		SecretKey:    cfg.S3SecretKey,
		SessionToken: cfg.S3SessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create replay payload storage: %w", err)
	}

	if cfg.JuiceFSEncryptionEnabled {
		keyPEM, err := juicefs.LoadEncryptionKey(cfg.JuiceFSEncryptionKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load encryption key: %w", err)
		}
		encryptor, err := juicefs.NewEncryptor(keyPEM, cfg.JuiceFSEncryptionPassphrase, cfg.JuiceFSEncryptionAlgo)
		if err != nil {
			return nil, fmt.Errorf("create encryptor: %w", err)
		}
		store = juicefs.WrapEncryptedStorage(store, encryptor)
	}

	return NewObjectReplayPayloadStore(store), nil
}

func (s *objectReplayPayloadStore) PutPayload(_ context.Context, volume *db.SandboxVolume, contentSHA256 string, payload []byte) (string, error) {
	if s == nil || s.store == nil {
		return "", fmt.Errorf("replay payload storage unavailable")
	}
	key, err := replayPayloadObjectKey(volume, contentSHA256)
	if err != nil {
		return "", err
	}
	if err := s.store.Put(key, bytes.NewReader(payload)); err != nil {
		return "", fmt.Errorf("put replay payload %q: %w", key, err)
	}
	return buildReplayPayloadRef(contentSHA256), nil
}

func (s *objectReplayPayloadStore) GetPayload(_ context.Context, volume *db.SandboxVolume, ref string) (io.ReadCloser, error) {
	if s == nil || s.store == nil {
		return nil, fmt.Errorf("replay payload storage unavailable")
	}
	contentSHA256, err := parseReplayPayloadRef(ref)
	if err != nil {
		return nil, err
	}
	key, err := replayPayloadObjectKey(volume, contentSHA256)
	if err != nil {
		return nil, err
	}
	reader, err := s.store.Get(key, 0, -1)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrReplayPayloadNotFound
		}
		return nil, fmt.Errorf("get replay payload %q: %w", key, err)
	}
	return reader, nil
}

func buildReplayPayloadRef(contentSHA256 string) string {
	return replayPayloadRefPrefix + contentSHA256
}

func parseReplayPayloadRef(ref string) (string, error) {
	value := strings.TrimSpace(ref)
	if !strings.HasPrefix(value, replayPayloadRefPrefix) {
		return "", ErrInvalidReplayPayloadRef
	}
	sum := strings.TrimPrefix(value, replayPayloadRefPrefix)
	if len(sum) != 64 {
		return "", ErrInvalidReplayPayloadRef
	}
	for _, r := range sum {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return "", ErrInvalidReplayPayloadRef
		}
	}
	return sum, nil
}

func replayPayloadObjectKey(volume *db.SandboxVolume, contentSHA256 string) (string, error) {
	if volume == nil {
		return "", fmt.Errorf("volume is nil")
	}
	prefix, err := naming.S3VolumeSyncReplayPrefix(volume.TeamID, volume.ID)
	if err != nil {
		return "", err
	}
	return prefix + "/sha256/" + contentSHA256, nil
}
