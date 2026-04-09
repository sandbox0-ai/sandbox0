package juicefs

import (
	"fmt"
	"strings"

	"github.com/juicedata/juicefs/pkg/object"
)

const (
	ObjectStorageTypeS3  = "s3"
	ObjectStorageTypeOSS = "oss"
	ObjectStorageTypeGCS = "gs"
)

type ObjectStorageConfig struct {
	Type         string
	Bucket       string
	Region       string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
}

func NormalizeObjectStorageType(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	switch value {
	case "", "builtin", ObjectStorageTypeS3:
		return ObjectStorageTypeS3
	case ObjectStorageTypeOSS:
		return ObjectStorageTypeOSS
	case "gcs", ObjectStorageTypeGCS:
		return ObjectStorageTypeGCS
	default:
		return value
	}
}

func BuildObjectStorageEndpoint(cfg ObjectStorageConfig) (string, string, error) {
	storageType := NormalizeObjectStorageType(cfg.Type)
	bucket := strings.TrimSpace(cfg.Bucket)
	if bucket == "" {
		return "", "", fmt.Errorf("object storage bucket is required")
	}

	switch storageType {
	case ObjectStorageTypeS3:
		endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
		if endpoint == "" {
			region := strings.TrimSpace(cfg.Region)
			if region == "" {
				return "", "", fmt.Errorf("object storage region or endpoint is required for s3")
			}
			endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
		}
		return storageType, fmt.Sprintf("%s/%s", endpoint, bucket), nil
	case ObjectStorageTypeOSS:
		endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
		if endpoint == "" {
			return "", "", fmt.Errorf("object storage endpoint is required for oss")
		}
		return storageType, fmt.Sprintf("%s/%s", endpoint, bucket), nil
	case ObjectStorageTypeGCS:
		return storageType, fmt.Sprintf("gs://%s", bucket), nil
	default:
		return "", "", fmt.Errorf("unsupported object storage type: %s", storageType)
	}
}

func CreateObjectStorage(cfg ObjectStorageConfig) (object.ObjectStorage, error) {
	storageType, endpoint, err := BuildObjectStorageEndpoint(cfg)
	if err != nil {
		return nil, err
	}
	store, err := object.CreateStorage(storageType, endpoint, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken)
	if err != nil {
		return nil, fmt.Errorf("create %s storage client: %w", storageType, err)
	}
	return store, nil
}
