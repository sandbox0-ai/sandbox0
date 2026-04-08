package juicefs

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"go.uber.org/zap"
)

// InitConfig holds configuration for JuiceFS initialization
type InitConfig struct {
	Name                 string
	MetaURL              string
	ObjectStorageType    string
	S3Bucket             string
	S3Region             string
	S3Endpoint           string
	S3AccessKey          string
	S3SecretKey          string
	S3SessionToken       string
	BlockSize            int
	Compression          string
	TrashDays            int
	MetaRetries          int
	EncryptionEnabled    bool
	EncryptionKeyPath    string
	EncryptionPassphrase string
	EncryptionAlgo       string
}

// Initializer handles JuiceFS filesystem initialization
type Initializer struct {
	config *InitConfig
	logger *zap.Logger
}

// NewInitializer creates a new JuiceFS initializer
func NewInitializer(config *InitConfig, logger *zap.Logger) *Initializer {
	return &Initializer{
		config: config,
		logger: logger,
	}
}

// Initialize initializes the JuiceFS filesystem if not already initialized
// It will:
// 1. Create the object storage bucket if it doesn't exist
// 2. Format JuiceFS metadata if not already formatted
func (i *Initializer) Initialize() error {
	i.logger.Info("Initializing JuiceFS filesystem")

	// Step 1: Ensure object storage bucket exists
	if err := i.ensureBucketExists(); err != nil {
		return fmt.Errorf("ensure bucket exists: %w", err)
	}

	// Step 2: Check if JuiceFS is already formatted
	isFormatted, err := i.isFormatted()
	if err != nil {
		return fmt.Errorf("check if formatted: %w", err)
	}

	if isFormatted {
		i.logger.Info("JuiceFS filesystem already initialized")
		return nil
	}

	// Step 3: Format JuiceFS
	if err := i.format(); err != nil {
		return fmt.Errorf("format filesystem: %w", err)
	}

	i.logger.Info("JuiceFS filesystem initialized successfully")
	return nil
}

// ensureBucketExists creates the object storage bucket if it doesn't exist.
func (i *Initializer) ensureBucketExists() error {
	i.logger.Info("Checking if object storage bucket exists",
		zap.String("bucket", i.config.S3Bucket),
		zap.String("storage_type", NormalizeObjectStorageType(i.config.ObjectStorageType)))

	// Create object storage client.
	store, err := i.createStorage()
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	// Check if bucket exists by listing root
	// If the bucket doesn't exist, this will fail
	_, err = store.Head("")
	if err == nil {
		i.logger.Info("Object storage bucket already exists", zap.String("bucket", i.config.S3Bucket))
		return nil
	}

	// Try to create the bucket. Some backends need explicit Create(), while
	// S3-compatible implementations may auto-create on first write.
	i.logger.Info("Object storage bucket may not exist, attempting to create marker file",
		zap.String("bucket", i.config.S3Bucket))

	// Create a marker file to trigger bucket creation in auto-create scenarios
	markerKey := ".juicefs-init-marker"
	markerData := bytes.NewReader([]byte("initialized"))
	if err := store.Put(markerKey, markerData); err != nil {
		i.logger.Warn("Failed to create marker file, bucket may need manual creation",
			zap.String("bucket", i.config.S3Bucket),
			zap.Error(err))
		// Don't fail here. Some implementations will create the bucket on first PUT.
	} else {
		i.logger.Info("Object storage bucket initialized successfully", zap.String("bucket", i.config.S3Bucket))
	}

	return nil
}

// isFormatted checks if JuiceFS is already formatted
func (i *Initializer) isFormatted() (bool, error) {
	metaConf := meta.DefaultConf()
	metaConf.Retries = i.config.MetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 3
	}

	metaClient := meta.NewClient(i.config.MetaURL, metaConf)

	// Try to load existing format
	_, err := metaClient.Load(false) // false = don't create if not exists
	if err != nil {
		// If error contains "not found" or "doesn't exist", it's not formatted
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "not found") ||
			strings.Contains(errStr, "doesn't exist") ||
			strings.Contains(errStr, "no such") ||
			strings.Contains(errStr, "not formatted") {
			return false, nil
		}
		// Other errors are real errors
		return false, fmt.Errorf("load metadata: %w", err)
	}

	return true, nil
}

// format formats the JuiceFS filesystem
func (i *Initializer) format() error {
	i.logger.Info("Formatting JuiceFS filesystem")

	// Create metadata client
	metaConf := meta.DefaultConf()
	metaConf.Retries = i.config.MetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 10
	}

	metaClient := meta.NewClient(i.config.MetaURL, metaConf)

	// Create storage client
	store, err := i.createStorage()
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}

	// Create format configuration
	format := &meta.Format{
		Name:         i.config.Name,
		UUID:         "", // Will be auto-generated
		Storage:      NormalizeObjectStorageType(i.config.ObjectStorageType),
		Bucket:       i.buildBucketURL(),
		AccessKey:    i.config.S3AccessKey,
		SecretKey:    i.config.S3SecretKey,
		SessionToken: i.config.S3SessionToken,
		BlockSize:    i.config.BlockSize,
		Compression:  i.config.Compression,
		TrashDays:    i.config.TrashDays,
		MetaVersion:  1,
	}

	if format.Name == "" {
		format.Name = "sandbox0"
	}
	if format.BlockSize == 0 {
		format.BlockSize = 4096
	}
	if format.Compression == "" {
		format.Compression = "lz4"
	}

	if i.config.EncryptionEnabled {
		keyPEM, err := LoadEncryptionKey(i.config.EncryptionKeyPath)
		if err != nil {
			return fmt.Errorf("load encryption key: %w", err)
		}
		format.EncryptKey = keyPEM
		if i.config.EncryptionAlgo != "" {
			format.EncryptAlgo = i.config.EncryptionAlgo
		}
		if format.EncryptAlgo == "" {
			format.EncryptAlgo = EncryptionAlgoAES256GCMRSA
		}
		if format.UUID == "" {
			format.UUID = uuid.NewString()
		}
		if _, err := NewEncryptor(keyPEM, i.config.EncryptionPassphrase, format.EncryptAlgo); err != nil {
			return fmt.Errorf("init encryption: %w", err)
		}
		if err := format.Encrypt(); err != nil {
			return fmt.Errorf("encrypt format secrets: %w", err)
		}
	}

	// Initialize the format in metadata engine
	if err := metaClient.Init(format, true); err != nil {
		return fmt.Errorf("initialize metadata: %w", err)
	}

	// Verify the storage is accessible
	verifyData := bytes.NewReader([]byte("OK"))
	if err := store.Put(".juicefs", verifyData); err != nil {
		return fmt.Errorf("verify storage access: %w", err)
	}

	i.logger.Info("JuiceFS filesystem formatted successfully",
		zap.String("name", format.Name),
		zap.String("bucket", format.Bucket),
		zap.Int("block_size_kb", format.BlockSize),
		zap.String("compression", format.Compression))

	return nil
}

// createStorage creates an object storage client.
func (i *Initializer) createStorage() (object.ObjectStorage, error) {
	store, err := CreateObjectStorage(ObjectStorageConfig{
		Type:         i.config.ObjectStorageType,
		Bucket:       i.config.S3Bucket,
		Region:       i.config.S3Region,
		Endpoint:     i.config.S3Endpoint,
		AccessKey:    i.config.S3AccessKey,
		SecretKey:    i.config.S3SecretKey,
		SessionToken: i.config.S3SessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create storage client: %w", err)
	}

	return store, nil
}

// buildBucketURL builds the bucket URL for JuiceFS format.
func (i *Initializer) buildBucketURL() string {
	_, endpoint, err := BuildObjectStorageEndpoint(ObjectStorageConfig{
		Type:     i.config.ObjectStorageType,
		Bucket:   i.config.S3Bucket,
		Region:   i.config.S3Region,
		Endpoint: i.config.S3Endpoint,
	})
	if err != nil {
		return ""
	}
	return endpoint
}
