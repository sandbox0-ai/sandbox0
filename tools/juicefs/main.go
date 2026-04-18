package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	jfsfuse "github.com/juicedata/juicefs/pkg/fuse"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	storagejuicefs "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
)

type mountConfig struct {
	metaURL              string
	mountPoint           string
	storageType          string
	bucket               string
	region               string
	endpoint             string
	accessKey            string
	secretKey            string
	sessionToken         string
	objectPrefix         string
	subdir               string
	cacheDir             string
	cacheSize            string
	prefetch             int
	bufferSize           string
	writeback            bool
	readOnly             bool
	attrCache            string
	entryCache           string
	dirEntryCache        string
	encryptionKeyPath    string
	encryptionPassphrase string
	encryptionAlgo       string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "mount" {
		return fmt.Errorf("usage: juicefs mount [options] META-URL MOUNTPOINT")
	}
	cfg, err := parseMountArgs(args[1:])
	if err != nil {
		return err
	}
	return mount(cfg)
}

func parseMountArgs(args []string) (*mountConfig, error) {
	fs := flag.NewFlagSet("mount", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	cfg := &mountConfig{}
	fs.StringVar(&cfg.storageType, "storage", "s3", "object storage type")
	fs.StringVar(&cfg.bucket, "bucket", "", "object storage bucket")
	fs.StringVar(&cfg.region, "region", "", "object storage region")
	fs.StringVar(&cfg.endpoint, "endpoint", "", "object storage endpoint")
	fs.StringVar(&cfg.accessKey, "access-key", "", "object storage access key")
	fs.StringVar(&cfg.secretKey, "secret-key", "", "object storage secret key")
	fs.StringVar(&cfg.sessionToken, "session-token", "", "object storage session token")
	fs.StringVar(&cfg.objectPrefix, "object-prefix", "", "object key prefix")
	fs.StringVar(&cfg.subdir, "subdir", "", "JuiceFS subdir to expose")
	fs.StringVar(&cfg.cacheDir, "cache-dir", "/var/lib/sandbox0/juicefs-cache", "cache directory")
	fs.StringVar(&cfg.cacheSize, "cache-size", "100", "cache size in MiB")
	fs.IntVar(&cfg.prefetch, "prefetch", 1, "prefetch blocks")
	fs.StringVar(&cfg.bufferSize, "buffer-size", "300", "buffer size in MiB")
	fs.BoolVar(&cfg.writeback, "writeback", false, "enable writeback")
	fs.BoolVar(&cfg.readOnly, "read-only", false, "mount read-only")
	fs.StringVar(&cfg.attrCache, "attr-cache", "1s", "attr cache timeout")
	fs.StringVar(&cfg.entryCache, "entry-cache", "1s", "entry cache timeout")
	fs.StringVar(&cfg.dirEntryCache, "dir-entry-cache", "1s", "dir entry cache timeout")
	fs.StringVar(&cfg.encryptionKeyPath, "encryption-key-path", "", "encryption private key path")
	fs.StringVar(&cfg.encryptionPassphrase, "encryption-passphrase", "", "encryption key passphrase")
	fs.StringVar(&cfg.encryptionAlgo, "encryption-algo", "", "encryption algorithm")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	rest := fs.Args()
	if len(rest) != 2 {
		return nil, fmt.Errorf("usage: juicefs mount [options] META-URL MOUNTPOINT")
	}
	cfg.metaURL = rest[0]
	cfg.mountPoint = rest[1]
	return cfg, nil
}

func mount(cfg *mountConfig) error {
	if cfg.metaURL == "" || cfg.mountPoint == "" {
		return fmt.Errorf("meta url and mount point are required")
	}
	if cfg.bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if err := os.MkdirAll(cfg.mountPoint, 0o755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}

	metaConf := meta.DefaultConf()
	metaConf.MountPoint = cfg.mountPoint
	metaConf.Subdir = cfg.subdir
	metaConf.ReadOnly = cfg.readOnly
	metaClient := meta.NewClient(cfg.metaURL, metaConf)
	format, err := metaClient.Load(true)
	if err != nil {
		return fmt.Errorf("load JuiceFS format: %w", err)
	}
	if cfg.storageType != "" {
		format.Storage = storagejuicefs.NormalizeObjectStorageType(cfg.storageType)
	}
	if cfg.bucket != "" {
		format.Bucket = cfg.bucket
	}
	if cfg.accessKey != "" {
		format.AccessKey = cfg.accessKey
	}
	if cfg.secretKey != "" {
		format.SecretKey = cfg.secretKey
	}
	if cfg.sessionToken != "" {
		format.SessionToken = cfg.sessionToken
	}
	if cfg.subdir != "" {
		if st := metaClient.Chroot(meta.Background(), cfg.subdir); st != 0 {
			return fmt.Errorf("chroot %s: %s", cfg.subdir, st.Error())
		}
	}

	store, err := storagejuicefs.CreateObjectStorage(storagejuicefs.ObjectStorageConfig{
		Type:         format.Storage,
		Bucket:       cfg.bucket,
		Region:       cfg.region,
		Endpoint:     cfg.endpoint,
		AccessKey:    format.AccessKey,
		SecretKey:    format.SecretKey,
		SessionToken: format.SessionToken,
	})
	if err != nil {
		return err
	}
	if prefix := strings.Trim(strings.TrimSpace(cfg.objectPrefix), "/"); prefix != "" {
		store = object.WithPrefix(store, prefix+"/")
	}
	if cfg.encryptionKeyPath != "" {
		keyPEM, err := storagejuicefs.LoadEncryptionKey(cfg.encryptionKeyPath)
		if err != nil {
			return fmt.Errorf("load encryption key: %w", err)
		}
		algo := cfg.encryptionAlgo
		if algo == "" {
			algo = storagejuicefs.EncryptionAlgoAES256GCMRSA
		}
		encryptor, err := storagejuicefs.NewEncryptor(keyPEM, cfg.encryptionPassphrase, algo)
		if err != nil {
			return fmt.Errorf("create encryptor: %w", err)
		}
		store = storagejuicefs.WrapEncryptedStorage(store, encryptor)
	}

	if err := os.MkdirAll(cfg.cacheDir, 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	chunkConf := chunk.Config{
		BlockSize:     int(format.BlockSize) * 1024,
		Compress:      format.Compression,
		MaxUpload:     20,
		MaxRetries:    10,
		Writeback:     cfg.writeback,
		Prefetch:      cfg.prefetch,
		BufferSize:    parseMiB(cfg.bufferSize, 300<<20),
		CacheDir:      cfg.cacheDir,
		CacheSize:     parseMiB(cfg.cacheSize, 100<<20),
		FreeSpace:     0.1,
		CacheMode:     0o600,
		AutoCreate:    true,
		UploadLimit:   format.UploadLimit,
		DownloadLimit: format.DownloadLimit,
	}
	registry := prometheus.NewRegistry()
	cachedStore := chunk.NewCachedStore(store, chunkConf, registry)
	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Chunk:           &chunkConf,
		Version:         "sandbox0",
		AttrTimeout:     parseDuration(cfg.attrCache, time.Second),
		EntryTimeout:    parseDuration(cfg.entryCache, time.Second),
		DirEntryTimeout: parseDuration(cfg.dirEntryCache, time.Second),
		Subdir:          cfg.subdir,
	}
	v := vfs.NewVFS(vfsConf, metaClient, cachedStore, registry, registry)
	return jfsfuse.Serve(v, "allow_other", true, false)
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseMiB(value string, fallback uint64) uint64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	multiplier := uint64(1 << 20)
	last := value[len(value)-1]
	switch last {
	case 'k', 'K':
		multiplier = 1 << 10
		value = value[:len(value)-1]
	case 'm', 'M':
		multiplier = 1 << 20
		value = value[:len(value)-1]
	case 'g', 'G':
		multiplier = 1 << 30
		value = value[:len(value)-1]
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed * multiplier
}
