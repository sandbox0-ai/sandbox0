package snapshot

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/juicefs"
)

const snapshotArchiveChunkSize = 128 * 1024

type ExportSnapshotRequest struct {
	VolumeID   string
	SnapshotID string
	TeamID     string
}

type archiveSessionFactory func(context.Context, *db.SandboxVolume) (*snapshotArchiveSession, error)

type snapshotArchiveSession struct {
	meta   snapshotArchiveMeta
	reader snapshotArchiveReader
	close  func() error
}

type snapshotArchiveMeta interface {
	GetAttr(meta.Context, meta.Ino, *meta.Attr) syscall.Errno
	Readdir(meta.Context, meta.Ino, uint8, *[]*meta.Entry) syscall.Errno
	ReadLink(meta.Context, meta.Ino, *[]byte) syscall.Errno
}

type snapshotArchiveReader interface {
	ReadFile(context.Context, meta.Ino, uint64, io.Writer) error
}

type vfsSnapshotArchiveReader struct {
	vfs *vfs.VFS
}

func (r *vfsSnapshotArchiveReader) ReadFile(ctx context.Context, inode meta.Ino, size uint64, w io.Writer) error {
	if size == 0 {
		return nil
	}
	vfsCtx := vfs.NewLogContext(meta.Background())
	_, handleID, errno := r.vfs.Open(vfsCtx, inode, 0)
	if errno != 0 {
		return fmt.Errorf("open snapshot file inode %d: %w", inode, syscall.Errno(errno))
	}
	defer r.vfs.Release(vfsCtx, inode, handleID)

	buf := make([]byte, snapshotArchiveChunkSize)
	var offset uint64
	for offset < size {
		readSize := len(buf)
		if remaining := size - offset; remaining < uint64(readSize) {
			readSize = int(remaining)
		}
		n, errno := r.vfs.Read(vfsCtx, inode, buf[:readSize], offset, handleID)
		if errno != 0 {
			return fmt.Errorf("read snapshot file inode %d at offset %d: %w", inode, offset, syscall.Errno(errno))
		}
		if n == 0 {
			return fmt.Errorf("short read for snapshot file inode %d at offset %d", inode, offset)
		}
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
		offset += uint64(n)
	}
	return nil
}

func (m *Manager) ExportSnapshotArchive(ctx context.Context, req *ExportSnapshotRequest, w io.Writer) error {
	if req == nil {
		return fmt.Errorf("export snapshot request is nil")
	}

	volume, err := m.repo.GetSandboxVolume(ctx, req.VolumeID)
	if err != nil {
		if err == db.ErrNotFound {
			return ErrVolumeNotFound
		}
		return fmt.Errorf("get volume: %w", err)
	}
	if volume.TeamID != req.TeamID {
		return ErrVolumeNotFound
	}

	snapshot, err := m.GetSnapshot(ctx, req.VolumeID, req.SnapshotID, req.TeamID)
	if err != nil {
		return err
	}

	sessionFactory := m.newArchiveSession
	if sessionFactory == nil {
		sessionFactory = m.openArchiveSession
	}
	session, err := sessionFactory(ctx, volume)
	if err != nil {
		return err
	}
	if session.close != nil {
		defer session.close()
	}

	rootInode := meta.Ino(snapshot.RootInode)
	rootAttr := &meta.Attr{}
	if errno := session.meta.GetAttr(meta.Background(), rootInode, rootAttr); errno != 0 {
		if errno == syscall.ENOENT {
			return ErrSnapshotNotFound
		}
		return fmt.Errorf("get snapshot root inode %d: %w", rootInode, syscall.Errno(errno))
	}

	gzipWriter := gzip.NewWriter(w)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	return writeSnapshotArchiveTree(ctx, tarWriter, session, rootInode, "", rootAttr)
}

func writeSnapshotArchiveTree(ctx context.Context, tw *tar.Writer, session *snapshotArchiveSession, inode meta.Ino, relPath string, attr *meta.Attr) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	switch attr.Typ {
	case meta.TypeDirectory:
		if relPath != "" {
			header := snapshotTarHeader(relPath+"/", attr, tar.TypeDir)
			if err := tw.WriteHeader(header); err != nil {
				return err
			}
		}

		var entries []*meta.Entry
		if errno := session.meta.Readdir(meta.Background(), inode, 1, &entries); errno != 0 {
			return fmt.Errorf("readdir inode %d: %w", inode, syscall.Errno(errno))
		}
		sort.Slice(entries, func(i, j int) bool {
			return string(entries[i].Name) < string(entries[j].Name)
		})
		for _, entry := range entries {
			name := string(entry.Name)
			if name == "." || name == ".." || name == "" {
				continue
			}
			nextPath := name
			if relPath != "" {
				nextPath = path.Join(relPath, name)
			}
			entryAttr := entry.Attr
			if entryAttr == nil {
				entryAttr = &meta.Attr{}
				if errno := session.meta.GetAttr(meta.Background(), entry.Inode, entryAttr); errno != 0 {
					return fmt.Errorf("getattr inode %d: %w", entry.Inode, syscall.Errno(errno))
				}
			}
			if err := writeSnapshotArchiveTree(ctx, tw, session, entry.Inode, nextPath, entryAttr); err != nil {
				return err
			}
		}
		return nil
	case meta.TypeFile:
		header := snapshotTarHeader(relPath, attr, tar.TypeReg)
		header.Size = int64(attr.Length)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		return session.reader.ReadFile(ctx, inode, attr.Length, tw)
	case meta.TypeSymlink:
		var target []byte
		if errno := session.meta.ReadLink(meta.Background(), inode, &target); errno != 0 {
			return fmt.Errorf("readlink inode %d: %w", inode, syscall.Errno(errno))
		}
		header := snapshotTarHeader(relPath, attr, tar.TypeSymlink)
		header.Linkname = string(target)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported snapshot inode type %d at %q", attr.Typ, relPath)
	}
}

func snapshotTarHeader(name string, attr *meta.Attr, typeFlag byte) *tar.Header {
	return &tar.Header{
		Name:     cleanArchivePath(name),
		Mode:     int64(attr.Mode),
		ModTime:  time.Unix(attr.Mtime, int64(attr.Mtimensec)).UTC(),
		Uid:      int(attr.Uid),
		Gid:      int(attr.Gid),
		Typeflag: typeFlag,
	}
}

func cleanArchivePath(name string) string {
	hasTrailingSlash := strings.HasSuffix(name, "/")
	cleaned := path.Clean("/" + strings.TrimSpace(name))
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "." {
		return ""
	}
	if hasTrailingSlash && cleaned != "" {
		return cleaned + "/"
	}
	return cleaned
}

func (m *Manager) openArchiveSession(_ context.Context, volume *db.SandboxVolume) (*snapshotArchiveSession, error) {
	if volume == nil {
		return nil, fmt.Errorf("volume is nil")
	}
	if m.config == nil {
		return nil, fmt.Errorf("storage proxy config is not available")
	}

	metaConf := meta.DefaultConf()
	metaConf.Retries = m.config.JuiceFSMetaRetries
	if metaConf.Retries == 0 {
		metaConf.Retries = 5
	}
	metaConf.ReadOnly = true

	metaClient := meta.NewClient(m.config.MetaURL, metaConf)
	format, err := metaClient.Load(true)
	if err != nil {
		return nil, fmt.Errorf("load juicefs format: %w", err)
	}
	if err := metaClient.NewSession(false); err != nil {
		return nil, fmt.Errorf("create meta session: %w", err)
	}

	volumeCfg := volumeConfigFromRecord(volume)
	prefix, err := naming.S3VolumePrefix(volume.TeamID, volume.ID)
	if err != nil {
		_ = metaClient.CloseSession()
		return nil, fmt.Errorf("build s3 prefix: %w", err)
	}
	obj, err := createSnapshotArchiveStorage(m.config, prefix)
	if err != nil {
		_ = metaClient.CloseSession()
		return nil, err
	}

	defaultCacheSize := parseSnapshotSizeString(m.config.DefaultCacheSize, 1<<30)
	cacheDir := filepath.Join(m.config.CacheDir, "snapshot-export", volume.ID)
	maxUpload := m.config.JuiceFSMaxUpload
	if maxUpload == 0 {
		maxUpload = 20
	}

	chunkConf := chunk.Config{
		BlockSize:     int(format.BlockSize) * 1024,
		Compress:      format.Compression,
		MaxUpload:     maxUpload,
		MaxRetries:    10,
		UploadLimit:   0,
		DownloadLimit: 0,
		Writeback:     false,
		Prefetch:      volumeCfg.Prefetch,
		BufferSize:    uint64(parseSnapshotSizeString(volumeCfg.BufferSize, 32<<20)),
		CacheDir:      cacheDir,
		CacheSize:     uint64(parseSnapshotSizeString(volumeCfg.CacheSize, defaultCacheSize)),
		FreeSpace:     0.1,
		CacheMode:     0o600,
		AutoCreate:    true,
	}

	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(obj, chunkConf, registry)

	attrTimeout, _ := time.ParseDuration(m.config.JuiceFSAttrTimeout)
	if attrTimeout == 0 {
		attrTimeout = time.Second
	}
	entryTimeout, _ := time.ParseDuration(m.config.JuiceFSEntryTimeout)
	if entryTimeout == 0 {
		entryTimeout = time.Second
	}
	dirEntryTimeout, _ := time.ParseDuration(m.config.JuiceFSDirEntryTimeout)
	if dirEntryTimeout == 0 {
		dirEntryTimeout = time.Second
	}

	vfsConf := &vfs.Config{
		Meta:            metaConf,
		Format:          *format,
		Chunk:           &chunkConf,
		Version:         "1.0.0",
		AttrTimeout:     attrTimeout,
		EntryTimeout:    entryTimeout,
		DirEntryTimeout: dirEntryTimeout,
	}
	vfsInst := vfs.NewVFS(vfsConf, metaClient, store, registry, registry)

	return &snapshotArchiveSession{
		meta:   metaClient,
		reader: &vfsSnapshotArchiveReader{vfs: vfsInst},
		close: func() error {
			return metaClient.CloseSession()
		},
	}, nil
}

func createSnapshotArchiveStorage(cfg *config.StorageProxyConfig, prefix string) (object.ObjectStorage, error) {
	endpoint := cfg.S3Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", cfg.S3Region)
	}
	endpoint = strings.TrimRight(endpoint, "/")
	s3Endpoint := fmt.Sprintf("%s/%s", endpoint, cfg.S3Bucket)

	obj, err := object.CreateStorage("s3", s3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3SessionToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 storage: %w", err)
	}
	if prefix != "" {
		p := strings.Trim(prefix, "/")
		if p != "" {
			p += "/"
		}
		obj = object.WithPrefix(obj, p)
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
		obj = juicefs.WrapEncryptedStorage(obj, encryptor)
	}

	return obj, nil
}

func volumeConfigFromRecord(volume *db.SandboxVolume) struct {
	CacheSize  string
	Prefetch   int
	BufferSize string
} {
	return struct {
		CacheSize  string
		Prefetch   int
		BufferSize string
	}{
		CacheSize:  volume.CacheSize,
		Prefetch:   volume.Prefetch,
		BufferSize: volume.BufferSize,
	}
}

func parseSnapshotSizeString(sizeStr string, defaultSize int64) int64 {
	if sizeStr == "" {
		return defaultSize
	}

	var multiplier int64 = 1
	numStr := sizeStr

	switch lastChar := sizeStr[len(sizeStr)-1]; lastChar {
	case 'K', 'k':
		multiplier = 1 << 10
		numStr = sizeStr[:len(sizeStr)-1]
	case 'M', 'm':
		multiplier = 1 << 20
		numStr = sizeStr[:len(sizeStr)-1]
	case 'G', 'g':
		multiplier = 1 << 30
		numStr = sizeStr[:len(sizeStr)-1]
	case 'T', 't':
		multiplier = 1 << 40
		numStr = sizeStr[:len(sizeStr)-1]
	}

	var size int64
	fmt.Sscanf(numStr, "%d", &size)
	if size == 0 {
		return defaultSize
	}
	return size * multiplier
}
