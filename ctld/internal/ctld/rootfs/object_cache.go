package rootfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	godigest "github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"
)

type ObjectCacheConfig struct {
	Dir           string
	MaxBytes      int64
	MinFreeBytes  int64
	MaxAge        time.Duration
	SweepInterval time.Duration
	Observer      *Observer
}

type ObjectCache struct {
	dir           string
	maxBytes      int64
	minFreeBytes  int64
	maxAge        time.Duration
	sweepInterval time.Duration
	mu            sync.Mutex
	validated     map[string]objectCacheFileIdentity
	observer      *Observer
}

type objectCacheFileIdentity struct {
	device      uint64
	inode       uint64
	size        int64
	modTimeNano int64
}

type objectCacheEntry struct {
	path    string
	size    int64
	modTime time.Time
}

func NewObjectCache(cfg ObjectCacheConfig) *ObjectCache {
	if cfg.MaxBytes <= 0 {
		return nil
	}
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		dir = filepath.Join(defaultRootFSCacheDir, "objects")
	}
	interval := cfg.SweepInterval
	if interval <= 0 {
		interval = time.Minute
	}
	return &ObjectCache{
		dir:           dir,
		maxBytes:      cfg.MaxBytes,
		minFreeBytes:  cfg.MinFreeBytes,
		maxAge:        cfg.MaxAge,
		sweepInterval: interval,
		validated:     make(map[string]objectCacheFileIdentity),
		observer:      cfg.Observer,
	}
}

func (c *ObjectCache) Start(ctx context.Context) {
	if c == nil {
		return
	}
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := c.Sweep(); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "rootfs object cache sweep failed: %v\n", err)
		}
	}, c.sweepInterval)
}

func (c *ObjectCache) GetOrFetch(ctx context.Context, store objectstore.Store, desc ctldapi.RootFSDiffDescriptor) (io.ReadCloser, bool, error) {
	if c == nil {
		reader, err := store.Get(desc.ObjectKey, 0, -1)
		return reader, false, err
	}
	started := time.Now()
	if reader, ok, err := c.Open(desc); err != nil {
		c.observer.ObservePhase("apply", "cache_lookup", started, err)
		c.observer.ObserveCache("error")
		return nil, false, err
	} else if ok {
		c.observer.ObservePhase("apply", "cache_lookup", started, nil)
		c.observer.ObserveCache("hit")
		return reader, true, nil
	}
	c.observer.ObservePhase("apply", "cache_lookup", started, nil)
	c.observer.ObserveCache("miss")
	started = time.Now()
	reader, err := store.Get(desc.ObjectKey, 0, -1)
	if err != nil {
		c.observer.ObservePhase("apply", "object_fetch_cache_fill", started, err)
		return nil, false, err
	}
	defer reader.Close()
	if err := c.Put(ctx, desc, reader); err != nil {
		c.observer.ObservePhase("apply", "object_fetch_cache_fill", started, err)
		return c.fetchUncached(desc, store, err)
	}
	c.observer.ObservePhase("apply", "object_fetch_cache_fill", started, nil)
	cached, ok, err := c.Open(desc)
	if err != nil {
		return c.fetchUncached(desc, store, err)
	}
	if !ok {
		return c.fetchUncached(desc, store, fmt.Errorf("rootfs object cache did not publish %s", desc.Digest))
	}
	return cached, false, nil
}

func (c *ObjectCache) fetchUncached(desc ctldapi.RootFSDiffDescriptor, store objectstore.Store, cacheErr error) (io.ReadCloser, bool, error) {
	reader, err := store.Get(desc.ObjectKey, 0, -1)
	if err != nil {
		return nil, false, fmt.Errorf("cache rootfs object: %v; fallback download: %w", cacheErr, err)
	}
	return reader, false, nil
}

func (c *ObjectCache) Open(desc ctldapi.RootFSDiffDescriptor) (io.ReadCloser, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	path, err := c.pathForDescriptor(desc)
	if err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			delete(c.validated, path)
			return nil, false, nil
		}
		return nil, false, err
	}
	if ok, err := c.validateOpenFile(desc, path, file); err != nil {
		_ = file.Close()
		delete(c.validated, path)
		_ = os.Remove(path)
		return nil, false, err
	} else if !ok {
		_ = file.Close()
		delete(c.validated, path)
		_ = os.Remove(path)
		return nil, false, nil
	}
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	if info, statErr := file.Stat(); statErr == nil {
		c.validated[path] = objectCacheIdentity(info)
	}
	return file, true, nil
}

func (c *ObjectCache) PutFile(ctx context.Context, desc ctldapi.RootFSDiffDescriptor, sourcePath string) error {
	if c == nil {
		return nil
	}
	path, err := c.pathForDescriptor(desc)
	if err != nil {
		return err
	}
	if info, err := os.Stat(sourcePath); err != nil {
		return err
	} else if desc.Size > 0 && info.Size() != desc.Size {
		return fmt.Errorf("rootfs object size mismatch: expected %d, got %d", desc.Size, info.Size())
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(parent, ".object-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	_ = os.Remove(tmpPath)
	if err := os.Link(sourcePath, tmpPath); err == nil {
		removeTmp := true
		defer func() {
			if removeTmp {
				_ = os.Remove(tmpPath)
			}
		}()
		if err := verifyRootFSObjectFile(desc, tmpPath); err != nil {
			return err
		}
		return c.publishTemp(path, tmpPath, &removeTmp)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	return c.Put(ctx, desc, source)
}

func (c *ObjectCache) Put(ctx context.Context, desc ctldapi.RootFSDiffDescriptor, reader io.Reader) error {
	if c == nil {
		return nil
	}
	path, err := c.pathForDescriptor(desc)
	if err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(parent, ".object-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		_ = tmp.Close()
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return fmt.Errorf("parse rootfs object digest: %w", err)
	}
	verifier := d.Verifier()
	written, err := copyContext(ctx, io.MultiWriter(tmp, verifier), reader)
	if err != nil {
		return err
	}
	if desc.Size > 0 && written != desc.Size {
		return fmt.Errorf("rootfs object size mismatch: expected %d, got %d", desc.Size, written)
	}
	if !verifier.Verified() {
		return fmt.Errorf("rootfs object digest mismatch: expected %s", d.String())
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return c.publishTemp(path, tmpPath, &removeTmp)
}

func (c *ObjectCache) publishTemp(path, tmpPath string, removeTmp *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if removeTmp != nil {
		*removeTmp = false
	}
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	if info, err := os.Stat(path); err == nil {
		c.validated[path] = objectCacheIdentity(info)
	}
	return c.sweepLocked()
}

func (c *ObjectCache) Sweep() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepLocked()
}

func (c *ObjectCache) sweepLocked() error {
	entries, total, err := c.entriesLocked()
	if err != nil {
		return err
	}
	now := time.Now()
	if c.maxAge > 0 {
		kept := entries[:0]
		total = 0
		for _, entry := range entries {
			if now.Sub(entry.modTime) > c.maxAge {
				_ = os.Remove(entry.path)
				delete(c.validated, entry.path)
				continue
			}
			kept = append(kept, entry)
			total += entry.size
		}
		entries = kept
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].modTime.Before(entries[j].modTime)
	})
	for _, entry := range entries {
		if total <= c.maxBytes && c.hasMinFreeLocked() {
			break
		}
		if err := os.Remove(entry.path); err == nil {
			delete(c.validated, entry.path)
			total -= entry.size
		}
	}
	return nil
}

func (c *ObjectCache) entriesLocked() ([]objectCacheEntry, int64, error) {
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return nil, 0, err
	}
	var entries []objectCacheEntry
	var total int64
	err := filepath.WalkDir(c.dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasPrefix(entry.Name(), ".") {
			_ = os.Remove(path)
			delete(c.validated, path)
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		entries = append(entries, objectCacheEntry{
			path:    path,
			size:    info.Size(),
			modTime: info.ModTime(),
		})
		total += info.Size()
		return nil
	})
	return entries, total, err
}

func (c *ObjectCache) hasMinFreeLocked() bool {
	if c.minFreeBytes <= 0 {
		return true
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(c.dir, &stat); err != nil {
		return true
	}
	free := int64(stat.Bavail) * int64(stat.Bsize)
	return free >= c.minFreeBytes
}

func (c *ObjectCache) validateOpenFile(desc ctldapi.RootFSDiffDescriptor, path string, file *os.File) (valid bool, resultErr error) {
	var validationStarted time.Time
	defer func() {
		if !validationStarted.IsZero() {
			c.observer.ObservePhase("apply", "cache_validation", validationStarted, resultErr)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, nil
	}
	if desc.Size > 0 && info.Size() != desc.Size {
		return false, nil
	}
	identity := objectCacheIdentity(info)
	if validated, ok := c.validated[path]; ok && validated == identity {
		return true, nil
	}
	validationStarted = time.Now()
	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return false, fmt.Errorf("parse rootfs object digest: %w", err)
	}
	verifier := d.Verifier()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	if _, err := io.Copy(verifier, file); err != nil {
		return false, err
	}
	after, err := file.Stat()
	if err != nil {
		return false, err
	}
	if objectCacheIdentity(after) != identity {
		return false, fmt.Errorf("rootfs object changed while validating %s", desc.Digest)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	if !verifier.Verified() {
		return false, nil
	}
	c.validated[path] = identity
	return true, nil
}

func verifyRootFSObjectFile(desc ctldapi.RootFSDiffDescriptor, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("rootfs object is not a regular file")
	}
	if desc.Size > 0 && info.Size() != desc.Size {
		return fmt.Errorf("rootfs object size mismatch: expected %d, got %d", desc.Size, info.Size())
	}
	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return fmt.Errorf("parse rootfs object digest: %w", err)
	}
	verifier := d.Verifier()
	if _, err := io.Copy(verifier, file); err != nil {
		return err
	}
	after, err := file.Stat()
	if err != nil {
		return err
	}
	if objectCacheIdentity(after) != objectCacheIdentity(info) {
		return fmt.Errorf("rootfs object changed while validating %s", desc.Digest)
	}
	if !verifier.Verified() {
		return fmt.Errorf("rootfs object digest mismatch: expected %s", d.String())
	}
	return nil
}

func objectCacheIdentity(info os.FileInfo) objectCacheFileIdentity {
	identity := objectCacheFileIdentity{
		size:        info.Size(),
		modTimeNano: info.ModTime().UnixNano(),
	}
	if stat, ok := info.Sys().(*unix.Stat_t); ok {
		identity.device = uint64(stat.Dev)
		identity.inode = stat.Ino
	}
	return identity
}

func (c *ObjectCache) pathForDescriptor(desc ctldapi.RootFSDiffDescriptor) (string, error) {
	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return "", fmt.Errorf("parse rootfs object digest: %w", err)
	}
	if strings.TrimSpace(desc.ObjectKey) == "" {
		return "", fmt.Errorf("%w: descriptor object_key is required", ErrBadRequest)
	}
	return filepath.Join(c.dir, d.Algorithm().String(), d.Encoded()+".tar"), nil
}

func copyContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	buf := make([]byte, 256*1024)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			written += int64(nw)
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}
