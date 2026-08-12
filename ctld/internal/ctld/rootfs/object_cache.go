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
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultObjectCacheDir   = "/var/lib/sandbox0/rootfs-snapshotter/objects"
	staleObjectCacheTempAge = 24 * time.Hour
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
	dir            string
	maxBytes       int64
	minFreeBytes   int64
	maxAge         time.Duration
	sweepInterval  time.Duration
	mu             sync.Mutex
	sweepMu        sync.Mutex
	validated      map[string]objectCacheFileIdentity
	lastAccess     map[string]time.Time
	activeTemps    map[string]struct{}
	lastSweep      time.Time
	sweepScheduled bool
	observer       *Observer
	fetches        singleflight.Group
}

type objectCacheFileIdentity struct {
	device      uint64
	inode       uint64
	size        int64
	modTimeNano int64
}

type objectCacheEntry struct {
	path     string
	size     int64
	lastUsed time.Time
	identity objectCacheFileIdentity
}

func NewObjectCache(cfg ObjectCacheConfig) *ObjectCache {
	if cfg.MaxBytes <= 0 {
		return nil
	}
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		dir = defaultObjectCacheDir
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
		lastAccess:    make(map[string]time.Time),
		activeTemps:   make(map[string]struct{}),
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

// GetOrFetchObject opens a verified v3 object from the node-local CAS cache or
// fills the cache from object storage on a miss.
func (c *ObjectCache) GetOrFetchObject(ctx context.Context, store objectstore.Store, object rootfshead.Object) (io.ReadCloser, bool, error) {
	if err := object.Validate(""); err != nil {
		return nil, false, err
	}
	if c == nil {
		reader, err := store.Get(object.Key, 0, object.Size)
		return reader, false, err
	}
	started := time.Now()
	if reader, ok, err := c.Open(object); err != nil {
		c.observer.ObservePhase("read", "cache_lookup", started, err)
		c.observer.ObserveCache("error")
		return nil, false, err
	} else if ok {
		c.observer.ObservePhase("read", "cache_lookup", started, nil)
		c.observer.ObserveCache("hit")
		return reader, true, nil
	}
	c.observer.ObservePhase("read", "cache_lookup", started, nil)
	c.observer.ObserveCache("miss")
	started = time.Now()
	_, fillErr, _ := c.fetches.Do(object.Key, func() (any, error) {
		if existing, ok, openErr := c.Open(object); openErr != nil {
			return nil, openErr
		} else if ok {
			return nil, existing.Close()
		}
		reader, fetchErr := store.Get(object.Key, 0, object.Size)
		if fetchErr != nil {
			return nil, fetchErr
		}
		defer reader.Close()
		return nil, c.Put(ctx, object, reader)
	})
	if fillErr != nil {
		c.observer.ObservePhase("read", "object_fetch_cache_fill", started, fillErr)
		return c.fetchUncached(object, store, fillErr)
	}
	c.observer.ObservePhase("read", "object_fetch_cache_fill", started, nil)
	cached, ok, err := c.Open(object)
	if err != nil {
		return c.fetchUncached(object, store, err)
	}
	if !ok {
		return c.fetchUncached(object, store, fmt.Errorf("rootfs object cache did not publish %s", object.Digest))
	}
	return cached, false, nil
}

func (c *ObjectCache) fetchUncached(object rootfshead.Object, store objectstore.Store, cacheErr error) (io.ReadCloser, bool, error) {
	reader, err := store.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, false, fmt.Errorf("cache rootfs object: %v; fallback download: %w", cacheErr, err)
	}
	return reader, false, nil
}

func (c *ObjectCache) Open(desc rootfshead.Object) (io.ReadCloser, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	path, err := c.pathForDescriptor(desc)
	if err != nil {
		return nil, false, err
	}
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.mu.Lock()
			delete(c.validated, path)
			delete(c.lastAccess, path)
			c.mu.Unlock()
			return nil, false, nil
		}
		return nil, false, err
	}
	identity, ok, err := c.validateOpenFile(desc, path, file)
	if err != nil {
		_ = file.Close()
		c.removeIfIdentity(path, identity)
		return nil, false, err
	} else if !ok {
		_ = file.Close()
		c.removeIfIdentity(path, identity)
		return nil, false, nil
	}
	c.mu.Lock()
	if current, statErr := os.Stat(path); statErr == nil && objectCacheIdentity(current) == identity {
		c.validated[path] = identity
		c.lastAccess[path] = time.Now()
	}
	c.mu.Unlock()
	return file, true, nil
}

func (c *ObjectCache) Put(ctx context.Context, desc rootfshead.Object, reader io.Reader) error {
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
	c.registerTemp(tmpPath)
	defer c.unregisterTemp(tmpPath)
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
	if err := os.Rename(tmpPath, path); err != nil {
		c.mu.Unlock()
		return err
	}
	if removeTmp != nil {
		*removeTmp = false
	}
	now := time.Now()
	_ = os.Chtimes(path, now, now)
	if info, err := os.Stat(path); err == nil {
		c.validated[path] = objectCacheIdentity(info)
		c.lastAccess[path] = now
	}
	due := !c.sweepScheduled && (c.lastSweep.IsZero() || now.Sub(c.lastSweep) >= c.sweepInterval)
	if due {
		// Reserve this sweep before releasing the state lock so a burst of
		// cache fills cannot all start the same directory traversal.
		c.lastSweep = now
		c.sweepScheduled = true
	}
	c.mu.Unlock()
	if due {
		go c.runScheduledSweep()
	}
	return nil
}

func (c *ObjectCache) runScheduledSweep() {
	c.sweepMu.Lock()
	err := c.sweep()
	c.sweepMu.Unlock()
	c.mu.Lock()
	c.sweepScheduled = false
	c.mu.Unlock()
	if err != nil {
		fmt.Fprintf(os.Stderr, "rootfs object cache sweep failed: %v\n", err)
	}
}

func (c *ObjectCache) Sweep() error {
	if c == nil {
		return nil
	}
	c.sweepMu.Lock()
	defer c.sweepMu.Unlock()
	c.mu.Lock()
	c.lastSweep = time.Now()
	c.mu.Unlock()
	return c.sweep()
}

func (c *ObjectCache) sweep() error {
	c.mu.Lock()
	accessed := make(map[string]time.Time, len(c.lastAccess))
	for path, timestamp := range c.lastAccess {
		accessed[path] = timestamp
	}
	c.mu.Unlock()

	now := time.Now()
	entries, total, err := c.entries(accessed, now)
	if err != nil {
		return err
	}
	if c.maxAge > 0 {
		kept := entries[:0]
		for _, entry := range entries {
			if now.Sub(entry.lastUsed) > c.maxAge && c.removeEntry(entry) {
				total -= entry.size
				continue
			}
			kept = append(kept, entry)
		}
		entries = kept
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUsed.Before(entries[j].lastUsed)
	})
	for _, entry := range entries {
		if total <= c.maxBytes && c.hasMinFree() {
			break
		}
		if c.removeEntry(entry) {
			total -= entry.size
		}
	}
	return nil
}

func (c *ObjectCache) entries(accessed map[string]time.Time, now time.Time) ([]objectCacheEntry, int64, error) {
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
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		identity := objectCacheIdentity(info)
		if strings.HasPrefix(entry.Name(), ".") {
			if now.Sub(info.ModTime()) > staleObjectCacheTempAge {
				c.removeStaleTemp(path, identity)
			}
			return nil
		}
		lastUsed := info.ModTime()
		if timestamp := accessed[path]; timestamp.After(lastUsed) {
			lastUsed = timestamp
		}
		entries = append(entries, objectCacheEntry{
			path:     path,
			size:     info.Size(),
			lastUsed: lastUsed,
			identity: identity,
		})
		total += info.Size()
		return nil
	})
	return entries, total, err
}

func (c *ObjectCache) hasMinFree() bool {
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

func (c *ObjectCache) removeEntry(entry objectCacheEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if accessed := c.lastAccess[entry.path]; accessed.After(entry.lastUsed) {
		return false
	}
	info, err := os.Stat(entry.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			delete(c.validated, entry.path)
			delete(c.lastAccess, entry.path)
		}
		return false
	}
	if objectCacheIdentity(info) != entry.identity {
		return false
	}
	if err := os.Remove(entry.path); err != nil {
		return false
	}
	delete(c.validated, entry.path)
	delete(c.lastAccess, entry.path)
	return true
}

func (c *ObjectCache) removeStaleTemp(path string, identity objectCacheFileIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, active := c.activeTemps[path]; active {
		return
	}
	info, err := os.Stat(path)
	if err == nil && objectCacheIdentity(info) == identity {
		_ = os.Remove(path)
	}
}

func (c *ObjectCache) registerTemp(path string) {
	c.mu.Lock()
	c.activeTemps[path] = struct{}{}
	c.mu.Unlock()
}

func (c *ObjectCache) unregisterTemp(path string) {
	c.mu.Lock()
	delete(c.activeTemps, path)
	c.mu.Unlock()
}

func (c *ObjectCache) validateOpenFile(desc rootfshead.Object, path string, file *os.File) (identity objectCacheFileIdentity, valid bool, resultErr error) {
	var validationStarted time.Time
	defer func() {
		if !validationStarted.IsZero() {
			c.observer.ObservePhase("read", "cache_validation", validationStarted, resultErr)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return identity, false, err
	}
	identity = objectCacheIdentity(info)
	if !info.Mode().IsRegular() {
		return identity, false, nil
	}
	if desc.Size > 0 && info.Size() != desc.Size {
		return identity, false, nil
	}
	c.mu.Lock()
	if validated, ok := c.validated[path]; ok && validated == identity {
		c.mu.Unlock()
		return identity, true, nil
	}
	c.mu.Unlock()
	validationStarted = time.Now()
	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return identity, false, fmt.Errorf("parse rootfs object digest: %w", err)
	}
	verifier := d.Verifier()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return identity, false, err
	}
	if _, err := io.Copy(verifier, file); err != nil {
		return identity, false, err
	}
	after, err := file.Stat()
	if err != nil {
		return identity, false, err
	}
	if objectCacheIdentity(after) != identity {
		return identity, false, fmt.Errorf("rootfs object changed while validating %s", desc.Digest)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return identity, false, err
	}
	if !verifier.Verified() {
		return identity, false, nil
	}
	return identity, true, nil
}

func (c *ObjectCache) removeIfIdentity(path string, identity objectCacheFileIdentity) {
	c.mu.Lock()
	defer c.mu.Unlock()
	info, err := os.Stat(path)
	if err == nil && objectCacheIdentity(info) == identity {
		_ = os.Remove(path)
	}
	delete(c.validated, path)
	delete(c.lastAccess, path)
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

func (c *ObjectCache) pathForDescriptor(desc rootfshead.Object) (string, error) {
	d, err := godigest.Parse(strings.TrimSpace(desc.Digest))
	if err != nil {
		return "", fmt.Errorf("parse rootfs object digest: %w", err)
	}
	prefix, err := rootfshead.TeamPrefixFromObjectKey(desc.Key)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	if err := rootfshead.ValidateObjectScope(prefix, desc); err != nil {
		return "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	namespace := godigest.FromString(prefix).Encoded()
	return filepath.Join(c.dir, "teams", namespace, d.Algorithm().String(), d.Encoded()+".object"), nil
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
