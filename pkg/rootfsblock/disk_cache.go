package rootfsblock

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"
)

const (
	diskCacheMaxEntries = 131072
	diskCacheQueueBytes = 8 << 20
)

// DiskCacheConfig enables a disposable, node-owned tier below the verified
// memory cache. It contains plaintext immutable ranges and must never be
// exposed to a sandbox. Both fields must be set together; zero disables it.
type DiskCacheConfig struct {
	Directory string
	MaxBytes  int64
}

// ReadCacheStats reports cumulative disk activity and bounded retained state.
type ReadCacheStats struct {
	DiskHits         uint64
	DiskMisses       uint64
	DiskWrites       uint64
	DiskErrors       uint64
	DiskWriteDrops   uint64
	DiskBytes        int64
	DiskEntries      int
	QueuedWriteBytes int64
}

type diskCacheWrite struct {
	key     readCacheKey
	payload []byte
}

// diskReadCache stores only content-addressed, verified ranges. Its index and
// write queue are bounded separately from disk occupancy. Startup streams the
// directory rather than loading an unbounded list; content is checked on use.
type diskReadCache struct {
	root     *os.Root
	lock     *os.File
	maxBytes int64
	mu       sync.Mutex
	items    map[readCacheKey]*list.Element
	pending  map[readCacheKey]struct{}
	order    *list.List
	bytes    int64
	queued   int64
	closed   bool
	queue    chan diskCacheWrite
	done     chan struct{}
	close    sync.Once
	closeErr error
	readers  sync.RWMutex
	slots    chan struct{}
	flights  singleflight.Group
	hits     atomic.Uint64
	misses   atomic.Uint64
	writes   atomic.Uint64
	errors   atomic.Uint64
	drops    atomic.Uint64
}

func openDiskReadCache(config DiskCacheConfig) (*diskReadCache, error) {
	if config.Directory == "" && config.MaxBytes == 0 {
		return nil, nil
	}
	if config.MaxBytes <= 0 || !filepath.IsAbs(config.Directory) || filepath.Clean(config.Directory) == string(os.PathSeparator) {
		return nil, fmt.Errorf("disk cache requires a non-root absolute directory and positive byte budget")
	}
	if err := os.MkdirAll(config.Directory, 0o700); err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(config.Directory)
	if err != nil || resolved != filepath.Clean(config.Directory) {
		return nil, fmt.Errorf("disk cache directory must not contain symlinks")
	}
	root, err := os.OpenRoot(resolved)
	if err != nil {
		return nil, err
	}
	directory, err := root.Open(".")
	if err == nil {
		err = validateDiskCacheOwner(directory)
		directory.Close()
	}
	if err != nil {
		root.Close()
		return nil, err
	}
	if err := root.Chmod(".", 0o700); err != nil {
		root.Close()
		return nil, err
	}
	lock, err := root.OpenFile(".lock", os.O_CREATE|os.O_RDWR|diskCacheOpenFlags, 0o600)
	if err != nil {
		root.Close()
		return nil, err
	}
	if info, err := lock.Stat(); err != nil || !info.Mode().IsRegular() {
		lock.Close()
		root.Close()
		return nil, fmt.Errorf("disk cache lock must be a regular file")
	}
	if err := validateDiskCacheOwner(lock); err != nil {
		lock.Close()
		root.Close()
		return nil, err
	}
	if err := lockDiskReadCache(lock); err != nil {
		lock.Close()
		root.Close()
		return nil, fmt.Errorf("lock node read cache: %w", err)
	}
	c := &diskReadCache{
		root: root, lock: lock, maxBytes: config.MaxBytes,
		items: make(map[readCacheKey]*list.Element), order: list.New(),
		pending: make(map[readCacheKey]struct{}),
		queue:   make(chan diskCacheWrite, 32), done: make(chan struct{}), slots: make(chan struct{}, 8),
	}
	if err := c.restore(); err != nil {
		lock.Close()
		root.Close()
		return nil, err
	}
	go c.writeLoop()
	return c, nil
}

func diskCacheName(key readCacheKey) (string, bool) {
	if key.length <= 0 || key.length > MaxMappingRootBytes || len(key.checksum) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(key.checksum, "sha256:") {
		return "", false
	}
	hexDigest := strings.TrimPrefix(key.checksum, "sha256:")
	if decoded, err := hex.DecodeString(hexDigest); err != nil || hex.EncodeToString(decoded) != hexDigest {
		return "", false
	}
	return hexDigest + "-" + strconv.FormatInt(key.length, 10), true
}

func parseDiskCacheName(name string) (readCacheKey, bool) {
	if len(name) < 66 || name[64] != '-' {
		return readCacheKey{}, false
	}
	length, err := strconv.ParseInt(name[65:], 10, 64)
	if err != nil {
		return readCacheKey{}, false
	}
	key := readCacheKey{checksum: "sha256:" + name[:64], length: length}
	canonical, ok := diskCacheName(key)
	return key, ok && canonical == name
}

func (c *diskReadCache) restore() error {
	directory, err := c.root.Open(".")
	if err != nil {
		return err
	}
	defer directory.Close()
	for {
		entries, readErr := directory.ReadDir(256)
		for _, entry := range entries {
			name := entry.Name()
			if name == ".lock" {
				continue
			}
			if name == ".pending" {
				if err := c.root.Remove(name); err != nil {
					return err
				}
				continue
			}
			key, ok := parseDiskCacheName(name)
			if !ok {
				return fmt.Errorf("unexpected entry in dedicated read cache directory: %q", name)
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() || info.Size() != key.length || key.length > c.maxBytes {
				if err := c.root.Remove(name); err != nil {
					return err
				}
				continue
			}
			if !c.makeRoom(key.length) {
				return fmt.Errorf("cannot enforce disk cache budget during recovery")
			}
			c.items[key] = c.order.PushFront(key)
			c.bytes += key.length
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// makeRoom is called with mu held, or before the cache is exposed. Failed
// removal does not free accounting: a full/unwritable cache simply stops filling.
func (c *diskReadCache) makeRoom(length int64) bool {
	for c.bytes > c.maxBytes-length || len(c.items) >= diskCacheMaxEntries {
		oldest := c.order.Back()
		if oldest == nil {
			return false
		}
		if !c.remove(oldest.Value.(readCacheKey)) {
			return false
		}
	}
	return true
}

func (c *diskReadCache) remove(key readCacheKey) bool {
	name, _ := diskCacheName(key)
	if err := c.root.Remove(name); err != nil && !os.IsNotExist(err) {
		c.errors.Add(1)
		return false
	}
	if element, ok := c.items[key]; ok {
		delete(c.items, key)
		c.order.Remove(element)
		c.bytes -= key.length
	}
	return true
}

func (c *diskReadCache) get(key readCacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.readers.RLock()
	defer c.readers.RUnlock()
	c.mu.Lock()
	_, found := c.items[key]
	closed := c.closed
	c.mu.Unlock()
	if !found || closed {
		c.misses.Add(1)
		return nil, false
	}
	value, _, _ := c.flights.Do(key.flightKey(), func() (any, error) {
		c.slots <- struct{}{}
		defer func() { <-c.slots }()
		name, _ := diskCacheName(key)
		c.mu.Lock()
		element, ok := c.items[key]
		if !ok {
			c.mu.Unlock()
			return nil, nil
		}
		file, err := c.root.OpenFile(name, os.O_RDONLY|diskCacheOpenFlags, 0)
		if err == nil {
			c.order.MoveToFront(element)
		}
		c.mu.Unlock()
		if err == nil {
			defer file.Close()
			var info os.FileInfo
			info, err = file.Stat()
			if err == nil && (!info.Mode().IsRegular() || info.Size() != key.length) {
				err = fmt.Errorf("invalid cached range length or type")
			}
		}
		var payload []byte
		if err == nil {
			payload = make([]byte, key.length+1)
			n, readErr := io.ReadFull(file, payload)
			payload = payload[:n]
			if readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
				err = readErr
			}
			if err == nil {
				sum := sha256.Sum256(payload)
				if int64(len(payload)) != key.length || "sha256:"+hex.EncodeToString(sum[:]) != key.checksum {
					err = fmt.Errorf("cached range checksum mismatch")
				}
			}
		}
		if err != nil {
			c.errors.Add(1)
			c.mu.Lock()
			c.remove(key)
			c.mu.Unlock()
			return nil, nil
		}
		return payload, nil
	})
	if payload, ok := value.([]byte); ok {
		c.hits.Add(1)
		return payload, true
	}
	c.misses.Add(1)
	return nil, false
}

// enqueue retains immutable caller-owned bytes without blocking a demand read
// on disk writes. The byte bound includes the write currently in progress.
func (c *diskReadCache) enqueue(key readCacheKey, payload []byte) {
	if c == nil {
		return
	}
	if _, ok := diskCacheName(key); !ok || int64(len(payload)) != key.length || key.length > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[key]; exists || c.closed {
		return
	}
	if _, exists := c.pending[key]; exists {
		return
	}
	if c.queued+int64(cap(payload)) > diskCacheQueueBytes {
		c.drops.Add(1)
		return
	}
	select {
	case c.queue <- diskCacheWrite{key, payload}:
		c.queued += int64(cap(payload))
		c.pending[key] = struct{}{}
	default:
		c.drops.Add(1)
	}
}

func (c *diskReadCache) writeLoop() {
	defer close(c.done)
	for item := range c.queue {
		c.write(item)
		c.mu.Lock()
		c.queued -= int64(cap(item.payload))
		delete(c.pending, item.key)
		c.mu.Unlock()
	}
}

func (c *diskReadCache) write(item diskCacheWrite) {
	// Check again at the storage boundary. Only authenticated immutable bytes
	// are recoverable cache entries, including after a process restart.
	sum := sha256.Sum256(item.payload)
	if "sha256:"+hex.EncodeToString(sum[:]) != item.key.checksum {
		c.errors.Add(1)
		return
	}
	c.mu.Lock()
	_, exists := c.items[item.key]
	ready := exists || c.makeRoom(item.key.length)
	c.mu.Unlock()
	if exists || !ready {
		return
	}
	file, err := c.root.OpenFile(".pending", os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscallNoFollow, 0o600)
	if err == nil {
		_, err = file.Write(item.payload)
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
	}
	name, _ := diskCacheName(item.key)
	if err == nil {
		// Cache durability is optional: no fsync on the startup path. A torn
		// entry after host failure fails checksum verification and is refetched.
		err = c.root.Rename(".pending", name)
	}
	if err != nil {
		c.errors.Add(1)
		_ = c.root.Remove(".pending")
		return
	}
	c.mu.Lock()
	c.items[item.key] = c.order.PushFront(item.key)
	c.bytes += item.key.length
	c.mu.Unlock()
	c.writes.Add(1)
}

func (c *diskReadCache) Close() error {
	if c == nil {
		return nil
	}
	c.close.Do(func() {
		c.mu.Lock()
		c.closed = true
		close(c.queue)
		c.mu.Unlock()
		<-c.done
		c.readers.Lock()
		defer c.readers.Unlock()
		c.closeErr = c.lock.Close()
		if err := c.root.Close(); c.closeErr == nil {
			c.closeErr = err
		}
	})
	return c.closeErr
}

func (c *diskReadCache) stats() ReadCacheStats {
	if c == nil {
		return ReadCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return ReadCacheStats{
		DiskHits: c.hits.Load(), DiskMisses: c.misses.Load(), DiskWrites: c.writes.Load(),
		DiskErrors: c.errors.Load(), DiskWriteDrops: c.drops.Load(),
		DiskBytes: c.bytes, DiskEntries: len(c.items), QueuedWriteBytes: c.queued,
	}
}
