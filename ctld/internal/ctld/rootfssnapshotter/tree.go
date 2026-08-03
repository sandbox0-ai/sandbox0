package rootfssnapshotter

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

const (
	defaultMetadataCacheEntries = 4096
	defaultChunkCacheBytes      = 256 << 20
)

type treeEntry struct {
	entry  rootfshead.Entry
	inode  uint64
	mode   uint32
	xattrs map[string][]byte
}

// LayerTree is a lazy view of one complete immutable rootfs head. It keeps no
// all-path map: lookup reads one bounded name shard per path component.
type LayerTree struct {
	store  objectstore.Store
	root   *treeEntry
	cache  *metadataCache
	chunks *chunkCache
}

func loadHead(ctx context.Context, store objectstore.Store, reference rootfshead.HeadReference) (rootfshead.Head, error) {
	if err := ctx.Err(); err != nil {
		return rootfshead.Head{}, err
	}
	if err := reference.Validate(); err != nil {
		return rootfshead.Head{}, err
	}
	payload, err := readImmutableObject(ctx, store, reference.Manifest)
	if err != nil {
		return rootfshead.Head{}, fmt.Errorf("read rootfs head manifest: %w", err)
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(payload))
	if err != nil {
		return rootfshead.Head{}, err
	}
	if head.HeadID != reference.HeadID {
		return rootfshead.Head{}, fmt.Errorf("rootfs head manifest id %q does not match reference %q", head.HeadID, reference.HeadID)
	}
	return head, nil
}

// LoadLayerTree validates only the bounded head and root entry. Descendant
// metadata and file payload stay lazy until lookup, readdir, or read.
func LoadLayerTree(ctx context.Context, store objectstore.Store, head rootfshead.Head) (*LayerTree, error) {
	return loadLayerTree(ctx, store, head, newChunkCache(defaultChunkCacheBytes))
}

func loadLayerTree(_ context.Context, store objectstore.Store, head rootfshead.Head, chunks *chunkCache) (*LayerTree, error) {
	if store == nil {
		return nil, fmt.Errorf("rootfs object store is required")
	}
	if err := head.Validate(); err != nil {
		return nil, err
	}
	return &LayerTree{
		store:  store,
		root:   newTreeEntry(head.Root, true),
		cache:  newMetadataCache(defaultMetadataCacheEntries),
		chunks: chunks,
	}, nil
}

func (t *LayerTree) lookup(ctx context.Context, directory *rootfshead.Object, name string) (*treeEntry, error) {
	if directory == nil {
		return nil, syscall.ENOTDIR
	}
	index, err := t.loadDirectoryIndex(ctx, *directory)
	if err != nil {
		return nil, err
	}
	bucket := rootfshead.NameBucket(name)
	position := sort.Search(len(index.Shards), func(i int) bool { return index.Shards[i].Bucket >= bucket })
	if position == len(index.Shards) || index.Shards[position].Bucket != bucket {
		return nil, syscall.ENOENT
	}
	shard, err := t.loadDirectoryShard(ctx, index.Shards[position].Object)
	if err != nil {
		return nil, err
	}
	position = sort.Search(len(shard.Entries), func(i int) bool { return shard.Entries[i].Name >= name })
	if position == len(shard.Entries) || shard.Entries[position].Name != name {
		return nil, syscall.ENOENT
	}
	return newTreeEntry(shard.Entries[position], false), nil
}

func (t *LayerTree) readDir(ctx context.Context, directory *rootfshead.Object) ([]*treeEntry, error) {
	if directory == nil {
		return nil, syscall.ENOTDIR
	}
	index, err := t.loadDirectoryIndex(ctx, *directory)
	if err != nil {
		return nil, err
	}
	shards := make([]rootfshead.DirectoryShard, len(index.Shards))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(16)
	for position := range index.Shards {
		position := position
		group.Go(func() error {
			shard, err := t.loadDirectoryShard(groupCtx, index.Shards[position].Object)
			if err == nil {
				shards[position] = shard
			}
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	count := 0
	for _, shard := range shards {
		count += len(shard.Entries)
	}
	entries := make([]*treeEntry, 0, count)
	for _, shard := range shards {
		for _, entry := range shard.Entries {
			entries = append(entries, newTreeEntry(entry, false))
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].entry.Name < entries[j].entry.Name })
	return entries, nil
}

func (t *LayerTree) loadDirectoryIndex(ctx context.Context, object rootfshead.Object) (rootfshead.DirectoryIndex, error) {
	payload, err := t.readMetadata(ctx, object)
	if err != nil {
		return rootfshead.DirectoryIndex{}, err
	}
	return rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
}

func (t *LayerTree) loadDirectoryShard(ctx context.Context, object rootfshead.Object) (rootfshead.DirectoryShard, error) {
	payload, err := t.readMetadata(ctx, object)
	if err != nil {
		return rootfshead.DirectoryShard{}, err
	}
	return rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
}

func (t *LayerTree) loadFile(ctx context.Context, object rootfshead.Object) (rootfshead.FileManifest, error) {
	payload, err := t.readMetadata(ctx, object)
	if err != nil {
		return rootfshead.FileManifest{}, err
	}
	return rootfshead.DecodeFileManifest(bytes.NewReader(payload))
}

func (t *LayerTree) readMetadata(ctx context.Context, object rootfshead.Object) ([]byte, error) {
	if payload, ok := t.cache.get(object.Digest); ok {
		return payload, nil
	}
	payload, err := readImmutableObject(ctx, t.store, object)
	if err != nil {
		return nil, err
	}
	t.cache.put(object.Digest, payload)
	return payload, nil
}

func readImmutableObject(ctx context.Context, store objectstore.Store, object rootfshead.Object) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := digest.Parse(strings.TrimSpace(object.Digest))
	if err != nil {
		return nil, fmt.Errorf("parse object digest: %w", err)
	}
	reader, err := store.Get(object.Key, 0, object.Size)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	payload, err := io.ReadAll(io.LimitReader(reader, object.Size+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(payload)) != object.Size {
		return nil, fmt.Errorf("object size is %d bytes, expected %d", len(payload), object.Size)
	}
	if actual := digest.FromBytes(payload); actual != parsed {
		return nil, fmt.Errorf("object digest is %s, expected %s", actual, parsed)
	}
	return payload, nil
}

func newTreeEntry(entry rootfshead.Entry, root bool) *treeEntry {
	inode := inodeForKey(entry.Inode)
	if root {
		inode = 1
	}
	return &treeEntry{
		entry:  entry,
		inode:  inode,
		mode:   entryMode(entry),
		xattrs: xattrMap(entry.XAttrs),
	}
}

func inodeForKey(key string) uint64 {
	sum := sha256.Sum256([]byte(key))
	inode := binary.LittleEndian.Uint64(sum[:8])
	if inode < 2 {
		inode += 2
	}
	return inode
}

func entryMode(entry rootfshead.Entry) uint32 {
	permissions := entry.Mode & 0o7777
	switch entry.Kind {
	case rootfshead.EntryDirectory:
		return uint32(syscall.S_IFDIR) | permissions
	case rootfshead.EntryFile:
		return uint32(syscall.S_IFREG) | permissions
	case rootfshead.EntrySymlink:
		return uint32(syscall.S_IFLNK) | permissions
	case rootfshead.EntryWhiteout, rootfshead.EntryChar:
		return uint32(syscall.S_IFCHR) | permissions
	case rootfshead.EntryBlock:
		return uint32(syscall.S_IFBLK) | permissions
	case rootfshead.EntryFIFO:
		return uint32(syscall.S_IFIFO) | permissions
	default:
		return permissions
	}
}

func xattrMap(attrs []rootfshead.XAttr) map[string][]byte {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(attrs))
	for _, attr := range attrs {
		out[attr.Name] = append([]byte(nil), attr.Value...)
	}
	return out
}

func (t *LayerTree) readEntryRange(ctx context.Context, entry *treeEntry, destination []byte, offset int64) (int, error) {
	if entry == nil || entry.entry.File == nil {
		return 0, syscall.EIO
	}
	if offset < 0 {
		return 0, syscall.EINVAL
	}
	manifest, err := t.loadFile(ctx, *entry.entry.File)
	if err != nil {
		return 0, err
	}
	if uint64(offset) >= manifest.Size || len(destination) == 0 {
		return 0, nil
	}
	limit := uint64(len(destination))
	if remaining := manifest.Size - uint64(offset); remaining < limit {
		limit = remaining
	}
	output := destination[:limit]
	clear(output)
	readStart := uint64(offset)
	readEnd := readStart + limit
	position := sort.Search(len(manifest.Extents), func(i int) bool {
		extent := manifest.Extents[i]
		return extent.Offset+extent.Length > readStart
	})
	for ; position < len(manifest.Extents); position++ {
		extent := manifest.Extents[position]
		if extent.Offset >= readEnd {
			break
		}
		start := max(readStart, extent.Offset)
		end := min(readEnd, extent.Offset+extent.Length)
		if start >= end {
			continue
		}
		objectOffset := extent.ObjectOffset + start - extent.Offset
		payload, err := t.chunks.load(ctx, t.store, extent.Object)
		if err != nil {
			return 0, err
		}
		target := output[start-readStart : end-readStart]
		objectEnd := objectOffset + uint64(len(target))
		if objectEnd > uint64(len(payload)) {
			return 0, fmt.Errorf("rootfs chunk extent exceeds cached object")
		}
		copy(target, payload[objectOffset:objectEnd])
	}
	return int(limit), nil
}

type chunkCacheEntry struct {
	key     string
	payload []byte
}

// chunkCache is shared by all mounted heads in one snapshotter process. Full
// chunk verification happens once, then ordinary page-sized FUSE reads avoid
// repeated object-store range requests.
type chunkCache struct {
	mu      sync.Mutex
	limit   int64
	used    int64
	entries map[string]*list.Element
	order   *list.List
	loads   singleflight.Group
}

func newChunkCache(limit int64) *chunkCache {
	return &chunkCache{limit: limit, entries: make(map[string]*list.Element), order: list.New()}
}

func (c *chunkCache) load(ctx context.Context, store objectstore.Store, object rootfshead.Object) ([]byte, error) {
	if c == nil {
		return readImmutableObject(ctx, store, object)
	}
	key := object.Key + "\x00" + object.Digest
	if payload, ok := c.get(key); ok {
		return payload, nil
	}
	result, err, _ := c.loads.Do(key, func() (any, error) {
		if payload, ok := c.get(key); ok {
			return payload, nil
		}
		payload, err := readImmutableObject(ctx, store, object)
		if err != nil {
			return nil, err
		}
		c.put(key, payload)
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	return result.([]byte), nil
}

func (c *chunkCache) get(key string) ([]byte, bool) {
	if c == nil || c.limit <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*chunkCacheEntry).payload, true
}

func (c *chunkCache) put(key string, payload []byte) {
	if c == nil || c.limit <= 0 || int64(len(payload)) > c.limit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		entry := element.Value.(*chunkCacheEntry)
		c.used -= int64(len(entry.payload))
		entry.payload = payload
		c.used += int64(len(payload))
		c.order.MoveToFront(element)
	} else {
		element := c.order.PushFront(&chunkCacheEntry{key: key, payload: payload})
		c.entries[key] = element
		c.used += int64(len(payload))
	}
	for c.used > c.limit {
		oldest := c.order.Back()
		entry := oldest.Value.(*chunkCacheEntry)
		delete(c.entries, entry.key)
		c.used -= int64(len(entry.payload))
		c.order.Remove(oldest)
	}
}

type metadataCacheEntry struct {
	digest  string
	payload []byte
}

type metadataCache struct {
	mu      sync.Mutex
	limit   int
	entries map[string]*list.Element
	order   *list.List
}

func newMetadataCache(limit int) *metadataCache {
	return &metadataCache{limit: limit, entries: make(map[string]*list.Element), order: list.New()}
}

func (c *metadataCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil, false
	}
	c.order.MoveToFront(element)
	entry := element.Value.(*metadataCacheEntry)
	return append([]byte(nil), entry.payload...), true
}

func (c *metadataCache) put(key string, payload []byte) {
	if c == nil || c.limit <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		element.Value.(*metadataCacheEntry).payload = append([]byte(nil), payload...)
		c.order.MoveToFront(element)
		return
	}
	element := c.order.PushFront(&metadataCacheEntry{digest: key, payload: append([]byte(nil), payload...)})
	c.entries[key] = element
	for c.order.Len() > c.limit {
		oldest := c.order.Back()
		entry := oldest.Value.(*metadataCacheEntry)
		delete(c.entries, entry.digest)
		c.order.Remove(oldest)
	}
}
