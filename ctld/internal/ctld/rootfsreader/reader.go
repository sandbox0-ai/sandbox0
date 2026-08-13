// Package rootfsreader lazily resolves immutable v3 rootfs metadata and data.
package rootfsreader

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/opencontainers/go-digest"
	ctldrootfs "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfs"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"golang.org/x/sync/singleflight"
)

const defaultMetadataCacheBytes = 64 << 20
const defaultDirectoryShardCacheEntries = 8

var (
	ErrNotFound               = errors.New("rootfs entry not found")
	ErrInvalidDirectoryOffset = errors.New("invalid rootfs directory offset")
)

type ReaderConfig struct {
	Store               objectstore.Store
	Prefix              string
	ObjectCache         *ctldrootfs.ObjectCache
	SharedMetadataCache *MetadataCache
	MetadataCacheBytes  int64
}

type Reader struct {
	store       objectstore.Store
	prefix      string
	objectCache *ctldrootfs.ObjectCache
	metadata    *MetadataCache
	loads       singleflight.Group
}

func New(cfg ReaderConfig) (*Reader, error) {
	if cfg.Store == nil {
		return nil, fmt.Errorf("rootfs reader object store is required")
	}
	maxMetadataBytes := cfg.MetadataCacheBytes
	if maxMetadataBytes == 0 {
		maxMetadataBytes = defaultMetadataCacheBytes
	}
	metadata := cfg.SharedMetadataCache
	if metadata == nil {
		metadata = NewMetadataCache(maxMetadataBytes)
	}
	reader := &Reader{
		store:       cfg.Store,
		prefix:      cfg.Prefix,
		objectCache: cfg.ObjectCache,
		metadata:    metadata,
	}
	if reader.prefix == "" {
		return nil, fmt.Errorf("rootfs reader object prefix is required")
	}
	return reader, nil
}

func NewForHead(ctx context.Context, cfg ReaderConfig, reference rootfshead.HeadReference) (*Reader, rootfshead.Head, error) {
	if err := reference.Validate(); err != nil {
		return nil, rootfshead.Head{}, err
	}
	prefix, err := rootfsstore.PrefixFromObject(reference.Manifest)
	if err != nil {
		return nil, rootfshead.Head{}, err
	}
	if cfg.Prefix != "" && cfg.Prefix != prefix {
		return nil, rootfshead.Head{}, fmt.Errorf("rootfs head prefix %q does not match configured prefix %q", prefix, cfg.Prefix)
	}
	cfg.Prefix = prefix
	reader, err := New(cfg)
	if err != nil {
		return nil, rootfshead.Head{}, err
	}
	head, err := reader.LoadHead(ctx, reference)
	if err != nil {
		return nil, rootfshead.Head{}, err
	}
	return reader, head, nil
}

func (r *Reader) LoadHead(ctx context.Context, reference rootfshead.HeadReference) (rootfshead.Head, error) {
	if err := reference.Validate(); err != nil {
		return rootfshead.Head{}, err
	}
	payload, err := r.readMetadata(ctx, reference.Manifest)
	if err != nil {
		return rootfshead.Head{}, err
	}
	head, err := rootfshead.DecodeHead(bytes.NewReader(payload))
	if err != nil {
		return rootfshead.Head{}, err
	}
	if head.HeadID != reference.HeadID {
		return rootfshead.Head{}, fmt.Errorf("rootfs head id %q does not match reference %q", head.HeadID, reference.HeadID)
	}
	return head, nil
}

func (r *Reader) Lookup(ctx context.Context, directory rootfshead.Entry, name string) (rootfshead.Entry, error) {
	view, err := r.OpenDirectory(ctx, directory)
	if err != nil {
		return rootfshead.Entry{}, err
	}
	return view.Lookup(ctx, name)
}

// Directory is one immutable directory view. It decodes the index once and
// retains only a bounded working set of decoded shards for metadata lookups.
type Directory struct {
	reader *Reader
	index  rootfshead.DirectoryIndex

	mu     sync.Mutex
	shards map[uint8]*list.Element
	order  *list.List
	loads  singleflight.Group
}

type decodedDirectoryShard struct {
	bucket uint8
	shard  rootfshead.DirectoryShard
}

// OpenDirectory loads and validates only the immutable directory index.
func (r *Reader) OpenDirectory(ctx context.Context, directory rootfshead.Entry) (*Directory, error) {
	if directory.Kind != rootfshead.EntryDirectory || directory.Directory == nil {
		return nil, fmt.Errorf("rootfs entry %q is not a directory", directory.Name)
	}
	index, err := r.loadDirectoryIndex(ctx, *directory.Directory)
	if err != nil {
		return nil, err
	}
	return &Directory{reader: r, index: index, shards: make(map[uint8]*list.Element), order: list.New()}, nil
}

// Lookup resolves one name while keeping decoded shard memory bounded.
func (d *Directory) Lookup(ctx context.Context, name string) (rootfshead.Entry, error) {
	if d == nil || d.reader == nil {
		return rootfshead.Entry{}, fmt.Errorf("rootfs directory view is not configured")
	}
	bucket := rootfshead.NameBucket(name)
	position := sort.Search(len(d.index.Shards), func(i int) bool { return d.index.Shards[i].Bucket >= bucket })
	if position == len(d.index.Shards) || d.index.Shards[position].Bucket != bucket {
		return rootfshead.Entry{}, ErrNotFound
	}
	shard, err := d.loadShard(ctx, d.index.Shards[position])
	if err != nil {
		return rootfshead.Entry{}, err
	}
	entryPosition := sort.Search(len(shard.Entries), func(i int) bool { return shard.Entries[i].Name >= name })
	if entryPosition == len(shard.Entries) || shard.Entries[entryPosition].Name != name {
		return rootfshead.Entry{}, ErrNotFound
	}
	return shard.Entries[entryPosition], nil
}

func (d *Directory) loadShard(ctx context.Context, reference rootfshead.ShardRef) (rootfshead.DirectoryShard, error) {
	d.mu.Lock()
	if element := d.shards[reference.Bucket]; element != nil {
		d.order.MoveToFront(element)
		shard := element.Value.(decodedDirectoryShard).shard
		d.mu.Unlock()
		return shard, nil
	}
	d.mu.Unlock()
	value, err, _ := d.loads.Do(fmt.Sprintf("%d:%s", reference.Bucket, reference.Object.Digest), func() (any, error) {
		d.mu.Lock()
		if element := d.shards[reference.Bucket]; element != nil {
			d.order.MoveToFront(element)
			shard := element.Value.(decodedDirectoryShard).shard
			d.mu.Unlock()
			return shard, nil
		}
		d.mu.Unlock()
		shard, err := d.reader.loadDirectoryShard(ctx, reference.Object)
		if err != nil {
			return rootfshead.DirectoryShard{}, err
		}
		if shard.Bucket != reference.Bucket {
			return rootfshead.DirectoryShard{}, fmt.Errorf("rootfs directory shard bucket %d does not match index bucket %d", shard.Bucket, reference.Bucket)
		}
		d.mu.Lock()
		d.shards[reference.Bucket] = d.order.PushFront(decodedDirectoryShard{bucket: reference.Bucket, shard: shard})
		for d.order.Len() > defaultDirectoryShardCacheEntries {
			element := d.order.Back()
			entry := element.Value.(decodedDirectoryShard)
			delete(d.shards, entry.bucket)
			d.order.Remove(element)
		}
		d.mu.Unlock()
		return shard, nil
	})
	if err != nil {
		return rootfshead.DirectoryShard{}, err
	}
	return value.(rootfshead.DirectoryShard), nil
}

func (r *Reader) ReadDir(ctx context.Context, directory rootfshead.Entry) ([]rootfshead.Entry, error) {
	iterator, err := r.OpenDir(ctx, directory)
	if err != nil {
		return nil, err
	}
	var entries []rootfshead.Entry
	for {
		entry, ok, err := iterator.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries, nil
}

// DirectoryIterator lazily decodes one immutable directory shard at a time.
// Bucket order is stable but intentionally not globally lexical.
type DirectoryIterator struct {
	directory     *Directory
	shards        []rootfshead.ShardRef
	shardPosition int
	entries       []rootfshead.Entry
	entryPosition int
}

// OpenDir loads only the small directory index. Individual shards are fetched
// by DirectoryIterator.Next so large FUSE readdir calls stay memory-bounded.
func (r *Reader) OpenDir(ctx context.Context, directory rootfshead.Entry) (*DirectoryIterator, error) {
	view, err := r.OpenDirectory(ctx, directory)
	if err != nil {
		return nil, err
	}
	return view.Iterator(), nil
}

// Iterator streams entries in stable bucket order without retaining every
// decoded shard.
func (d *Directory) Iterator() *DirectoryIterator {
	if d == nil {
		return &DirectoryIterator{}
	}
	return &DirectoryIterator{directory: d, shards: d.index.Shards}
}

// Seek resets the iterator to the directory offset returned by a previous
// Next call. Directory offsets are entry ordinals; replay is bounded in memory
// because it still loads only one shard at a time.
func (i *DirectoryIterator) Seek(ctx context.Context, offset uint64) error {
	if i == nil || i.directory == nil || i.directory.reader == nil {
		return fmt.Errorf("rootfs directory iterator is not configured")
	}
	candidate := i.directory.Iterator()
	for position := uint64(0); position < offset; position++ {
		_, ok, err := candidate.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("rootfs directory offset %d is past the end: %w", offset, ErrInvalidDirectoryOffset)
		}
	}
	*i = *candidate
	return nil
}

// Next returns the next entry in stable bucket order.
func (i *DirectoryIterator) Next(ctx context.Context) (rootfshead.Entry, bool, error) {
	if i == nil || i.directory == nil || i.directory.reader == nil {
		return rootfshead.Entry{}, false, fmt.Errorf("rootfs directory iterator is not configured")
	}
	for i.entryPosition >= len(i.entries) {
		if i.shardPosition >= len(i.shards) {
			return rootfshead.Entry{}, false, nil
		}
		shard, err := i.directory.loadShard(ctx, i.shards[i.shardPosition])
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		i.shardPosition++
		i.entries = shard.Entries
		i.entryPosition = 0
	}
	entry := i.entries[i.entryPosition]
	i.entryPosition++
	return entry, true, nil
}

func (r *Reader) LoadFileManifest(ctx context.Context, file rootfshead.Entry) (rootfshead.FileManifest, error) {
	if file.Kind != rootfshead.EntryFile || file.File == nil {
		return rootfshead.FileManifest{}, fmt.Errorf("rootfs entry %q is not a file", file.Name)
	}
	payload, err := r.readMetadata(ctx, *file.File)
	if err != nil {
		return rootfshead.FileManifest{}, err
	}
	manifest, err := rootfshead.DecodeFileManifest(bytes.NewReader(payload))
	if err != nil {
		return rootfshead.FileManifest{}, err
	}
	if manifest.Size != file.Size || manifest.Blocks != file.Blocks {
		return rootfshead.FileManifest{}, fmt.Errorf(
			"rootfs file %q metadata mismatch: entry size/blocks=%d/%d manifest=%d/%d",
			file.Name, file.Size, file.Blocks, manifest.Size, manifest.Blocks,
		)
	}
	return manifest, nil
}

func (r *Reader) ReadFile(ctx context.Context, file rootfshead.Entry, destination []byte, offset int64) (int, error) {
	manifest, err := r.LoadFileManifest(ctx, file)
	if err != nil {
		return 0, err
	}
	return r.ReadFileManifest(ctx, manifest, destination, offset)
}

// ReadFileManifest reads from a validated immutable manifest returned by
// LoadFileManifest. Keeping the decoded manifest at the FUSE inode avoids
// decompressing and decoding it for every kernel read request.
func (r *Reader) ReadFileManifest(ctx context.Context, manifest rootfshead.FileManifest, destination []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("rootfs file offset %d is invalid", offset)
	}
	if uint64(offset) >= manifest.Size || len(destination) == 0 {
		return 0, io.EOF
	}
	length := min(uint64(len(destination)), manifest.Size-uint64(offset))
	clear(destination[:length])
	readStart := uint64(offset)
	readEnd := readStart + length
	position := sort.Search(len(manifest.Extents), func(i int) bool {
		extent := manifest.Extents[i]
		return extent.Offset+extent.Length > readStart
	})
	for ; position < len(manifest.Extents); position++ {
		extent := manifest.Extents[position]
		if extent.Offset >= readEnd {
			break
		}
		overlapStart := max(readStart, extent.Offset)
		overlapEnd := min(readEnd, extent.Offset+extent.Length)
		if overlapStart >= overlapEnd {
			continue
		}
		sourceStart := extent.ObjectOffset + overlapStart - extent.Offset
		destinationStart := overlapStart - readStart
		destinationEnd := destinationStart + overlapEnd - overlapStart
		if _, err := r.readObjectRange(ctx, extent.Object, destination[destinationStart:destinationEnd], sourceStart); err != nil {
			return 0, err
		}
	}
	if length < uint64(len(destination)) {
		return int(length), io.EOF
	}
	return int(length), nil
}

func (r *Reader) readObjectRange(ctx context.Context, object rootfshead.Object, destination []byte, offset uint64) (int, error) {
	if err := rootfshead.ValidateReadableObjectScope(r.prefix, object); err != nil {
		return 0, err
	}
	if object.Size < 0 {
		return 0, fmt.Errorf("rootfs object %s has negative size", object.Key)
	}
	if offset > uint64(object.Size) || uint64(len(destination)) > uint64(object.Size)-offset {
		return 0, fmt.Errorf("rootfs object %s range [%d,%d) exceeds size %d", object.Key, offset, offset+uint64(len(destination)), object.Size)
	}
	if len(destination) == 0 {
		return 0, nil
	}
	if r.objectCache != nil {
		reader, _, err := r.objectCache.GetOrFetchObject(ctx, r.store, object)
		if err != nil {
			return 0, fmt.Errorf("fetch rootfs object %s: %w", object.Key, err)
		}
		if readerAt, ok := reader.(io.ReaderAt); ok {
			read, readErr := readerAt.ReadAt(destination, int64(offset))
			closeErr := reader.Close()
			if readErr == io.EOF && read == len(destination) {
				readErr = nil
			}
			if readErr != nil || closeErr != nil {
				return read, fmt.Errorf("read cached rootfs object %s range: %w", object.Key, errors.Join(readErr, closeErr))
			}
			return read, nil
		}
		read, readErr := readVerifiedObjectRange(ctx, reader, object, destination, offset)
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return read, fmt.Errorf("read uncached rootfs object %s range: %w", object.Key, errors.Join(readErr, closeErr))
		}
		return read, nil
	}
	payload, err := r.readObject(ctx, object, false)
	if err != nil {
		return 0, err
	}
	return copy(destination, payload[offset:offset+uint64(len(destination))]), nil
}

// readVerifiedObjectRange consumes one complete fallback object stream so a
// cache write failure does not trigger another object-store download. Bytes
// outside the requested range are hashed without being retained in memory.
func readVerifiedObjectRange(ctx context.Context, reader io.Reader, object rootfshead.Object, destination []byte, offset uint64) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("rootfs object stream is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	digestValue, err := digest.Parse(object.Digest)
	if err != nil {
		return 0, err
	}
	verifier := digestValue.Verifier()
	if offset > 0 {
		if _, err := io.CopyN(verifier, reader, int64(offset)); err != nil {
			return 0, err
		}
	}
	read, err := io.ReadFull(io.TeeReader(reader, verifier), destination)
	if err != nil {
		return read, err
	}
	remainder := object.Size - int64(offset) - int64(len(destination))
	if remainder > 0 {
		if _, err := io.CopyN(verifier, reader, remainder); err != nil {
			return read, err
		}
	}
	trailing, err := io.ReadAll(io.LimitReader(reader, 1))
	if err != nil {
		return read, err
	}
	if len(trailing) != 0 {
		return read, fmt.Errorf("rootfs object %s exceeds declared size %d", object.Key, object.Size)
	}
	if !verifier.Verified() {
		return read, fmt.Errorf("rootfs object %s failed digest validation", object.Key)
	}
	return read, ctx.Err()
}

func (r *Reader) loadDirectoryIndex(ctx context.Context, object rootfshead.Object) (rootfshead.DirectoryIndex, error) {
	payload, err := r.readMetadata(ctx, object)
	if err != nil {
		return rootfshead.DirectoryIndex{}, err
	}
	return rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
}

func (r *Reader) loadDirectoryShard(ctx context.Context, object rootfshead.Object) (rootfshead.DirectoryShard, error) {
	payload, err := r.readMetadata(ctx, object)
	if err != nil {
		return rootfshead.DirectoryShard{}, err
	}
	return rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
}

func (r *Reader) readMetadata(ctx context.Context, object rootfshead.Object) ([]byte, error) {
	return r.readObject(ctx, object, true)
}

func (r *Reader) readObject(ctx context.Context, object rootfshead.Object, cacheMetadata bool) ([]byte, error) {
	if err := rootfshead.ValidateReadableObjectScope(r.prefix, object); err != nil {
		return nil, err
	}
	cacheKey := fmt.Sprintf("%s\x00%s\x00%s\x00%d", object.Key, object.Digest, object.MediaType, object.Size)
	if cacheMetadata {
		if payload, ok := r.metadata.get(cacheKey); ok {
			return payload, nil
		}
	}
	value, err, _ := r.loads.Do(cacheKey, func() (any, error) {
		if cacheMetadata {
			if payload, ok := r.metadata.get(cacheKey); ok {
				return payload, nil
			}
		}
		reader, _, err := r.objectCache.GetOrFetchObject(ctx, r.store, object)
		if err != nil {
			return nil, fmt.Errorf("fetch rootfs object %s: %w", object.Key, err)
		}
		payload, readErr := io.ReadAll(io.LimitReader(reader, object.Size+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil {
			return nil, fmt.Errorf("read rootfs object %s: %w", object.Key, errors.Join(readErr, closeErr))
		}
		if int64(len(payload)) != object.Size || digest.FromBytes(payload).String() != object.Digest {
			return nil, fmt.Errorf("rootfs object %s failed size or digest validation", object.Key)
		}
		if cacheMetadata {
			r.metadata.put(cacheKey, payload)
		}
		return payload, nil
	})
	if err != nil {
		return nil, err
	}
	payload := value.([]byte)
	return append([]byte(nil), payload...), nil
}

type metadataCacheEntry struct {
	key     string
	payload []byte
}

type MetadataCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	entries  map[string]*list.Element
	order    *list.List
}

func NewMetadataCache(maxBytes int64) *MetadataCache {
	return &MetadataCache{maxBytes: maxBytes, entries: make(map[string]*list.Element), order: list.New()}
}

func (c *MetadataCache) get(key string) ([]byte, bool) {
	if c == nil || c.maxBytes <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element := c.entries[key]
	if element == nil {
		return nil, false
	}
	c.order.MoveToFront(element)
	return append([]byte(nil), element.Value.(metadataCacheEntry).payload...), true
}

func (c *MetadataCache) put(key string, payload []byte) {
	if c == nil || c.maxBytes <= 0 || int64(len(payload)) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.entries[key]; element != nil {
		entry := element.Value.(metadataCacheEntry)
		c.bytes -= int64(len(entry.payload))
		entry.payload = append(entry.payload[:0], payload...)
		element.Value = entry
		c.bytes += int64(len(entry.payload))
		c.order.MoveToFront(element)
	} else {
		entry := metadataCacheEntry{key: key, payload: append([]byte(nil), payload...)}
		c.entries[key] = c.order.PushFront(entry)
		c.bytes += int64(len(entry.payload))
	}
	for c.bytes > c.maxBytes {
		element := c.order.Back()
		entry := element.Value.(metadataCacheEntry)
		delete(c.entries, entry.key)
		c.bytes -= int64(len(entry.payload))
		c.order.Remove(element)
	}
}
