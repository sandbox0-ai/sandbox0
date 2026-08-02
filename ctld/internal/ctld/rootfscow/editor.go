package rootfscow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

type Editor struct {
	mu     sync.Mutex
	store  objectstore.Store
	writer *ObjectWriter
	root   rootfshead.Entry
	dir    *mutableDirectory
}

type mutableDirectory struct {
	ref         *rootfshead.Object
	indexLoaded bool
	shardRefs   map[uint8]rootfshead.Object
	shards      map[uint8]*mutableShard
	dirty       bool
}

type mutableShard struct {
	ref     *rootfshead.Object
	loaded  bool
	base    map[string]rootfshead.Entry
	entries map[string]*mutableEntry
	dirty   bool
}

type mutableEntry struct {
	value rootfshead.Entry
	child *mutableDirectory
}

func NewEditor(store objectstore.Store, writer *ObjectWriter, parent *rootfshead.Head) (*Editor, error) {
	if store == nil || writer == nil {
		return nil, fmt.Errorf("rootfs editor store and writer are required")
	}
	editor := &Editor{store: store, writer: writer}
	if parent != nil {
		if err := parent.Validate(); err != nil {
			return nil, err
		}
		editor.root = cloneEntry(parent.Root)
		rootRef := *parent.Root.Directory
		editor.dir = &mutableDirectory{ref: &rootRef}
		return editor, nil
	}
	editor.root = rootDirectoryEntry()
	editor.dir = newMutableDirectory()
	return editor, nil
}

func (e *Editor) SetRoot(entry rootfshead.Entry) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if entry.Kind != rootfshead.EntryDirectory {
		return fmt.Errorf("rootfs editor root must be a directory")
	}
	entry.Name = ""
	entry.Inode = "root"
	entry.Directory = e.root.Directory
	e.root = cloneEntry(entry)
	return nil
}

// Set applies one complete current-generation upper entry. A non-opaque
// directory preserves inherited children; an opaque directory starts empty.
func (e *Editor) Set(ctx context.Context, relativePath string, entry rootfshead.Entry, opaque bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	parts, err := splitRelativePath(relativePath)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return e.setRootLocked(entry)
	}
	parent, err := e.directoryLocked(ctx, parts[:len(parts)-1], true)
	if err != nil {
		return err
	}
	shard, err := e.shardLocked(ctx, parent, rootfshead.NameBucket(parts[len(parts)-1]))
	if err != nil {
		return err
	}
	name := parts[len(parts)-1]
	entry.Name = name
	current := shard.entries[name]
	if entry.Kind == rootfshead.EntryDirectory {
		var child *mutableDirectory
		if !opaque && current != nil && current.value.Kind == rootfshead.EntryDirectory {
			child = current.child
			if child == nil && current.value.Directory != nil {
				ref := *current.value.Directory
				child = &mutableDirectory{ref: &ref}
			}
		}
		if child == nil {
			child = newMutableDirectory()
		}
		entry.Directory = cloneObject(child.ref)
		entry.File = nil
		shard.entries[name] = &mutableEntry{value: cloneEntry(entry), child: child}
	} else {
		shard.entries[name] = &mutableEntry{value: cloneEntry(entry)}
	}
	shard.dirty = true
	parent.dirty = true
	return nil
}

// Reset removes a current-generation mutation and restores the entry from the
// immutable parent head, if one existed.
func (e *Editor) Reset(ctx context.Context, relativePath string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	parts, err := splitRelativePath(relativePath)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		return nil
	}
	parent, err := e.directoryLocked(ctx, parts[:len(parts)-1], false)
	if err != nil {
		return err
	}
	if parent == nil {
		return nil
	}
	shard, err := e.shardLocked(ctx, parent, rootfshead.NameBucket(parts[len(parts)-1]))
	if err != nil {
		return err
	}
	name := parts[len(parts)-1]
	if base, ok := shard.base[name]; ok {
		value := cloneEntry(base)
		var child *mutableDirectory
		if value.Kind == rootfshead.EntryDirectory && value.Directory != nil {
			ref := *value.Directory
			child = &mutableDirectory{ref: &ref}
		}
		shard.entries[name] = &mutableEntry{value: value, child: child}
	} else {
		delete(shard.entries, name)
	}
	shard.dirty = true
	parent.dirty = true
	return nil
}

func (e *Editor) Flush(ctx context.Context) (rootfshead.Entry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rootObject, err := e.flushDirectoryLocked(ctx, e.dir)
	if err != nil {
		return rootfshead.Entry{}, err
	}
	e.root.Directory = &rootObject
	return cloneEntry(e.root), nil
}

func (e *Editor) setRootLocked(entry rootfshead.Entry) error {
	if entry.Kind != rootfshead.EntryDirectory {
		return fmt.Errorf("rootfs editor root must be a directory")
	}
	entry.Name = ""
	entry.Inode = "root"
	entry.Directory = e.root.Directory
	e.root = cloneEntry(entry)
	return nil
}

func (e *Editor) directoryLocked(ctx context.Context, parts []string, create bool) (*mutableDirectory, error) {
	directory := e.dir
	for _, name := range parts {
		shard, err := e.shardLocked(ctx, directory, rootfshead.NameBucket(name))
		if err != nil {
			return nil, err
		}
		entry := shard.entries[name]
		if entry == nil || entry.value.Kind != rootfshead.EntryDirectory {
			if !create {
				return nil, nil
			}
			child := newMutableDirectory()
			value := rootDirectoryEntry()
			value.Name = name
			value.Inode = "synthetic:" + strings.Join(parts, "/")
			entry = &mutableEntry{value: value, child: child}
			shard.entries[name] = entry
			shard.dirty = true
			directory.dirty = true
		}
		if entry.child == nil {
			if entry.value.Directory == nil {
				entry.child = newMutableDirectory()
			} else {
				ref := *entry.value.Directory
				entry.child = &mutableDirectory{ref: &ref}
			}
		}
		directory = entry.child
	}
	return directory, nil
}

func (e *Editor) shardLocked(ctx context.Context, directory *mutableDirectory, bucket uint8) (*mutableShard, error) {
	if err := e.loadIndexLocked(ctx, directory); err != nil {
		return nil, err
	}
	if shard := directory.shards[bucket]; shard != nil {
		if err := e.loadShardLocked(ctx, bucket, shard); err != nil {
			return nil, err
		}
		return shard, nil
	}
	shard := &mutableShard{loaded: true, base: make(map[string]rootfshead.Entry), entries: make(map[string]*mutableEntry)}
	if ref, ok := directory.shardRefs[bucket]; ok {
		copyRef := ref
		shard.ref = &copyRef
		shard.loaded = false
	}
	directory.shards[bucket] = shard
	if err := e.loadShardLocked(ctx, bucket, shard); err != nil {
		return nil, err
	}
	return shard, nil
}

func (e *Editor) loadIndexLocked(ctx context.Context, directory *mutableDirectory) error {
	if directory.indexLoaded {
		return nil
	}
	directory.indexLoaded = true
	directory.shardRefs = make(map[uint8]rootfshead.Object)
	directory.shards = make(map[uint8]*mutableShard)
	if directory.ref == nil {
		return nil
	}
	payload, err := readObject(ctx, e.store, *directory.ref)
	if err != nil {
		return err
	}
	index, err := rootfshead.DecodeDirectoryIndex(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	for _, shard := range index.Shards {
		directory.shardRefs[shard.Bucket] = shard.Object
	}
	return nil
}

func (e *Editor) loadShardLocked(ctx context.Context, bucket uint8, shard *mutableShard) error {
	if shard.loaded {
		return nil
	}
	shard.loaded = true
	shard.base = make(map[string]rootfshead.Entry)
	shard.entries = make(map[string]*mutableEntry)
	if shard.ref == nil {
		return nil
	}
	payload, err := readObject(ctx, e.store, *shard.ref)
	if err != nil {
		return err
	}
	decoded, err := rootfshead.DecodeDirectoryShard(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	if decoded.Bucket != bucket {
		return fmt.Errorf("rootfs directory shard bucket %d, expected %d", decoded.Bucket, bucket)
	}
	for _, value := range decoded.Entries {
		value = cloneEntry(value)
		shard.base[value.Name] = cloneEntry(value)
		var child *mutableDirectory
		if value.Kind == rootfshead.EntryDirectory && value.Directory != nil {
			ref := *value.Directory
			child = &mutableDirectory{ref: &ref}
		}
		shard.entries[value.Name] = &mutableEntry{value: value, child: child}
	}
	return nil
}

func (e *Editor) flushDirectoryLocked(ctx context.Context, directory *mutableDirectory) (rootfshead.Object, error) {
	if err := ctx.Err(); err != nil {
		return rootfshead.Object{}, err
	}
	if !directory.indexLoaded && !directory.dirty && directory.ref != nil {
		return *directory.ref, nil
	}
	if err := e.loadIndexLocked(ctx, directory); err != nil {
		return rootfshead.Object{}, err
	}
	for bucket, shard := range directory.shards {
		if !shard.loaded && !shard.dirty {
			continue
		}
		if err := e.loadShardLocked(ctx, bucket, shard); err != nil {
			return rootfshead.Object{}, err
		}
		for _, entry := range shard.entries {
			if entry == nil || entry.value.Kind != rootfshead.EntryDirectory || entry.child == nil {
				continue
			}
			before := ""
			if entry.value.Directory != nil {
				before = entry.value.Directory.Digest
			}
			childObject, err := e.flushDirectoryLocked(ctx, entry.child)
			if err != nil {
				return rootfshead.Object{}, err
			}
			entry.value.Directory = &childObject
			if before != childObject.Digest {
				shard.dirty = true
				directory.dirty = true
			}
		}
		if !shard.dirty && shard.ref != nil {
			continue
		}
		entries := make([]rootfshead.Entry, 0, len(shard.entries))
		for _, entry := range shard.entries {
			if entry != nil {
				entries = append(entries, cloneEntry(entry.value))
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		if len(entries) == 0 {
			delete(directory.shardRefs, bucket)
			shard.ref = nil
			shard.dirty = false
			directory.dirty = true
			continue
		}
		payload, err := rootfshead.EncodeDirectoryShard(rootfshead.DirectoryShard{Version: rootfshead.Version, Bucket: bucket, Entries: entries})
		if err != nil {
			return rootfshead.Object{}, err
		}
		object, err := e.writer.Put(ctx, rootfshead.DirectoryShardMediaType, payload)
		if err != nil {
			return rootfshead.Object{}, err
		}
		shard.ref = &object
		shard.dirty = false
		directory.shardRefs[bucket] = object
		directory.dirty = true
	}
	if !directory.dirty && directory.ref != nil {
		return *directory.ref, nil
	}
	buckets := make([]int, 0, len(directory.shardRefs))
	for bucket := range directory.shardRefs {
		buckets = append(buckets, int(bucket))
	}
	sort.Ints(buckets)
	index := rootfshead.DirectoryIndex{Version: rootfshead.Version, Shards: make([]rootfshead.ShardRef, 0, len(buckets))}
	for _, raw := range buckets {
		bucket := uint8(raw)
		index.Shards = append(index.Shards, rootfshead.ShardRef{Bucket: bucket, Object: directory.shardRefs[bucket]})
	}
	payload, err := rootfshead.EncodeDirectoryIndex(index)
	if err != nil {
		return rootfshead.Object{}, err
	}
	object, err := e.writer.Put(ctx, rootfshead.DirectoryIndexMediaType, payload)
	if err != nil {
		return rootfshead.Object{}, err
	}
	directory.ref = &object
	directory.dirty = false
	return object, nil
}

func readObject(ctx context.Context, store objectstore.Store, object rootfshead.Object) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsed, err := digest.Parse(object.Digest)
	if err != nil {
		return nil, err
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
	if int64(len(payload)) != object.Size || digest.FromBytes(payload) != parsed {
		return nil, fmt.Errorf("rootfs object %s failed size or digest validation", object.Key)
	}
	return payload, ctx.Err()
}

func newMutableDirectory() *mutableDirectory {
	return &mutableDirectory{
		indexLoaded: true,
		shardRefs:   make(map[uint8]rootfshead.Object),
		shards:      make(map[uint8]*mutableShard),
		dirty:       true,
	}
}

func rootDirectoryEntry() rootfshead.Entry {
	return rootfshead.Entry{Inode: "root", Kind: rootfshead.EntryDirectory, Mode: 0o755, Nlink: 2}
}

func splitRelativePath(value string) ([]string, error) {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" || value == "." {
		return nil, nil
	}
	parts := strings.Split(value, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsRune(part, 0) {
			return nil, fmt.Errorf("invalid rootfs relative path %q", value)
		}
	}
	return parts, nil
}

func cloneEntry(value rootfshead.Entry) rootfshead.Entry {
	value.XAttrs = append([]rootfshead.XAttr(nil), value.XAttrs...)
	for position := range value.XAttrs {
		value.XAttrs[position].Value = append([]byte(nil), value.XAttrs[position].Value...)
	}
	value.Directory = cloneObject(value.Directory)
	value.File = cloneObject(value.File)
	return value
}

func cloneObject(value *rootfshead.Object) *rootfshead.Object {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
