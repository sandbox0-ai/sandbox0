package rootfscow

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

// Editor incrementally builds a complete immutable head while lazily retaining
// unchanged metadata from a parent head.
type Editor struct {
	mu     sync.Mutex
	store  objectstore.Store
	prefix string
	writer *rootfsstore.Writer
	root   rootfshead.Entry
	dir    *mutableDirectory
}

type mutableDirectory struct {
	ref         *rootfshead.Object
	parent      *mutableDirectory
	parentShard *mutableShard
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
	value   rootfshead.Entry
	child   *mutableDirectory
	current bool
}

func NewEditor(store objectstore.Store, writer *rootfsstore.Writer, parent *rootfshead.Head) (*Editor, error) {
	if store == nil || writer == nil {
		return nil, fmt.Errorf("rootfs editor store and writer are required")
	}
	editor := &Editor{store: store, writer: writer, prefix: writer.Prefix()}
	if parent == nil {
		editor.root = rootDirectoryEntry()
		editor.dir = newMutableDirectory()
		return editor, nil
	}
	if err := parent.Validate(); err != nil {
		return nil, err
	}
	if err := rootfshead.ValidateReadableObjectScope(editor.prefix, *parent.Root.Directory); err != nil {
		return nil, fmt.Errorf("parent rootfs head escapes tenant and public ImageFS scopes: %w", err)
	}
	editor.root = cloneEntry(parent.Root)
	rootRef := *parent.Root.Directory
	editor.dir = &mutableDirectory{ref: &rootRef}
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
	entry.Opaque = entry.Opaque || e.root.Opaque
	entry.Directory = cloneObject(e.root.Directory)
	entry.File = nil
	e.root = cloneEntry(entry)
	return nil
}

// Set replaces one current-generation entry. Opaque directories retain only
// children independently observed in the current generation.
func (e *Editor) Set(ctx context.Context, relativePath string, entry rootfshead.Entry, opaque bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	parts, err := splitRelativePath(relativePath)
	if err != nil {
		return err
	}
	if len(parts) == 0 {
		if entry.Kind != rootfshead.EntryDirectory {
			return fmt.Errorf("rootfs editor root must be a directory")
		}
		entry.Name = ""
		entry.Inode = "root"
		entry.Opaque = entry.Opaque || e.root.Opaque
		entry.Directory = cloneObject(e.root.Directory)
		entry.File = nil
		e.root = cloneEntry(entry)
		return nil
	}

	parent, err := e.directoryLocked(ctx, parts[:len(parts)-1], true)
	if err != nil {
		return err
	}
	name := parts[len(parts)-1]
	shard, err := e.shardLocked(ctx, parent, rootfshead.NameBucket(name))
	if err != nil {
		return err
	}
	entry.Name = name
	if err := validateMutableEntry(entry); err != nil {
		return err
	}
	current := shard.entries[name]
	if entry.Kind == rootfshead.EntryDirectory {
		entry.Opaque = entry.Opaque || opaque
		if current != nil && current.value.Kind == rootfshead.EntryDirectory {
			entry.Opaque = entry.Opaque || current.value.Opaque
		}
		child := mutableEntryDirectory(current)
		if child == nil {
			child = newMutableDirectory()
		}
		attachMutableDirectory(parent, shard, child)
		if opaque {
			e.makeOpaqueLocked(child)
		}
		entry.Directory = cloneObject(child.ref)
		entry.File = nil
		shard.entries[name] = &mutableEntry{value: cloneEntry(entry), child: child, current: true}
	} else {
		entry.Directory = nil
		shard.entries[name] = &mutableEntry{value: cloneEntry(entry), current: true}
	}
	shard.dirty = true
	markMutableDirectoryDirty(parent)
	return nil
}

// Reset drops a transient current-generation mutation and restores the parent
// entry when one exists.
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
	if err != nil || parent == nil {
		return err
	}
	name := parts[len(parts)-1]
	shard, err := e.shardLocked(ctx, parent, rootfshead.NameBucket(name))
	if err != nil {
		return err
	}
	if base, ok := shard.base[name]; ok {
		value := cloneEntry(base)
		child := directoryFromEntry(value)
		attachMutableDirectory(parent, shard, child)
		shard.entries[name] = &mutableEntry{value: value, child: child}
	} else {
		delete(shard.entries, name)
	}
	shard.dirty = true
	markMutableDirectoryDirty(parent)
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
	if err := e.root.Validate(true); err != nil {
		return rootfshead.Entry{}, err
	}
	return cloneEntry(e.root), nil
}

func (e *Editor) directoryLocked(ctx context.Context, parts []string, create bool) (*mutableDirectory, error) {
	directory := e.dir
	for index, name := range parts {
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
			value.Inode = "synthetic:" + strings.Join(parts[:index+1], "/")
			entry = &mutableEntry{value: value, child: child, current: true}
			shard.entries[name] = entry
			shard.dirty = true
			attachMutableDirectory(directory, shard, child)
			markMutableDirectoryDirty(directory)
		}
		if create {
			entry.current = true
		}
		if entry.child == nil {
			entry.child = directoryFromEntry(entry.value)
			if entry.child == nil {
				entry.child = newMutableDirectory()
			}
		}
		attachMutableDirectory(directory, shard, entry.child)
		directory = entry.child
	}
	return directory, nil
}

func (e *Editor) makeOpaqueLocked(directory *mutableDirectory) {
	if !directory.indexLoaded {
		directory.indexLoaded = true
		directory.shardRefs = make(map[uint8]rootfshead.Object)
		directory.shards = make(map[uint8]*mutableShard)
		directory.ref = nil
		markMutableDirectoryDirty(directory)
		return
	}
	for bucket, shard := range directory.shards {
		if !shard.loaded {
			delete(directory.shards, bucket)
			continue
		}
		for name, entry := range shard.entries {
			if entry == nil || !entry.current {
				delete(shard.entries, name)
				continue
			}
			if entry.value.Kind == rootfshead.EntryDirectory {
				if entry.child == nil {
					entry.child = directoryFromEntry(entry.value)
					if entry.child == nil {
						entry.child = newMutableDirectory()
					}
				}
				attachMutableDirectory(directory, shard, entry.child)
				e.makeOpaqueLocked(entry.child)
			}
		}
		shard.ref = nil
		shard.base = make(map[string]rootfshead.Entry)
		shard.dirty = true
	}
	directory.shardRefs = make(map[uint8]rootfshead.Object)
	directory.ref = nil
	markMutableDirectoryDirty(directory)
}

func (e *Editor) shardLocked(ctx context.Context, directory *mutableDirectory, bucket uint8) (*mutableShard, error) {
	if err := e.loadIndexLocked(ctx, directory); err != nil {
		return nil, err
	}
	shard := directory.shards[bucket]
	if shard == nil {
		shard = &mutableShard{loaded: true, base: make(map[string]rootfshead.Entry), entries: make(map[string]*mutableEntry)}
		if ref, ok := directory.shardRefs[bucket]; ok {
			copyRef := ref
			shard.ref = &copyRef
			shard.loaded = false
		}
		directory.shards[bucket] = shard
	}
	if err := e.loadShardLocked(ctx, directory, bucket, shard); err != nil {
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
	payload, err := rootfsstore.Read(ctx, e.store, e.prefix, *directory.ref)
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

func (e *Editor) loadShardLocked(ctx context.Context, directory *mutableDirectory, bucket uint8, shard *mutableShard) error {
	if shard.loaded {
		return nil
	}
	shard.loaded = true
	shard.base = make(map[string]rootfshead.Entry)
	shard.entries = make(map[string]*mutableEntry)
	if shard.ref == nil {
		return nil
	}
	payload, err := rootfsstore.Read(ctx, e.store, e.prefix, *shard.ref)
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
		child := directoryFromEntry(value)
		attachMutableDirectory(directory, shard, child)
		shard.base[value.Name] = cloneEntry(value)
		shard.entries[value.Name] = &mutableEntry{value: value, child: child}
	}
	return nil
}

func (e *Editor) flushDirectoryLocked(ctx context.Context, directory *mutableDirectory) (rootfshead.Object, error) {
	if err := ctx.Err(); err != nil {
		return rootfshead.Object{}, err
	}
	if !directory.dirty && directory.ref != nil {
		return *directory.ref, nil
	}
	if err := e.loadIndexLocked(ctx, directory); err != nil {
		return rootfshead.Object{}, err
	}
	for bucket, shard := range directory.shards {
		if !shard.dirty {
			continue
		}
		if err := e.loadShardLocked(ctx, directory, bucket, shard); err != nil {
			return rootfshead.Object{}, err
		}
		for _, entry := range shard.entries {
			if entry == nil || entry.value.Kind != rootfshead.EntryDirectory || entry.child == nil || !entry.child.dirty {
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

func validateMutableEntry(entry rootfshead.Entry) error {
	if strings.TrimSpace(entry.Inode) == "" {
		return fmt.Errorf("rootfs entry %q has no inode identity", entry.Name)
	}
	switch entry.Kind {
	case rootfshead.EntryDirectory:
		return nil
	case rootfshead.EntryFile:
		if entry.File == nil {
			return fmt.Errorf("rootfs file %q has no manifest", entry.Name)
		}
		return entry.File.Validate(rootfshead.FileMediaType)
	case rootfshead.EntrySymlink, rootfshead.EntryWhiteout, rootfshead.EntryChar, rootfshead.EntryBlock, rootfshead.EntryFIFO:
		return nil
	default:
		return fmt.Errorf("rootfs entry %q has unsupported kind %q", entry.Name, entry.Kind)
	}
}

func mutableEntryDirectory(entry *mutableEntry) *mutableDirectory {
	if entry == nil || entry.value.Kind != rootfshead.EntryDirectory {
		return nil
	}
	if entry.child != nil {
		return entry.child
	}
	return directoryFromEntry(entry.value)
}

func directoryFromEntry(entry rootfshead.Entry) *mutableDirectory {
	if entry.Kind != rootfshead.EntryDirectory || entry.Directory == nil {
		return nil
	}
	ref := *entry.Directory
	return &mutableDirectory{ref: &ref}
}

func attachMutableDirectory(parent *mutableDirectory, shard *mutableShard, child *mutableDirectory) {
	if child == nil {
		return
	}
	child.parent = parent
	child.parentShard = shard
}

func markMutableDirectoryDirty(directory *mutableDirectory) {
	for current := directory; current != nil; current = current.parent {
		current.dirty = true
		if current.parentShard != nil {
			current.parentShard.dirty = true
		}
	}
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
	return rootfshead.Entry{Inode: "root", Kind: rootfshead.EntryDirectory, Mode: 0o040755, Nlink: 2}
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
