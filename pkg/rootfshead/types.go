// Package rootfshead defines the immutable persistent rootfs format.
package rootfshead

import (
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
)

const (
	// Version is the team-scoped, content-addressed persistent rootfs format.
	Version = 3

	// AnnotationHead identifies a Head marker snapshot. Materialization sets
	// this label explicitly; OCI descriptor annotations are not inherited.
	AnnotationHead = "containerd.io/snapshot/sandbox0.rootfs-head.v3"
	// LabelBaseChainID preserves the durable logical base identity across the
	// snapshot proxy's internal namespaced key mapping.
	LabelBaseChainID = "containerd.io/snapshot/sandbox0.rootfs-base-chain.v3"

	// SnapshotterName is the external snapshotter used by gvisor-rootfs.
	SnapshotterName = "sandbox0"
	// RuntimeClassName is the single sandbox runtime family backed by SnapshotterName.
	RuntimeClassName = "gvisor-rootfs"

	HeadMediaType           = "application/vnd.sandbox0.rootfs.head.v3+json+gzip"
	DirectoryIndexMediaType = "application/vnd.sandbox0.rootfs.directory-index.v3+json+gzip"
	DirectoryShardMediaType = "application/vnd.sandbox0.rootfs.directory-shard.v3+json+gzip"
	FileMediaType           = "application/vnd.sandbox0.rootfs.file.v3+json+gzip"
	ChunkMediaType          = "application/vnd.sandbox0.rootfs.chunk.v3"
	ExportLayerMediaType    = "application/vnd.oci.image.layer.v1.tar+gzip"

	maxAnnotationBytes = 3800

	// MaxMetadataObjectBytes bounds both encoded metadata object descriptors
	// and decoded gzip JSON to prevent corrupt Heads from causing unbounded I/O.
	MaxMetadataObjectBytes int64 = 64 << 20
)

type EntryKind string

const (
	EntryDirectory EntryKind = "directory"
	EntryFile      EntryKind = "file"
	EntrySymlink   EntryKind = "symlink"
	EntryWhiteout  EntryKind = "whiteout"
	EntryChar      EntryKind = "char"
	EntryBlock     EntryKind = "block"
	EntryFIFO      EntryKind = "fifo"
)

// Object identifies immutable plaintext content in the rootfs object store.
type Object struct {
	Key       string `json:"key"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	MediaType string `json:"media_type"`
}

func (o Object) Validate(expectedMediaType string) error {
	key := strings.TrimSpace(o.Key)
	if key == "" || strings.HasPrefix(key, "/") || path.Clean(key) != key || key == "." || key == ".." || strings.HasPrefix(key, "../") {
		return fmt.Errorf("invalid rootfs object key %q", o.Key)
	}
	parsed, err := digest.Parse(strings.TrimSpace(o.Digest))
	if err != nil || parsed.Algorithm() != digest.Canonical {
		return fmt.Errorf("invalid rootfs object digest %q", o.Digest)
	}
	if o.Size <= 0 {
		return fmt.Errorf("rootfs object %s has invalid size %d", o.Key, o.Size)
	}
	if strings.TrimSpace(o.MediaType) == "" {
		return fmt.Errorf("rootfs object %s has no media type", o.Key)
	}
	switch o.MediaType {
	case HeadMediaType, DirectoryIndexMediaType, DirectoryShardMediaType, FileMediaType, ImageEnvelopeMediaType:
		if o.Size > MaxMetadataObjectBytes {
			return fmt.Errorf("rootfs metadata object %s exceeds %d bytes", o.Key, MaxMetadataObjectBytes)
		}
	case MarkerMediaType:
		if o.Size > int64(MaxMarkerBytes) {
			return fmt.Errorf("rootfs marker object %s exceeds %d bytes", o.Key, MaxMarkerBytes)
		}
	}
	if expectedMediaType != "" && o.MediaType != expectedMediaType {
		return fmt.Errorf("rootfs object %s has media type %q, expected %q", o.Key, o.MediaType, expectedMediaType)
	}
	return nil
}

// Timestamp preserves filesystem timestamps without verbose text encoding.
type Timestamp struct {
	Seconds     int64  `json:"seconds"`
	Nanoseconds uint32 `json:"nanoseconds,omitempty"`
}

func NewTimestamp(value time.Time) Timestamp {
	if value.IsZero() {
		return Timestamp{}
	}
	return Timestamp{Seconds: value.Unix(), Nanoseconds: uint32(value.Nanosecond())}
}

func (t Timestamp) Time() time.Time {
	if t.Seconds == 0 && t.Nanoseconds == 0 {
		return time.Time{}
	}
	return time.Unix(t.Seconds, int64(t.Nanoseconds))
}

func (t Timestamp) validate() error {
	if t.Nanoseconds >= 1_000_000_000 {
		return fmt.Errorf("timestamp nanoseconds %d are invalid", t.Nanoseconds)
	}
	return nil
}

type XAttr struct {
	Name  string `json:"name"`
	Value []byte `json:"value"`
}

// Entry is one immutable overlay-layer inode.
type Entry struct {
	Name       string    `json:"name,omitempty"`
	Inode      string    `json:"inode"`
	Kind       EntryKind `json:"kind"`
	Mode       uint32    `json:"mode"`
	UID        uint32    `json:"uid,omitempty"`
	GID        uint32    `json:"gid,omitempty"`
	Nlink      uint32    `json:"nlink,omitempty"`
	Size       uint64    `json:"size,omitempty"`
	Blocks     uint64    `json:"blocks,omitempty"`
	Rdev       uint32    `json:"rdev,omitempty"`
	ModTime    Timestamp `json:"mtime,omitempty"`
	AccessTime Timestamp `json:"atime,omitempty"`
	ChangeTime Timestamp `json:"ctime,omitempty"`
	Target     string    `json:"target,omitempty"`
	Opaque     bool      `json:"opaque,omitempty"`
	XAttrs     []XAttr   `json:"xattrs,omitempty"`
	Directory  *Object   `json:"directory,omitempty"`
	File       *Object   `json:"file,omitempty"`
}

func (e Entry) Validate(root bool) error {
	if root {
		if e.Name != "" {
			return fmt.Errorf("root entry name must be empty")
		}
	} else if err := validateEntryName(e.Name); err != nil {
		return err
	}
	if strings.TrimSpace(e.Inode) == "" {
		return fmt.Errorf("rootfs entry %q has no inode identity", e.Name)
	}
	if err := e.ModTime.validate(); err != nil {
		return fmt.Errorf("rootfs entry %q mtime: %w", e.Name, err)
	}
	if err := e.AccessTime.validate(); err != nil {
		return fmt.Errorf("rootfs entry %q atime: %w", e.Name, err)
	}
	if err := e.ChangeTime.validate(); err != nil {
		return fmt.Errorf("rootfs entry %q ctime: %w", e.Name, err)
	}
	for i, xattr := range e.XAttrs {
		if strings.TrimSpace(xattr.Name) == "" || strings.ContainsRune(xattr.Name, '\x00') {
			return fmt.Errorf("rootfs entry %q has invalid xattr name", e.Name)
		}
		if strings.HasPrefix(xattr.Name, "trusted.overlay.") || strings.HasPrefix(xattr.Name, "user.overlay.") {
			return fmt.Errorf("rootfs entry %q contains internal overlay xattr %q", e.Name, xattr.Name)
		}
		if i > 0 && e.XAttrs[i-1].Name >= xattr.Name {
			return fmt.Errorf("rootfs entry %q xattrs are not strictly sorted", e.Name)
		}
	}
	switch e.Kind {
	case EntryDirectory:
		if e.Directory == nil || e.File != nil || e.Target != "" {
			return fmt.Errorf("rootfs directory %q has invalid payload references", e.Name)
		}
		if err := e.Directory.Validate(DirectoryIndexMediaType); err != nil {
			return fmt.Errorf("rootfs directory %q: %w", e.Name, err)
		}
	case EntryFile:
		if e.Opaque {
			return fmt.Errorf("rootfs file %q cannot be opaque", e.Name)
		}
		if e.File == nil || e.Directory != nil || e.Target != "" {
			return fmt.Errorf("rootfs file %q has invalid payload references", e.Name)
		}
		if err := e.File.Validate(FileMediaType); err != nil {
			return fmt.Errorf("rootfs file %q: %w", e.Name, err)
		}
	case EntrySymlink:
		if e.Opaque {
			return fmt.Errorf("rootfs symlink %q cannot be opaque", e.Name)
		}
		if e.Directory != nil || e.File != nil {
			return fmt.Errorf("rootfs symlink %q has payload references", e.Name)
		}
	case EntryWhiteout, EntryChar, EntryBlock, EntryFIFO:
		if e.Opaque {
			return fmt.Errorf("rootfs special entry %q cannot be opaque", e.Name)
		}
		if e.Directory != nil || e.File != nil || e.Target != "" {
			return fmt.Errorf("rootfs special entry %q has payload references", e.Name)
		}
	default:
		return fmt.Errorf("rootfs entry %q has unsupported kind %q", e.Name, e.Kind)
	}
	return nil
}

type DirectoryIndex struct {
	Version int        `json:"version"`
	Shards  []ShardRef `json:"shards,omitempty"`
}

type ShardRef struct {
	Bucket uint8  `json:"bucket"`
	Object Object `json:"object"`
}

func (d DirectoryIndex) Validate() error {
	if d.Version != Version {
		return fmt.Errorf("unsupported rootfs directory index version %d", d.Version)
	}
	for i, shard := range d.Shards {
		if i > 0 && d.Shards[i-1].Bucket >= shard.Bucket {
			return fmt.Errorf("rootfs directory shards are not strictly sorted")
		}
		if err := shard.Object.Validate(DirectoryShardMediaType); err != nil {
			return err
		}
	}
	return nil
}

type DirectoryShard struct {
	Version int     `json:"version"`
	Bucket  uint8   `json:"bucket"`
	Entries []Entry `json:"entries,omitempty"`
}

func (d DirectoryShard) Validate() error {
	if d.Version != Version {
		return fmt.Errorf("unsupported rootfs directory shard version %d", d.Version)
	}
	for i, entry := range d.Entries {
		if err := entry.Validate(false); err != nil {
			return err
		}
		if NameBucket(entry.Name) != d.Bucket {
			return fmt.Errorf("rootfs entry %q is in directory bucket %d, expected %d", entry.Name, d.Bucket, NameBucket(entry.Name))
		}
		if i > 0 && d.Entries[i-1].Name >= entry.Name {
			return fmt.Errorf("rootfs directory entries are not strictly sorted")
		}
	}
	return nil
}

type FileManifest struct {
	Version int          `json:"version"`
	Size    uint64       `json:"size"`
	Blocks  uint64       `json:"blocks,omitempty"`
	Extents []FileExtent `json:"extents,omitempty"`
}

type FileExtent struct {
	Offset       uint64 `json:"offset"`
	Length       uint64 `json:"length"`
	ObjectOffset uint64 `json:"object_offset,omitempty"`
	Object       Object `json:"object"`
}

func (f FileManifest) Validate() error {
	if f.Version != Version {
		return fmt.Errorf("unsupported rootfs file manifest version %d", f.Version)
	}
	var previousEnd uint64
	for i, extent := range f.Extents {
		if extent.Length == 0 {
			return fmt.Errorf("rootfs file extent %d has zero length", i)
		}
		end, ok := addUint64(extent.Offset, extent.Length)
		if !ok || end > f.Size {
			return fmt.Errorf("rootfs file extent %d exceeds logical size", i)
		}
		objectEnd, ok := addUint64(extent.ObjectOffset, extent.Length)
		if !ok || objectEnd > uint64(extent.Object.Size) {
			return fmt.Errorf("rootfs file extent %d exceeds chunk size", i)
		}
		if i > 0 && extent.Offset < previousEnd {
			return fmt.Errorf("rootfs file extents overlap or are not sorted")
		}
		if err := extent.Object.Validate(ChunkMediaType); err != nil {
			return err
		}
		previousEnd = end
	}
	return nil
}

// BaseIdentity is the portable identity of the canonical OCI base.
type BaseIdentity struct {
	ImageReference string `json:"image_reference"`
	ManifestDigest string `json:"manifest_digest"`
	ChainID        string `json:"chain_id"`
	OS             string `json:"os"`
	Architecture   string `json:"architecture"`
	Variant        string `json:"variant,omitempty"`
}

func (b BaseIdentity) Validate() error {
	if strings.TrimSpace(b.ImageReference) == "" {
		return fmt.Errorf("rootfs base image reference is required")
	}
	if err := validateCanonicalDigest(b.ManifestDigest); err != nil {
		return fmt.Errorf("rootfs base image manifest: %w", err)
	}
	if err := validateCanonicalDigest(b.ChainID); err != nil {
		return fmt.Errorf("rootfs base chain id: %w", err)
	}
	if strings.TrimSpace(b.OS) == "" || strings.TrimSpace(b.Architecture) == "" {
		return fmt.Errorf("rootfs base platform is required")
	}
	return nil
}

// Head is a complete current persistent overlay state.
type Head struct {
	Version int          `json:"version"`
	HeadID  string       `json:"head_id"`
	Base    BaseIdentity `json:"base"`
	Root    Entry        `json:"root"`
}

func (h Head) Validate() error {
	if h.Version != Version {
		return fmt.Errorf("unsupported rootfs head version %d", h.Version)
	}
	if strings.TrimSpace(h.HeadID) == "" {
		return fmt.Errorf("rootfs head id is required")
	}
	if err := h.Base.Validate(); err != nil {
		return err
	}
	if h.Root.Kind != EntryDirectory {
		return fmt.Errorf("rootfs head root is not a directory")
	}
	return h.Root.Validate(true)
}

// HeadReference is the bounded reference carried by the marker layer.
type HeadReference struct {
	Version  int    `json:"version"`
	HeadID   string `json:"head_id"`
	Manifest Object `json:"manifest"`
}

func (r HeadReference) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("unsupported rootfs head reference version %d", r.Version)
	}
	if strings.TrimSpace(r.HeadID) == "" {
		return fmt.Errorf("rootfs head reference id is required")
	}
	return r.Manifest.Validate(HeadMediaType)
}

func SortXAttrs(values []XAttr) {
	slices.SortFunc(values, func(a, b XAttr) int { return strings.Compare(a.Name, b.Name) })
}

func validateEntryName(name string) error {
	if name == "" || name == "." || name == ".." || path.Base(name) != name || strings.ContainsRune(name, '\x00') {
		return fmt.Errorf("invalid rootfs entry name %q", name)
	}
	return nil
}

func validateCanonicalDigest(value string) error {
	parsed, err := digest.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Algorithm() != digest.Canonical {
		return fmt.Errorf("invalid sha256 digest %q", value)
	}
	return nil
}

func addUint64(a, b uint64) (uint64, bool) {
	value := a + b
	return value, value >= a
}
