package rootfshead

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
)

const (
	// Version is the sharded, content-addressed persistent rootfs format.
	Version = 2

	// AnnotationHead is inherited by containerd as a snapshotter label. The
	// value is a bounded reference to an immutable Head document.
	AnnotationHead = "containerd.io/snapshot/sandbox0.rootfs-head.v2"

	// SnapshotterName is the external containerd snapshotter used for lazy
	// persistent rootfs heads.
	SnapshotterName = "sandbox0"

	HeadMediaType           = "application/vnd.sandbox0.rootfs.head.v2+json+gzip"
	DirectoryIndexMediaType = "application/vnd.sandbox0.rootfs.directory-index.v2+json+gzip"
	DirectoryShardMediaType = "application/vnd.sandbox0.rootfs.directory-shard.v2+json+gzip"
	FileMediaType           = "application/vnd.sandbox0.rootfs.file.v2+json+gzip"
	ChunkMediaType          = "application/vnd.sandbox0.rootfs.chunk.v2"

	maxAnnotationBytes = 3800
	maxMetadataBytes   = 64 << 20
)

// EntryKind describes a persisted overlay entry. Whiteouts remain explicit
// because the head is mounted as an overlay layer above the template image.
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

// Timestamp avoids verbose RFC3339 strings in metadata shards while retaining
// nanosecond precision.
type Timestamp struct {
	Seconds     int64  `json:"seconds"`
	Nanoseconds uint32 `json:"nanoseconds,omitempty"`
}

// XAttr is sorted by Name before encoding so metadata objects are stable.
type XAttr struct {
	Name  string `json:"name"`
	Value []byte `json:"value"`
}

// Entry is one overlay-layer inode. Directory and File point to independent
// immutable metadata objects, so path lookup reads only bounded shards.
type Entry struct {
	Name       string    `json:"name,omitempty"`
	Inode      string    `json:"inode"`
	Kind       EntryKind `json:"kind"`
	Mode       uint32    `json:"mode"`
	UID        uint32    `json:"uid,omitempty"`
	GID        uint32    `json:"gid,omitempty"`
	Nlink      uint32    `json:"nlink,omitempty"`
	Size       uint64    `json:"size,omitempty"`
	Rdev       uint32    `json:"rdev,omitempty"`
	ModTime    Timestamp `json:"mtime,omitempty"`
	AccessTime Timestamp `json:"atime,omitempty"`
	ChangeTime Timestamp `json:"ctime,omitempty"`
	Target     string    `json:"target,omitempty"`
	XAttrs     []XAttr   `json:"xattrs,omitempty"`
	Directory  *Object   `json:"directory,omitempty"`
	File       *Object   `json:"file,omitempty"`
}

// DirectoryIndex points to at most 256 immutable name-hash shards. Lookup
// reads one shard; readdir reads only the shards of the requested directory.
type DirectoryIndex struct {
	Version int        `json:"version"`
	Shards  []ShardRef `json:"shards,omitempty"`
}

type ShardRef struct {
	Bucket uint8  `json:"bucket"`
	Object Object `json:"object"`
}

// DirectoryShard contains one sorted subset of directory entries.
type DirectoryShard struct {
	Version int     `json:"version"`
	Bucket  uint8   `json:"bucket"`
	Entries []Entry `json:"entries,omitempty"`
}

// FileManifest maps logical file extents to immutable content chunks. Gaps are
// sparse holes and read as zeroes.
type FileManifest struct {
	Version int          `json:"version"`
	Size    uint64       `json:"size"`
	Extents []FileExtent `json:"extents,omitempty"`
}

type FileExtent struct {
	Offset       uint64 `json:"offset"`
	Length       uint64 `json:"length"`
	ObjectOffset uint64 `json:"object_offset,omitempty"`
	Object       Object `json:"object"`
}

// Head is a complete current persistent overlay state. Parent checkpoint
// lineage remains control-plane metadata and is never traversed on resume.
type Head struct {
	Version         int    `json:"version"`
	HeadID          string `json:"head_id"`
	BaseImageDigest string `json:"base_image_digest"`
	BaseSnapshotKey string `json:"base_snapshot_key"`
	Root            Entry  `json:"root"`
}

// HeadReference is the bounded OCI annotation payload.
type HeadReference struct {
	Version  int    `json:"version"`
	HeadID   string `json:"head_id"`
	Manifest Object `json:"manifest"`
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

func EncodeHead(value Head) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeHead(reader io.Reader) (Head, error) {
	var value Head
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return Head{}, fmt.Errorf("decode rootfs head: %w", err)
	}
	if err := value.Validate(); err != nil {
		return Head{}, err
	}
	return value, nil
}

func EncodeDirectoryIndex(value DirectoryIndex) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeDirectoryIndex(reader io.Reader) (DirectoryIndex, error) {
	var value DirectoryIndex
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return DirectoryIndex{}, fmt.Errorf("decode rootfs directory index: %w", err)
	}
	if err := value.Validate(); err != nil {
		return DirectoryIndex{}, err
	}
	return value, nil
}

func EncodeDirectoryShard(value DirectoryShard) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeDirectoryShard(reader io.Reader) (DirectoryShard, error) {
	var value DirectoryShard
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return DirectoryShard{}, fmt.Errorf("decode rootfs directory shard: %w", err)
	}
	if err := value.Validate(); err != nil {
		return DirectoryShard{}, err
	}
	return value, nil
}

func EncodeFileManifest(value FileManifest) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeFileManifest(reader io.Reader) (FileManifest, error) {
	var value FileManifest
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return FileManifest{}, fmt.Errorf("decode rootfs file manifest: %w", err)
	}
	if err := value.Validate(); err != nil {
		return FileManifest{}, err
	}
	return value, nil
}

func EncodeHeadAnnotation(reference HeadReference) (string, error) {
	if err := reference.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(reference)
	if err != nil {
		return "", fmt.Errorf("encode rootfs head reference: %w", err)
	}
	value := base64.RawURLEncoding.EncodeToString(payload)
	if len(value) > maxAnnotationBytes {
		return "", fmt.Errorf("rootfs head annotation is %d bytes, exceeds %d-byte limit", len(value), maxAnnotationBytes)
	}
	return value, nil
}

func DecodeHeadAnnotation(value string) (HeadReference, error) {
	var reference HeadReference
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return reference, fmt.Errorf("decode rootfs head annotation: %w", err)
	}
	if err := json.Unmarshal(payload, &reference); err != nil {
		return reference, fmt.Errorf("decode rootfs head reference: %w", err)
	}
	if err := reference.Validate(); err != nil {
		return HeadReference{}, err
	}
	return reference, nil
}

func (h Head) Validate() error {
	if h.Version != Version {
		return fmt.Errorf("unsupported rootfs head version %d", h.Version)
	}
	if strings.TrimSpace(h.HeadID) == "" {
		return fmt.Errorf("rootfs head id is required")
	}
	if strings.TrimSpace(h.BaseImageDigest) == "" {
		return fmt.Errorf("rootfs head base image digest is required")
	}
	if strings.TrimSpace(h.BaseSnapshotKey) == "" {
		return fmt.Errorf("rootfs head base snapshot key is required")
	}
	if err := h.Root.validate(false); err != nil {
		return fmt.Errorf("rootfs head root: %w", err)
	}
	if h.Root.Kind != EntryDirectory {
		return fmt.Errorf("rootfs head root must be a directory")
	}
	return nil
}

func (r HeadReference) Validate() error {
	if r.Version != Version {
		return fmt.Errorf("unsupported rootfs head reference version %d", r.Version)
	}
	if strings.TrimSpace(r.HeadID) == "" {
		return fmt.Errorf("rootfs head reference id is required")
	}
	if err := r.Manifest.Validate(HeadMediaType); err != nil {
		return fmt.Errorf("rootfs head manifest: %w", err)
	}
	return nil
}

func (d DirectoryIndex) Validate() error {
	if d.Version != Version {
		return fmt.Errorf("unsupported rootfs directory index version %d", d.Version)
	}
	last := -1
	for position, shard := range d.Shards {
		if int(shard.Bucket) <= last {
			return fmt.Errorf("rootfs directory shard %d is not strictly ordered", position)
		}
		last = int(shard.Bucket)
		if err := shard.Object.Validate(DirectoryShardMediaType); err != nil {
			return fmt.Errorf("rootfs directory shard %d: %w", position, err)
		}
	}
	return nil
}

func (d DirectoryShard) Validate() error {
	if d.Version != Version {
		return fmt.Errorf("unsupported rootfs directory shard version %d", d.Version)
	}
	last := ""
	for position, entry := range d.Entries {
		if err := entry.validate(true); err != nil {
			return fmt.Errorf("rootfs directory entry %d: %w", position, err)
		}
		if position > 0 && entry.Name <= last {
			return fmt.Errorf("rootfs directory entries are not strictly ordered")
		}
		if NameBucket(entry.Name) != d.Bucket {
			return fmt.Errorf("rootfs directory entry %q is in the wrong shard", entry.Name)
		}
		last = entry.Name
	}
	return nil
}

func (f FileManifest) Validate() error {
	if f.Version != Version {
		return fmt.Errorf("unsupported rootfs file manifest version %d", f.Version)
	}
	var lastEnd uint64
	for position, extent := range f.Extents {
		if extent.Length == 0 {
			return fmt.Errorf("rootfs file extent %d is empty", position)
		}
		end := extent.Offset + extent.Length
		if end < extent.Offset || end > f.Size {
			return fmt.Errorf("rootfs file extent %d exceeds file size", position)
		}
		objectEnd := extent.ObjectOffset + extent.Length
		if objectEnd < extent.ObjectOffset || objectEnd > uint64(extent.Object.Size) {
			return fmt.Errorf("rootfs file extent %d exceeds chunk size", position)
		}
		if position > 0 && extent.Offset < lastEnd {
			return fmt.Errorf("rootfs file extents overlap")
		}
		if err := extent.Object.Validate(ChunkMediaType); err != nil {
			return fmt.Errorf("rootfs file extent %d: %w", position, err)
		}
		lastEnd = end
	}
	return nil
}

func (e Entry) Validate() error { return e.validate(true) }

func (e Entry) validate(requireName bool) error {
	if requireName && (strings.TrimSpace(e.Name) == "" || strings.Contains(e.Name, "/") || e.Name == "." || e.Name == "..") {
		return fmt.Errorf("rootfs entry name %q is invalid", e.Name)
	}
	if strings.TrimSpace(e.Inode) == "" {
		return fmt.Errorf("rootfs entry inode is required")
	}
	if !slices.IsSortedFunc(e.XAttrs, func(a, b XAttr) int { return strings.Compare(a.Name, b.Name) }) {
		return fmt.Errorf("rootfs entry xattrs are not ordered")
	}
	for position, attr := range e.XAttrs {
		if strings.TrimSpace(attr.Name) == "" || (position > 0 && attr.Name == e.XAttrs[position-1].Name) {
			return fmt.Errorf("rootfs entry xattr name is empty or duplicated")
		}
	}
	switch e.Kind {
	case EntryDirectory:
		if e.Directory == nil {
			return fmt.Errorf("rootfs directory object is required")
		}
		if err := e.Directory.Validate(DirectoryIndexMediaType); err != nil {
			return err
		}
		if e.File != nil || e.Target != "" {
			return fmt.Errorf("rootfs directory has incompatible payload metadata")
		}
	case EntryFile:
		if e.File == nil {
			return fmt.Errorf("rootfs file object is required")
		}
		if err := e.File.Validate(FileMediaType); err != nil {
			return err
		}
		if e.Directory != nil || e.Target != "" {
			return fmt.Errorf("rootfs file has incompatible payload metadata")
		}
	case EntrySymlink:
		if e.Directory != nil || e.File != nil {
			return fmt.Errorf("rootfs symlink has incompatible payload metadata")
		}
	case EntryWhiteout, EntryChar, EntryBlock, EntryFIFO:
		if e.Directory != nil || e.File != nil || e.Target != "" {
			return fmt.Errorf("rootfs special entry has incompatible payload metadata")
		}
	default:
		return fmt.Errorf("unsupported rootfs entry kind %q", e.Kind)
	}
	return nil
}

func (o Object) Validate(mediaType string) error {
	if strings.TrimSpace(o.Key) == "" {
		return fmt.Errorf("object key is required")
	}
	parsed, err := digest.Parse(strings.TrimSpace(o.Digest))
	if err != nil || parsed.Algorithm() != digest.SHA256 {
		return fmt.Errorf("object digest %q is not sha256", o.Digest)
	}
	if o.Size <= 0 {
		return fmt.Errorf("object size must be positive")
	}
	if o.MediaType != mediaType {
		return fmt.Errorf("object media type %q, expected %q", o.MediaType, mediaType)
	}
	return nil
}

func encodeGZIPJSON(value any) ([]byte, error) {
	var out bytes.Buffer
	writer, err := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	writer.Header.ModTime = time.Unix(0, 0).UTC()
	writer.Header.OS = 255
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func decodeGZIPJSON(reader io.Reader, out any) error {
	if reader == nil {
		return fmt.Errorf("metadata reader is required")
	}
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	defer gzipReader.Close()
	limited := io.LimitReader(gzipReader, maxMetadataBytes+1)
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if decoder.InputOffset() > maxMetadataBytes {
		return fmt.Errorf("rootfs metadata exceeds %d bytes", maxMetadataBytes)
	}
	return nil
}
