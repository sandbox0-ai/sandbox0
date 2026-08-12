package s0fs

import (
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const RootInode uint64 = 1

type FileType string

const (
	TypeDirectory FileType = "directory"
	TypeFile      FileType = "file"
	TypeSymlink   FileType = "symlink"
	TypeFIFO      FileType = "fifo"
	TypeChar      FileType = "char_device"
	TypeBlock     FileType = "block_device"
	TypeSocket    FileType = "socket"
)

type SegmentValidationMode string

const (
	// SegmentValidationLazy trusts the committed-head publication protocol at
	// open time. Immutable objects are authenticated on first read and by the
	// background scrubber, keeping activation independent of segment count.
	SegmentValidationLazy SegmentValidationMode = "lazy"
	// SegmentValidationStrict verifies every referenced remote object before the
	// filesystem is exposed. It is intentionally O(number of segments).
	SegmentValidationStrict SegmentValidationMode = "strict"
)

type Config struct {
	VolumeID             string
	WALPath              string
	ObjectStore          objectstore.Store
	ObjectStoreForVolume ObjectStoreResolver
	HeadStore            HeadStore
	Encryption           *EncryptionConfig
	StateFormatVersion   int
	MaterializeInterval  time.Duration
	SegmentTargetSize    uint64
	WALSyncHook          func()
	OpenObserver         OpenObserver
	LocalDiskGuard       *LocalDiskGuard
	RetainUnlinked       bool
	// MetadataPath enables the rebuildable disk-backed namespace index. The
	// committed state plus WAL remain authoritative.
	MetadataPath       string
	MetadataCacheBytes int64
	SegmentValidation  SegmentValidationMode
}

type ObjectStoreResolver func(volumeID string) (objectstore.Store, error)

const (
	StateFormatV1 = 1
	StateFormatV2 = 2
)

// OpenObservation describes one bounded S0FS engine-open phase. Callers must
// keep metric labels low-cardinality; VolumeID is intended for logs only.
type OpenObservation struct {
	VolumeID           string
	Phase              string
	Source             string
	Format             int
	Duration           time.Duration
	Bytes              int64
	WALRecords         int
	WALRecordsScanned  int
	WALRecordsSkipped  int
	WALMaxRecordBytes  int64
	WALMaxDecodedBytes int64
	Nodes              int
	DirectoryEntries   int
	Segments           int
	Err                error
}

type OpenObserver func(OpenObservation)

type SnapshotState struct {
	NextSeq   uint64                       `json:"next_seq"`
	NextInode uint64                       `json:"next_inode"`
	Nodes     map[uint64]*Node             `json:"nodes"`
	Children  map[uint64]map[string]uint64 `json:"children"`
	Data      map[uint64][]byte            `json:"data,omitempty"`
	ColdFiles map[uint64][]FileExtent      `json:"cold_files,omitempty"`
	Segments  map[string]*Segment          `json:"segments,omitempty"`
}

type FileExtent struct {
	SegmentID string `json:"segment_id"`
	Offset    uint64 `json:"offset"`
	Length    uint64 `json:"length"`
}

type Segment struct {
	ID          string             `json:"id"`
	VolumeID    string             `json:"volume_id,omitempty"`
	Key         string             `json:"key"`
	Length      uint64             `json:"length"`
	SHA256      string             `json:"sha256,omitempty"`
	ChunkSize   uint64             `json:"chunk_size,omitempty"`
	ChunkSHA256 [][]byte           `json:"chunk_sha256,omitempty"`
	Encryption  *SegmentEncryption `json:"encryption,omitempty"`
	InlineData  []byte             `json:"inline_data,omitempty"`
}

type Node struct {
	Inode  uint64
	Type   FileType
	Mode   uint32
	UID    uint32
	GID    uint32
	Nlink  uint32
	Size   uint64
	Target string
	Rdev   uint64
	Xattrs map[string][]byte `json:",omitempty"`
	Atime  time.Time
	Mtime  time.Time
	Ctime  time.Time
}

type DirEntry struct {
	Name  string
	Inode uint64
	Type  FileType
	Node  *Node
}

type CreateOptions struct {
	UID  uint32
	GID  uint32
	Rdev uint64
}

func cloneNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	clone := *node
	if node.Xattrs != nil {
		clone.Xattrs = make(map[string][]byte, len(node.Xattrs))
		for name, value := range node.Xattrs {
			clone.Xattrs[name] = append([]byte(nil), value...)
		}
	}
	return &clone
}

type walRecord struct {
	Seq          uint64   `json:"seq"`
	Op           string   `json:"op"`
	Inode        uint64   `json:"inode,omitempty"`
	Parent       uint64   `json:"parent,omitempty"`
	Name         string   `json:"name,omitempty"`
	NewParent    uint64   `json:"new_parent,omitempty"`
	NewName      string   `json:"new_name,omitempty"`
	Type         FileType `json:"type,omitempty"`
	Mode         uint32   `json:"mode,omitempty"`
	UID          uint32   `json:"uid,omitempty"`
	GID          uint32   `json:"gid,omitempty"`
	Offset       uint64   `json:"offset,omitempty"`
	Length       uint64   `json:"length,omitempty"`
	SourceInode  uint64   `json:"source_inode,omitempty"`
	SourceOffset uint64   `json:"source_offset,omitempty"`
	Data         []byte   `json:"data,omitempty"`
	Target       string   `json:"target,omitempty"`
	Rdev         uint64   `json:"rdev,omitempty"`
	AtimeUnix    int64    `json:"atime_unix,omitempty"`
	MtimeUnix    int64    `json:"mtime_unix,omitempty"`
	TimeUnix     int64    `json:"time_unix"`
}
