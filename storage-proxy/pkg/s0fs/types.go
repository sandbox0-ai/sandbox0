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
)

type Config struct {
	VolumeID            string
	WALPath             string
	ObjectStore         objectstore.Store
	HeadStore           HeadStore
	MaterializeInterval time.Duration
	WALSyncHook         func()
}

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
	ID     string `json:"id"`
	Key    string `json:"key"`
	Length uint64 `json:"length"`
	SHA256 string `json:"sha256,omitempty"`
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
	Atime  time.Time
	Mtime  time.Time
	Ctime  time.Time
}

type DirEntry struct {
	Name  string
	Inode uint64
	Type  FileType
}

func cloneNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	clone := *node
	return &clone
}

type walRecord struct {
	Seq       uint64   `json:"seq"`
	Op        string   `json:"op"`
	Inode     uint64   `json:"inode,omitempty"`
	Parent    uint64   `json:"parent,omitempty"`
	Name      string   `json:"name,omitempty"`
	NewParent uint64   `json:"new_parent,omitempty"`
	NewName   string   `json:"new_name,omitempty"`
	Type      FileType `json:"type,omitempty"`
	Mode      uint32   `json:"mode,omitempty"`
	Offset    uint64   `json:"offset,omitempty"`
	Data      []byte   `json:"data,omitempty"`
	Target    string   `json:"target,omitempty"`
	TimeUnix  int64    `json:"time_unix"`
}
