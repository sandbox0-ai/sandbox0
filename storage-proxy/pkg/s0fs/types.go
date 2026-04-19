package s0fs

import "time"

const RootInode uint64 = 1

type FileType string

const (
	TypeDirectory FileType = "directory"
	TypeFile      FileType = "file"
	TypeSymlink   FileType = "symlink"
)

type Config struct {
	VolumeID string
	WALPath  string
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
