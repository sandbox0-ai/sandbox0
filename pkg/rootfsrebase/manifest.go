// Package rootfsrebase provides the privileged, offline filesystem metadata
// and extent primitives used by the RootFS three-way rebase worker.
package rootfsrebase

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
)

const ManifestVersion = 1

type NodeType string

const (
	NodeDirectory   NodeType = "directory"
	NodeRegular     NodeType = "regular"
	NodeSymlink     NodeType = "symlink"
	NodeCharDevice  NodeType = "char_device"
	NodeBlockDevice NodeType = "block_device"
	NodeFIFO        NodeType = "fifo"
	NodeSocket      NodeType = "socket"
)

// Extent is one FIEMAP result. Physical offsets are relative to the exact
// block device backing the scanned filesystem and are never portable to a
// different filesystem instance.
type Extent struct {
	Logical  uint64 `json:"logical"`
	Physical uint64 `json:"physical"`
	Length   uint64 `json:"length"`
	Flags    uint32 `json:"flags,omitempty"`
}

// Node records semantic metadata plus the inode identity and physical extent
// map needed to turn branch dirty LBAs into file-relative change ranges.
type Node struct {
	Path       string   `json:"path"`
	Type       NodeType `json:"type"`
	Mode       uint32   `json:"mode"`
	UID        uint32   `json:"uid"`
	GID        uint32   `json:"gid"`
	Size       int64    `json:"size"`
	ModTimeNS  int64    `json:"mtime_ns"`
	Device     uint64   `json:"device"`
	Inode      uint64   `json:"inode"`
	Generation uint32   `json:"generation,omitempty"`
	// GenerationKnown distinguishes a real zero inode generation from a
	// filesystem that does not implement FS_IOC_GETVERSION.
	GenerationKnown bool              `json:"generation_known,omitempty"`
	LinkCount       uint64            `json:"link_count"`
	Rdev            uint64            `json:"rdev,omitempty"`
	LinkTarget      string            `json:"link_target,omitempty"`
	Xattrs          map[string][]byte `json:"xattrs,omitempty"`
	Extents         []Extent          `json:"extents,omitempty"`
}

// Manifest is a bounded-control view of a quiesced filesystem tree. File
// contents are deliberately absent. Its digest is local execution evidence,
// not a portable content digest, because device IDs and physical extents are
// intentionally included.
type Manifest struct {
	Version   int    `json:"version"`
	LineageID string `json:"lineage_id,omitempty"`
	Nodes     []Node `json:"nodes"`
}

func (m Manifest) Validate() error {
	if m.Version != ManifestVersion {
		return fmt.Errorf("unsupported RootFS manifest version %d", m.Version)
	}
	if len(m.Nodes) == 0 || m.Nodes[0].Path != "." {
		return fmt.Errorf("RootFS manifest must start with the root node")
	}
	if m.Nodes[0].Type != NodeDirectory {
		return fmt.Errorf("RootFS manifest root must be a directory")
	}
	if strings.TrimSpace(m.LineageID) != m.LineageID {
		return fmt.Errorf("lineage_id must use canonical whitespace-free encoding")
	}
	previous := ""
	for index := range m.Nodes {
		node := &m.Nodes[index]
		if err := node.validate(); err != nil {
			return fmt.Errorf("node %d: %w", index, err)
		}
		if index > 0 && node.Path <= previous {
			return fmt.Errorf("manifest paths must be unique and sorted")
		}
		previous = node.Path
	}
	return nil
}

func (m Manifest) Digest() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (n Node) validate() error {
	if !validManifestPath(n.Path) {
		return fmt.Errorf("path %q is not canonical", n.Path)
	}
	switch n.Type {
	case NodeDirectory, NodeRegular, NodeSymlink, NodeCharDevice, NodeBlockDevice, NodeFIFO, NodeSocket:
	default:
		return fmt.Errorf("unsupported node type %q", n.Type)
	}
	if n.Size < 0 || n.LinkCount == 0 {
		return fmt.Errorf("size or link count is invalid")
	}
	if n.Type != NodeRegular && len(n.Extents) != 0 {
		return fmt.Errorf("only regular files may contain extents")
	}
	if n.Type == NodeSymlink && n.LinkTarget == "" {
		return fmt.Errorf("symlink target is required")
	}
	var previousEnd uint64
	for index, extent := range n.Extents {
		if extent.Length == 0 || extent.Logical > math.MaxUint64-extent.Length ||
			extent.Physical > math.MaxUint64-extent.Length {
			return fmt.Errorf("extent %d overflows", index)
		}
		if index > 0 && extent.Logical < previousEnd {
			return fmt.Errorf("extents overlap or are unsorted")
		}
		previousEnd = extent.Logical + extent.Length
	}
	for name := range n.Xattrs {
		if strings.TrimSpace(name) == "" || strings.ContainsRune(name, '\x00') {
			return fmt.Errorf("xattr name is invalid")
		}
	}
	return nil
}

func validManifestPath(value string) bool {
	return value == "." || value != "" && !strings.HasPrefix(value, "/") &&
		path.Clean(value) == value && value != ".." && !strings.HasPrefix(value, "../")
}

func sortManifest(manifest *Manifest) {
	sort.Slice(manifest.Nodes, func(i, j int) bool { return manifest.Nodes[i].Path < manifest.Nodes[j].Path })
}

func cloneXattrs(source map[string][]byte) map[string][]byte {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string][]byte, len(source))
	for name, value := range source {
		result[name] = append([]byte(nil), value...)
	}
	return result
}
