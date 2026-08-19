//go:build linux

package rootfsrebase

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	fiemapIoctl            = uintptr(0xc020660b)
	fiemapHeaderBytes      = 32
	fiemapExtentBytes      = 56
	fiemapExtentBatch      = 256
	fiemapFlagSync         = uint32(1)
	fiemapExtentLast       = uint32(1)
	fiemapExtentUnknown    = uint32(2)
	fiemapExtentDelalloc   = uint32(4)
	fiemapExtentEncoded    = uint32(8)
	fiemapExtentEncrypted  = uint32(0x80)
	fiemapExtentNotAligned = uint32(0x100)
	fiemapExtentInline     = uint32(0x200)
	fiemapExtentTail       = uint32(0x400)
	fsIoctlGetVersion      = uintptr(2<<30 | unsafe.Sizeof(uintptr(0))<<16 | 'v'<<8 | 1)
	maxXattrListBytes      = 1 << 20
)

const unsupportedFiemapFlags = fiemapExtentUnknown | fiemapExtentDelalloc | fiemapExtentEncoded |
	fiemapExtentEncrypted | fiemapExtentNotAligned | fiemapExtentInline | fiemapExtentTail

type inodeKey struct{ device, inode uint64 }

// Scan reads a quiesced tree without following symlinks. FIEMAP is requested
// with SYNC so delayed allocation cannot silently disappear from dirty-LBA
// attribution.
func Scan(root string) (*Manifest, error) {
	return ScanWithOptions(root, ScanOptions{})
}

type ScanOptions struct {
	// LineageID is supplied by the trusted generation controller. Equal,
	// non-empty values let Diff identify stable inode renames between two
	// mounts of the same logical XFS filesystem.
	LineageID string
}

func ScanWithOptions(root string, options ScanOptions) (*Manifest, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if !filepath.IsAbs(root) || root == string(filepath.Separator) {
		return nil, fmt.Errorf("RootFS scan root must be a non-root absolute path")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("inspect RootFS scan root: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("RootFS scan root must be a real directory")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve RootFS scan root: %w", err)
	}
	if strings.TrimSpace(options.LineageID) != options.LineageID {
		return nil, fmt.Errorf("RootFS lineage ID must use canonical whitespace-free encoding")
	}
	manifest := &Manifest{Version: ManifestVersion, LineageID: options.LineageID}
	extentCache := make(map[inodeKey][]Extent)
	xattrCache := make(map[inodeKey]map[string][]byte)
	err = filepath.WalkDir(resolved, func(hostPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(resolved, hostPath)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !validManifestPath(relative) {
			return fmt.Errorf("non-canonical RootFS path %q", relative)
		}
		node, err := scanNode(hostPath, relative, extentCache, xattrCache)
		if err != nil {
			return err
		}
		manifest.Nodes = append(manifest.Nodes, node)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan RootFS tree: %w", err)
	}
	sortManifest(manifest)
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return manifest, nil
}

func scanNode(
	hostPath, relative string,
	extentCache map[inodeKey][]Extent,
	xattrCache map[inodeKey]map[string][]byte,
) (Node, error) {
	var stat unix.Stat_t
	if err := unix.Lstat(hostPath, &stat); err != nil {
		return Node{}, fmt.Errorf("lstat %s: %w", relative, err)
	}
	nodeType, err := nodeTypeFromMode(stat.Mode)
	if err != nil {
		return Node{}, fmt.Errorf("inspect %s: %w", relative, err)
	}
	node := Node{
		Path: relative, Type: nodeType, Mode: stat.Mode, UID: stat.Uid, GID: stat.Gid,
		Size: stat.Size, ModTimeNS: stat.Mtim.Sec*1_000_000_000 + stat.Mtim.Nsec,
		Device: uint64(stat.Dev), Inode: stat.Ino, LinkCount: uint64(stat.Nlink), Rdev: uint64(stat.Rdev),
	}
	key := inodeKey{node.Device, node.Inode}
	if node.Type == NodeSymlink {
		node.LinkTarget, err = os.Readlink(hostPath)
		if err != nil {
			return Node{}, fmt.Errorf("readlink %s: %w", relative, err)
		}
	}
	if cached, ok := xattrCache[key]; ok {
		node.Xattrs = cloneXattrs(cached)
	} else {
		node.Xattrs, err = readXattrs(hostPath)
		if err != nil {
			return Node{}, fmt.Errorf("read xattrs %s: %w", relative, err)
		}
		xattrCache[key] = cloneXattrs(node.Xattrs)
	}
	if node.Type == NodeRegular {
		node.Generation, node.GenerationKnown, err = inodeGeneration(hostPath)
		if err != nil {
			return Node{}, fmt.Errorf("read inode generation %s: %w", relative, err)
		}
		if cached, ok := extentCache[key]; ok {
			node.Extents = append([]Extent(nil), cached...)
		} else {
			node.Extents, err = fiemap(hostPath)
			if err != nil {
				return Node{}, fmt.Errorf("map extents %s: %w", relative, err)
			}
			extentCache[key] = append([]Extent(nil), node.Extents...)
		}
	}
	return node, nil
}

func inodeGeneration(path string) (uint32, bool, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return 0, false, err
	}
	defer unix.Close(fd)
	var generation uint64
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), fsIoctlGetVersion, uintptr(unsafe.Pointer(&generation)))
	runtime.KeepAlive(&generation)
	if errno == unix.ENOTTY || errno == unix.EOPNOTSUPP || errno == unix.ENOSYS || errno == unix.EINVAL {
		return 0, false, nil
	}
	if errno != 0 {
		return 0, false, errno
	}
	return uint32(generation), true, nil
}

func nodeTypeFromMode(mode uint32) (NodeType, error) {
	switch mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return NodeDirectory, nil
	case unix.S_IFREG:
		return NodeRegular, nil
	case unix.S_IFLNK:
		return NodeSymlink, nil
	case unix.S_IFCHR:
		return NodeCharDevice, nil
	case unix.S_IFBLK:
		return NodeBlockDevice, nil
	case unix.S_IFIFO:
		return NodeFIFO, nil
	case unix.S_IFSOCK:
		return NodeSocket, nil
	default:
		return "", fmt.Errorf("unsupported inode mode %#o", mode)
	}
}

func readXattrs(path string) (map[string][]byte, error) {
	size, err := unix.Llistxattr(path, nil)
	if errors.Is(err, unix.ENOTSUP) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	if size > maxXattrListBytes {
		return nil, fmt.Errorf("xattr name list exceeds %d bytes", maxXattrListBytes)
	}
	buffer := make([]byte, size)
	size, err = unix.Llistxattr(path, buffer)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]byte)
	for _, name := range strings.Split(string(buffer[:size]), "\x00") {
		if name == "" {
			continue
		}
		valueSize, err := unix.Lgetxattr(path, name, nil)
		if err != nil {
			return nil, fmt.Errorf("get size of %s: %w", name, err)
		}
		value := make([]byte, valueSize)
		if valueSize > 0 {
			read, err := unix.Lgetxattr(path, name, value)
			if err != nil {
				return nil, fmt.Errorf("get %s: %w", name, err)
			}
			value = value[:read]
		}
		result[name] = value
	}
	return result, nil
}

func fiemap(path string) ([]Extent, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	start := uint64(0)
	var result []Extent
	for {
		buffer := make([]byte, fiemapHeaderBytes+fiemapExtentBatch*fiemapExtentBytes)
		binary.LittleEndian.PutUint64(buffer[0:8], start)
		binary.LittleEndian.PutUint64(buffer[8:16], math.MaxUint64-start)
		binary.LittleEndian.PutUint32(buffer[16:20], fiemapFlagSync)
		binary.LittleEndian.PutUint32(buffer[24:28], fiemapExtentBatch)
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), fiemapIoctl, uintptr(unsafe.Pointer(&buffer[0])))
		runtime.KeepAlive(buffer)
		if errno != 0 {
			return nil, errno
		}
		mapped := binary.LittleEndian.Uint32(buffer[20:24])
		if mapped > fiemapExtentBatch {
			return nil, fmt.Errorf("kernel returned %d FIEMAP extents into a %d-extent buffer", mapped, fiemapExtentBatch)
		}
		if mapped == 0 {
			break
		}
		last := false
		for index := uint32(0); index < mapped; index++ {
			offset := fiemapHeaderBytes + int(index)*fiemapExtentBytes
			extent := Extent{
				Logical:  binary.LittleEndian.Uint64(buffer[offset : offset+8]),
				Physical: binary.LittleEndian.Uint64(buffer[offset+8 : offset+16]),
				Length:   binary.LittleEndian.Uint64(buffer[offset+16 : offset+24]),
				Flags:    binary.LittleEndian.Uint32(buffer[offset+40 : offset+44]),
			}
			if extent.Length == 0 || extent.Logical > math.MaxUint64-extent.Length {
				return nil, fmt.Errorf("kernel returned an invalid FIEMAP extent")
			}
			if extent.Flags&unsupportedFiemapFlags != 0 {
				return nil, fmt.Errorf("FIEMAP extent at %d has unsupported flags %#x", extent.Logical, extent.Flags)
			}
			result = append(result, extent)
			start = extent.Logical + extent.Length
			last = extent.Flags&fiemapExtentLast != 0
		}
		if last || start == math.MaxUint64 {
			break
		}
	}
	return result, nil
}
