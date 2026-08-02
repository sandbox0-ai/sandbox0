package rootfscow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	defaultChunkSize    = 4 << 20
	defaultChunkWorkers = 8
)

var ErrUnstable = errors.New("rootfs entry changed while being persisted")

type CaptureConfig struct {
	Root          string
	Prefix        string
	GenerationID  string
	ExcludedPaths []string
	ChunkSize     int
	ChunkWorkers  int
	OpaqueRoot    bool
	Editor        *Editor
	Writer        *ObjectWriter
}

// Capture converts one local overlay upper into immutable metadata and chunks.
// It never reads the parent head or template payload eagerly.
type Capture struct {
	root         string
	prefix       string
	generationID string
	excluded     []string
	chunkSize    int
	chunkWorkers int
	opaqueRoot   bool
	editor       *Editor
	writer       *ObjectWriter
}

func NewCapture(cfg CaptureConfig) (*Capture, error) {
	root := filepath.Clean(strings.TrimSpace(cfg.Root))
	if root == "" || root == "." || !filepath.IsAbs(root) {
		return nil, fmt.Errorf("rootfs upper directory is required")
	}
	if strings.TrimSpace(cfg.GenerationID) == "" {
		return nil, fmt.Errorf("rootfs generation id is required")
	}
	if cfg.Editor == nil || cfg.Writer == nil {
		return nil, fmt.Errorf("rootfs capture editor and writer are required")
	}
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	chunkWorkers := cfg.ChunkWorkers
	if chunkWorkers <= 0 {
		chunkWorkers = defaultChunkWorkers
	}
	prefix := strings.Trim(cleanLogicalPath(cfg.Prefix), "/")
	excluded := make([]string, 0, len(cfg.ExcludedPaths))
	for _, value := range cfg.ExcludedPaths {
		value = cleanLogicalPath(value)
		if value != "/" {
			excluded = append(excluded, value)
		}
	}
	sort.Strings(excluded)
	return &Capture{
		root:         root,
		prefix:       prefix,
		generationID: strings.TrimSpace(cfg.GenerationID),
		excluded:     excluded,
		chunkSize:    chunkSize,
		chunkWorkers: chunkWorkers,
		opaqueRoot:   cfg.OpaqueRoot,
		editor:       cfg.Editor,
		writer:       cfg.Writer,
	}, nil
}

func (c *Capture) Root() string { return c.root }

func (c *Capture) Excludes(relative string) bool { return c.excludes(c.logicalPath(relative)) }

func (c *Capture) Scan(ctx context.Context, mark func(string)) error {
	return filepath.WalkDir(c.root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(c.root, current)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "." {
			relative = ""
		}
		if c.excludes(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		mark(relative)
		return nil
	})
}

func (c *Capture) Path(ctx context.Context, relative string) (bool, error) {
	relative = cleanRelativePath(relative)
	logical := c.logicalPath(relative)
	if c.excludes(logical) {
		return false, nil
	}
	hostPath := filepath.Join(c.root, filepath.FromSlash(relative))
	info, err := os.Lstat(hostPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, c.editor.Reset(ctx, logical)
		}
		return false, err
	}
	entry, opaque, err := c.entry(ctx, hostPath, relative, info)
	if err != nil {
		return false, err
	}
	if logical == "" {
		return true, c.editor.SetRoot(entry)
	}
	if relative == "" && c.opaqueRoot {
		opaque = true
	}
	return true, c.editor.Set(ctx, logical, entry, opaque)
}

// CaptureTree snapshots a complete mapped tree into the shared editor. Portal
// roots use this at seal time; they are small control-state directories and
// are made opaque so deletions from an older head cannot reappear.
func (c *Capture) CaptureTree(ctx context.Context) error {
	var paths []string
	if err := c.Scan(ctx, func(relative string) { paths = append(paths, relative) }); err != nil {
		return err
	}
	for _, relative := range paths {
		if _, err := c.Path(ctx, relative); err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) entry(ctx context.Context, hostPath, relative string, info os.FileInfo) (rootfshead.Entry, bool, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return rootfshead.Entry{}, false, fmt.Errorf("inspect rootfs entry %s: stat metadata unavailable", hostPath)
	}
	xattrs, err := readXAttrs(hostPath)
	if err != nil {
		return rootfshead.Entry{}, false, err
	}
	entry := rootfshead.Entry{
		Inode:      c.inodeKey(stat),
		Mode:       uint32(stat.Mode & 0o7777),
		UID:        stat.Uid,
		GID:        stat.Gid,
		Nlink:      uint32(stat.Nlink),
		Size:       uint64(max(info.Size(), 0)),
		Rdev:       uint32(stat.Rdev),
		ModTime:    rootfshead.NewTimestamp(info.ModTime()),
		AccessTime: rootfshead.Timestamp{Seconds: stat.Atim.Sec, Nanoseconds: uint32(stat.Atim.Nsec)},
		ChangeTime: rootfshead.Timestamp{Seconds: stat.Ctim.Sec, Nanoseconds: uint32(stat.Ctim.Nsec)},
		XAttrs:     xattrs,
	}
	opaque := hasOpaqueXAttr(xattrs)
	switch {
	case info.Mode().IsDir():
		entry.Kind = rootfshead.EntryDirectory
		entry.Size = 0
	case info.Mode().IsRegular():
		entry.Kind = rootfshead.EntryFile
		manifest, err := c.captureFile(ctx, hostPath, stat)
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		entry.File = &manifest
	case info.Mode()&os.ModeSymlink != 0:
		entry.Kind = rootfshead.EntrySymlink
		target, err := os.Readlink(hostPath)
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		entry.Target = target
		entry.Size = uint64(len(target))
	case info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice != 0 && stat.Rdev == 0:
		entry.Kind = rootfshead.EntryWhiteout
		entry.Size = 0
	case info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice != 0:
		entry.Kind = rootfshead.EntryChar
		entry.Size = 0
	case info.Mode()&os.ModeDevice != 0:
		entry.Kind = rootfshead.EntryBlock
		entry.Size = 0
	case info.Mode()&os.ModeNamedPipe != 0:
		entry.Kind = rootfshead.EntryFIFO
		entry.Size = 0
	case info.Mode()&os.ModeSocket != 0:
		return rootfshead.Entry{}, false, fmt.Errorf("rootfs socket %q cannot be persisted", relative)
	default:
		return rootfshead.Entry{}, false, fmt.Errorf("unsupported rootfs entry mode %s for %q", info.Mode(), relative)
	}
	return entry, opaque, nil
}

func (c *Capture) captureFile(ctx context.Context, hostPath string, before *syscall.Stat_t) (rootfshead.Object, error) {
	file, err := os.Open(hostPath)
	if err != nil {
		return rootfshead.Object{}, err
	}
	defer file.Close()
	var opened syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &opened); err != nil {
		return rootfshead.Object{}, err
	}
	if !sameFileVersion(before, &opened) {
		return rootfshead.Object{}, ErrUnstable
	}
	manifest := rootfshead.FileManifest{Version: rootfshead.Version, Size: uint64(max(opened.Size, 0))}
	if manifest.Size > 0 {
		extents, err := c.captureFileExtents(ctx, file, manifest.Size)
		if err != nil {
			return rootfshead.Object{}, err
		}
		manifest.Extents = extents
	}
	var after syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &after); err != nil {
		return rootfshead.Object{}, err
	}
	if !sameFileVersion(&opened, &after) {
		return rootfshead.Object{}, ErrUnstable
	}
	payload, err := rootfshead.EncodeFileManifest(manifest)
	if err != nil {
		return rootfshead.Object{}, err
	}
	return c.writer.Put(ctx, rootfshead.FileMediaType, payload)
}

func (c *Capture) captureFileExtents(ctx context.Context, file *os.File, size uint64) ([]rootfshead.FileExtent, error) {
	ranges, sparse, err := dataRanges(int(file.Fd()), size)
	if err != nil {
		return nil, err
	}
	if !sparse {
		ranges = []fileRange{{offset: 0, length: size}}
	}
	type chunkJob struct {
		offset uint64
		length uint64
	}
	var jobs []chunkJob
	for _, dataRange := range ranges {
		for offset, remaining := dataRange.offset, dataRange.length; remaining > 0; {
			length := min(uint64(c.chunkSize), remaining)
			jobs = append(jobs, chunkJob{offset: offset, length: length})
			offset += length
			remaining -= length
		}
	}
	extents := make([]rootfshead.FileExtent, len(jobs))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(c.chunkWorkers)
	for position := range jobs {
		position := position
		group.Go(func() error {
			job := jobs[position]
			payload := make([]byte, int(job.length))
			n, readErr := file.ReadAt(payload, int64(job.offset))
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			if n != len(payload) {
				return io.ErrUnexpectedEOF
			}
			object, err := c.writer.Put(groupCtx, rootfshead.ChunkMediaType, payload)
			if err != nil {
				return err
			}
			extents[position] = rootfshead.FileExtent{Offset: job.offset, Length: job.length, Object: object}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return extents, nil
}

type fileRange struct {
	offset uint64
	length uint64
}

func dataRanges(fd int, size uint64) ([]fileRange, bool, error) {
	var ranges []fileRange
	for offset := uint64(0); offset < size; {
		data, err := unix.Seek(fd, int64(offset), unix.SEEK_DATA)
		if errors.Is(err, syscall.ENXIO) {
			return ranges, true, nil
		}
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		end := min(uint64(hole), size)
		if uint64(data) < end {
			ranges = append(ranges, fileRange{offset: uint64(data), length: end - uint64(data)})
		}
		offset = end
	}
	return ranges, true, nil
}

func readXAttrs(path string) ([]rootfshead.XAttr, error) {
	size, err := unix.Llistxattr(path, nil)
	if errors.Is(err, syscall.ENOTSUP) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list rootfs xattrs for %s: %w", path, err)
	}
	if size == 0 {
		return nil, nil
	}
	namesPayload := make([]byte, size)
	n, err := unix.Llistxattr(path, namesPayload)
	if err != nil {
		return nil, fmt.Errorf("list rootfs xattrs for %s: %w", path, err)
	}
	var attrs []rootfshead.XAttr
	for _, raw := range strings.Split(string(namesPayload[:n]), "\x00") {
		if raw == "" {
			continue
		}
		valueSize, err := unix.Lgetxattr(path, raw, nil)
		if err != nil {
			return nil, fmt.Errorf("read rootfs xattr %s for %s: %w", raw, path, err)
		}
		value := make([]byte, valueSize)
		if valueSize > 0 {
			read, err := unix.Lgetxattr(path, raw, value)
			if err != nil {
				return nil, fmt.Errorf("read rootfs xattr %s for %s: %w", raw, path, err)
			}
			value = value[:read]
		}
		attrs = append(attrs, rootfshead.XAttr{Name: raw, Value: value})
	}
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Name < attrs[j].Name })
	return attrs, nil
}

func hasOpaqueXAttr(attrs []rootfshead.XAttr) bool {
	for _, attr := range attrs {
		if (attr.Name == "trusted.overlay.opaque" || attr.Name == "user.overlay.opaque") && (string(attr.Value) == "y" || string(attr.Value) == "x") {
			return true
		}
	}
	return false
}

func (c *Capture) inodeKey(stat *syscall.Stat_t) string {
	return c.generationID + ":" + strconv.FormatUint(uint64(stat.Dev), 16) + ":" + strconv.FormatUint(stat.Ino, 16)
}

func sameFileVersion(left, right *syscall.Stat_t) bool {
	return left != nil && right != nil && left.Dev == right.Dev && left.Ino == right.Ino && left.Size == right.Size &&
		left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func (c *Capture) excludes(relative string) bool {
	logical := cleanLogicalPath(relative)
	for _, excluded := range c.excluded {
		if logical == excluded || strings.HasPrefix(logical, excluded+"/") {
			return true
		}
	}
	return false
}

func (c *Capture) logicalPath(relative string) string {
	relative = cleanRelativePath(relative)
	if c.prefix == "" {
		return relative
	}
	if relative == "" {
		return c.prefix
	}
	return path.Join(c.prefix, relative)
}

func cleanRelativePath(value string) string {
	value = filepath.ToSlash(filepath.Clean("/" + value))
	return strings.TrimPrefix(value, "/")
}

func cleanLogicalPath(value string) string {
	return filepath.ToSlash(filepath.Clean("/" + strings.TrimSpace(value)))
}
