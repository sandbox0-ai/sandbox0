package rootfscow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"golang.org/x/sys/unix"
)

const (
	// One MiB bounds a cold 4 KiB FUSE read to at most one MiB of verified
	// object I/O. The size remains per-manifest so it can evolve independently.
	defaultChunkSize    = 1 << 20
	defaultChunkWorkers = 8
)

var (
	ErrUnstable                   = errors.New("rootfs entry changed while being persisted")
	ErrUnsupportedOverlayMetadata = errors.New("unsupported overlay metadata")
)

type FileVersion struct {
	Device     uint64
	Inode      uint64
	Mode       uint32
	Rdev       uint64
	Size       int64
	ModSeconds int64
	ModNanos   int64
	CTimeSec   int64
	CTimeNanos int64
}

type CaptureResult struct {
	Exists  bool
	Version FileVersion
}

type CaptureConfig struct {
	Root          string
	GenerationID  string
	ExcludedPaths []string
	ChunkSize     int
	ChunkWorkers  int
	Editor        *Editor
	Writer        *rootfsstore.Writer
	OpaqueRoot    bool
}

// Capture converts one local overlay upper into immutable metadata and chunks.
type Capture struct {
	root         string
	generationID string
	excluded     []string
	chunkSize    int
	chunkWorkers int
	editor       *Editor
	writer       *rootfsstore.Writer
	opaqueRoot   bool
	fileMu       sync.Mutex
	fileCache    map[inodeIdentity]capturedFileManifest
	fileLoads    singleflight.Group
	scanOverride func(context.Context, func(string, FileVersion)) error
}

type capturedFileManifest struct {
	version FileVersion
	object  rootfshead.Object
	size    uint64
	blocks  uint64
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
		generationID: strings.TrimSpace(cfg.GenerationID),
		excluded:     excluded,
		chunkSize:    chunkSize,
		chunkWorkers: chunkWorkers,
		editor:       cfg.Editor,
		writer:       cfg.Writer,
		opaqueRoot:   cfg.OpaqueRoot,
		fileCache:    make(map[inodeIdentity]capturedFileManifest),
	}, nil
}

func (c *Capture) Root() string { return c.root }

func (c *Capture) Excludes(relative string) bool { return c.excludes(relative) }

func (c *Capture) Scan(ctx context.Context, visit func(string, FileVersion)) error {
	if c.scanOverride != nil {
		return c.scanOverride(ctx, visit)
	}
	return c.scan(ctx, visit)
}

func (c *Capture) scan(ctx context.Context, visit func(string, FileVersion)) error {
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
		if c.Excludes(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		version, err := versionFromInfo(info)
		if err != nil {
			return err
		}
		if visit != nil {
			visit(relative, version)
		}
		return nil
	})
}

func (c *Capture) Version(relative string) (FileVersion, bool, error) {
	relative = cleanRelativePath(relative)
	if c.Excludes(relative) {
		return FileVersion{}, false, nil
	}
	info, err := os.Lstat(c.hostPath(relative))
	if errors.Is(err, os.ErrNotExist) {
		return FileVersion{}, false, nil
	}
	if err != nil {
		return FileVersion{}, false, err
	}
	version, err := versionFromInfo(info)
	return version, err == nil, err
}

func (c *Capture) Path(ctx context.Context, relative string) (CaptureResult, error) {
	relative = cleanRelativePath(relative)
	logical := relative
	if c.excludes(logical) {
		return CaptureResult{}, nil
	}
	hostPath := c.hostPath(relative)
	info, err := os.Lstat(hostPath)
	if errors.Is(err, os.ErrNotExist) {
		return CaptureResult{}, c.editor.Reset(ctx, logical)
	}
	if err != nil {
		return CaptureResult{}, err
	}
	before, err := versionFromInfo(info)
	if err != nil {
		return CaptureResult{}, err
	}
	entry, opaque, err := c.entry(ctx, hostPath, relative, info)
	if err != nil {
		return CaptureResult{}, err
	}
	if logical == "" {
		if c.opaqueRoot {
			entry.Opaque = true
		}
		err = c.editor.SetRoot(entry)
	} else {
		err = c.editor.Set(ctx, logical, entry, opaque)
	}
	if err != nil {
		return CaptureResult{}, err
	}
	after, exists, err := c.Version(relative)
	if err != nil {
		return CaptureResult{}, err
	}
	if !exists || before != after {
		c.ForgetFile(before)
		return CaptureResult{}, ErrUnstable
	}
	return CaptureResult{Exists: true, Version: after}, nil
}

func (c *Capture) CaptureTree(ctx context.Context) error {
	var paths []string
	if err := c.Scan(ctx, func(relative string, _ FileVersion) { paths = append(paths, relative) }); err != nil {
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
	xattrs, overlay, err := readXAttrs(hostPath)
	if err != nil {
		return rootfshead.Entry{}, false, err
	}
	entry := rootfshead.Entry{
		Inode:      c.inodeKey(stat),
		Mode:       uint32(stat.Mode),
		UID:        stat.Uid,
		GID:        stat.Gid,
		Nlink:      uint32(stat.Nlink),
		Size:       uint64(max(info.Size(), 0)),
		Blocks:     uint64(max(stat.Blocks, 0)),
		Rdev:       uint32(stat.Rdev),
		ModTime:    rootfshead.Timestamp{Seconds: stat.Mtim.Sec, Nanoseconds: uint32(stat.Mtim.Nsec)},
		AccessTime: rootfshead.Timestamp{Seconds: stat.Atim.Sec, Nanoseconds: uint32(stat.Atim.Nsec)},
		ChangeTime: rootfshead.Timestamp{Seconds: stat.Ctim.Sec, Nanoseconds: uint32(stat.Ctim.Nsec)},
		XAttrs:     xattrs,
		Opaque:     overlay.opaque,
	}
	opaque := overlay.opaque
	switch {
	case overlay.whiteout:
		normalizeWhiteoutEntry(&entry)
	case info.Mode().IsDir():
		entry.Kind = rootfshead.EntryDirectory
		entry.Size = 0
	case info.Mode().IsRegular():
		entry.Kind = rootfshead.EntryFile
		manifest, err := c.captureFile(ctx, hostPath, stat)
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		entry.File = &manifest.object
		// Opening an overlay file can complete copy-up and change st_blocks
		// without changing the file version fields. Keep the directory entry
		// metadata identical to the manifest captured from the open descriptor.
		entry.Size = manifest.size
		entry.Blocks = manifest.blocks
	case info.Mode()&os.ModeSymlink != 0:
		entry.Kind = rootfshead.EntrySymlink
		target, err := os.Readlink(hostPath)
		if err != nil {
			return rootfshead.Entry{}, false, err
		}
		entry.Target = target
		entry.Size = uint64(len(target))
		entry.Blocks = 0
	case info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice != 0 && stat.Rdev == 0:
		normalizeWhiteoutEntry(&entry)
	case info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice != 0:
		entry.Kind = rootfshead.EntryChar
		entry.Size = 0
		entry.Blocks = 0
	case info.Mode()&os.ModeDevice != 0:
		entry.Kind = rootfshead.EntryBlock
		entry.Size = 0
		entry.Blocks = 0
	case info.Mode()&os.ModeNamedPipe != 0:
		entry.Kind = rootfshead.EntryFIFO
		entry.Size = 0
		entry.Blocks = 0
	case info.Mode()&os.ModeSocket != 0:
		// UNIX sockets are live process state and cannot be recreated as an
		// immutable lower entry. A whiteout also prevents an older parent/base
		// node at this path from reappearing after resume.
		normalizeWhiteoutEntry(&entry)
	default:
		return rootfshead.Entry{}, false, fmt.Errorf("unsupported rootfs entry mode %s for %q", info.Mode(), relative)
	}
	return entry, opaque, nil
}

func normalizeWhiteoutEntry(entry *rootfshead.Entry) {
	entry.Kind = rootfshead.EntryWhiteout
	entry.Mode = uint32(syscall.S_IFCHR) | entry.Mode&0o7777
	entry.Size = 0
	entry.Blocks = 0
	entry.Rdev = 0
	entry.XAttrs = nil
	entry.Opaque = false
}

func (c *Capture) captureFile(ctx context.Context, hostPath string, before *syscall.Stat_t) (capturedFileManifest, error) {
	version := fileVersionFromStat(before)
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	if object, ok := c.cachedFileManifest(identity, version); ok {
		return object, nil
	}
	value, err, _ := c.fileLoads.Do(fileVersionKey(version), func() (any, error) {
		if object, ok := c.cachedFileManifest(identity, version); ok {
			return object, nil
		}
		manifest, err := c.captureFileUncached(ctx, hostPath, before)
		if err != nil {
			return capturedFileManifest{}, err
		}
		c.fileMu.Lock()
		c.fileCache[identity] = manifest
		c.fileMu.Unlock()
		return manifest, nil
	})
	if err != nil {
		return capturedFileManifest{}, err
	}
	return value.(capturedFileManifest), nil
}

func (c *Capture) captureFileUncached(ctx context.Context, hostPath string, before *syscall.Stat_t) (capturedFileManifest, error) {
	file, err := openFileNoAtime(hostPath)
	if err != nil {
		return capturedFileManifest{}, err
	}
	defer file.Close()
	var opened syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &opened); err != nil {
		return capturedFileManifest{}, err
	}
	if !sameFileVersion(before, &opened) || opened.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return capturedFileManifest{}, ErrUnstable
	}
	manifest := rootfshead.FileManifest{
		Version: rootfshead.Version,
		Size:    uint64(max(opened.Size, 0)),
		Blocks:  uint64(max(opened.Blocks, 0)),
	}
	if manifest.Size > 0 {
		manifest.Extents, err = c.captureFileExtents(ctx, file, manifest.Size)
		if err != nil {
			return capturedFileManifest{}, err
		}
	}
	var after syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &after); err != nil {
		return capturedFileManifest{}, err
	}
	if !sameFileVersion(&opened, &after) {
		return capturedFileManifest{}, ErrUnstable
	}
	payload, err := rootfshead.EncodeFileManifest(manifest)
	if err != nil {
		return capturedFileManifest{}, err
	}
	object, err := c.writer.Put(ctx, rootfshead.FileMediaType, payload)
	if err != nil {
		return capturedFileManifest{}, err
	}
	return capturedFileManifest{
		version: fileVersionFromStat(&after),
		object:  object,
		size:    manifest.Size,
		blocks:  manifest.Blocks,
	}, nil
}

func (c *Capture) cachedFileManifest(identity inodeIdentity, version FileVersion) (capturedFileManifest, bool) {
	c.fileMu.Lock()
	defer c.fileMu.Unlock()
	cached, ok := c.fileCache[identity]
	return cached, ok && cached.version == version
}

// ForgetFile releases the manifest cache entry after the last known alias of
// an inode disappears. The session's path index remains the source of truth.
func (c *Capture) ForgetFile(version FileVersion) {
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	c.fileMu.Lock()
	if cached, ok := c.fileCache[identity]; ok && cached.version == version {
		delete(c.fileCache, identity)
	}
	c.fileMu.Unlock()
}

func fileVersionKey(version FileVersion) string {
	return fmt.Sprintf(
		"%d:%d:%d:%d:%d:%d:%d:%d:%d",
		version.Device, version.Inode, version.Mode, version.Rdev, version.Size,
		version.ModSeconds, version.ModNanos, version.CTimeSec, version.CTimeNanos,
	)
}

func openFileNoAtime(value string) (*os.File, error) {
	fd, err := unix.Open(value, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOATIME|unix.O_NOFOLLOW, 0)
	if err == nil {
		return os.NewFile(uintptr(fd), value), nil
	}
	if !errors.Is(err, syscall.EPERM) && !errors.Is(err, syscall.EINVAL) {
		return nil, err
	}
	fd, err = unix.Open(value, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), value), nil
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
		if data < 0 || uint64(data) >= size {
			return nil, false, fmt.Errorf("SEEK_DATA returned invalid offset %d for size %d", data, size)
		}
		hole, err := unix.Seek(fd, data, unix.SEEK_HOLE)
		if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if hole <= data {
			return nil, false, fmt.Errorf("SEEK_HOLE did not advance from data offset %d", data)
		}
		end := min(uint64(hole), size)
		if uint64(data) < end {
			ranges = append(ranges, fileRange{offset: uint64(data), length: end - uint64(data)})
		}
		offset = end
	}
	return ranges, true, nil
}

type overlayMetadata struct {
	opaque   bool
	whiteout bool
}

func readXAttrs(value string) ([]rootfshead.XAttr, overlayMetadata, error) {
	size, err := unix.Llistxattr(value, nil)
	if errors.Is(err, syscall.ENOTSUP) {
		return nil, overlayMetadata{}, nil
	}
	if err != nil {
		return nil, overlayMetadata{}, fmt.Errorf("list rootfs xattrs for %s: %w", value, err)
	}
	if size == 0 {
		return nil, overlayMetadata{}, nil
	}
	namesPayload := make([]byte, size)
	n, err := unix.Llistxattr(value, namesPayload)
	if err != nil {
		return nil, overlayMetadata{}, fmt.Errorf("list rootfs xattrs for %s: %w", value, err)
	}
	var attrs []rootfshead.XAttr
	var overlay overlayMetadata
	for _, name := range strings.Split(string(namesPayload[:n]), "\x00") {
		if name == "" {
			continue
		}
		valueSize, err := unix.Lgetxattr(value, name, nil)
		if err != nil {
			return nil, overlayMetadata{}, fmt.Errorf("read rootfs xattr %s for %s: %w", name, value, err)
		}
		payload := make([]byte, valueSize)
		if valueSize > 0 {
			read, err := unix.Lgetxattr(value, name, payload)
			if err != nil {
				return nil, overlayMetadata{}, fmt.Errorf("read rootfs xattr %s for %s: %w", name, value, err)
			}
			payload = payload[:read]
		}
		action, err := classifyOverlayXAttr(name, payload)
		if err != nil {
			return nil, overlayMetadata{}, fmt.Errorf("rootfs xattr %s for %s: %w", name, value, err)
		}
		switch action {
		case overlayXAttrOpaque:
			overlay.opaque = true
		case overlayXAttrWhiteout:
			overlay.whiteout = true
		case overlayXAttrDrop:
			continue
		default:
			attrs = append(attrs, rootfshead.XAttr{Name: name, Value: payload})
		}
	}
	rootfshead.SortXAttrs(attrs)
	return attrs, overlay, nil
}

type overlayXAttrAction uint8

const (
	overlayXAttrKeep overlayXAttrAction = iota
	overlayXAttrOpaque
	overlayXAttrWhiteout
	overlayXAttrDrop
)

func classifyOverlayXAttr(name string, value []byte) (overlayXAttrAction, error) {
	if name != "trusted.overlay.opaque" && name != "user.overlay.opaque" &&
		name != "trusted.overlay.whiteout" && name != "user.overlay.whiteout" &&
		!strings.HasPrefix(name, "trusted.overlay.") && !strings.HasPrefix(name, "user.overlay.") {
		return overlayXAttrKeep, nil
	}
	suffix := strings.TrimPrefix(strings.TrimPrefix(name, "trusted.overlay."), "user.overlay.")
	switch suffix {
	case "opaque":
		if string(value) == "y" || string(value) == "x" {
			return overlayXAttrOpaque, nil
		}
		return overlayXAttrKeep, nil
	case "whiteout":
		return overlayXAttrWhiteout, nil
	case "origin", "impure", "uuid":
		return overlayXAttrDrop, nil
	case "metacopy", "redirect":
		return overlayXAttrKeep, fmt.Errorf("%w: %s", ErrUnsupportedOverlayMetadata, name)
	default:
		return overlayXAttrKeep, fmt.Errorf("%w: %s", ErrUnsupportedOverlayMetadata, name)
	}
}

func versionFromInfo(info os.FileInfo) (FileVersion, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil {
		return FileVersion{}, fmt.Errorf("rootfs stat metadata is unavailable")
	}
	return fileVersionFromStat(stat), nil
}

func fileVersionFromStat(stat *syscall.Stat_t) FileVersion {
	return FileVersion{
		Device:     uint64(stat.Dev),
		Inode:      stat.Ino,
		Mode:       stat.Mode,
		Rdev:       uint64(stat.Rdev),
		Size:       stat.Size,
		ModSeconds: stat.Mtim.Sec,
		ModNanos:   stat.Mtim.Nsec,
		CTimeSec:   stat.Ctim.Sec,
		CTimeNanos: stat.Ctim.Nsec,
	}
}

func (c *Capture) inodeKey(stat *syscall.Stat_t) string {
	return c.generationID + ":" + strconv.FormatUint(uint64(stat.Dev), 16) + ":" + strconv.FormatUint(stat.Ino, 16)
}

func sameFileVersion(left, right *syscall.Stat_t) bool {
	return left != nil && right != nil && left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode &&
		left.Size == right.Size && left.Mtim == right.Mtim && left.Ctim == right.Ctim
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

func (c *Capture) hostPath(relative string) string {
	return filepath.Join(c.root, filepath.FromSlash(cleanRelativePath(relative)))
}

func cleanRelativePath(value string) string {
	value = filepath.ToSlash(filepath.Clean("/" + value))
	return strings.TrimPrefix(value, "/")
}

func cleanLogicalPath(value string) string {
	return filepath.ToSlash(filepath.Clean("/" + strings.TrimSpace(value)))
}
