//go:build linux

package rootfsartifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

const (
	xfsScanMaxEntries   = 10_000_000
	xfsScanMaxExtents   = 1 << 20
	xfsScanMaxHardlinks = 1 << 20
	xfsScanMaxDepth     = 256
	xfsScanMaxPath      = 4096
	xfsScanBatch        = 128
	fiemapExtentLast    = 0x1
	fiemapExtentShared  = 0x2000
	fsIOCFiemap         = 0xc020660b
)

// CollectReadOnlyXFSDataRanges discovers complete file-relative data units using
// FIEMAP. The caller must exclusively own this 4KiB-block XFS image, keep it
// read-only during discovery and cleanly unmount it before reading raw image
// bytes. This function reads metadata only; it never synthesizes sparse or
// unwritten data and does not follow symlinks or cross filesystem boundaries.
// Limits are explicit errors, never a successful partial plan.
func CollectReadOnlyXFSDataRanges(ctx context.Context, rootPath string, logicalSize int64, rangeBytes int) (XFSDataRangePlan, error) {
	if _, err := rootfsblock.NewDataRangeLayout(logicalSize, rangeBytes, nil); err != nil {
		return XFSDataRangePlan{}, err
	}
	if err := validateCanonicalXFSPath("scan root", rootPath); err != nil {
		return XFSDataRangePlan{}, err
	}
	if resolved, err := filepath.EvalSymlinks(rootPath); err != nil || resolved != rootPath {
		return XFSDataRangePlan{}, fmt.Errorf("scan root must not traverse symlinks")
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return XFSDataRangePlan{}, err
	}
	defer root.Close()
	directory, err := root.OpenFile(".", os.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return XFSDataRangePlan{}, err
	}
	defer directory.Close()
	var fs unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &fs); err != nil {
		return XFSDataRangePlan{}, err
	}
	if fs.Type != unix.XFS_SUPER_MAGIC || fs.Bsize != rootfsblock.LogicalBlockSize || fs.Flags&unix.ST_RDONLY == 0 {
		return XFSDataRangePlan{}, fmt.Errorf("data range discovery requires read-only 4KiB-block XFS")
	}
	return collectXFSDataRanges(ctx, root, directory, logicalSize, rangeBytes, queryFiemap)
}

type fiemapExtent struct {
	Logical, Physical, Length uint64
	Reserved64                [2]uint64
	Flags                     uint32
	Reserved                  [3]uint32
}

type fiemapHeader struct {
	Start, Length                  uint64
	Flags, Mapped, Count, Reserved uint32
}

type fiemapQuery func(context.Context, *os.File, uint64, uint64) ([]fiemapExtent, error)

// Use the native Linux UAPI layout, including reserved fields. The ioctl's size
// is the 32-byte header, not the size of the following fixed-capacity array.
func queryFiemap(ctx context.Context, file *os.File, start, length uint64) ([]fiemapExtent, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		request := struct {
			fiemapHeader
			Extents [xfsScanBatch]fiemapExtent
		}{fiemapHeader: fiemapHeader{Start: start, Length: length, Count: xfsScanBatch}}
		_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), fsIOCFiemap, uintptr(unsafe.Pointer(&request)))
		runtime.KeepAlive(file)
		if errno == unix.EINTR {
			continue
		}
		if errno != 0 {
			return nil, errno
		}
		if request.Mapped > xfsScanBatch {
			return nil, fmt.Errorf("FIEMAP returned more extents than requested")
		}
		return request.Extents[:request.Mapped], nil
	}
}

type xfsRangeCollector struct {
	logicalSize uint64
	unit        uint64
	spans       []rootfsblock.DataRangeSpan
	stats       XFSDataRangeStats
	maxExtents  int64
}

func collectXFSDataRanges(ctx context.Context, root *os.Root, directory *os.File, logicalSize int64, rangeBytes int, query fiemapQuery) (XFSDataRangePlan, error) {
	info, err := directory.Stat()
	if err != nil {
		return XFSDataRangePlan{}, err
	}
	device := info.Sys().(*syscall.Stat_t).Dev
	c := xfsRangeCollector{logicalSize: uint64(logicalSize), unit: uint64(rangeBytes), maxExtents: xfsScanMaxExtents}
	hardlinks := make(map[uint64]struct{})
	ancestors := make(map[uint64]struct{})
	var walk func(*os.File, string, int) error
	walk = func(dir *os.File, path string, depth int) error {
		if depth > xfsScanMaxDepth {
			return fmt.Errorf("%w: XFS scan directory depth limit exceeded", ErrXFSDataRangeLimit)
		}
		dirInfo, err := dir.Stat()
		if err != nil {
			return err
		}
		inode := dirInfo.Sys().(*syscall.Stat_t).Ino
		if _, exists := ancestors[inode]; exists {
			return fmt.Errorf("XFS scan encountered directory cycle")
		}
		ancestors[inode] = struct{}{}
		defer delete(ancestors, inode)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries, readErr := dir.ReadDir(xfsScanBatch)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			for _, entry := range entries {
				if err := ctx.Err(); err != nil {
					return err
				}
				c.stats.Entries++
				if c.stats.Entries > xfsScanMaxEntries {
					return fmt.Errorf("%w: XFS scan entry limit exceeded", ErrXFSDataRangeLimit)
				}
				name := path + "/" + entry.Name()
				if len(name) > xfsScanMaxPath {
					return fmt.Errorf("%w: XFS scan path limit exceeded", ErrXFSDataRangeLimit)
				}
				before, err := root.Lstat(name)
				if err != nil {
					return err
				}
				stat := before.Sys().(*syscall.Stat_t)
				if stat.Dev != device {
					return fmt.Errorf("XFS scan crossed filesystem boundary (device %d, expected %d)", stat.Dev, device)
				}
				if !before.IsDir() && !before.Mode().IsRegular() {
					continue
				}
				if before.Mode().IsRegular() {
					c.stats.RegularPaths++
					if before.Size() < int64(rangeBytes) {
						continue
					}
					if _, seen := hardlinks[stat.Ino]; seen {
						c.stats.HardlinksSkipped++
						continue
					}
				}
				flags := os.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK
				if before.IsDir() {
					flags |= unix.O_DIRECTORY
				}
				file, err := root.OpenFile(name, flags, 0)
				if err != nil {
					return err
				}
				// Close each leaf promptly; the maximum simultaneous descriptors is
				// bounded by directory depth, not the total number of image files.
				err = func() (resultErr error) {
					defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
					after, err := file.Stat()
					if err != nil {
						return err
					}
					if !sameScanFile(before, after) {
						return fmt.Errorf("XFS scan file identity changed")
					}
					if before.IsDir() {
						return walk(file, name, depth+1)
					}
					c.stats.FilesScanned++
					if stat.Nlink > 1 {
						if len(hardlinks) >= xfsScanMaxHardlinks {
							return fmt.Errorf("%w: XFS scan hardlink limit exceeded", ErrXFSDataRangeLimit)
						}
						hardlinks[stat.Ino] = struct{}{}
					}
					if err := c.scanFile(ctx, file, uint64(before.Size()), query); err != nil {
						return err
					}
					after, err = file.Stat()
					if err != nil {
						return err
					}
					if !sameScanFile(before, after) {
						return fmt.Errorf("XFS scan file changed during FIEMAP")
					}
					return nil
				}()
				if err != nil {
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
		}
	}
	if err := walk(directory, ".", 0); err != nil {
		return XFSDataRangePlan{}, err
	}
	if err := ctx.Err(); err != nil {
		return XFSDataRangePlan{}, err
	}
	layout, err := rootfsblock.NewDataRangeLayout(logicalSize, rangeBytes, c.spans)
	if err != nil {
		return XFSDataRangePlan{}, fmt.Errorf("canonicalize XFS data ranges: %w", err)
	}
	return XFSDataRangePlan{Layout: layout, Stats: c.stats}, nil
}

func sameScanFile(a, b os.FileInfo) bool {
	x, y := a.Sys().(*syscall.Stat_t), b.Sys().(*syscall.Stat_t)
	return x.Dev == y.Dev && x.Ino == y.Ino && x.Mode == y.Mode && x.Size == y.Size &&
		x.Nlink == y.Nlink && x.Mtim == y.Mtim && x.Ctim == y.Ctim
}

func (c *xfsRangeCollector) scanFile(ctx context.Context, file *os.File, size uint64, query fiemapQuery) error {
	for cursor := uint64(0); cursor < size; {
		if err := ctx.Err(); err != nil {
			return err
		}
		extents, err := query(ctx, file, cursor, size-cursor)
		if err != nil {
			return fmt.Errorf("query XFS FIEMAP: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(extents) == 0 {
			return nil
		}
		if len(extents) > xfsScanBatch {
			return fmt.Errorf("FIEMAP returned more extents than requested")
		}
		if int64(len(extents)) > c.maxExtents-c.stats.Extents {
			return fmt.Errorf("%w: XFS scan extent limit exceeded", ErrXFSDataRangeLimit)
		}
		for i, extent := range extents {
			c.stats.Extents++
			if extent.Length == 0 || extent.Logical > math.MaxUint64-extent.Length ||
				extent.Logical+extent.Length <= cursor || extent.Logical >= size ||
				(i > 0 && extent.Logical < cursor) ||
				(extent.Flags&fiemapExtentLast != 0 && i != len(extents)-1) {
				return fmt.Errorf("invalid or non-progressing XFS FIEMAP extent")
			}
			if err := c.addExtent(extent, max(cursor, extent.Logical), size); err != nil {
				return err
			}
			cursor = extent.Logical + extent.Length
		}
		if extents[len(extents)-1].Flags&fiemapExtentLast != 0 {
			return nil
		}
	}
	return ctx.Err()
}

func (c *xfsRangeCollector) addExtent(extent fiemapExtent, start, size uint64) error {
	// Anything other than LAST is ineligible, including future flags. Shared
	// extents cannot safely have two competing file-relative phases. Skipping a
	// preference leaves the original bytes on the canonical global-grid path.
	if extent.Flags & ^uint32(fiemapExtentLast) != 0 {
		c.stats.FlaggedExtents++
		if extent.Flags&fiemapExtentShared != 0 {
			c.stats.SharedExtents++
		}
		return nil
	}
	if extent.Logical%rootfsblock.LogicalBlockSize != 0 || extent.Physical%rootfsblock.LogicalBlockSize != 0 ||
		extent.Length%rootfsblock.LogicalBlockSize != 0 || extent.Physical > c.logicalSize || extent.Length > c.logicalSize-extent.Physical {
		return fmt.Errorf("invalid XFS physical extent geometry")
	}
	end := min(size, extent.Logical+extent.Length)
	delta := (c.unit - start%c.unit) % c.unit
	if delta > end-start || end-start-delta < c.unit {
		return nil
	}
	length := (end - start - delta) / c.unit * c.unit
	physical := extent.Physical + start - extent.Logical + delta
	if len(c.spans) >= rootfsblock.MaxDataRangeSpans {
		return fmt.Errorf("%w: XFS scan preferred span limit exceeded", ErrXFSDataRangeLimit)
	}
	c.spans = append(c.spans, rootfsblock.DataRangeSpan{Start: int64(physical), End: int64(physical + length)})
	c.stats.PreferredSpans++
	c.stats.PreferredBytes += int64(length)
	return nil
}
