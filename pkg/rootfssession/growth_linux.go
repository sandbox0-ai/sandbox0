//go:build linux

package session

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
)

// Linux XFS UAPI: _IOR('X',124,struct xfs_fsop_geom_v4) and
// _IOW('X',110,struct xfs_growfs_data). The 64-bit amd64/arm64 layouts are
// 112 and 16 bytes respectively. See fs/xfs/libxfs/xfs_fs.h.
const (
	xfsGeometryV4 = uintptr(0x8070587c)
	xfsGrowData   = uintptr(0x4010586e)
)

func (r *LinuxRuntime) InspectXFS(devicePath, xfsRoot string) (XFSUsage, error) {
	file, err := r.openGrowthXFS(devicePath, xfsRoot)
	if err != nil {
		return XFSUsage{}, err
	}
	defer file.Close()
	usage, _, err := inspectGrowthXFS(file)
	return usage, err
}

func (r *LinuxRuntime) SyncXFS(devicePath, xfsRoot string) error {
	file, err := r.openGrowthXFS(devicePath, xfsRoot)
	if err != nil {
		return err
	}
	defer file.Close()
	return unix.Syncfs(int(file.Fd()))
}

func (r *LinuxRuntime) GrowXFS(ctx context.Context, devicePath, xfsRoot string, targetBytes int64) (XFSUsage, error) {
	if err := ctx.Err(); err != nil {
		return XFSUsage{}, err
	}
	file, err := r.openGrowthXFS(devicePath, xfsRoot)
	if err != nil {
		return XFSUsage{}, err
	}
	defer file.Close()
	usage, inodePercent, err := inspectGrowthXFS(file)
	if err != nil {
		return XFSUsage{}, err
	}
	if targetBytes < usage.SizeBytes || targetBytes%usage.BlockBytes != 0 {
		return XFSUsage{}, fmt.Errorf("XFS growth target cannot shrink or be misaligned")
	}
	if targetBytes == usage.SizeBytes {
		return usage, nil
	}
	device, err := os.OpenFile(devicePath, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return XFSUsage{}, err
	}
	var deviceSize [8]byte
	sizeErr := xfsGrowthIOCTL(device, unix.BLKGETSIZE64, deviceSize[:])
	deviceBytes := binary.NativeEndian.Uint64(deviceSize[:])
	closeErr := device.Close()
	if sizeErr != nil {
		return XFSUsage{}, sizeErr
	}
	if closeErr != nil {
		return XFSUsage{}, closeErr
	}
	if uint64(targetBytes) > deviceBytes {
		return XFSUsage{}, fmt.Errorf("XFS growth target exceeds the attached device")
	}
	var request [16]byte
	binary.NativeEndian.PutUint64(request[:8], uint64(targetBytes/usage.BlockBytes))
	binary.NativeEndian.PutUint32(request[8:12], inodePercent)
	if err := xfsGrowthIOCTL(file, xfsGrowData, request[:]); err != nil {
		return XFSUsage{}, fmt.Errorf("grow mounted XFS: %w", err)
	}
	after, _, err := inspectGrowthXFS(file)
	if err == nil && after.SizeBytes < targetBytes {
		return after, fmt.Errorf("XFS did not reach its requested growth target")
	}
	return after, err
}

func (r *LinuxRuntime) openGrowthXFS(devicePath, xfsRoot string) (*os.File, error) {
	if !r.configuredDevice(devicePath) || !filepath.IsAbs(xfsRoot) || filepath.Clean(xfsRoot) != xfsRoot || xfsRoot == "/" {
		return nil, fmt.Errorf("invalid exact XFS growth mount or device")
	}
	entries, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}
	device, err := os.Stat(devicePath)
	if err != nil {
		return nil, err
	}
	found := false
	for _, entry := range entries {
		if entry.Mountpoint == xfsRoot && entry.FSType == "xfs" {
			source, statErr := os.Stat(entry.Source)
			found = statErr == nil && os.SameFile(device, source)
		}
	}
	if !found {
		return nil, fmt.Errorf("XFS growth mount does not belong to the exact NBD device")
	}
	fd, err := unix.Open(xfsRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), xfsRoot), nil
}

func inspectGrowthXFS(file *os.File) (XFSUsage, uint32, error) {
	var geometry [112]byte
	if err := xfsGrowthIOCTL(file, xfsGeometryV4, geometry[:]); err != nil {
		return XFSUsage{}, 0, err
	}
	blockBytes := uint64(binary.NativeEndian.Uint32(geometry[:4]))
	blocks := binary.NativeEndian.Uint64(geometry[32:40])
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return XFSUsage{}, 0, err
	}
	if stat.Type != unix.XFS_SUPER_MAGIC || blockBytes == 0 || blocks > math.MaxInt64/blockBytes ||
		stat.Bsize <= 0 || stat.Bavail > math.MaxInt64/uint64(stat.Bsize) {
		return XFSUsage{}, 0, fmt.Errorf("invalid mounted XFS geometry or free space")
	}
	return XFSUsage{SizeBytes: int64(blocks * blockBytes), FreeBytes: int64(stat.Bavail * uint64(stat.Bsize)), BlockBytes: int64(blockBytes)}, binary.NativeEndian.Uint32(geometry[28:32]), nil
}

func xfsGrowthIOCTL(file *os.File, request uintptr, payload []byte) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), request, uintptr(unsafe.Pointer(&payload[0])))
	runtime.KeepAlive(payload)
	if errno != 0 {
		return errno
	}
	return nil
}
