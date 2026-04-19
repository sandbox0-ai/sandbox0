package volume

import (
	"context"
	"fmt"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

const (
	BackendS0FS = "s0fs"
)

func DefaultBackendType() string {
	return BackendS0FS
}

// Backend is the storage engine boundary behind storage-proxy volumes.
// Implementations own filesystem metadata, file data, durability, and backend
// cleanup for mounted volumes.
type Backend interface {
	MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error)
	UnmountVolume(ctx context.Context, volCtx *VolumeContext) error
}

// BackendMountRequest is the storage-engine input for mounting a volume.
type BackendMountRequest struct {
	S3Prefix   string
	VolumeID   string
	TeamID     string
	AccessMode AccessMode
	MountedAt  time.Time
	Metrics    *obsmetrics.StorageProxyMetrics
}

// FlushAll flushes dirty data for the mounted volume.
func (v *VolumeContext) FlushAll(path string) error {
	if v == nil {
		return fmt.Errorf("volume context is nil")
	}
	if v.S0FS != nil {
		return v.S0FS.Fsync(uint64(v.RootInode))
	}
	if v.VFS == nil {
		return fmt.Errorf("volume %s backend %q does not expose flush", v.VolumeID, v.Backend)
	}
	return v.VFS.FlushAll(path)
}

func (v *VolumeContext) IsS0FS() bool {
	return v != nil && v.Backend == BackendS0FS && v.S0FS != nil
}

func (v *VolumeContext) OpenFileHandle(inode uint64) uint64 {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	v.nextHandleID++
	if v.fileHandles == nil {
		v.fileHandles = make(map[uint64]uint64)
	}
	if v.openFileCount == nil {
		v.openFileCount = make(map[uint64]int)
	}
	v.fileHandles[v.nextHandleID] = inode
	v.openFileCount[inode]++
	return v.nextHandleID
}

func (v *VolumeContext) OpenDirHandle(inode uint64) uint64 {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	v.nextHandleID++
	if v.dirHandleIDs == nil {
		v.dirHandleIDs = make(map[uint64]uint64)
	}
	v.dirHandleIDs[v.nextHandleID] = inode
	return v.nextHandleID
}

func (v *VolumeContext) ReleaseHandle(handleID uint64) (uint64, int, bool) {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	inode, ok := v.fileHandles[handleID]
	if ok {
		if v.openFileCount[inode] > 1 {
			v.openFileCount[inode]--
		} else {
			delete(v.openFileCount, inode)
		}
	}
	delete(v.fileHandles, handleID)
	delete(v.dirHandleIDs, handleID)
	return inode, v.openFileCount[inode], ok
}

func (v *VolumeContext) HandleInode(handleID uint64) (uint64, bool) {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	inode, ok := v.fileHandles[handleID]
	return inode, ok
}
