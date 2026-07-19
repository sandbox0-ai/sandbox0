package volume

import (
	"context"
	"fmt"
	"time"

	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

const (
	BackendS0FS = "s0fs"
	BackendS3   = "s3"
)

func DefaultBackendType() string {
	return BackendS0FS
}

// Backend is the storage engine boundary behind manager storage runtime volumes.
// Implementations own filesystem metadata, file data, durability, and backend
// cleanup for mounted volumes.
type Backend interface {
	MountVolume(ctx context.Context, req BackendMountRequest) (*VolumeContext, error)
	UnmountVolume(ctx context.Context, volCtx *VolumeContext) error
}

type StorageObserver interface {
	ObserveVolumeState(ctx context.Context, volumeID, teamID string, state *s0fs.SnapshotState, observedAt time.Time) error
}

type HandleState struct {
	NextHandleID uint64            `json:"next_handle_id"`
	FileHandles  map[uint64]uint64 `json:"file_handles,omitempty"`
	// DirHandles is accepted when loading legacy recovery snapshots. Directory
	// operations use the request inode and do not need handle recovery.
	DirHandles    map[uint64]uint64 `json:"dir_handles,omitempty"`
	UnlinkedFiles []uint64          `json:"unlinked_files,omitempty"`
}

// BackendMountRequest is the storage-engine input for mounting a volume.
type BackendMountRequest struct {
	S3Prefix        string
	VolumeID        string
	TeamID          string
	AccessMode      AccessMode
	MountedAt       time.Time
	Metrics         *obsmetrics.StorageProxyMetrics
	StorageObserver StorageObserver
}

// FlushAll flushes dirty data for the mounted volume.
func (v *VolumeContext) FlushAll(path string) error {
	if v == nil {
		return fmt.Errorf("volume context is nil")
	}
	if v.S0FS != nil {
		return v.S0FS.Fsync(uint64(v.RootInode))
	}
	return fmt.Errorf("volume %s backend %q does not expose flush", v.VolumeID, v.Backend)
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

func (v *VolumeContext) OpenDirHandle(uint64) uint64 {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	v.nextHandleID++
	return v.nextHandleID
}

func (v *VolumeContext) MarkUnlinkedFileIfOpen(inode uint64) bool {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	if v.openFileCount[inode] == 0 {
		return false
	}
	if v.unlinkedFiles == nil {
		v.unlinkedFiles = make(map[uint64]struct{})
	}
	v.unlinkedFiles[inode] = struct{}{}
	return true
}

func (v *VolumeContext) FileOpenCount(inode uint64) int {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	return v.openFileCount[inode]
}

func (v *VolumeContext) ReleaseFileHandle(handleID uint64) (uint64, int, bool, bool) {
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
	remaining := v.openFileCount[inode]
	_, unlinked := v.unlinkedFiles[inode]
	if unlinked && remaining == 0 {
		delete(v.unlinkedFiles, inode)
	}
	return inode, remaining, unlinked, ok
}

func (v *VolumeContext) HandleInode(handleID uint64) (uint64, bool) {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	inode, ok := v.fileHandles[handleID]
	return inode, ok
}

func (v *VolumeContext) SnapshotHandleState() HandleState {
	if v == nil {
		return HandleState{}
	}
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	state := HandleState{
		NextHandleID: v.nextHandleID,
		FileHandles:  make(map[uint64]uint64, len(v.fileHandles)),
	}
	for handle, inode := range v.fileHandles {
		state.FileHandles[handle] = inode
	}
	for inode := range v.unlinkedFiles {
		state.UnlinkedFiles = append(state.UnlinkedFiles, inode)
	}
	return state
}

func (v *VolumeContext) RestoreHandleState(state HandleState) {
	if v == nil {
		return
	}
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	v.nextHandleID = state.NextHandleID
	v.fileHandles = make(map[uint64]uint64, len(state.FileHandles))
	v.openFileCount = make(map[uint64]int)
	v.unlinkedFiles = make(map[uint64]struct{}, len(state.UnlinkedFiles))
	for handle, inode := range state.FileHandles {
		v.fileHandles[handle] = inode
		v.openFileCount[inode]++
		if handle > v.nextHandleID {
			v.nextHandleID = handle
		}
	}
	for handle := range state.DirHandles {
		if handle > v.nextHandleID {
			v.nextHandleID = handle
		}
	}
	for _, inode := range state.UnlinkedFiles {
		v.unlinkedFiles[inode] = struct{}{}
	}
}
