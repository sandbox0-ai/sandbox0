package portal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type localVolumeManager struct {
	mu       sync.RWMutex
	volumes  map[string]*volume.VolumeContext
	requests map[string]*localVolumeRequestState
}

type localVolumeRequestState struct {
	inFlight     int
	transferring bool
	lastAccess   time.Time
	done         chan struct{}
	cond         *sync.Cond
}

func newLocalVolumeManager() *localVolumeManager {
	return &localVolumeManager{
		volumes:  make(map[string]*volume.VolumeContext),
		requests: make(map[string]*localVolumeRequestState),
	}
}

func (m *localVolumeManager) add(volCtx *volume.VolumeContext) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.volumes[volCtx.VolumeID] = volCtx
	if _, ok := m.requests[volCtx.VolumeID]; !ok {
		state := &localVolumeRequestState{}
		state.cond = sync.NewCond(&m.mu)
		state.lastAccess = time.Now().UTC()
		m.requests[volCtx.VolumeID] = state
	}
}

func (m *localVolumeManager) remove(volumeID string) (*volume.VolumeContext, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	volCtx, ok := m.volumes[volumeID]
	delete(m.volumes, volumeID)
	if state := m.requests[volumeID]; state != nil {
		if state.transferring && state.done != nil {
			close(state.done)
		}
		delete(m.requests, volumeID)
	}
	return volCtx, ok
}

func (m *localVolumeManager) acquire(ctx context.Context, volumeID string) (func(), error) {
	for {
		m.mu.Lock()
		volCtx := m.volumes[volumeID]
		state := m.requests[volumeID]
		if volCtx == nil || state == nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("volume %s not mounted", volumeID)
		}
		if state.transferring && state.done != nil {
			done := state.done
			m.mu.Unlock()
			select {
			case <-done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		state.inFlight++
		state.lastAccess = time.Now().UTC()
		m.mu.Unlock()
		return func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			state := m.requests[volumeID]
			if state == nil {
				return
			}
			if state.inFlight > 0 {
				state.inFlight--
			}
			state.lastAccess = time.Now().UTC()
			if state.inFlight == 0 && state.cond != nil {
				state.cond.Broadcast()
			}
		}, nil
	}
}

func (m *localVolumeManager) prepareHandoff(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.requests[volumeID]
	if m.volumes[volumeID] == nil || state == nil {
		return fmt.Errorf("volume %s not mounted", volumeID)
	}
	if state.transferring {
		return fmt.Errorf("volume %s handoff already in progress", volumeID)
	}
	state.transferring = true
	state.done = make(chan struct{})
	for state.inFlight > 0 {
		if err := ctx.Err(); err != nil {
			state.transferring = false
			close(state.done)
			state.done = nil
			return err
		}
		state.cond.Wait()
	}
	return nil
}

func (m *localVolumeManager) abortHandoff(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.requests[volumeID]
	if state == nil || !state.transferring {
		return
	}
	state.transferring = false
	state.lastAccess = time.Now().UTC()
	if state.done != nil {
		close(state.done)
		state.done = nil
	}
}

func (m *localVolumeManager) touch(volumeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.requests[volumeID]
	if state == nil {
		return
	}
	state.lastAccess = time.Now().UTC()
}

func (m *localVolumeManager) canCleanupOwnerOnly(volumeID string, cutoff time.Time) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state := m.requests[volumeID]
	if m.volumes[volumeID] == nil || state == nil {
		return false
	}
	if state.transferring || state.inFlight > 0 {
		return false
	}
	if state.lastAccess.IsZero() {
		return false
	}
	return !state.lastAccess.After(cutoff)
}

func (m *localVolumeManager) MountVolume(_ context.Context, _ string, volumeID string, _ string, _ volume.AccessMode) (string, time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	volCtx := m.volumes[volumeID]
	if volCtx == nil {
		return "", time.Time{}, fmt.Errorf("volume %s not mounted", volumeID)
	}
	return "local-" + volumeID, time.Now().UTC(), nil
}

func (m *localVolumeManager) UnmountVolume(_ context.Context, volumeID, _ string) error {
	volCtx, ok := m.remove(volumeID)
	if !ok || volCtx == nil || volCtx.S0FS == nil {
		return nil
	}
	if _, err := volCtx.S0FS.SyncMaterialize(context.Background()); err != nil {
		return err
	}
	if err := volCtx.S0FS.Close(); err != nil {
		return err
	}
	if volCtx.CacheDir != "" {
		if err := os.RemoveAll(volCtx.CacheDir); err != nil {
			return err
		}
	}
	return nil
}

func (m *localVolumeManager) AckInvalidate(string, string, string, bool, string) error {
	return nil
}

func (m *localVolumeManager) AcquireDirectVolumeFileMount(ctx context.Context, volumeID string, mountFn func(context.Context) (string, error)) (func(), error) {
	release, err := m.acquire(ctx, volumeID)
	if err != nil {
		return nil, err
	}
	if mountFn != nil {
		if _, err := mountFn(ctx); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

func (m *localVolumeManager) CleanupIdleDirectVolumeFileMount(context.Context, string) (bool, error) {
	return false, nil
}

func (m *localVolumeManager) SyncDirectVolumeFileMount(_ context.Context, volumeID string) error {
	m.mu.RLock()
	volCtx := m.volumes[volumeID]
	m.mu.RUnlock()
	if volCtx == nil || volCtx.S0FS == nil {
		return fmt.Errorf("volume %s not mounted", volumeID)
	}
	_, err := volCtx.S0FS.SyncMaterialize(context.Background())
	return err
}

func (m *localVolumeManager) GetVolume(volumeID string) (*volume.VolumeContext, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	volCtx := m.volumes[volumeID]
	if volCtx == nil {
		return nil, fmt.Errorf("volume %s not mounted", volumeID)
	}
	return volCtx, nil
}

type localSession struct {
	volumeID         string
	mgr              *localVolumeManager
	fs               *fsserver.FileSystemServer
	baseCtx          context.Context
	readOnlyHandleMu sync.Mutex
	readOnlyHandles  map[string]struct{}
	readCacheMu      sync.RWMutex
	readCache        map[string][]byte
	readCacheBytes   int
}

func newLocalSession(volumeID string, mgr *localVolumeManager, logger *logrus.Logger) *localSession {
	if logger == nil {
		logger = logrus.New()
	}
	return &localSession{
		volumeID:        volumeID,
		mgr:             mgr,
		fs:              fsserver.NewFileSystemServer(mgr, nil, nil, nil, logger, nil, nil),
		readOnlyHandles: make(map[string]struct{}),
		readCache:       make(map[string][]byte),
		baseCtx: internalauth.WithClaims(context.Background(), &internalauth.Claims{
			Caller:   internalauth.ServiceCtld,
			Target:   internalauth.ServiceCtld,
			IsSystem: true,
		}),
	}
}

func (s *localSession) Close() {}

func (s *localSession) ctx(ctx context.Context) context.Context {
	if s != nil && s.baseCtx != nil {
		return s.baseCtx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return internalauth.WithClaims(ctx, &internalauth.Claims{
		Caller:   internalauth.ServiceCtld,
		Target:   internalauth.ServiceCtld,
		IsSystem: true,
	})
}

func (s *localSession) fix(volumeID *string) {
	if volumeID != nil {
		*volumeID = s.volumeID
	}
}

func (s *localSession) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Lookup(s.ctx(ctx), req)
}
func (s *localSession) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.GetAttr(s.ctx(ctx), req)
}
func (s *localSession) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	s.fix(&req.VolumeId)
	resp, err := s.fs.SetAttr(s.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	if req.Valid&uint32(fsmeta.SetAttrSize) != 0 {
		volCtx, volErr := s.localS0FSVolume(req.VolumeId)
		if volErr == nil && volCtx != nil {
			size := uint64(0)
			if req.Attr != nil {
				size = req.Attr.Size
			}
			s.resizeReadCache(volCtx, req.Inode, size)
		}
	}
	return resp, nil
}
func (s *localSession) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Mkdir(s.ctx(ctx), req)
}
func (s *localSession) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Create(s.ctx(ctx), req)
}
func (s *localSession) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Unlink(s.ctx(ctx), req)
}
func (s *localSession) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Rmdir(s.ctx(ctx), req)
}
func (s *localSession) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Rename(s.ctx(ctx), req)
}
func (s *localSession) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Link(s.ctx(ctx), req)
}
func (s *localSession) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Symlink(s.ctx(ctx), req)
}
func (s *localSession) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Readlink(s.ctx(ctx), req)
}
func (s *localSession) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Access(s.ctx(ctx), req)
}
func (s *localSession) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.fix(&req.VolumeId)
	resp, err := s.fs.Open(s.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	if req.Flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		if volCtx, err := s.localS0FSVolume(req.VolumeId); err == nil && volCtx != nil {
			s.trackReadOnlyHandle(req.VolumeId, resp.HandleId)
		}
	}
	return resp, nil
}
func (s *localSession) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Read(s.ctx(ctx), req)
}
func (s *localSession) ReadInto(ctx context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	if ctx != nil && ctx.Err() != nil {
		return 0, false, ctx.Err()
	}
	if req.Offset < 0 || req.Size < 0 {
		return 0, false, fserror.New(fserror.InvalidArgument, "negative read offset or size")
	}
	if int64(len(dest)) > req.Size {
		dest = dest[:req.Size]
	}
	if len(dest) == 0 {
		return 0, true, nil
	}
	s.fix(&req.VolumeId)
	if !s.hasReadOnlyHandle(req.VolumeId, req.HandleId) {
		return s.readIntoViaFSServer(ctx, req, dest)
	}
	volCtx, err := s.localS0FSVolume(req.VolumeId)
	if err != nil {
		return 0, false, err
	}
	if volCtx == nil {
		return 0, false, fserror.New(fserror.Internal, "local ReadInto requires s0fs volume")
	}
	if n, eof, ok := s.readCacheLookup(volCtx, req.Inode, uint64(req.Offset), dest); ok {
		return n, eof, nil
	}
	n, err := volCtx.S0FS.ReadInto(req.Inode, uint64(req.Offset), dest)
	if err != nil {
		return 0, false, mapLocalS0FSError(err)
	}
	if n <= len(dest) && n <= maxLocalReadCacheFileSize && n < len(dest) && req.Offset == 0 {
		s.storeCompleteReadCache(volCtx, req.Inode, dest[:n])
	}
	return n, n < len(dest), nil
}
func (s *localSession) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.fix(&req.VolumeId)
	resp, err := s.fs.Write(s.ctx(ctx), req)
	if err != nil {
		return nil, err
	}
	if resp != nil && resp.BytesWritten > 0 {
		volCtx, volErr := s.localS0FSVolume(req.VolumeId)
		if volErr == nil && volCtx != nil {
			s.updateReadCacheAfterWrite(volCtx, req, int(resp.BytesWritten))
		}
	}
	return resp, nil
}
func (s *localSession) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	if s.takeReadOnlyHandle(req.VolumeId, req.HandleId) {
		if volCtx, err := s.localS0FSVolume(req.VolumeId); err == nil && volCtx != nil {
			if inode, remaining, unlinked, ok := volCtx.ReleaseFileHandle(req.HandleId); ok && remaining == 0 && unlinked {
				s.evictReadCache(volCtx, inode)
				_ = volCtx.S0FS.Forget(inode)
			}
			return &pb.Empty{}, nil
		}
	}
	return s.fs.Release(s.ctx(ctx), req)
}
func (s *localSession) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Flush(s.ctx(ctx), req)
}
func (s *localSession) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Fsync(s.ctx(ctx), req)
}
func (s *localSession) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	resp, err := s.fs.Fallocate(s.ctx(ctx), req)
	if err == nil {
		if volCtx, volErr := s.localS0FSVolume(req.VolumeId); volErr == nil && volCtx != nil {
			s.evictReadCache(volCtx, req.Inode)
		}
	}
	return resp, err
}
func (s *localSession) CopyFileRange(ctx context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	s.fix(&req.VolumeId)
	resp, err := s.fs.CopyFileRange(s.ctx(ctx), req)
	if err == nil {
		if volCtx, volErr := s.localS0FSVolume(req.VolumeId); volErr == nil && volCtx != nil {
			s.evictReadCache(volCtx, req.InodeOut)
		}
	}
	return resp, err
}
func (s *localSession) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.OpenDir(s.ctx(ctx), req)
}
func (s *localSession) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.ReadDir(s.ctx(ctx), req)
}
func (s *localSession) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.ReleaseDir(s.ctx(ctx), req)
}
func (s *localSession) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.StatFs(s.ctx(ctx), req)
}
func (s *localSession) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.GetXattr(s.ctx(ctx), req)
}
func (s *localSession) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.SetXattr(s.ctx(ctx), req)
}
func (s *localSession) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.ListXattr(s.ctx(ctx), req)
}
func (s *localSession) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.RemoveXattr(s.ctx(ctx), req)
}
func (s *localSession) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.Mknod(s.ctx(ctx), req)
}
func (s *localSession) GetLk(ctx context.Context, req *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	s.fix(&req.VolumeId)
	return s.fs.GetLk(s.ctx(ctx), req)
}
func (s *localSession) SetLk(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.SetLk(s.ctx(ctx), req)
}
func (s *localSession) SetLkw(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.SetLkw(s.ctx(ctx), req)
}
func (s *localSession) Flock(ctx context.Context, req *pb.FlockRequest) (*pb.Empty, error) {
	s.fix(&req.VolumeId)
	return s.fs.Flock(s.ctx(ctx), req)
}

var _ volumefuse.Session = (*localSession)(nil)
var _ volumefuse.ReadIntoSession = (*localSession)(nil)

func (s *localSession) trackReadOnlyHandle(volumeID string, handleID uint64) {
	if s == nil {
		return
	}
	s.readOnlyHandleMu.Lock()
	defer s.readOnlyHandleMu.Unlock()
	if s.readOnlyHandles == nil {
		s.readOnlyHandles = make(map[string]struct{})
	}
	s.readOnlyHandles[localHandleKey(volumeID, handleID)] = struct{}{}
}

func (s *localSession) takeReadOnlyHandle(volumeID string, handleID uint64) bool {
	if s == nil {
		return false
	}
	s.readOnlyHandleMu.Lock()
	defer s.readOnlyHandleMu.Unlock()
	key := localHandleKey(volumeID, handleID)
	if _, ok := s.readOnlyHandles[key]; !ok {
		return false
	}
	delete(s.readOnlyHandles, key)
	return true
}

func localHandleKey(volumeID string, handleID uint64) string {
	return volumeID + "|" + strconv.FormatUint(handleID, 10)
}

func localReadCacheKey(volumeID string, inode uint64) string {
	return volumeID + "|" + strconv.FormatUint(inode, 10)
}

const (
	maxLocalReadCacheFileSize = 1 << 20
	maxLocalReadCacheBytes    = 64 << 20
)

func (s *localSession) hasReadOnlyHandle(volumeID string, handleID uint64) bool {
	if s == nil {
		return false
	}
	s.readOnlyHandleMu.Lock()
	defer s.readOnlyHandleMu.Unlock()
	_, ok := s.readOnlyHandles[localHandleKey(volumeID, handleID)]
	return ok
}

func (s *localSession) readIntoViaFSServer(ctx context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	resp, err := s.fs.Read(s.ctx(ctx), req)
	if err != nil {
		return 0, false, err
	}
	n := copy(dest, resp.Data)
	return n, resp.Eof || n < len(dest), nil
}

func (s *localSession) localS0FSVolume(volumeID string) (*volume.VolumeContext, error) {
	if s == nil || s.mgr == nil {
		return nil, fserror.New(fserror.FailedPrecondition, "local session is not bound")
	}
	volCtx, err := s.mgr.GetVolume(volumeID)
	if err != nil {
		return nil, err
	}
	if volCtx.IsS0FS() {
		return volCtx, nil
	}
	return nil, nil
}

func (s *localSession) localReadCacheEnabled(volCtx *volume.VolumeContext) bool {
	if s == nil || volCtx == nil || !volCtx.IsS0FS() {
		return false
	}
	return volume.NormalizeAccessMode(string(volCtx.Access)) != volume.AccessModeRWX
}

func (s *localSession) readCacheLookup(volCtx *volume.VolumeContext, inode uint64, offset uint64, dest []byte) (int, bool, bool) {
	if !s.localReadCacheEnabled(volCtx) {
		return 0, false, false
	}
	s.readCacheMu.RLock()
	defer s.readCacheMu.RUnlock()
	data, ok := s.readCache[localReadCacheKey(volCtx.VolumeID, inode)]
	if !ok {
		return 0, false, false
	}
	if offset >= uint64(len(data)) {
		return 0, true, true
	}
	n := copy(dest, data[offset:])
	return n, offset+uint64(n) >= uint64(len(data)), true
}

func (s *localSession) storeCompleteReadCache(volCtx *volume.VolumeContext, inode uint64, data []byte) {
	if !s.localReadCacheEnabled(volCtx) || len(data) > maxLocalReadCacheFileSize {
		return
	}
	s.readCacheMu.Lock()
	defer s.readCacheMu.Unlock()
	s.putReadCacheLocked(localReadCacheKey(volCtx.VolumeID, inode), slices.Clone(data))
}

func (s *localSession) updateReadCacheAfterWrite(volCtx *volume.VolumeContext, req *pb.WriteRequest, written int) {
	if !s.localReadCacheEnabled(volCtx) || req == nil || req.Offset < 0 || written < 0 {
		return
	}
	if written > len(req.Data) {
		written = len(req.Data)
	}
	if written == 0 {
		return
	}
	offset := uint64(req.Offset)
	end := offset + uint64(written)
	if end > maxLocalReadCacheFileSize {
		s.evictReadCache(volCtx, req.Inode)
		return
	}

	key := localReadCacheKey(volCtx.VolumeID, req.Inode)
	s.readCacheMu.Lock()
	data, ok := s.readCache[key]
	if ok {
		if int(end) > len(data) {
			data = append(data, make([]byte, int(end)-len(data))...)
		}
		copy(data[offset:end], req.Data[:written])
		s.putReadCacheLocked(key, data)
		s.readCacheMu.Unlock()
		return
	}
	s.readCacheMu.Unlock()

	if offset != 0 {
		return
	}
	node, err := volCtx.S0FS.GetAttr(req.Inode)
	if err != nil || node.Size != end {
		return
	}
	s.storeCompleteReadCache(volCtx, req.Inode, req.Data[:written])
}

func (s *localSession) resizeReadCache(volCtx *volume.VolumeContext, inode uint64, size uint64) {
	if !s.localReadCacheEnabled(volCtx) {
		s.evictReadCache(volCtx, inode)
		return
	}
	if size > maxLocalReadCacheFileSize {
		s.evictReadCache(volCtx, inode)
		return
	}
	key := localReadCacheKey(volCtx.VolumeID, inode)
	s.readCacheMu.Lock()
	defer s.readCacheMu.Unlock()
	data, ok := s.readCache[key]
	if !ok {
		if size == 0 {
			s.putReadCacheLocked(key, nil)
		}
		return
	}
	switch {
	case size < uint64(len(data)):
		s.putReadCacheLocked(key, data[:size])
	case size > uint64(len(data)):
		s.putReadCacheLocked(key, append(data, make([]byte, int(size)-len(data))...))
	}
}

func (s *localSession) evictReadCache(volCtx *volume.VolumeContext, inode uint64) {
	if s == nil || volCtx == nil {
		return
	}
	s.readCacheMu.Lock()
	defer s.readCacheMu.Unlock()
	key := localReadCacheKey(volCtx.VolumeID, inode)
	s.readCacheBytes -= len(s.readCache[key])
	if s.readCacheBytes < 0 {
		s.readCacheBytes = 0
	}
	delete(s.readCache, key)
}

func (s *localSession) putReadCacheLocked(key string, data []byte) {
	if s.readCache == nil {
		s.readCache = make(map[string][]byte)
	}
	oldLen := len(s.readCache[key])
	if len(data) > maxLocalReadCacheFileSize {
		delete(s.readCache, key)
		s.readCacheBytes -= oldLen
		if s.readCacheBytes < 0 {
			s.readCacheBytes = 0
		}
		return
	}
	delta := len(data) - oldLen
	if s.readCacheBytes+delta > maxLocalReadCacheBytes {
		clear(s.readCache)
		s.readCacheBytes = 0
		oldLen = 0
	}
	s.readCache[key] = data
	s.readCacheBytes += len(data) - oldLen
}

func mapLocalS0FSError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, s0fs.ErrNotFound):
		return fserror.New(fserror.NotFound, err.Error())
	case errors.Is(err, s0fs.ErrExists):
		return fserror.New(fserror.AlreadyExists, err.Error())
	case errors.Is(err, s0fs.ErrNotEmpty), errors.Is(err, s0fs.ErrIsDir):
		return fserror.New(fserror.FailedPrecondition, err.Error())
	case errors.Is(err, s0fs.ErrInvalidInput), errors.Is(err, s0fs.ErrNotDir):
		return fserror.New(fserror.InvalidArgument, err.Error())
	case errors.Is(err, s0fs.ErrClosed):
		return fserror.New(fserror.FailedPrecondition, err.Error())
	default:
		return fserror.New(fserror.Internal, err.Error())
	}
}

type unboundSession struct{}

func (unboundSession) Close() {}
func unboundError() error {
	return fserror.New(fserror.FailedPrecondition, "volume portal is not bound")
}

func (unboundSession) Lookup(context.Context, *pb.LookupRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) GetAttr(context.Context, *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	now := time.Now()
	return &pb.GetAttrResponse{
		Ino:       s0fs.RootInode,
		Mode:      0o755 | 0o040000,
		Nlink:     1,
		AtimeSec:  now.Unix(),
		AtimeNsec: int64(now.Nanosecond()),
		MtimeSec:  now.Unix(),
		MtimeNsec: int64(now.Nanosecond()),
		CtimeSec:  now.Unix(),
		CtimeNsec: int64(now.Nanosecond()),
	}, nil
}
func (unboundSession) SetAttr(context.Context, *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Mkdir(context.Context, *pb.MkdirRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Create(context.Context, *pb.CreateRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Unlink(context.Context, *pb.UnlinkRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) Rmdir(context.Context, *pb.RmdirRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) Rename(context.Context, *pb.RenameRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) Link(context.Context, *pb.LinkRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Symlink(context.Context, *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Readlink(context.Context, *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Access(context.Context, *pb.AccessRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) Open(context.Context, *pb.OpenRequest) (*pb.OpenResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Write(context.Context, *pb.WriteRequest) (*pb.WriteResponse, error) {
	return nil, unboundError()
}
func (unboundSession) Release(context.Context, *pb.ReleaseRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) Flush(context.Context, *pb.FlushRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) Fsync(context.Context, *pb.FsyncRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) Fallocate(context.Context, *pb.FallocateRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) CopyFileRange(context.Context, *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) OpenDir(context.Context, *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	return &pb.OpenDirResponse{HandleId: 1}, nil
}
func (unboundSession) ReadDir(context.Context, *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	return &pb.ReadDirResponse{}, nil
}
func (unboundSession) ReleaseDir(context.Context, *pb.ReleaseDirRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) StatFs(context.Context, *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	return &pb.StatFsResponse{Bsize: 4096, Frsize: 4096, Namelen: 255}, nil
}
func (unboundSession) GetXattr(context.Context, *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	return nil, unboundError()
}
func (unboundSession) SetXattr(context.Context, *pb.SetXattrRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) ListXattr(context.Context, *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	return &pb.ListXattrResponse{}, nil
}
func (unboundSession) RemoveXattr(context.Context, *pb.RemoveXattrRequest) (*pb.Empty, error) {
	return nil, unboundError()
}
func (unboundSession) Mknod(context.Context, *pb.MknodRequest) (*pb.NodeResponse, error) {
	return nil, unboundError()
}
func (unboundSession) GetLk(context.Context, *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	return &pb.GetLkResponse{}, nil
}
func (unboundSession) SetLk(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) SetLkw(context.Context, *pb.SetLkRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}
func (unboundSession) Flock(context.Context, *pb.FlockRequest) (*pb.Empty, error) {
	return &pb.Empty{}, nil
}

var _ volumefuse.Session = unboundSession{}
