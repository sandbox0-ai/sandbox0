package fsserver

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/legacyfs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/router"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

// FileSystemServer implements storage-proxy filesystem operations.
type FileSystemServer struct {
	volMgr            volumeManager
	volumeRepo        VolumeRepository
	eventHub          *notify.Hub
	eventBroadcaster  notify.Broadcaster
	syncRecorder      syncRecorder
	mutationBarrier   volumeMutationBarrier
	volumeRouter      *router.VolumeRouter
	logger            *logrus.Logger
	now               func() time.Time
	dirtyWriteMu      sync.Mutex
	dirtyWriteHandles map[string]dirtyWriteHandle
}

type dirtyWriteHandle struct {
	volumeID string
	inode    uint64
}

type volumeManager interface {
	MountVolume(ctx context.Context, s3Prefix, volumeID, teamID string, accessMode volume.AccessMode) (string, time.Time, error)
	UnmountVolume(ctx context.Context, volumeID, sessionID string) error
	AckInvalidate(volumeID, sessionID, invalidateID string, success bool, errorMessage string) error
	GetVolume(volumeID string) (*volume.VolumeContext, error)
}

// VolumeRepository provides volume metadata lookup for access mode enforcement.
type VolumeRepository interface {
	GetSandboxVolume(ctx context.Context, id string) (*db.SandboxVolume, error)
	GetSandboxVolumeOwner(ctx context.Context, volumeID string) (*db.SandboxVolumeOwner, error)
}

type syncRecorder interface {
	RecordRemoteChange(ctx context.Context, change *volsync.RemoteChange) error
	ValidateNamespaceMutation(ctx context.Context, req *volsync.NamespaceMutationRequest) error
}

type volumeMutationBarrier interface {
	WithShared(ctx context.Context, volumeID string, fn func(context.Context) error) error
}

// NewFileSystemServer creates a new file system server
func NewFileSystemServer(volMgr volumeManager, volumeRepo VolumeRepository, eventHub *notify.Hub, eventBroadcaster notify.Broadcaster, logger *logrus.Logger, syncRecorder syncRecorder, mutationBarrier volumeMutationBarrier) *FileSystemServer {
	if eventBroadcaster == nil && eventHub != nil {
		eventBroadcaster = notify.NewLocalBroadcaster(eventHub)
	}
	return &FileSystemServer{
		volMgr:            volMgr,
		volumeRepo:        volumeRepo,
		eventHub:          eventHub,
		eventBroadcaster:  eventBroadcaster,
		syncRecorder:      syncRecorder,
		mutationBarrier:   mutationBarrier,
		volumeRouter:      router.NewVolumeRouter(),
		logger:            logger,
		now:               func() time.Time { return time.Now().UTC() },
		dirtyWriteHandles: make(map[string]dirtyWriteHandle),
	}
}

func (s *FileSystemServer) SetVolumeRouter(volumeRouter *router.VolumeRouter) {
	if s == nil || volumeRouter == nil {
		return
	}
	s.volumeRouter = volumeRouter
}

func (s *FileSystemServer) SetNowFunc(now func() time.Time) {
	if now == nil {
		return
	}
	s.now = func() time.Time { return now().UTC() }
}

func (s *FileSystemServer) currentTime() time.Time {
	if s != nil && s.now != nil {
		return s.now()
	}
	return time.Now().UTC()
}

func (s *FileSystemServer) primaryRoute(volumeID string) router.Route {
	if s == nil || s.volumeRouter == nil {
		return router.Route{
			VolumeID:     volumeID,
			LocalPrimary: true,
		}
	}
	return s.volumeRouter.Resolve(volumeID)
}

func (s *FileSystemServer) requireLocalPrimary(volumeID string) error {
	route := s.primaryRoute(volumeID)
	if route.LocalPrimary {
		return nil
	}

	return fserror.WithRedirect(fserror.New(fserror.FailedPrecondition, "volume primary is remote"), &pb.PrimaryRedirect{
		VolumeId:      volumeID,
		PrimaryNodeId: route.PrimaryNodeID,
		PrimaryAddr:   route.PrimaryAddr,
		Epoch:         route.Epoch,
	})
}

func vfsContextForActor(actor *pb.PosixActor) legacyfs.LogContext {
	if actor == nil {
		return legacyfs.NewLogContext(fsmeta.Background())
	}
	return legacyfs.NewLogContext(fsmeta.NewContext(actor.Pid, actor.Uid, actor.Gids))
}

func ensureLazyRootPosixIdentity(volCtx *volume.VolumeContext, actor *pb.PosixActor, inodes ...fsmeta.Ino) error {
	if volCtx == nil || actor == nil || len(actor.Gids) == 0 {
		return nil
	}
	rootInode := volumeRootInode(volCtx)
	for _, inode := range inodes {
		if inode != rootInode {
			continue
		}
		return volume.EnsureLazyRootPosixIdentity(volCtx, actor.Uid, actor.Gids[0])
	}
	return nil
}

func accessActor(req *pb.AccessRequest) *pb.PosixActor {
	if req != nil && req.Actor != nil {
		return req.Actor
	}
	if req == nil {
		return nil
	}
	return &pb.PosixActor{
		Uid:  req.Uid,
		Gids: req.Gids,
	}
}

type syncRecordSuppressedKey struct{}

func suppressSyncRecord(ctx context.Context) context.Context {
	return context.WithValue(ctx, syncRecordSuppressedKey{}, true)
}

func shouldSkipSyncRecord(ctx context.Context) bool {
	skip, _ := ctx.Value(syncRecordSuppressedKey{}).(bool)
	return skip
}

func dirtyWriteKey(volumeID string, handleID uint64) string {
	return volumeID + "|" + strconv.FormatUint(handleID, 10)
}

func (s *FileSystemServer) markDirtyWrite(volumeID string, inode, handleID uint64) {
	if s == nil {
		return
	}
	s.dirtyWriteMu.Lock()
	defer s.dirtyWriteMu.Unlock()
	s.dirtyWriteHandles[dirtyWriteKey(volumeID, handleID)] = dirtyWriteHandle{
		volumeID: volumeID,
		inode:    inode,
	}
}

func (s *FileSystemServer) takeDirtyWrite(volumeID string, handleID uint64) (dirtyWriteHandle, bool) {
	if s == nil {
		return dirtyWriteHandle{}, false
	}
	key := dirtyWriteKey(volumeID, handleID)
	s.dirtyWriteMu.Lock()
	defer s.dirtyWriteMu.Unlock()
	dirty, ok := s.dirtyWriteHandles[key]
	if ok {
		delete(s.dirtyWriteHandles, key)
	}
	return dirty, ok
}

func (s *FileSystemServer) peekDirtyWrite(volumeID string, handleID uint64) (dirtyWriteHandle, bool) {
	if s == nil {
		return dirtyWriteHandle{}, false
	}
	key := dirtyWriteKey(volumeID, handleID)
	s.dirtyWriteMu.Lock()
	defer s.dirtyWriteMu.Unlock()
	dirty, ok := s.dirtyWriteHandles[key]
	return dirty, ok
}

func (s *FileSystemServer) clearDirtyWrite(volumeID string, handleID uint64) {
	if s == nil {
		return
	}
	key := dirtyWriteKey(volumeID, handleID)
	s.dirtyWriteMu.Lock()
	defer s.dirtyWriteMu.Unlock()
	delete(s.dirtyWriteHandles, key)
}

func (s *FileSystemServer) syncS0FSHandle(volCtx *volume.VolumeContext, inode uint64) error {
	if volCtx == nil || volCtx.S0FS == nil {
		return nil
	}
	if inode == 0 {
		return nil
	}
	if err := volCtx.S0FS.Fsync(inode); err != nil {
		return mapS0FSError(err)
	}
	return nil
}

func (s *FileSystemServer) recordRemoteSyncChange(ctx context.Context, change *volsync.RemoteChange) context.Context {
	if s.syncRecorder == nil || change == nil {
		return ctx
	}
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		return ctx
	}

	clone := *change
	if clone.TeamID == "" {
		clone.TeamID = claims.TeamID
	}
	if clone.SandboxID == "" {
		clone.SandboxID = claims.SandboxID
	}
	if clone.OccurredAt.IsZero() {
		clone.OccurredAt = s.currentTime()
	}

	if err := s.syncRecorder.RecordRemoteChange(ctx, &clone); err != nil {
		s.logger.WithError(err).WithField("volume_id", clone.VolumeID).Warn("Failed to record remote sync journal entry")
		return ctx
	}
	return suppressSyncRecord(ctx)
}

func captureInodeReplayState(volCtx *volume.VolumeContext, inode uint64) ([]byte, uint32, error) {
	if volCtx == nil {
		return nil, 0, errors.New("volume context is nil")
	}

	var attr fsmeta.Attr
	if errno := volCtx.Meta.GetAttr(fsmeta.Background(), mapInode(volCtx, inode), &attr); errno != 0 {
		return nil, 0, syscall.Errno(errno)
	}

	vfsCtx := legacyfs.NewLogContext(fsmeta.Background())
	_, handleID, errno := volCtx.VFS.Open(vfsCtx, mapInode(volCtx, inode), 0)
	if errno != 0 {
		return nil, 0, syscall.Errno(errno)
	}
	defer volCtx.VFS.Release(vfsCtx, mapInode(volCtx, inode), handleID)

	payload := make([]byte, 0)
	buf := make([]byte, 128*1024)
	var offset uint64
	for offset < attr.Length {
		readSize := len(buf)
		if remaining := attr.Length - offset; remaining < uint64(readSize) {
			readSize = int(remaining)
		}
		n, errno := volCtx.VFS.Read(vfsCtx, mapInode(volCtx, inode), buf[:readSize], offset, handleID)
		if errno != 0 {
			return nil, 0, syscall.Errno(errno)
		}
		if n == 0 {
			return nil, 0, io.ErrUnexpectedEOF
		}
		payload = append(payload, buf[:n]...)
		offset += uint64(n)
	}
	return payload, uint32(attr.Mode), nil
}

func uint32Ptr(value uint32) *uint32 {
	return &value
}

func (s *FileSystemServer) recordDirtyWriteReplayPayload(ctx context.Context, volCtx *volume.VolumeContext, volumeID string, dirty dirtyWriteHandle, warnContext string) bool {
	if volCtx == nil {
		return false
	}
	path := resolveInodePath(volCtx, uint64(mapInode(volCtx, dirty.inode)))
	if path == "" {
		return false
	}
	payload, mode, err := captureInodeReplayState(volCtx, dirty.inode)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to capture replay payload for " + warnContext)
		return false
	}
	s.recordRemoteSyncChange(ctx, &volsync.RemoteChange{
		VolumeID:         volumeID,
		EventType:        db.SyncEventWrite,
		Path:             path,
		EntryKind:        "file",
		Mode:             uint32Ptr(mode),
		ContentAvailable: true,
		ContentBytes:     payload,
	})
	return true
}

// MountVolume mounts a volume
func (s *FileSystemServer) MountVolume(ctx context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error) {
	// Extract team ID from context for multi-tenant isolation
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		s.logger.WithField("volume_id", req.VolumeId).Error("TeamID not found in context")
		return nil, fserror.New(fserror.Unauthenticated, "team id not found in context")
	}

	vol, err := s.authorizeVolumeMount(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if err := s.requireLocalPrimary(req.VolumeId); err != nil {
		return nil, err
	}
	accessMode := volume.NormalizeAccessMode(vol.AccessMode)

	// Build S3 prefix with team ID for multi-tenant isolation (object-store namespace).
	prefix, err := naming.S3VolumePrefix(claims.TeamID, req.VolumeId)
	if err != nil {
		return nil, fserror.New(fserror.InvalidArgument, err.Error())
	}

	sessionID, mountedAt, err := s.volMgr.MountVolume(ctx, prefix, req.VolumeId, claims.TeamID, accessMode)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", req.VolumeId).Error("Failed to mount volume")
		if strings.Contains(err.Error(), "another team") {
			return nil, fserror.New(fserror.PermissionDenied, err.Error())
		}
		return nil, fserror.New(fserror.Internal, err.Error())
	}

	s.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"team_id":   claims.TeamID,
		"prefix":    prefix,
	}).Info("Volume mounted with team prefix")

	return &pb.MountVolumeResponse{
		VolumeId:       req.VolumeId,
		MountedAt:      mountedAt.Unix(),
		MountSessionId: sessionID,
	}, nil
}

// UnmountVolume unmounts a volume
func (s *FileSystemServer) UnmountVolume(ctx context.Context, req *pb.UnmountVolumeRequest) (*pb.Empty, error) {
	if req.MountSessionId == "" {
		return nil, fserror.New(fserror.InvalidArgument, "mount_session_id is required")
	}
	if _, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId); err != nil {
		return nil, err
	}
	err := s.volMgr.UnmountVolume(ctx, req.VolumeId, req.MountSessionId)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", req.VolumeId).Error("Failed to unmount volume")
		if strings.Contains(err.Error(), "not mounted") || strings.Contains(err.Error(), "not found") {
			return nil, fserror.New(fserror.NotFound, err.Error())
		}
		return nil, fserror.New(fserror.Internal, err.Error())
	}

	return &pb.Empty{}, nil
}

// AckInvalidate acknowledges a volume invalidate event after remount.
func (s *FileSystemServer) AckInvalidate(ctx context.Context, req *pb.AckInvalidateRequest) (*pb.Empty, error) {
	if req == nil || req.VolumeId == "" || req.MountSessionId == "" || req.InvalidateId == "" {
		return nil, fserror.New(fserror.InvalidArgument, "volume_id, mount_session_id and invalidate_id are required")
	}
	if _, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId); err != nil {
		return nil, err
	}
	if err := s.volMgr.AckInvalidate(req.VolumeId, req.MountSessionId, req.InvalidateId, req.Success, req.ErrorMessage); err != nil {
		s.logger.WithError(err).WithField("volume_id", req.VolumeId).Error("Failed to ack invalidate")
		return nil, fserror.New(fserror.Internal, err.Error())
	}
	return &pb.Empty{}, nil
}

func (s *FileSystemServer) publishEvent(ctx context.Context, event *pb.WatchEvent) {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims != nil && event != nil && event.OriginSandboxId == "" {
		event.OriginSandboxId = claims.SandboxID
	}
	if s.eventBroadcaster == nil || event == nil {
		goto recordSync
	}
	if event.TimestampUnix == 0 {
		event.TimestampUnix = s.currentTime().Unix()
	}
	s.eventBroadcaster.Publish(ctx, event)

recordSync:
	if s.syncRecorder == nil || event == nil {
		return
	}
	if shouldSkipSyncRecord(ctx) {
		return
	}
	if event.TimestampUnix == 0 {
		event.TimestampUnix = s.currentTime().Unix()
	}
	if claims == nil || claims.TeamID == "" {
		return
	}
	if err := s.syncRecorder.RecordRemoteChange(ctx, &volsync.RemoteChange{
		VolumeID:   event.VolumeId,
		TeamID:     claims.TeamID,
		SandboxID:  claims.SandboxID,
		EventType:  watchEventTypeToSyncEvent(event.EventType),
		Path:       event.Path,
		OldPath:    event.OldPath,
		OccurredAt: time.Unix(event.TimestampUnix, 0),
	}); err != nil {
		s.logger.WithError(err).WithField("volume_id", event.VolumeId).Warn("Failed to record remote sync journal entry")
	}
}

func withAuthorizedVolumeMutation[T any](s *FileSystemServer, ctx context.Context, volumeID string, fn func(context.Context, *volume.VolumeContext) (T, error)) (T, error) {
	var zero T
	run := func(runCtx context.Context) (T, error) {
		if err := s.requireLocalPrimary(volumeID); err != nil {
			return zero, err
		}
		volCtx, err := s.getAuthorizedMountedVolume(runCtx, volumeID)
		if err != nil {
			return zero, err
		}
		return fn(runCtx, volCtx)
	}
	if s.mutationBarrier == nil {
		return run(ctx)
	}

	var out T
	err := s.mutationBarrier.WithShared(ctx, volumeID, func(runCtx context.Context) error {
		var err error
		out, err = run(runCtx)
		return err
	})
	if err != nil {
		return zero, err
	}
	return out, nil
}

func (s *FileSystemServer) validateNamespaceMutation(ctx context.Context, req *volsync.NamespaceMutationRequest) error {
	if s.syncRecorder == nil || req == nil {
		return nil
	}
	if err := s.syncRecorder.ValidateNamespaceMutation(ctx, req); err != nil {
		if errors.Is(err, volsync.ErrNamespaceIncompatible) {
			return fserror.New(fserror.FailedPrecondition, err.Error())
		}
		return fserror.New(fserror.Internal, err.Error())
	}
	return nil
}

func buildNamespaceMutationRequest(ctx context.Context, volumeID, eventType, path, oldPath string) *volsync.NamespaceMutationRequest {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		return nil
	}
	return &volsync.NamespaceMutationRequest{
		VolumeID:  volumeID,
		TeamID:    claims.TeamID,
		EventType: eventType,
		Path:      path,
		OldPath:   oldPath,
	}
}

func watchEventTypeToSyncEvent(eventType pb.WatchEventType) string {
	switch eventType {
	case pb.WatchEventType_WATCH_EVENT_TYPE_CREATE:
		return db.SyncEventCreate
	case pb.WatchEventType_WATCH_EVENT_TYPE_WRITE:
		return db.SyncEventWrite
	case pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE:
		return db.SyncEventRemove
	case pb.WatchEventType_WATCH_EVENT_TYPE_RENAME:
		return db.SyncEventRename
	case pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD:
		return db.SyncEventChmod
	default:
		return db.SyncEventInvalidate
	}
}

func (s *FileSystemServer) authorizeVolumeMount(ctx context.Context, volumeID string) (*db.SandboxVolume, error) {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		return nil, fserror.New(fserror.Unauthenticated, "team id not found in context")
	}
	if s.volumeRepo == nil {
		return nil, fserror.New(fserror.FailedPrecondition, "volume authorization unavailable")
	}

	vol, err := s.volumeRepo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if err == db.ErrNotFound {
			return nil, fserror.New(fserror.NotFound, "sandbox volume not found")
		}
		s.logger.WithError(err).WithField("volume_id", volumeID).Error("Failed to load sandbox volume")
		return nil, fserror.New(fserror.Internal, "failed to load sandbox volume")
	}
	if vol.TeamID != claims.TeamID {
		s.logUnauthorizedVolumeAccess(volumeID, claims.TeamID, vol.TeamID, "mount")
		return nil, fserror.New(fserror.PermissionDenied, "access denied to volume")
	}
	owner, err := s.volumeRepo.GetSandboxVolumeOwner(ctx, volumeID)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		s.logger.WithError(err).WithField("volume_id", volumeID).Error("Failed to load sandbox volume owner")
		return nil, fserror.New(fserror.Internal, "failed to load sandbox volume owner")
	}
	if owner != nil && !claims.IsSystemToken() && claims.SandboxID != owner.OwnerSandboxID {
		s.logUnauthorizedVolumeAccess(volumeID, claims.TeamID, vol.TeamID, "mount_owned")
		return nil, fserror.New(fserror.PermissionDenied, "access denied to system volume")
	}
	return vol, nil
}

func (s *FileSystemServer) getAuthorizedMountedVolume(ctx context.Context, volumeID string) (*volume.VolumeContext, error) {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil {
		return nil, fserror.New(fserror.Unauthenticated, "missing auth claims")
	}

	volCtx, err := s.volMgr.GetVolume(volumeID)
	if err != nil {
		return nil, fserror.New(fserror.NotFound, err.Error())
	}
	if claims.IsSystemToken() {
		return volCtx, nil
	}
	if claims.TeamID == "" {
		return nil, fserror.New(fserror.Unauthenticated, "team id not found in context")
	}

	ownerTeamID := volCtx.TeamID
	if ownerTeamID == "" && s.volumeRepo != nil {
		vol, repoErr := s.volumeRepo.GetSandboxVolume(ctx, volumeID)
		if repoErr != nil {
			if repoErr == db.ErrNotFound {
				return nil, fserror.New(fserror.NotFound, "sandbox volume not found")
			}
			s.logger.WithError(repoErr).WithField("volume_id", volumeID).Error("Failed to load sandbox volume")
			return nil, fserror.New(fserror.Internal, "failed to load sandbox volume")
		}
		ownerTeamID = vol.TeamID
	}
	if ownerTeamID == "" {
		return nil, fserror.New(fserror.FailedPrecondition, "volume authorization unavailable")
	}
	if ownerTeamID != claims.TeamID {
		s.logUnauthorizedVolumeAccess(volumeID, claims.TeamID, ownerTeamID, "access")
		return nil, fserror.New(fserror.PermissionDenied, "access denied to volume")
	}
	return volCtx, nil
}

func (s *FileSystemServer) logUnauthorizedVolumeAccess(volumeID, tokenTeamID, ownerTeamID, action string) {
	s.logger.WithFields(logrus.Fields{
		"volume_id":  volumeID,
		"token_team": tokenTeamID,
		"owner_team": ownerTeamID,
		"action":     action,
	}).Warn("Unauthorized volume access attempt")
}

// GetAttr implements FUSE getattr
func (s *FileSystemServer) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {

	// Get volume context
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		node, err := volCtx.S0FS.GetAttr(req.Inode)
		if err != nil {
			return nil, mapS0FSError(err)
		}
		return s0fsAttr(node), nil
	}

	inode := mapInode(volCtx, req.Inode)
	vfsCtx := vfsContextForActor(req.Actor)
	entry, st := volCtx.VFS.GetAttr(vfsCtx, inode, 0)
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"error":     st,
		}).Error("GetAttr failed")
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return convertAttr(fsmeta.Ino(req.Inode), entry.Attr), nil
}

// Lookup implements FUSE lookup
func (s *FileSystemServer) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {

	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		node, err := volCtx.S0FS.Lookup(req.Parent, req.Name)
		if err != nil {
			return nil, mapS0FSError(err)
		}
		return s0fsNodeResponse(node, 0), nil
	}

	parent := mapInode(volCtx, req.Parent)
	vfsCtx := vfsContextForActor(req.Actor)
	entry, st := volCtx.VFS.Lookup(vfsCtx, parent, req.Name)
	if st != 0 {
		if st == syscall.ENOENT {
			return nil, fserror.New(fserror.NotFound, "entry not found")
		}
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return &pb.NodeResponse{
		Inode:      uint64(entry.Inode),
		Generation: 0,
		Attr:       convertAttr(entry.Inode, entry.Attr),
	}, nil
}

// Open implements FUSE open using S0FS VFS layer
func (s *FileSystemServer) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {

	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		if req.Flags&uint32(syscall.O_TRUNC) != 0 {
			return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.OpenResponse, error) {
				return s.openS0FS(runCtx, volCtx, req)
			})
		}
		return s.openS0FS(ctx, volCtx, req)
	}

	inode := mapInode(volCtx, req.Inode)

	// Open file using VFS (which creates proper handle with reader/writer)
	vfsCtx := vfsContextForActor(req.Actor)

	// VFS.Open returns (Entry, handleID, errno)
	_, handleID, errno := volCtx.VFS.Open(vfsCtx, inode, req.Flags)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"flags":     req.Flags,
			"error":     errno,
		}).Error("Open failed")
		return nil, fserror.New(fserror.Internal, syscall.Errno(errno).Error())
	}

	return &pb.OpenResponse{
		HandleId: handleID,
	}, nil
}

// Read implements FUSE read using S0FS VFS layer
func (s *FileSystemServer) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {

	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		data, err := volCtx.S0FS.Read(req.Inode, uint64(req.Offset), uint64(req.Size))
		if err != nil {
			return nil, mapS0FSError(err)
		}
		return &pb.ReadResponse{
			Data: data,
			Eof:  len(data) < int(req.Size),
		}, nil
	}

	// Allocate buffer for read
	buf := make([]byte, req.Size)

	// Create VFS context
	vfsCtx := vfsContextForActor(req.Actor)

	// Read from S0FS VFS (convert offset to uint64)
	n, errno := volCtx.VFS.Read(vfsCtx, mapInode(volCtx, req.Inode), buf, uint64(req.Offset), req.HandleId)
	if errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"offset":    req.Offset,
			"size":      req.Size,
			"handle_id": req.HandleId,
			"error":     errno,
		}).Error("Read failed")

		return nil, fserror.New(fserror.Internal, syscall.Errno(errno).Error())
	}

	// Check if EOF
	eof := false
	if n < len(buf) {
		eof = true
		buf = buf[:n]
	}

	return &pb.ReadResponse{
		Data: buf,
		Eof:  eof,
	}, nil
}

// Write implements FUSE write using S0FS VFS layer
func (s *FileSystemServer) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.WriteResponse, error) {
		if isS0FSVolume(volCtx) {
			if _, err := volCtx.S0FS.Write(req.Inode, uint64(req.Offset), req.Data); err != nil {
				return nil, mapS0FSError(err)
			}
			s.markDirtyWrite(req.VolumeId, req.Inode, req.HandleId)
			path := resolveInodePath(volCtx, req.Inode)
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(suppressSyncRecord(runCtx), &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     req.Inode,
			})
			return &pb.WriteResponse{BytesWritten: int64(len(req.Data))}, nil
		}
		vfsCtx := vfsContextForActor(req.Actor)
		errno := volCtx.VFS.Write(vfsCtx, mapInode(volCtx, req.Inode), req.Data, uint64(req.Offset), req.HandleId)
		if errno != 0 {
			s.logger.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"inode":     req.Inode,
				"offset":    req.Offset,
				"size":      len(req.Data),
				"handle_id": req.HandleId,
				"error":     errno,
			}).Error("Write failed")

			return nil, fserror.New(fserror.Internal, syscall.Errno(errno).Error())
		}

		s.markDirtyWrite(req.VolumeId, req.Inode, req.HandleId)
		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.Inode)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(suppressSyncRecord(runCtx), &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})

		return &pb.WriteResponse{
			BytesWritten: int64(len(req.Data)),
		}, nil
	})
}

// Create implements FUSE create using S0FS VFS layer
func (s *FileSystemServer) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.NodeResponse, error) {
		if isS0FSVolume(volCtx) {
			if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, fsmeta.Ino(req.Parent)); err != nil {
				return nil, fserror.New(fserror.Internal, err.Error())
			}
			path := resolveChildPath(volCtx, req.Parent, req.Name)
			node, err := volCtx.S0FS.CreateFile(req.Parent, req.Name, req.Mode)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			if req.Actor != nil && len(req.Actor.Gids) > 0 {
				if err := volCtx.S0FS.SetOwner(node.Inode, req.Actor.Uid, req.Actor.Gids[0]); err != nil {
					return nil, fserror.New(fserror.Internal, err.Error())
				}
				node, err = volCtx.S0FS.GetAttr(node.Inode)
				if err != nil {
					return nil, mapS0FSError(err)
				}
			}
			if path == "" {
				path = resolveInodePath(volCtx, node.Inode)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     node.Inode,
			})
			return s0fsNodeResponse(node, volCtx.OpenFileHandle(node.Inode)), nil
		}
		parent := mapInode(volCtx, req.Parent)
		path := resolveChildPath(volCtx, uint64(parent), req.Name)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventCreate, path, "")); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		entry, handleID, errno := volCtx.VFS.Create(vfsCtx, parent, req.Name, uint16(req.Mode), uint16(req.Umask), req.Flags)
		if errno != 0 {
			s.logger.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"parent":    req.Parent,
				"name":      req.Name,
				"mode":      req.Mode,
				"error":     errno,
			}).Error("Create failed")
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(errno)), syscall.Errno(errno).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventCreate,
				Path:      path,
				EntryKind: "file",
				Mode:      uint32Ptr(uint32(entry.Attr.Mode)),
			})
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     uint64(entry.Inode),
		})

		return &pb.NodeResponse{
			Inode:      uint64(entry.Inode),
			Generation: 0,
			Attr:       convertAttr(entry.Inode, entry.Attr),
			HandleId:   handleID,
		}, nil
	})
}

// Mkdir implements FUSE mkdir
func (s *FileSystemServer) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.NodeResponse, error) {
		if isS0FSVolume(volCtx) {
			if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, fsmeta.Ino(req.Parent)); err != nil {
				return nil, fserror.New(fserror.Internal, err.Error())
			}
			path := resolveChildPath(volCtx, req.Parent, req.Name)
			node, err := volCtx.S0FS.Mkdir(req.Parent, req.Name, req.Mode)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			if req.Actor != nil && len(req.Actor.Gids) > 0 {
				if err := volCtx.S0FS.SetOwner(node.Inode, req.Actor.Uid, req.Actor.Gids[0]); err != nil {
					return nil, fserror.New(fserror.Internal, err.Error())
				}
				node, err = volCtx.S0FS.GetAttr(node.Inode)
				if err != nil {
					return nil, mapS0FSError(err)
				}
			}
			if path == "" {
				path = resolveInodePath(volCtx, node.Inode)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     node.Inode,
			})
			return s0fsNodeResponse(node, 0), nil
		}
		parent := mapInode(volCtx, req.Parent)
		path := resolveChildPath(volCtx, uint64(parent), req.Name)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventCreate, path, "")); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		entry, st := volCtx.VFS.Mkdir(vfsCtx, parent, req.Name, uint16(req.Mode), uint16(req.Umask))
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventCreate,
				Path:      path,
				EntryKind: "directory",
				Mode:      uint32Ptr(uint32(entry.Attr.Mode)),
			})
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     uint64(entry.Inode),
		})

		return &pb.NodeResponse{
			Inode:      uint64(entry.Inode),
			Generation: 0,
			Attr:       convertAttr(entry.Inode, entry.Attr),
		}, nil
	})
}

// Mknod implements FUSE mknod
func (s *FileSystemServer) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.NodeResponse, error) {
		if isS0FSVolume(volCtx) {
			return nil, fserror.New(fserror.Unimplemented, "mknod is not implemented for s0fs")
		}
		parent := mapInode(volCtx, req.Parent)
		path := resolveChildPath(volCtx, uint64(parent), req.Name)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventCreate, path, "")); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		entry, st := volCtx.VFS.Mknod(vfsCtx, parent, req.Name, uint16(req.Mode), uint16(req.Umask), uint32(req.Rdev))
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(runCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     uint64(entry.Inode),
		})

		return &pb.NodeResponse{
			Inode:      uint64(entry.Inode),
			Generation: 0,
			Attr:       convertAttr(entry.Inode, entry.Attr),
		}, nil
	})
}

func mapErrnoToCode(errno syscall.Errno) fserror.Code {
	switch errno {
	case syscall.EEXIST:
		return fserror.AlreadyExists
	case syscall.ENOENT:
		return fserror.NotFound
	case syscall.EACCES, syscall.EPERM:
		return fserror.PermissionDenied
	case syscall.ENOSPC:
		return fserror.ResourceExhausted
	case syscall.EINVAL, syscall.ENOTDIR:
		return fserror.InvalidArgument
	default:
		return fserror.Internal
	}
}

func (s *FileSystemServer) openS0FS(ctx context.Context, volCtx *volume.VolumeContext, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	node, err := volCtx.S0FS.GetAttr(req.Inode)
	if err != nil {
		return nil, mapS0FSError(err)
	}
	if node.Type == s0fs.TypeDirectory {
		return nil, fserror.New(fserror.FailedPrecondition, "inode is a directory")
	}
	if err := checkS0FSAccess(node, req.Actor, s0fsOpenAccessMask(req.Flags)); err != nil {
		return nil, err
	}
	if req.Flags&uint32(syscall.O_TRUNC) != 0 {
		if err := volCtx.S0FS.Truncate(req.Inode, 0); err != nil {
			return nil, mapS0FSError(err)
		}
		path := resolveInodePath(volCtx, req.Inode)
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(suppressSyncRecord(ctx), &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})
	}
	return &pb.OpenResponse{HandleId: volCtx.OpenFileHandle(node.Inode)}, nil
}

// Unlink implements FUSE unlink
func (s *FileSystemServer) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			path := resolveChildPath(volCtx, req.Parent, req.Name)
			inode, err := volCtx.S0FS.UnlinkWithInode(req.Parent, req.Name)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			if !volCtx.MarkUnlinkedFileIfOpen(inode) {
				_ = volCtx.S0FS.Forget(inode)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     inode,
			})
			return &pb.Empty{}, nil
		}
		parent := mapInode(volCtx, req.Parent)
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		st := volCtx.VFS.Unlink(vfsCtx, parent, req.Name)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		path := resolveChildPath(volCtx, uint64(mapInode(volCtx, req.Parent)), req.Name)
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventRemove,
				Path:      path,
			})
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
		})

		return &pb.Empty{}, nil
	})
}

// ReadDir implements FUSE readdir
func (s *FileSystemServer) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {

	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		entries, err := volCtx.S0FS.ReadDir(req.Inode)
		if err != nil {
			return nil, mapS0FSError(err)
		}
		start := int(req.Offset)
		if start < 0 {
			start = 0
		}
		if start > len(entries) {
			start = len(entries)
		}
		result := make([]*pb.DirEntry, 0, len(entries)-start)
		for i, entry := range entries[start:] {
			item := &pb.DirEntry{
				Inode:  entry.Inode,
				Offset: uint64(start + i + 1),
				Name:   entry.Name,
				Type:   s0fsTypeNumber(entry.Type),
			}
			if req.Plus {
				node, err := volCtx.S0FS.GetAttr(entry.Inode)
				if err != nil {
					return nil, mapS0FSError(err)
				}
				item.Attr = s0fsAttr(node)
			}
			result = append(result, item)
		}
		return &pb.ReadDirResponse{Entries: result, Eof: true}, nil
	}

	inode := mapInode(volCtx, req.Inode)
	vfsCtx := vfsContextForActor(req.Actor)
	size := req.Size
	if size == 0 {
		size = 1024
	}
	entries, _, st := volCtx.VFS.Readdir(vfsCtx, inode, size, int(req.Offset), req.HandleId, req.Plus)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	var result []*pb.DirEntry
	for _, e := range entries {
		entryType := uint32(0)
		if e.Attr != nil {
			entryType = uint32(e.Attr.Typ)
		}
		result = append(result, &pb.DirEntry{
			Inode:  uint64(e.Inode),
			Offset: 0,
			Name:   string(e.Name),
			Type:   entryType,
			Attr:   convertAttr(e.Inode, e.Attr),
		})
	}

	return &pb.ReadDirResponse{
		Entries: result,
		Eof:     false,
	}, nil
}

// OpenDir implements FUSE opendir
func (s *FileSystemServer) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		if _, err := volCtx.S0FS.GetAttr(req.Inode); err != nil {
			return nil, mapS0FSError(err)
		}
		return &pb.OpenDirResponse{HandleId: volCtx.OpenDirHandle(req.Inode)}, nil
	}

	inode := mapInode(volCtx, req.Inode)
	vfsCtx := vfsContextForActor(req.Actor)
	fh, st := volCtx.VFS.Opendir(vfsCtx, inode, req.Flags)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return &pb.OpenDirResponse{
		HandleId: fh,
	}, nil
}

// ReleaseDir implements FUSE releasedir
func (s *FileSystemServer) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		_, _, _ = volCtx.ReleaseHandle(req.HandleId)
		return &pb.Empty{}, nil
	}

	inode := mapInode(volCtx, req.Inode)
	vfsCtx := vfsContextForActor(req.Actor)
	_ = volCtx.VFS.Releasedir(vfsCtx, inode, req.HandleId)
	return &pb.Empty{}, nil
}

// Rename implements FUSE rename
func (s *FileSystemServer) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			oldPath := resolveChildPath(volCtx, req.OldParent, req.OldName)
			newPath := resolveChildPath(volCtx, req.NewParent, req.NewName)
			if err := volCtx.S0FS.Rename(req.OldParent, req.OldName, req.NewParent, req.NewName); err != nil {
				return nil, mapS0FSError(err)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_RENAME
			if oldPath == "" && newPath == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      newPath,
				OldPath:   oldPath,
			})
			return &pb.Empty{}, nil
		}
		oldParent := mapInode(volCtx, req.OldParent)
		newParent := mapInode(volCtx, req.NewParent)
		oldPath := resolveChildPath(volCtx, uint64(oldParent), req.OldName)
		newPath := resolveChildPath(volCtx, uint64(newParent), req.NewName)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventRename, newPath, oldPath)); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, oldParent, newParent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}

		vfsCtx := vfsContextForActor(req.Actor)
		st := volCtx.VFS.Rename(vfsCtx, oldParent, req.OldName, newParent, req.NewName, req.Flags)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_RENAME
		if oldPath == "" && newPath == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if oldPath != "" || newPath != "" {
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventRename,
				Path:      newPath,
				OldPath:   oldPath,
			})
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      newPath,
			OldPath:   oldPath,
		})

		return &pb.Empty{}, nil
	})
}

// SetAttr implements FUSE setattr
func (s *FileSystemServer) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.SetAttrResponse, error) {
		if isS0FSVolume(volCtx) {
			attr := req.Attr
			if attr == nil {
				attr = &pb.GetAttrResponse{}
			}
			path := resolveInodePath(volCtx, req.Inode)
			if req.Valid&uint32(fsmeta.SetAttrMode) != 0 {
				if err := volCtx.S0FS.SetMode(req.Inode, attr.Mode&0o7777); err != nil {
					return nil, mapS0FSError(err)
				}
			}
			if req.Valid&(uint32(fsmeta.SetAttrUID)|uint32(fsmeta.SetAttrGID)) != 0 {
				current, err := volCtx.S0FS.GetAttr(req.Inode)
				if err != nil {
					return nil, mapS0FSError(err)
				}
				uid := current.UID
				gid := current.GID
				if req.Valid&uint32(fsmeta.SetAttrUID) != 0 {
					uid = attr.Uid
				}
				if req.Valid&uint32(fsmeta.SetAttrGID) != 0 {
					gid = attr.Gid
				}
				if err := volCtx.S0FS.SetOwner(req.Inode, uid, gid); err != nil {
					return nil, mapS0FSError(err)
				}
			}
			if req.Valid&uint32(fsmeta.SetAttrSize) != 0 {
				if err := volCtx.S0FS.Truncate(req.Inode, attr.Size); err != nil {
					return nil, mapS0FSError(err)
				}
			}
			updated, err := volCtx.S0FS.GetAttr(req.Inode)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			if req.Valid&uint32(fsmeta.SetAttrMode) != 0 {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
			} else if path != "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
			}
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     req.Inode,
			})
			return &pb.SetAttrResponse{Attr: s0fsAttr(updated)}, nil
		}
		inode := mapInode(volCtx, req.Inode)
		attr := req.Attr
		if attr == nil {
			attr = &pb.GetAttrResponse{}
		}

		vfsCtx := vfsContextForActor(req.Actor)
		entry, st := volCtx.VFS.SetAttr(
			vfsCtx,
			inode,
			int(req.Valid),
			req.HandleId,
			attr.Mode,
			attr.Uid,
			attr.Gid,
			attr.AtimeSec,
			attr.MtimeSec,
			uint32(attr.AtimeNsec),
			uint32(attr.MtimeNsec),
			attr.Size,
		)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.Inode)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		if req.Valid&uint32(fsmeta.SetAttrMode) != 0 {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
		} else if path != "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
		}
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		switch {
		case path != "" && req.Valid&uint32(fsmeta.SetAttrMode) != 0:
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventChmod,
				Path:      path,
				Mode:      uint32Ptr(uint32(entry.Attr.Mode)),
			})
		case path != "":
			payload, mode, err := captureInodeReplayState(volCtx, req.Inode)
			if err != nil {
				s.logger.WithError(err).WithField("volume_id", req.VolumeId).Warn("Failed to capture replay payload for setattr")
			} else {
				recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
					VolumeID:         req.VolumeId,
					EventType:        db.SyncEventWrite,
					Path:             path,
					EntryKind:        "file",
					Mode:             uint32Ptr(mode),
					ContentAvailable: true,
					ContentBytes:     payload,
				})
			}
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})

		return &pb.SetAttrResponse{
			Attr: convertAttr(entry.Inode, entry.Attr),
		}, nil
	})
}

// Flush implements FUSE flush
func (s *FileSystemServer) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		if dirty, ok := s.peekDirtyWrite(req.VolumeId, req.HandleId); ok {
			if err := s.syncS0FSHandle(volCtx, dirty.inode); err != nil {
				return nil, err
			}
			s.clearDirtyWrite(req.VolumeId, req.HandleId)
		}
		return &pb.Empty{}, nil
	}

	dirty, ok := s.peekDirtyWrite(req.VolumeId, req.HandleId)
	if !ok {
		return &pb.Empty{}, nil
	}

	vfsCtx := vfsContextForActor(req.Actor)
	if errno := volCtx.VFS.Flush(vfsCtx, mapInode(volCtx, dirty.inode), req.HandleId, 0); errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     dirty.inode,
			"handle_id": req.HandleId,
			"error":     errno,
		}).Error("Flush failed")
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(errno)), syscall.Errno(errno).Error())
	}

	return &pb.Empty{}, nil
}

// Fsync implements FUSE fsync
func (s *FileSystemServer) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		inode, ok := volCtx.HandleInode(req.HandleId)
		if dirty, dirtyOK := s.peekDirtyWrite(req.VolumeId, req.HandleId); dirtyOK {
			inode = dirty.inode
			ok = true
		}
		if !ok || inode == 0 {
			return &pb.Empty{}, nil
		}
		if err := s.syncS0FSHandle(volCtx, inode); err != nil {
			return nil, err
		}
		s.clearDirtyWrite(req.VolumeId, req.HandleId)
		return &pb.Empty{}, nil
	}

	dirty, ok := s.peekDirtyWrite(req.VolumeId, req.HandleId)
	if !ok {
		return &pb.Empty{}, nil
	}

	datasync := 0
	if req.Datasync {
		datasync = 1
	}
	vfsCtx := vfsContextForActor(req.Actor)
	if errno := volCtx.VFS.Fsync(vfsCtx, mapInode(volCtx, dirty.inode), datasync, req.HandleId); errno != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     dirty.inode,
			"handle_id": req.HandleId,
			"datasync":  req.Datasync,
			"error":     errno,
		}).Error("Fsync failed")
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(errno)), syscall.Errno(errno).Error())
	}

	if s.recordDirtyWriteReplayPayload(ctx, volCtx, req.VolumeId, dirty, "fsync") {
		s.clearDirtyWrite(req.VolumeId, req.HandleId)
	}
	return &pb.Empty{}, nil
}

// Release implements FUSE release (close) using S0FS VFS layer
func (s *FileSystemServer) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		var syncErr error
		if dirty, ok := s.peekDirtyWrite(req.VolumeId, req.HandleId); ok {
			syncErr = s.syncS0FSHandle(volCtx, dirty.inode)
		}
		if inode, remaining, unlinked, ok := volCtx.ReleaseFileHandle(req.HandleId); ok && remaining == 0 && unlinked {
			_ = volCtx.S0FS.Forget(inode)
		}
		if syncErr != nil {
			s.clearDirtyWrite(req.VolumeId, req.HandleId)
			return nil, syncErr
		}
		s.clearDirtyWrite(req.VolumeId, req.HandleId)
		return &pb.Empty{}, nil
	}

	// Release the file handle in VFS
	vfsCtx := vfsContextForActor(req.Actor)
	volCtx.VFS.Release(vfsCtx, mapInode(volCtx, req.Inode), req.HandleId)

	if dirty, ok := s.takeDirtyWrite(req.VolumeId, req.HandleId); ok {
		s.recordDirtyWriteReplayPayload(ctx, volCtx, req.VolumeId, dirty, "release")
	}

	s.logger.WithFields(logrus.Fields{
		"volume_id": req.VolumeId,
		"inode":     req.Inode,
		"handle_id": req.HandleId,
	}).Debug("Released file handle")

	return &pb.Empty{}, nil
}

// Rmdir implements FUSE rmdir (remove directory)
func (s *FileSystemServer) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			path := resolveChildPath(volCtx, req.Parent, req.Name)
			if err := volCtx.S0FS.RemoveDir(req.Parent, req.Name); err != nil {
				return nil, mapS0FSError(err)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
			})
			return &pb.Empty{}, nil
		}
		parent := mapInode(volCtx, req.Parent)
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		st := volCtx.VFS.Rmdir(vfsCtx, parent, req.Name)
		if st != 0 {
			if st == syscall.ENOTEMPTY {
				return nil, fserror.New(fserror.FailedPrecondition, "directory not empty")
			}
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		path := resolveChildPath(volCtx, uint64(mapInode(volCtx, req.Parent)), req.Name)
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
				VolumeID:  req.VolumeId,
				EventType: db.SyncEventRemove,
				Path:      path,
			})
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
		})

		return &pb.Empty{}, nil
	})
}

// StatFs implements FUSE statfs (filesystem statistics)
func (s *FileSystemServer) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return &pb.StatFsResponse{
			Blocks:  262144,
			Bfree:   131072,
			Bavail:  131072,
			Files:   1048576,
			Ffree:   1048576,
			Bsize:   4096,
			Namelen: 255,
			Frsize:  4096,
		}, nil
	}

	// Get filesystem statistics from S0FS
	vfsCtx := vfsContextForActor(req.Actor)
	var totalSpace, availSpace, iused, iavail uint64
	st := volCtx.Meta.StatFS(vfsCtx, fsmeta.RootInode, &totalSpace, &availSpace, &iused, &iavail)
	if st != 0 {
		return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
	}

	// Use configured block size if available, otherwise default to 4096
	blockSize := uint64(volCtx.VFS.Config().Format.BlockSize) * 1024
	if blockSize == 0 {
		blockSize = 4096
	}
	blocks := totalSpace / blockSize
	bavail := availSpace / blockSize

	return &pb.StatFsResponse{
		Blocks:  blocks,
		Bfree:   bavail,
		Bavail:  bavail,
		Files:   iused + iavail,
		Ffree:   iavail,
		Bsize:   uint32(blockSize),
		Namelen: 255,
		Frsize:  uint32(blockSize),
	}, nil
}

// Symlink implements FUSE symlink (create symbolic link)
func (s *FileSystemServer) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.NodeResponse, error) {
		if isS0FSVolume(volCtx) {
			if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, fsmeta.Ino(req.Parent)); err != nil {
				return nil, fserror.New(fserror.Internal, err.Error())
			}
			path := resolveChildPath(volCtx, req.Parent, req.Name)
			node, err := volCtx.S0FS.Symlink(req.Parent, req.Name, req.Target, 0o777)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			if req.Actor != nil && len(req.Actor.Gids) > 0 {
				if err := volCtx.S0FS.SetOwner(node.Inode, req.Actor.Uid, req.Actor.Gids[0]); err != nil {
					return nil, fserror.New(fserror.Internal, err.Error())
				}
				node, err = volCtx.S0FS.GetAttr(node.Inode)
				if err != nil {
					return nil, mapS0FSError(err)
				}
			}
			if path == "" {
				path = resolveInodePath(volCtx, node.Inode)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     node.Inode,
			})
			return s0fsNodeResponse(node, 0), nil
		}
		parent := mapInode(volCtx, req.Parent)
		path := resolveChildPath(volCtx, uint64(parent), req.Name)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventCreate, path, "")); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, parent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}
		vfsCtx := vfsContextForActor(req.Actor)
		entry, st := volCtx.VFS.Symlink(vfsCtx, req.Target, parent, req.Name)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(runCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     uint64(entry.Inode),
		})

		return &pb.NodeResponse{
			Inode:      uint64(entry.Inode),
			Generation: 0,
			Attr:       convertAttr(entry.Inode, entry.Attr),
		}, nil
	})
}

// Readlink implements FUSE readlink (read symbolic link target)
func (s *FileSystemServer) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		node, err := volCtx.S0FS.GetAttr(req.Inode)
		if err != nil {
			return nil, mapS0FSError(err)
		}
		if node.Type != s0fs.TypeSymlink {
			return nil, fserror.New(fserror.FailedPrecondition, "inode is not a symbolic link")
		}
		return &pb.ReadlinkResponse{Target: node.Target}, nil
	}

	// Read symbolic link from S0FS
	inode := mapInode(volCtx, req.Inode)
	vfsCtx := vfsContextForActor(req.Actor)
	target, st := volCtx.VFS.Readlink(vfsCtx, inode)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return &pb.ReadlinkResponse{
		Target: string(target),
	}, nil
}

// Link implements FUSE link (create hard link)
func (s *FileSystemServer) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.NodeResponse, error) {
		if isS0FSVolume(volCtx) {
			path := resolveChildPath(volCtx, req.NewParent, req.NewName)
			node, err := volCtx.S0FS.Link(req.Inode, req.NewParent, req.NewName)
			if err != nil {
				return nil, mapS0FSError(err)
			}
			eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
			if path == "" {
				eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
			}
			s.publishEvent(runCtx, &pb.WatchEvent{
				VolumeId:  req.VolumeId,
				EventType: eventType,
				Path:      path,
				Inode:     node.Inode,
			})
			return s0fsNodeResponse(node, 0), nil
		}
		inode := mapInode(volCtx, req.Inode)
		newParent := mapInode(volCtx, req.NewParent)
		path := resolveChildPath(volCtx, uint64(newParent), req.NewName)
		if err := s.validateNamespaceMutation(runCtx, buildNamespaceMutationRequest(runCtx, req.VolumeId, db.SyncEventCreate, path, "")); err != nil {
			return nil, err
		}
		if err := ensureLazyRootPosixIdentity(volCtx, req.Actor, newParent); err != nil {
			return nil, fserror.New(fserror.Internal, err.Error())
		}

		vfsCtx := vfsContextForActor(req.Actor)
		entry, st := volCtx.VFS.Link(vfsCtx, inode, newParent, req.NewName)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CREATE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(runCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     uint64(entry.Inode),
		})

		return &pb.NodeResponse{
			Inode:      uint64(entry.Inode),
			Generation: 0,
			Attr:       convertAttr(entry.Inode, entry.Attr),
		}, nil
	})
}

// Access implements FUSE access (check file access permissions)
func (s *FileSystemServer) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		node, err := volCtx.S0FS.GetAttr(req.Inode)
		if err != nil {
			return nil, mapS0FSError(err)
		}
		if err := checkS0FSAccess(node, accessActor(req), req.Mask); err != nil {
			return nil, err
		}
		return &pb.Empty{}, nil
	}

	// Create S0FS context with caller's uid/gid for permission checking
	inode := mapInode(volCtx, req.Inode)

	actor := accessActor(req)
	uid := actor.Uid
	gids := actor.Gids

	// Create context with user credentials
	vfsCtx := vfsContextForActor(actor)

	// Use S0FS VFS Access which implements full POSIX permission checking.
	st := volCtx.VFS.Access(vfsCtx, inode, int(req.Mask))
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"mask":      req.Mask,
			"pid":       actor.Pid,
			"uid":       uid,
			"gids":      gids,
			"error":     st,
		}).Debug("Access denied")
		return nil, fserror.New(fserror.PermissionDenied, syscall.Errno(st).Error())
	}

	return &pb.Empty{}, nil
}

// Fallocate preallocates or deallocates space for a file
func (s *FileSystemServer) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			return nil, fserror.New(fserror.Unimplemented, "fallocate is not implemented for s0fs")
		}
		vfsCtx := vfsContextForActor(req.Actor)
		inode := mapInode(volCtx, req.Inode)
		st := volCtx.VFS.Fallocate(vfsCtx, inode, uint8(req.Mode), req.Offset, req.Length, req.HandleId)
		if st != 0 {
			s.logger.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"inode":     req.Inode,
				"mode":      req.Mode,
				"offset":    req.Offset,
				"length":    req.Length,
				"error":     st,
			}).Error("Fallocate failed")
			return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
		}

		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"mode":      req.Mode,
			"offset":    req.Offset,
			"length":    req.Length,
		}).Debug("Fallocate succeeded")

		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.Inode)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			payload, mode, err := captureInodeReplayState(volCtx, req.Inode)
			if err != nil {
				s.logger.WithError(err).WithField("volume_id", req.VolumeId).Warn("Failed to capture replay payload for fallocate")
			} else {
				recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
					VolumeID:         req.VolumeId,
					EventType:        db.SyncEventWrite,
					Path:             path,
					EntryKind:        "file",
					Mode:             uint32Ptr(mode),
					ContentAvailable: true,
					ContentBytes:     payload,
				})
			}
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})

		return &pb.Empty{}, nil
	})
}

// CopyFileRange implements FUSE copy_file_range
func (s *FileSystemServer) CopyFileRange(ctx context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.CopyFileRangeResponse, error) {
		if isS0FSVolume(volCtx) {
			return nil, fserror.New(fserror.Unimplemented, "copy_file_range is not implemented for s0fs")
		}
		vfsCtx := vfsContextForActor(req.Actor)
		copied, st := volCtx.VFS.CopyFileRange(
			vfsCtx,
			mapInode(volCtx, req.InodeIn),
			req.HandleIn,
			req.OffsetIn,
			mapInode(volCtx, req.InodeOut),
			req.HandleOut,
			req.OffsetOut,
			req.Length,
			uint64(req.Flags),
		)
		if st != 0 {
			return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
		}

		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.InodeOut)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_WRITE
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		recordCtx := runCtx
		if path != "" {
			payload, mode, err := captureInodeReplayState(volCtx, req.InodeOut)
			if err != nil {
				s.logger.WithError(err).WithField("volume_id", req.VolumeId).Warn("Failed to capture replay payload for copy_file_range")
			} else {
				recordCtx = s.recordRemoteSyncChange(runCtx, &volsync.RemoteChange{
					VolumeID:         req.VolumeId,
					EventType:        db.SyncEventWrite,
					Path:             path,
					EntryKind:        "file",
					Mode:             uint32Ptr(mode),
					ContentAvailable: true,
					ContentBytes:     payload,
				})
			}
		}
		s.publishEvent(recordCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.InodeOut,
		})

		return &pb.CopyFileRangeResponse{
			BytesCopied: copied,
		}, nil
	})
}

// GetLk implements FUSE getlk
func (s *FileSystemServer) GetLk(ctx context.Context, req *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "locks are not implemented for s0fs")
	}
	if req.Lock == nil {
		return nil, fserror.New(fserror.InvalidArgument, "lock is required")
	}

	start := req.Lock.Start
	end := req.Lock.End
	typ := req.Lock.Typ
	pid := req.Lock.Pid

	vfsCtx := vfsContextForActor(req.Actor)
	st := volCtx.VFS.Getlk(vfsCtx, mapInode(volCtx, req.Inode), req.HandleId, req.Owner, &start, &end, &typ, &pid)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return &pb.GetLkResponse{
		Lock: &pb.FileLock{
			Start: start,
			End:   end,
			Typ:   typ,
			Pid:   pid,
		},
	}, nil
}

// SetLk implements FUSE setlk/setlkw
func (s *FileSystemServer) SetLk(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "locks are not implemented for s0fs")
	}
	if req.Lock == nil {
		return nil, fserror.New(fserror.InvalidArgument, "lock is required")
	}

	vfsCtx := vfsContextForActor(req.Actor)
	st := volCtx.VFS.Setlk(
		vfsCtx,
		mapInode(volCtx, req.Inode),
		req.HandleId,
		req.Owner,
		req.Lock.Start,
		req.Lock.End,
		req.Lock.Typ,
		req.Lock.Pid,
		req.Block,
	)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}
	return &pb.Empty{}, nil
}

// SetLkw implements FUSE setlkw (blocking)
func (s *FileSystemServer) SetLkw(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	if req != nil {
		req.Block = true
	}
	return s.SetLk(ctx, req)
}

// Flock implements FUSE flock
func (s *FileSystemServer) Flock(ctx context.Context, req *pb.FlockRequest) (*pb.Empty, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "flock is not implemented for s0fs")
	}

	vfsCtx := vfsContextForActor(req.Actor)
	st := volCtx.VFS.Flock(
		vfsCtx,
		mapInode(volCtx, req.Inode),
		req.HandleId,
		req.Owner,
		req.Typ,
		req.Block,
	)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}
	return &pb.Empty{}, nil
}

// Ioctl implements FUSE ioctl
func (s *FileSystemServer) Ioctl(ctx context.Context, req *pb.IoctlRequest) (*pb.IoctlResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "ioctl is not implemented for s0fs")
	}

	bufOut := make([]byte, req.DataOutSize)
	vfsCtx := vfsContextForActor(req.Actor)
	st := volCtx.VFS.Ioctl(vfsCtx, mapInode(volCtx, req.Inode), req.Cmd, req.Arg, req.DataIn, bufOut)
	if st != 0 {
		return nil, fserror.New(mapErrnoToCode(syscall.Errno(st)), syscall.Errno(st).Error())
	}

	return &pb.IoctlResponse{
		DataOut: bufOut,
	}, nil
}

// GetXattr gets an extended attribute
func (s *FileSystemServer) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "xattr is not implemented for s0fs")
	}

	// Call S0FS VFS GetXattr
	vfsCtx := vfsContextForActor(req.Actor)
	inode := mapInode(volCtx, req.Inode)
	value, st := volCtx.VFS.GetXattr(vfsCtx, inode, req.Name, req.Size)
	if st != 0 {
		// ENODATA/ENOATTR is not an error, just means attribute doesn't exist
		if st == syscall.ENODATA || st == fsmeta.ENOATTR {
			return nil, fserror.New(fserror.NotFound, "attribute not found")
		}
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"name":      req.Name,
			"error":     st,
		}).Error("GetXattr failed")
		return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
	}

	return &pb.GetXattrResponse{
		Value: value,
	}, nil
}

// SetXattr sets an extended attribute
func (s *FileSystemServer) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			return nil, fserror.New(fserror.Unimplemented, "xattr is not implemented for s0fs")
		}
		vfsCtx := vfsContextForActor(req.Actor)
		inode := mapInode(volCtx, req.Inode)
		st := volCtx.VFS.SetXattr(vfsCtx, inode, req.Name, req.Value, req.Flags)
		if st != 0 {
			s.logger.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"inode":     req.Inode,
				"name":      req.Name,
				"flags":     req.Flags,
				"error":     st,
			}).Error("SetXattr failed")
			return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
		}

		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.Inode)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(runCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})

		return &pb.Empty{}, nil
	})
}

// ListXattr lists all extended attributes
func (s *FileSystemServer) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	volCtx, err := s.getAuthorizedMountedVolume(ctx, req.VolumeId)
	if err != nil {
		return nil, err
	}
	if isS0FSVolume(volCtx) {
		return nil, fserror.New(fserror.Unimplemented, "xattr is not implemented for s0fs")
	}

	// Call S0FS VFS ListXattr
	vfsCtx := vfsContextForActor(req.Actor)
	inode := mapInode(volCtx, req.Inode)
	data, st := volCtx.VFS.ListXattr(vfsCtx, inode, int(req.Size))
	if st != 0 {
		s.logger.WithFields(logrus.Fields{
			"volume_id": req.VolumeId,
			"inode":     req.Inode,
			"error":     st,
		}).Error("ListXattr failed")
		return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
	}

	return &pb.ListXattrResponse{
		Data: data,
	}, nil
}

// RemoveXattr removes an extended attribute
func (s *FileSystemServer) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	return withAuthorizedVolumeMutation(s, ctx, req.VolumeId, func(runCtx context.Context, volCtx *volume.VolumeContext) (*pb.Empty, error) {
		if isS0FSVolume(volCtx) {
			return nil, fserror.New(fserror.Unimplemented, "xattr is not implemented for s0fs")
		}
		vfsCtx := vfsContextForActor(req.Actor)
		inode := mapInode(volCtx, req.Inode)
		st := volCtx.VFS.RemoveXattr(vfsCtx, inode, req.Name)
		if st != 0 {
			s.logger.WithFields(logrus.Fields{
				"volume_id": req.VolumeId,
				"inode":     req.Inode,
				"name":      req.Name,
				"error":     st,
			}).Error("RemoveXattr failed")
			return nil, fserror.New(fserror.Internal, syscall.Errno(st).Error())
		}

		path := resolveInodePath(volCtx, uint64(mapInode(volCtx, req.Inode)))
		eventType := pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD
		if path == "" {
			eventType = pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE
		}
		s.publishEvent(runCtx, &pb.WatchEvent{
			VolumeId:  req.VolumeId,
			EventType: eventType,
			Path:      path,
			Inode:     req.Inode,
		})

		return &pb.Empty{}, nil
	})
}

func resolveInodePath(volCtx *volume.VolumeContext, inode uint64) string {
	if volCtx == nil {
		return ""
	}
	if isS0FSVolume(volCtx) {
		path, ok := volCtx.S0FS.Path(inode)
		if !ok {
			return ""
		}
		return path
	}
	if volCtx.Meta == nil {
		return ""
	}
	paths := volCtx.Meta.GetPaths(fsmeta.Background(), fsmeta.Ino(inode))
	if len(paths) == 0 {
		return ""
	}
	return trimVolumeRoot(volCtx, paths[0])
}

func resolveChildPath(volCtx *volume.VolumeContext, parent uint64, name string) string {
	if volCtx == nil {
		return ""
	}
	if isS0FSVolume(volCtx) {
		path, ok := volCtx.S0FS.ChildPath(parent, name)
		if !ok {
			return ""
		}
		return path
	}
	if volCtx.Meta == nil {
		return ""
	}
	parentPaths := volCtx.Meta.GetPaths(fsmeta.Background(), fsmeta.Ino(parent))
	if len(parentPaths) == 0 {
		return ""
	}
	parentPath := trimVolumeRoot(volCtx, parentPaths[0])
	if parentPath == "/" {
		return "/" + name
	}
	return parentPath + "/" + name
}

func trimVolumeRoot(volCtx *volume.VolumeContext, path string) string {
	if volCtx == nil || volCtx.RootPath == "" {
		return path
	}
	if path == volCtx.RootPath {
		return "/"
	}
	if strings.HasPrefix(path, volCtx.RootPath+"/") {
		return strings.TrimPrefix(path, volCtx.RootPath)
	}
	return path
}

func volumeRootInode(volCtx *volume.VolumeContext) fsmeta.Ino {
	if volCtx == nil || volCtx.RootInode == 0 {
		return fsmeta.RootInode
	}
	return volCtx.RootInode
}

func mapInode(volCtx *volume.VolumeContext, inode uint64) fsmeta.Ino {
	if inode == uint64(fsmeta.RootInode) {
		return volumeRootInode(volCtx)
	}
	return fsmeta.Ino(inode)
}

// Helper: convert fsmeta.Attr to protobuf GetAttrResponse
func convertAttr(inode fsmeta.Ino, attr *fsmeta.Attr) *pb.GetAttrResponse {
	if attr == nil {
		return &pb.GetAttrResponse{
			Ino: uint64(inode),
		}
	}

	size := uint64(0)
	blocks := uint64(0)
	if attr.Typ == fsmeta.TypeFile || attr.Typ == fsmeta.TypeDirectory || attr.Typ == fsmeta.TypeSymlink {
		size = attr.Length
		blocks = (size + 511) / 512
	}

	return &pb.GetAttrResponse{
		Ino:       uint64(inode),
		Mode:      attr.SMode(),
		Nlink:     attr.Nlink,
		Uid:       attr.Uid,
		Gid:       attr.Gid,
		Rdev:      uint64(attr.Rdev),
		Size:      size,
		Blocks:    blocks,
		AtimeSec:  attr.Atime,
		AtimeNsec: int64(attr.Atimensec),
		MtimeSec:  attr.Mtime,
		MtimeNsec: int64(attr.Mtimensec),
		CtimeSec:  attr.Ctime,
		CtimeNsec: int64(attr.Ctimensec),
	}
}

func isS0FSVolume(volCtx *volume.VolumeContext) bool {
	return volCtx != nil && volCtx.IsS0FS()
}

const (
	s0fsAccessExists  = uint32(0)
	s0fsAccessExecute = uint32(1)
	s0fsAccessWrite   = uint32(2)
	s0fsAccessRead    = uint32(4)
)

func s0fsAttr(node *s0fs.Node) *pb.GetAttrResponse {
	if node == nil {
		return &pb.GetAttrResponse{}
	}
	mode := node.Mode & 0o7777
	switch node.Type {
	case s0fs.TypeDirectory:
		mode |= syscall.S_IFDIR
	case s0fs.TypeSymlink:
		mode |= syscall.S_IFLNK
	default:
		mode |= syscall.S_IFREG
	}
	size := node.Size
	if node.Type == s0fs.TypeSymlink {
		size = uint64(len(node.Target))
	}
	return &pb.GetAttrResponse{
		Ino:       node.Inode,
		Mode:      mode,
		Nlink:     node.Nlink,
		Uid:       node.UID,
		Gid:       node.GID,
		Size:      size,
		Blocks:    (size + 511) / 512,
		AtimeSec:  node.Atime.Unix(),
		AtimeNsec: int64(node.Atime.Nanosecond()),
		MtimeSec:  node.Mtime.Unix(),
		MtimeNsec: int64(node.Mtime.Nanosecond()),
		CtimeSec:  node.Ctime.Unix(),
		CtimeNsec: int64(node.Ctime.Nanosecond()),
	}
}

func s0fsOpenAccessMask(flags uint32) uint32 {
	mask := s0fsAccessRead
	switch flags & uint32(syscall.O_ACCMODE) {
	case uint32(syscall.O_WRONLY):
		mask = s0fsAccessWrite
	case uint32(syscall.O_RDWR):
		mask = s0fsAccessRead | s0fsAccessWrite
	}
	if flags&(uint32(syscall.O_TRUNC)|uint32(syscall.O_APPEND)) != 0 {
		mask |= s0fsAccessWrite
	}
	return mask
}

func checkS0FSAccess(node *s0fs.Node, actor *pb.PosixActor, mask uint32) error {
	if node == nil {
		return fserror.New(fserror.NotFound, "entry not found")
	}
	if actor == nil || actor.Uid == 0 || mask == s0fsAccessExists {
		return nil
	}

	perm := node.Mode & 0o7
	switch {
	case actor.Uid == node.UID:
		perm = (node.Mode >> 6) & 0o7
	case containsGID(actor.Gids, node.GID):
		perm = (node.Mode >> 3) & 0o7
	}

	if mask&s0fsAccessRead != 0 && perm&0o4 == 0 {
		return fserror.New(fserror.PermissionDenied, "read permission denied")
	}
	if mask&s0fsAccessWrite != 0 && perm&0o2 == 0 {
		return fserror.New(fserror.PermissionDenied, "write permission denied")
	}
	if mask&s0fsAccessExecute != 0 && perm&0o1 == 0 {
		return fserror.New(fserror.PermissionDenied, "execute permission denied")
	}
	return nil
}

func containsGID(gids []uint32, gid uint32) bool {
	for _, candidate := range gids {
		if candidate == gid {
			return true
		}
	}
	return false
}

func s0fsNodeResponse(node *s0fs.Node, handleID uint64) *pb.NodeResponse {
	if node == nil {
		return &pb.NodeResponse{}
	}
	return &pb.NodeResponse{
		Inode:      node.Inode,
		Generation: 0,
		Attr:       s0fsAttr(node),
		HandleId:   handleID,
	}
}

func s0fsTypeNumber(typ s0fs.FileType) uint32 {
	switch typ {
	case s0fs.TypeDirectory:
		return uint32(fsmeta.TypeDirectory)
	case s0fs.TypeSymlink:
		return uint32(fsmeta.TypeSymlink)
	default:
		return uint32(fsmeta.TypeFile)
	}
}

func mapS0FSError(err error) error {
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
