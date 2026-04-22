package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	pathpkg "path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	httpproxy "github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

const maxVolumeFileSize = 100 * 1024 * 1024

const maxPOSIXID = int64(^uint32(0))

var (
	errVolumeFileUnavailable = errors.New("volume file operations unavailable")
	errUnauthorized          = errors.New("unauthorized")
	errVolumeNotFound        = errors.New("sandbox volume not found")
	errFileNotFound          = errors.New("file not found")
	errDirNotFound           = errors.New("directory not found")
	errFileTooLarge          = errors.New("file too large")
	errPermissionDenied      = errors.New("permission denied")
	errDefaultPosixIdentity  = errors.New("volume default_posix_uid/default_posix_gid is required for external file access")
	errPathAlreadyExists     = errors.New("path already exists")
	errPathNotDir            = errors.New("path is not a directory")
	errDirectoryNotEmpty     = errors.New("directory not empty")
	errInvalidPath           = errors.New("invalid path")
)

type volumeFileInfo struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Type       string    `json:"type"`
	Size       int64     `json:"size"`
	Mode       string    `json:"mode"`
	ModTime    time.Time `json:"mod_time"`
	IsLink     bool      `json:"is_link"`
	LinkTarget string    `json:"link_target,omitempty"`
}

type volumeFileMoveRequest struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type volumeResolvedPath struct {
	Clean  string
	Parent uint64
	Inode  uint64
	Base   string
	Attr   *pb.GetAttrResponse
	Exists bool
}

type volumeFileActorKey struct{}

func defaultVolumeFileActor(volumeRecord *db.SandboxVolume) (*pb.PosixActor, error) {
	if volumeRecord == nil || volumeRecord.DefaultPosixUID == nil || volumeRecord.DefaultPosixGID == nil {
		return nil, errDefaultPosixIdentity
	}
	if *volumeRecord.DefaultPosixUID < 0 || *volumeRecord.DefaultPosixUID > maxPOSIXID {
		return nil, fmt.Errorf("default_posix_uid out of range: %d", *volumeRecord.DefaultPosixUID)
	}
	if *volumeRecord.DefaultPosixGID < 0 || *volumeRecord.DefaultPosixGID > maxPOSIXID {
		return nil, fmt.Errorf("default_posix_gid out of range: %d", *volumeRecord.DefaultPosixGID)
	}
	return &pb.PosixActor{
		Uid:  uint32(*volumeRecord.DefaultPosixUID),
		Gids: []uint32{uint32(*volumeRecord.DefaultPosixGID)},
	}, nil
}

func withVolumeFileActor(ctx context.Context, actor *pb.PosixActor) context.Context {
	if actor == nil {
		return ctx
	}
	return context.WithValue(ctx, volumeFileActorKey{}, actor)
}

func volumeFileActor(ctx context.Context) *pb.PosixActor {
	actor, _ := ctx.Value(volumeFileActorKey{}).(*pb.PosixActor)
	return actor
}

func (s *Server) handleVolumeFileOperation(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.readVolumeFile(w, r, volumeID, filePath)
	case http.MethodPost:
		s.writeVolumeFile(w, r, volumeID, filePath)
	case http.MethodDelete:
		s.deleteVolumeFile(w, r, volumeID, filePath)
	default:
		_ = spec.WriteError(w, http.StatusMethodNotAllowed, spec.CodeBadRequest, "method not allowed")
	}
}

func (s *Server) handleVolumeFileStat(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	filePath := r.URL.Query().Get("path")
	if filePath == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	resolved, err := s.lookupVolumePath(ctx, volumeID, filePath, true)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if !resolved.Exists {
		s.writeVolumeFileError(w, errFileNotFound)
		return
	}

	name := pathpkg.Base(resolved.Clean)
	if resolved.Clean == "/" {
		name = "/"
	}
	_ = spec.WriteSuccess(w, http.StatusOK, attrToVolumeFileInfo(name, resolved.Clean, resolved.Attr))
}

func (s *Server) handleVolumeFileList(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}
	dirPath := r.URL.Query().Get("path")
	if dirPath == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "path is required")
		return
	}

	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	resolved, err := s.lookupVolumePath(ctx, volumeID, dirPath, true)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if !resolved.Exists {
		s.writeVolumeFileError(w, errDirNotFound)
		return
	}
	if !attrModeIsDir(resolved.Attr.Mode) {
		s.writeVolumeFileError(w, errPathNotDir)
		return
	}

	handleID, releaseDir, err := s.openVolumeDir(ctx, volumeID, resolved.Inode)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	defer releaseDir()

	dirResp, err := s.fileRPC.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Inode:    resolved.Inode,
		HandleId: handleID,
		Size:     16384,
		Plus:     true,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		s.writeVolumeFileError(w, translateVolumeRPCError(err))
		return
	}

	entries := make([]*volumeFileInfo, 0, len(dirResp.Entries))
	for _, entry := range dirResp.Entries {
		if entry == nil || entry.Name == "" || entry.Name == "." || entry.Name == ".." {
			continue
		}
		entryPath := joinVolumePath(resolved.Clean, entry.Name)
		entries = append(entries, attrToVolumeFileInfo(entry.Name, entryPath, entry.Attr))
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) handleVolumeFileMove(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}

	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	var req volumeFileMoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if req.Source == "" || req.Destination == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "source and destination are required")
		return
	}

	if err := s.moveVolumePath(ctx, volumeID, req.Source, req.Destination); err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
		s.writeVolumeFileError(w, err)
		return
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"moved": true})
}

func (s *Server) handleVolumeFileWatch(w http.ResponseWriter, r *http.Request) {
	volumeID := r.PathValue("id")
	if volumeID == "" {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, "volume id is required")
		return
	}

	volumeRecord, err := s.loadAuthorizedVolume(r.Context(), volumeID)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if err := httpproxy.DisableResponseDeadlines(w); err != nil {
		s.logger.WithError(err).WithField("volume_id", volumeID).Debug("Failed to disable volume file watch response deadlines")
	}
	proxied, err := s.proxyVolumeRequestToOwnerIfNeeded(w, r, volumeRecord)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if proxied {
		return
	}
	if s.eventHub == nil {
		s.writeVolumeFileError(w, errVolumeFileUnavailable)
		return
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.WithError(err).WithField("volume_id", volumeID).Warn("Failed to upgrade volume file watch websocket")
		return
	}
	defer conn.Close()
	if err := httpproxy.DisableConnectionDeadlines(conn.UnderlyingConn()); err != nil {
		s.logger.WithError(err).WithField("volume_id", volumeID).Debug("Failed to clear volume file watch websocket deadlines")
	}

	type watchSubscription struct {
		cancel func()
	}
	watches := make(map[string]watchSubscription)
	var writeMu sync.Mutex
	writeJSON := func(payload any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(payload)
	}
	defer func() {
		for id, watch := range watches {
			if watch.cancel != nil {
				watch.cancel()
			}
			delete(watches, id)
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var req struct {
			Action    string `json:"action"`
			Path      string `json:"path"`
			Recursive bool   `json:"recursive"`
			WatchID   string `json:"watch_id"`
		}
		if err := json.Unmarshal(msg, &req); err != nil {
			_ = writeJSON(map[string]any{"type": "error", "error": err.Error()})
			continue
		}

		switch req.Action {
		case "subscribe":
			cleanedPath, err := cleanVolumePath(req.Path, true)
			if err != nil {
				_ = writeJSON(map[string]any{"type": "error", "error": err.Error()})
				continue
			}

			watchID, ch, cancel := s.eventHub.Subscribe(&pb.WatchRequest{
				VolumeId:    volumeID,
				PathPrefix:  normalizeVolumeWatchPrefix(cleanedPath),
				Recursive:   req.Recursive,
				IncludeSelf: true,
			})
			watches[watchID] = watchSubscription{cancel: cancel}

			go func(id string, events <-chan *pb.WatchEvent) {
				for event := range events {
					if event == nil {
						continue
					}
					if err := writeJSON(map[string]any{
						"type":     "event",
						"watch_id": id,
						"event":    volumeWatchEventType(event.EventType),
						"path":     event.Path,
						"old_path": event.OldPath,
					}); err != nil {
						return
					}
				}
			}(watchID, ch)

			_ = writeJSON(map[string]any{
				"type":     "subscribed",
				"watch_id": watchID,
				"path":     cleanedPath,
			})
		case "unsubscribe":
			watch, ok := watches[req.WatchID]
			if !ok {
				continue
			}
			if watch.cancel != nil {
				watch.cancel()
			}
			delete(watches, req.WatchID)
			_ = writeJSON(map[string]any{
				"type":     "unsubscribed",
				"watch_id": req.WatchID,
			})
		default:
			_ = writeJSON(map[string]any{"type": "error", "error": "unsupported action"})
		}
	}
}

func (s *Server) readVolumeFile(w http.ResponseWriter, r *http.Request, volumeID, logicalPath string) {
	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	resolved, err := s.lookupVolumePath(ctx, volumeID, logicalPath, false)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if !resolved.Exists {
		s.writeVolumeFileError(w, errFileNotFound)
		return
	}
	if attrModeIsDir(resolved.Attr.Mode) {
		s.writeVolumeFileError(w, errPathNotDir)
		return
	}

	handleID, releaseFile, err := s.openVolumeFile(ctx, volumeID, resolved.Inode, uint32(syscall.O_RDONLY))
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	defer releaseFile()

	readResp, err := s.fileRPC.Read(ctx, &pb.ReadRequest{
		VolumeId: volumeID,
		Inode:    resolved.Inode,
		Offset:   0,
		Size:     int64(resolved.Attr.Size),
		HandleId: handleID,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		s.writeVolumeFileError(w, translateVolumeRPCError(err))
		return
	}

	if acceptsVolumeFileJSON(r) {
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]string{
			"content":  base64.StdEncoding.EncodeToString(readResp.Data),
			"encoding": "base64",
		})
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(readResp.Data)
}

func (s *Server) writeVolumeFile(w http.ResponseWriter, r *http.Request, volumeID, logicalPath string) {
	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	if r.URL.Query().Get("mkdir") == "true" {
		changed, err := s.mkdirVolumePath(ctx, volumeID, logicalPath, r.URL.Query().Get("recursive") == "true")
		if err != nil {
			s.writeVolumeFileError(w, err)
			return
		}
		if changed {
			if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
				s.writeVolumeFileError(w, err)
				return
			}
		}
		_ = spec.WriteSuccess(w, http.StatusCreated, map[string]bool{"created": true})
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	if len(data) > maxVolumeFileSize {
		s.writeVolumeFileError(w, errFileTooLarge)
		return
	}

	changed, err := s.writeVolumePath(ctx, volumeID, logicalPath, data)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if changed {
		if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
			s.writeVolumeFileError(w, err)
			return
		}
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"written": true})
}

func (s *Server) deleteVolumeFile(w http.ResponseWriter, r *http.Request, volumeID, logicalPath string) {
	ctx, _, cleanup, handled := s.prepareOrProxyVolumeFileRequest(w, r, volumeID)
	if handled {
		return
	}
	defer cleanup()

	resolved, err := s.lookupVolumePath(ctx, volumeID, logicalPath, false)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return
	}
	if resolved.Exists {
		if err := s.removeVolumePath(ctx, volumeID, resolved); err != nil {
			s.writeVolumeFileError(w, err)
			return
		}
		if err := s.finalizeVolumeFileMutation(ctx, volumeID); err != nil {
			s.writeVolumeFileError(w, err)
			return
		}
	}

	_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
}

func (s *Server) prepareVolumeFileRequest(ctx context.Context, volumeID string) (context.Context, *db.SandboxVolume, func(), error) {
	volumeRecord, err := s.loadAuthorizedVolume(ctx, volumeID)
	if err != nil {
		return ctx, nil, func() {}, err
	}
	if err := s.ensureCtldVolumeOwner(ctx, volumeRecord); err != nil {
		return ctx, nil, func() {}, err
	}
	actor, err := defaultVolumeFileActor(volumeRecord)
	if err != nil {
		return ctx, nil, func() {}, err
	}
	cleanup, err := s.prepareVolumeFileMount(ctx, volumeID, volumeRecord.TeamID)
	if err != nil {
		return ctx, nil, func() {}, err
	}
	return withVolumeFileActor(ctx, actor), volumeRecord, cleanup, nil
}

func (s *Server) finalizeVolumeFileMutation(ctx context.Context, volumeID string) error {
	if s == nil || s.volMgr == nil {
		return nil
	}
	syncer, ok := s.volMgr.(directVolumeMountSyncer)
	if !ok {
		return nil
	}
	return syncer.SyncDirectVolumeFileMount(ctx, volumeID)
}

func (s *Server) loadAuthorizedVolume(ctx context.Context, volumeID string) (*db.SandboxVolume, error) {
	claims := internalauth.ClaimsFromContext(ctx)
	if claims == nil || claims.TeamID == "" {
		return nil, errUnauthorized
	}
	if s.repo == nil {
		return nil, errVolumeFileUnavailable
	}

	volumeRecord, err := s.repo.GetSandboxVolume(ctx, volumeID)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, errVolumeNotFound
		}
		return nil, err
	}
	if volumeRecord.TeamID != claims.TeamID {
		return nil, errVolumeNotFound
	}
	if s.isOwnedSandboxVolume(ctx, volumeID) {
		return nil, errVolumeNotFound
	}
	return volumeRecord, nil
}

func (s *Server) lookupVolumePath(ctx context.Context, volumeID, raw string, allowRoot bool) (*volumeResolvedPath, error) {
	cleaned, err := cleanVolumePath(raw, allowRoot)
	if err != nil {
		return nil, err
	}
	if cleaned == "/" {
		attr, err := s.fileRPC.GetAttr(ctx, &pb.GetAttrRequest{
			VolumeId: volumeID,
			Inode:    1,
			Actor:    volumeFileActor(ctx),
		})
		if err != nil {
			return nil, translateVolumeRPCError(err)
		}
		return &volumeResolvedPath{
			Clean:  cleaned,
			Parent: 0,
			Inode:  1,
			Base:   "",
			Attr:   attr,
			Exists: true,
		}, nil
	}

	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	current := uint64(1)
	for i, part := range parts {
		node, err := s.fileRPC.Lookup(ctx, &pb.LookupRequest{
			VolumeId: volumeID,
			Parent:   current,
			Name:     part,
			Actor:    volumeFileActor(ctx),
		})
		if err != nil {
			if fserror.CodeOf(err) == fserror.NotFound {
				return &volumeResolvedPath{
					Clean:  cleaned,
					Parent: current,
					Base:   part,
					Exists: false,
				}, nil
			}
			return nil, translateVolumeRPCError(err)
		}
		if i == len(parts)-1 {
			return &volumeResolvedPath{
				Clean:  cleaned,
				Parent: current,
				Inode:  node.Inode,
				Base:   part,
				Attr:   node.Attr,
				Exists: true,
			}, nil
		}
		if node.Attr != nil && !attrModeIsDir(node.Attr.Mode) {
			return nil, errPathNotDir
		}
		current = node.Inode
	}

	return nil, errInvalidPath
}

func (s *Server) ensureVolumeParent(ctx context.Context, volumeID, raw string, recursive bool) (uint64, string, error) {
	cleaned, err := cleanVolumePath(raw, false)
	if err != nil {
		return 0, "", err
	}
	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	base := parts[len(parts)-1]
	current := uint64(1)

	for _, part := range parts[:len(parts)-1] {
		node, err := s.fileRPC.Lookup(ctx, &pb.LookupRequest{
			VolumeId: volumeID,
			Parent:   current,
			Name:     part,
			Actor:    volumeFileActor(ctx),
		})
		if err != nil {
			if fserror.CodeOf(err) == fserror.NotFound {
				if !recursive {
					return 0, "", errDirNotFound
				}
				node, err = s.fileRPC.Mkdir(ctx, &pb.MkdirRequest{
					VolumeId: volumeID,
					Parent:   current,
					Name:     part,
					Mode:     0o755,
					Umask:    0,
					Actor:    volumeFileActor(ctx),
				})
				if err != nil {
					if fserror.CodeOf(err) == fserror.AlreadyExists {
						node, err = s.fileRPC.Lookup(ctx, &pb.LookupRequest{
							VolumeId: volumeID,
							Parent:   current,
							Name:     part,
							Actor:    volumeFileActor(ctx),
						})
					}
					if err != nil {
						return 0, "", translateVolumeRPCError(err)
					}
				}
			} else {
				return 0, "", translateVolumeRPCError(err)
			}
		}
		if node.Attr != nil && !attrModeIsDir(node.Attr.Mode) {
			return 0, "", errPathNotDir
		}
		current = node.Inode
	}

	return current, base, nil
}

func (s *Server) mkdirVolumePath(ctx context.Context, volumeID, raw string, recursive bool) (bool, error) {
	resolved, err := s.lookupVolumePath(ctx, volumeID, raw, false)
	if err != nil {
		return false, err
	}
	if resolved.Exists {
		if attrModeIsDir(resolved.Attr.Mode) && recursive {
			return false, nil
		}
		if attrModeIsDir(resolved.Attr.Mode) {
			return false, errPathAlreadyExists
		}
		return false, errPathNotDir
	}

	parent, base, err := s.ensureVolumeParent(ctx, volumeID, raw, recursive)
	if err != nil {
		return false, err
	}
	_, err = s.fileRPC.Mkdir(ctx, &pb.MkdirRequest{
		VolumeId: volumeID,
		Parent:   parent,
		Name:     base,
		Mode:     0o755,
		Umask:    0,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		if fserror.CodeOf(err) == fserror.AlreadyExists && recursive {
			return false, nil
		}
		return false, translateVolumeRPCError(err)
	}
	return true, nil
}

func (s *Server) writeVolumePath(ctx context.Context, volumeID, raw string, data []byte) (bool, error) {
	resolved, err := s.lookupVolumePath(ctx, volumeID, raw, false)
	if err != nil {
		return false, err
	}

	var (
		inode    uint64
		handleID uint64
	)

	if resolved.Exists {
		if attrModeIsDir(resolved.Attr.Mode) {
			return false, errPathNotDir
		}
		if same, err := s.volumeFileContentMatches(ctx, volumeID, resolved, data); err != nil {
			return false, err
		} else if same {
			return false, nil
		}
		openResp, err := s.fileRPC.Open(ctx, &pb.OpenRequest{
			VolumeId: volumeID,
			Inode:    resolved.Inode,
			Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
			Actor:    volumeFileActor(ctx),
		})
		if err != nil {
			return false, translateVolumeRPCError(err)
		}
		inode = resolved.Inode
		handleID = openResp.HandleId
	} else {
		parent, base, err := s.ensureVolumeParent(ctx, volumeID, raw, true)
		if err != nil {
			return false, err
		}
		nodeResp, err := s.fileRPC.Create(ctx, &pb.CreateRequest{
			VolumeId: volumeID,
			Parent:   parent,
			Name:     base,
			Mode:     0o644,
			Flags:    uint32(syscall.O_WRONLY),
			Umask:    0,
			Actor:    volumeFileActor(ctx),
		})
		if err != nil {
			return false, translateVolumeRPCError(err)
		}
		inode = nodeResp.Inode
		handleID = nodeResp.HandleId
	}

	defer func() {
		if handleID == 0 {
			return
		}
		_, _ = s.fileRPC.Release(ctx, &pb.ReleaseRequest{
			VolumeId: volumeID,
			Inode:    inode,
			HandleId: handleID,
			Actor:    volumeFileActor(ctx),
		})
	}()

	if len(data) == 0 {
		return true, nil
	}
	_, err = s.fileRPC.Write(ctx, &pb.WriteRequest{
		VolumeId: volumeID,
		Inode:    inode,
		Offset:   0,
		Data:     data,
		HandleId: handleID,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return false, translateVolumeRPCError(err)
	}
	return true, nil
}

func (s *Server) moveVolumePath(ctx context.Context, volumeID, src, dst string) error {
	source, err := s.lookupVolumePath(ctx, volumeID, src, false)
	if err != nil {
		return err
	}
	if !source.Exists {
		return errFileNotFound
	}
	dstParent, dstBase, err := s.ensureVolumeParent(ctx, volumeID, dst, true)
	if err != nil {
		return err
	}
	_, err = s.fileRPC.Rename(ctx, &pb.RenameRequest{
		VolumeId:  volumeID,
		OldParent: source.Parent,
		OldName:   source.Base,
		NewParent: dstParent,
		NewName:   dstBase,
		Actor:     volumeFileActor(ctx),
	})
	if err != nil {
		return translateVolumeRPCError(err)
	}
	return nil
}

func (s *Server) removeVolumePath(ctx context.Context, volumeID string, resolved *volumeResolvedPath) error {
	if resolved == nil || !resolved.Exists {
		return nil
	}
	if resolved.Clean == "/" {
		return errInvalidPath
	}
	if !attrModeIsDir(resolved.Attr.Mode) {
		_, err := s.fileRPC.Unlink(ctx, &pb.UnlinkRequest{
			VolumeId: volumeID,
			Parent:   resolved.Parent,
			Name:     resolved.Base,
			Actor:    volumeFileActor(ctx),
		})
		return translateVolumeRPCError(err)
	}

	handleID, releaseDir, err := s.openVolumeDir(ctx, volumeID, resolved.Inode)
	if err != nil {
		return err
	}
	defer releaseDir()

	dirResp, err := s.fileRPC.ReadDir(ctx, &pb.ReadDirRequest{
		VolumeId: volumeID,
		Inode:    resolved.Inode,
		HandleId: handleID,
		Size:     16384,
		Plus:     true,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return translateVolumeRPCError(err)
	}

	for _, entry := range dirResp.Entries {
		if entry == nil || entry.Name == "" || entry.Name == "." || entry.Name == ".." {
			continue
		}
		child := &volumeResolvedPath{
			Clean:  joinVolumePath(resolved.Clean, entry.Name),
			Parent: resolved.Inode,
			Inode:  entry.Inode,
			Base:   entry.Name,
			Attr:   entry.Attr,
			Exists: true,
		}
		if err := s.removeVolumePath(ctx, volumeID, child); err != nil {
			return err
		}
	}

	_, err = s.fileRPC.Rmdir(ctx, &pb.RmdirRequest{
		VolumeId: volumeID,
		Parent:   resolved.Parent,
		Name:     resolved.Base,
		Actor:    volumeFileActor(ctx),
	})
	return translateVolumeRPCError(err)
}

func (s *Server) openVolumeFile(ctx context.Context, volumeID string, inode uint64, flags uint32) (uint64, func(), error) {
	openResp, err := s.fileRPC.Open(ctx, &pb.OpenRequest{
		VolumeId: volumeID,
		Inode:    inode,
		Flags:    flags,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return 0, func() {}, translateVolumeRPCError(err)
	}
	release := func() {
		_, _ = s.fileRPC.Release(ctx, &pb.ReleaseRequest{
			VolumeId: volumeID,
			Inode:    inode,
			HandleId: openResp.HandleId,
			Actor:    volumeFileActor(ctx),
		})
	}
	return openResp.HandleId, release, nil
}

func (s *Server) openVolumeDir(ctx context.Context, volumeID string, inode uint64) (uint64, func(), error) {
	openResp, err := s.fileRPC.OpenDir(ctx, &pb.OpenDirRequest{
		VolumeId: volumeID,
		Inode:    inode,
		Flags:    uint32(syscall.O_RDONLY),
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return 0, func() {}, translateVolumeRPCError(err)
	}
	release := func() {
		_, _ = s.fileRPC.ReleaseDir(ctx, &pb.ReleaseDirRequest{
			VolumeId: volumeID,
			Inode:    inode,
			HandleId: openResp.HandleId,
			Actor:    volumeFileActor(ctx),
		})
	}
	return openResp.HandleId, release, nil
}

func (s *Server) volumeFileContentMatches(ctx context.Context, volumeID string, resolved *volumeResolvedPath, expected []byte) (bool, error) {
	if resolved == nil || !resolved.Exists || resolved.Attr == nil {
		return false, nil
	}
	if attrModeIsDir(resolved.Attr.Mode) || resolved.Attr.Size != uint64(len(expected)) {
		return false, nil
	}
	handleID, releaseFile, err := s.openVolumeFile(ctx, volumeID, resolved.Inode, uint32(syscall.O_RDONLY))
	if err != nil {
		return false, err
	}
	defer releaseFile()
	readResp, err := s.fileRPC.Read(ctx, &pb.ReadRequest{
		VolumeId: volumeID,
		Inode:    resolved.Inode,
		Offset:   0,
		Size:     int64(resolved.Attr.Size),
		HandleId: handleID,
		Actor:    volumeFileActor(ctx),
	})
	if err != nil {
		return false, translateVolumeRPCError(err)
	}
	return bytes.Equal(readResp.Data, expected), nil
}

func cleanVolumePath(raw string, allowRoot bool) (string, error) {
	cleaned := pathpkg.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "/" && allowRoot {
		return "/", nil
	}
	if cleaned == "/" {
		return "", errInvalidPath
	}
	return cleaned, nil
}

func normalizeVolumeWatchPrefix(cleaned string) string {
	if cleaned == "/" {
		return ""
	}
	return strings.TrimRight(cleaned, "/")
}

func joinVolumePath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

func attrModeIsDir(mode uint32) bool {
	return mode&syscall.S_IFMT == syscall.S_IFDIR
}

func attrModeIsSymlink(mode uint32) bool {
	return mode&syscall.S_IFMT == syscall.S_IFLNK
}

func attrToVolumeFileInfo(name, logicalPath string, attr *pb.GetAttrResponse) *volumeFileInfo {
	info := &volumeFileInfo{
		Name: name,
		Path: logicalPath,
		Type: "file",
	}
	if attr == nil {
		return info
	}
	info.Size = int64(attr.Size)
	info.Mode = fmt.Sprintf("%04o", attr.Mode&0o777)
	info.ModTime = time.Unix(attr.MtimeSec, attr.MtimeNsec).UTC()
	if attrModeIsDir(attr.Mode) {
		info.Type = "dir"
	} else if attrModeIsSymlink(attr.Mode) {
		info.Type = "symlink"
		info.IsLink = true
	}
	return info
}

func volumeWatchEventType(eventType pb.WatchEventType) string {
	switch eventType {
	case pb.WatchEventType_WATCH_EVENT_TYPE_CREATE:
		return "create"
	case pb.WatchEventType_WATCH_EVENT_TYPE_WRITE:
		return "write"
	case pb.WatchEventType_WATCH_EVENT_TYPE_REMOVE:
		return "remove"
	case pb.WatchEventType_WATCH_EVENT_TYPE_RENAME:
		return "rename"
	case pb.WatchEventType_WATCH_EVENT_TYPE_CHMOD:
		return "chmod"
	default:
		return "invalidate"
	}
}

func acceptsVolumeFileJSON(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		return true
	}
	contentType := r.Header.Get("Content-Type")
	return strings.Contains(contentType, "application/json")
}

func translateVolumeRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch fserror.CodeOf(err) {
	case fserror.NotFound:
		return errFileNotFound
	case fserror.PermissionDenied:
		return errPermissionDenied
	case fserror.AlreadyExists:
		return errPathAlreadyExists
	case fserror.FailedPrecondition:
		if strings.Contains(strings.ToLower(err.Error()), "not empty") {
			return errDirectoryNotEmpty
		}
		return err
	default:
		return err
	}
}

func (s *Server) writeVolumeFileError(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, errUnauthorized):
		_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "unauthorized")
	case errors.Is(err, errVolumeFileUnavailable):
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
	case errors.Is(err, errVolumeNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
	case errors.Is(err, errFileNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, err.Error())
	case errors.Is(err, errDirNotFound):
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, err.Error())
	case errors.Is(err, errFileTooLarge):
		_ = spec.WriteError(w, http.StatusRequestEntityTooLarge, spec.CodeBadRequest, err.Error())
	case errors.Is(err, errDefaultPosixIdentity):
		_ = spec.WriteError(w, http.StatusPreconditionFailed, spec.CodeBadRequest, err.Error())
	case errors.Is(err, errPermissionDenied):
		_ = spec.WriteError(w, http.StatusForbidden, spec.CodeForbidden, err.Error())
	case errors.Is(err, errPathAlreadyExists), errors.Is(err, errPathNotDir), errors.Is(err, errDirectoryNotEmpty):
		_ = spec.WriteError(w, http.StatusConflict, spec.CodeConflict, err.Error())
	case errors.Is(err, errInvalidPath):
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
	default:
		_ = spec.WriteError(w, http.StatusInternalServerError, spec.CodeInternal, err.Error())
	}
}
