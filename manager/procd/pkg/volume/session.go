package volume

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const mountSessionHeartbeatInterval = 15 * time.Second

type sessionResult struct {
	resp *pb.MountSessionResponse
	err  error
}

type mountSession struct {
	stream       pb.FileSystem_MountSessionClient
	logger       *zap.Logger
	cancel       context.CancelFunc
	pendingMu    sync.Mutex
	pending      map[uint64]chan sessionResult
	sendMu       sync.Mutex
	nextRequest  atomic.Uint64
	onInvalidate func(*pb.WatchEvent)
}

type sessionFS struct {
	session  *mountSession
	volumeID string
}

func newSessionFS(volumeID, sessionID, sessionSecret string, client pb.FileSystemClient, logger *zap.Logger, onInvalidate func(*pb.WatchEvent)) (*sessionFS, error) {
	if client == nil {
		return nil, fmt.Errorf("filesystem client is required")
	}
	callCtx, err := withSessionCredential(context.Background(), volumeID, sessionID, sessionSecret)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithCancel(callCtx)

	stream, err := client.MountSession(callCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open mount session: %w", err)
	}

	session := &mountSession{
		stream:       stream,
		logger:       logger,
		cancel:       cancel,
		pending:      make(map[uint64]chan sessionResult),
		onInvalidate: onInvalidate,
	}
	go session.recvLoop()
	go session.heartbeatLoop()

	return &sessionFS{
		session:  session,
		volumeID: volumeID,
	}, nil
}

func (s *sessionFS) Close() {
	if s == nil || s.session == nil {
		return
	}
	s.session.close(nil)
}

func (s *sessionFS) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Lookup{Lookup: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetNode(), nil
}

func (s *sessionFS) LookupBatch(ctx context.Context, req *pb.LookupBatchRequest) (*pb.LookupBatchResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_LookupBatch{LookupBatch: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetLookupBatch(), nil
}

func (s *sessionFS) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_GetAttr{GetAttr: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetAttr(), nil
}

func (s *sessionFS) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_SetAttr{SetAttr: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSetAttr(), nil
}

func (s *sessionFS) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Mkdir{Mkdir: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetNode(), nil
}

func (s *sessionFS) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Create{Create: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetNode(), nil
}

func (s *sessionFS) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Unlink{Unlink: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Rmdir{Rmdir: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Rename{Rename: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Open{Open: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetOpen(), nil
}

func (s *sessionFS) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Read{Read: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetRead(), nil
}

func (s *sessionFS) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Write{Write: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetWrite(), nil
}

func (s *sessionFS) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Release{Release: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Flush{Flush: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Fsync{Fsync: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_OpenDir{OpenDir: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetOpenDir(), nil
}

func (s *sessionFS) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_ReadDir{ReadDir: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetReadDir(), nil
}

func (s *sessionFS) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_ReleaseDir{ReleaseDir: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_StatFs{StatFs: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetStatFs(), nil
}

func (s *sessionFS) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Access{Access: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetEmpty(), nil
}

func (s *sessionFS) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Symlink{Symlink: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetNode(), nil
}

func (s *sessionFS) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	resp, err := s.call(ctx, &pb.MountSessionRequest{
		VolumeId: s.volumeID,
		Payload:  &pb.MountSessionRequest_Readlink{Readlink: req},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetReadlink(), nil
}

func (s *sessionFS) call(ctx context.Context, req *pb.MountSessionRequest) (*pb.MountSessionResponse, error) {
	if s == nil || s.session == nil {
		return nil, fmt.Errorf("mount session is not available")
	}
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}
	if req.VolumeId == "" {
		req.VolumeId = s.volumeID
	}
	return s.session.call(ctx, req)
}

func (s *mountSession) call(ctx context.Context, req *pb.MountSessionRequest) (*pb.MountSessionResponse, error) {
	requestID := s.nextRequest.Add(1)
	req.RequestId = requestID

	resultCh := make(chan sessionResult, 1)
	s.pendingMu.Lock()
	s.pending[requestID] = resultCh
	s.pendingMu.Unlock()

	s.sendMu.Lock()
	err := s.stream.Send(req)
	s.sendMu.Unlock()
	if err != nil {
		s.removePending(requestID)
		return nil, err
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			return nil, result.err
		}
		if result.resp != nil && result.resp.GetError() != nil {
			return nil, mountSessionError(result.resp.GetError())
		}
		return result.resp, nil
	case <-ctx.Done():
		s.removePending(requestID)
		return nil, ctx.Err()
	}
}

func (s *mountSession) removePending(requestID uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, requestID)
}

func (s *mountSession) recvLoop() {
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			s.close(err)
			return
		}
		if resp == nil {
			continue
		}
		if event := resp.GetWatchEvent(); event != nil {
			if event.EventType == pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE && s.onInvalidate != nil {
				s.onInvalidate(event)
			}
			continue
		}

		s.pendingMu.Lock()
		resultCh := s.pending[resp.RequestId]
		delete(s.pending, resp.RequestId)
		s.pendingMu.Unlock()
		if resultCh != nil {
			resultCh <- sessionResult{resp: resp}
		}
	}
}

func (s *mountSession) heartbeatLoop() {
	ticker := time.NewTicker(mountSessionHeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		_, err := s.call(context.Background(), &pb.MountSessionRequest{
			Payload: &pb.MountSessionRequest_Heartbeat{
				Heartbeat: &pb.MountSessionHeartbeat{SentAtUnix: time.Now().Unix()},
			},
		})
		if err != nil {
			if s.logger != nil {
				s.logger.Debug("Mount session heartbeat stopped", zap.Error(err))
			}
			return
		}
	}
}

func (s *mountSession) close(err error) {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.stream != nil {
		_ = s.stream.CloseSend()
	}

	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for requestID, resultCh := range s.pending {
		resultCh <- sessionResult{err: err}
		delete(s.pending, requestID)
	}
}

func mountSessionError(body *pb.MountSessionError) error {
	if body == nil {
		return nil
	}
	st := status.New(codes.Code(body.Code), body.Message)
	if body.Redirect != nil {
		withDetails, err := st.WithDetails(body.Redirect)
		if err == nil {
			return withDetails.Err()
		}
	}
	return st.Err()
}
