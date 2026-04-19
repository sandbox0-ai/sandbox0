package volume

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volproto"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"go.uber.org/zap"
)

const mountSessionHeartbeatInterval = 15 * time.Second

type sessionResult struct {
	frame volproto.Frame
	err   error
}

type binarySession struct {
	conn        net.Conn
	logger      *zap.Logger
	cancel      context.CancelFunc
	pendingMu   sync.Mutex
	pending     map[uint64]chan sessionResult
	sendMu      sync.Mutex
	nextRequest atomic.Uint64
	onEvent     func(*pb.WatchEvent)
}

func mountVolumeBinary(ctx context.Context, addr, volumeID, token string, cfg *pb.VolumeConfig) (string, string, error) {
	conn, err := dialBinary(ctx, addr)
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	payload := volproto.EncodeMountVolumeRequest(token, &pb.MountVolumeRequest{VolumeId: volumeID, Config: cfg})
	if err := volproto.WriteFrame(conn, volproto.Frame{RequestID: 1, Op: volproto.OpMountVolume, Payload: payload}); err != nil {
		return "", "", err
	}
	frame, err := volproto.ReadFrame(conn)
	if err != nil {
		return "", "", err
	}
	if frame.Flags&volproto.FlagError != 0 {
		return "", "", frameError(frame)
	}
	resp, err := volproto.DecodeMountVolumeResponse(frame.Payload)
	if err != nil {
		return "", "", err
	}
	return resp.MountSessionId, resp.MountSessionSecret, nil
}

func newBinarySession(ctx context.Context, addr, volumeID, sessionID, sessionSecret string, logger *zap.Logger, onEvent func(*pb.WatchEvent)) (*binarySession, error) {
	conn, err := dialBinary(ctx, addr)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithCancel(context.Background())
	session := &binarySession{
		conn:    conn,
		logger:  logger,
		cancel:  cancel,
		pending: make(map[uint64]chan sessionResult),
		onEvent: onEvent,
	}
	go session.recvLoop(callCtx)
	if _, err := session.rawCall(ctx, volproto.OpHello, volproto.EncodeHelloRequest(volumeID, sessionID, sessionSecret)); err != nil {
		session.Close()
		return nil, err
	}
	go session.heartbeatLoop()
	return session, nil
}

func dialBinary(ctx context.Context, addr string) (net.Conn, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial storage-proxy s0vp: %w", err)
	}
	return conn, nil
}

func (s *binarySession) Close() {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for requestID, ch := range s.pending {
		ch <- sessionResult{err: fmt.Errorf("s0vp session closed")}
		delete(s.pending, requestID)
	}
}

func (s *binarySession) rawCall(ctx context.Context, op volproto.Op, payload []byte) (volproto.Frame, error) {
	requestID := s.nextRequest.Add(1)
	resultCh := make(chan sessionResult, 1)
	s.pendingMu.Lock()
	s.pending[requestID] = resultCh
	s.pendingMu.Unlock()

	s.sendMu.Lock()
	err := volproto.WriteFrame(s.conn, volproto.Frame{RequestID: requestID, Op: op, Payload: payload})
	s.sendMu.Unlock()
	if err != nil {
		s.removePending(requestID)
		return volproto.Frame{}, err
	}
	select {
	case result := <-resultCh:
		if result.err != nil {
			return volproto.Frame{}, result.err
		}
		return result.frame, nil
	case <-ctx.Done():
		s.removePending(requestID)
		return volproto.Frame{}, ctx.Err()
	}
}

func (s *binarySession) call(ctx context.Context, op volproto.Op, req any) (any, error) {
	payload, err := volproto.EncodeRequest(op, req)
	if err != nil {
		return nil, err
	}
	frame, err := s.rawCall(ctx, op, payload)
	if err != nil {
		return nil, err
	}
	if frame.Flags&volproto.FlagError != 0 {
		return nil, frameError(frame)
	}
	return volproto.DecodeResponse(op, frame.Payload)
}

func (s *binarySession) removePending(requestID uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pending, requestID)
}

func (s *binarySession) recvLoop(ctx context.Context) {
	for {
		frame, err := volproto.ReadFrame(s.conn)
		if err != nil {
			s.closePending(err)
			return
		}
		if frame.Flags&volproto.FlagEvent != 0 || frame.Op == volproto.OpWatchEvent {
			if event, err := volproto.DecodeResponse(volproto.OpWatchEvent, frame.Payload); err == nil && s.onEvent != nil {
				if watchEvent, ok := event.(*pb.WatchEvent); ok {
					s.onEvent(watchEvent)
				}
			}
			continue
		}
		s.pendingMu.Lock()
		ch := s.pending[frame.RequestID]
		delete(s.pending, frame.RequestID)
		s.pendingMu.Unlock()
		if ch != nil {
			ch <- sessionResult{frame: frame}
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func (s *binarySession) closePending(err error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	for requestID, ch := range s.pending {
		ch <- sessionResult{err: err}
		delete(s.pending, requestID)
	}
}

func (s *binarySession) heartbeatLoop() {
	ticker := time.NewTicker(mountSessionHeartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := s.rawCall(ctx, volproto.OpHeartbeat, nil)
		cancel()
		if err != nil {
			if s.logger != nil {
				s.logger.Debug("s0vp heartbeat stopped", zap.Error(err))
			}
			return
		}
	}
}

func frameError(frame volproto.Frame) error {
	code, msg, redirect, err := volproto.ReadError(frame.Payload)
	if err != nil {
		return err
	}
	return remoteError{code: code, message: msg, redirect: redirect}
}

type remoteError struct {
	code     volproto.StatusCode
	message  string
	redirect *pb.PrimaryRedirect
}

func (e remoteError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("s0vp remote error: %d", e.code)
}

func (s *binarySession) Lookup(ctx context.Context, req *pb.LookupRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpLookup, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) GetAttr(ctx context.Context, req *pb.GetAttrRequest) (*pb.GetAttrResponse, error) {
	resp, err := s.call(ctx, volproto.OpGetAttr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetAttrResponse), nil
}
func (s *binarySession) SetAttr(ctx context.Context, req *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	resp, err := s.call(ctx, volproto.OpSetAttr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SetAttrResponse), nil
}
func (s *binarySession) Mkdir(ctx context.Context, req *pb.MkdirRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpMkdir, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) Create(ctx context.Context, req *pb.CreateRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpCreate, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpUnlink, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Rmdir(ctx context.Context, req *pb.RmdirRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpRmdir, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpRename, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Link(ctx context.Context, req *pb.LinkRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpLink, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpSymlink, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	resp, err := s.call(ctx, volproto.OpReadlink, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ReadlinkResponse), nil
}
func (s *binarySession) Access(ctx context.Context, req *pb.AccessRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpAccess, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	resp, err := s.call(ctx, volproto.OpOpen, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.OpenResponse), nil
}
func (s *binarySession) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	resp, err := s.call(ctx, volproto.OpRead, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ReadResponse), nil
}
func (s *binarySession) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	resp, err := s.call(ctx, volproto.OpWrite, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.WriteResponse), nil
}
func (s *binarySession) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpRelease, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Flush(ctx context.Context, req *pb.FlushRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpFlush, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Fsync(ctx context.Context, req *pb.FsyncRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpFsync, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Fallocate(ctx context.Context, req *pb.FallocateRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpFallocate, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) CopyFileRange(ctx context.Context, req *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error) {
	resp, err := s.call(ctx, volproto.OpCopyFileRange, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.CopyFileRangeResponse), nil
}
func (s *binarySession) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	resp, err := s.call(ctx, volproto.OpOpenDir, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.OpenDirResponse), nil
}
func (s *binarySession) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	resp, err := s.call(ctx, volproto.OpReadDir, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ReadDirResponse), nil
}
func (s *binarySession) ReleaseDir(ctx context.Context, req *pb.ReleaseDirRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpReleaseDir, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) StatFs(ctx context.Context, req *pb.StatFsRequest) (*pb.StatFsResponse, error) {
	resp, err := s.call(ctx, volproto.OpStatFs, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.StatFsResponse), nil
}
func (s *binarySession) GetXattr(ctx context.Context, req *pb.GetXattrRequest) (*pb.GetXattrResponse, error) {
	resp, err := s.call(ctx, volproto.OpGetXattr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetXattrResponse), nil
}
func (s *binarySession) SetXattr(ctx context.Context, req *pb.SetXattrRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpSetXattr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) ListXattr(ctx context.Context, req *pb.ListXattrRequest) (*pb.ListXattrResponse, error) {
	resp, err := s.call(ctx, volproto.OpListXattr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ListXattrResponse), nil
}
func (s *binarySession) RemoveXattr(ctx context.Context, req *pb.RemoveXattrRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpRemoveXattr, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Mknod(ctx context.Context, req *pb.MknodRequest) (*pb.NodeResponse, error) {
	resp, err := s.call(ctx, volproto.OpMknod, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.NodeResponse), nil
}
func (s *binarySession) GetLk(ctx context.Context, req *pb.GetLkRequest) (*pb.GetLkResponse, error) {
	resp, err := s.call(ctx, volproto.OpGetLk, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.GetLkResponse), nil
}
func (s *binarySession) SetLk(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpSetLk, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) SetLkw(ctx context.Context, req *pb.SetLkRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpSetLkw, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
func (s *binarySession) Flock(ctx context.Context, req *pb.FlockRequest) (*pb.Empty, error) {
	resp, err := s.call(ctx, volproto.OpFlock, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.Empty), nil
}
