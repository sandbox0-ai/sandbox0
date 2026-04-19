package volumeserver

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/auth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fserror"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volproto"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type Server struct {
	listener        net.Listener
	fs              *fsserver.FileSystemServer
	validator       *internalauth.Validator
	sessionResolver auth.SessionClaimsResolver
	eventHub        *notify.Hub
	logger          *logrus.Logger
}

func New(listener net.Listener, fs *fsserver.FileSystemServer, validator *internalauth.Validator, sessionResolver auth.SessionClaimsResolver, eventHub *notify.Hub, logger *logrus.Logger) *Server {
	return &Server{
		listener:        listener,
		fs:              fs,
		validator:       validator,
		sessionResolver: sessionResolver,
		eventHub:        eventHub,
		logger:          logger,
	}
}

func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.serveConn(conn)
	}
}

type connState struct {
	conn     net.Conn
	sendMu   sync.Mutex
	volumeID string
	claims   *internalauth.Claims
	cancel   context.CancelFunc
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	state := &connState{conn: conn}
	for {
		frame, err := volproto.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && s.logger != nil {
				s.logger.WithError(err).Debug("s0vp connection closed")
			}
			if state.cancel != nil {
				state.cancel()
			}
			return
		}
		if frame.Op == volproto.OpHello {
			s.handleHello(state, frame)
			continue
		}
		if frame.Op == volproto.OpMountVolume {
			s.handleMount(state, frame)
			continue
		}
		go s.handleSessionFrame(state, frame)
	}
}

func (s *Server) handleMount(state *connState, frame volproto.Frame) {
	token, req, err := volproto.DecodeMountVolumeRequest(frame.Payload)
	if err != nil {
		s.sendError(state, frame, fserror.InvalidArgument, err.Error())
		return
	}
	claims, err := s.validator.Validate(token)
	if err != nil {
		s.sendError(state, frame, fserror.Unauthenticated, err.Error())
		return
	}
	ctx := internalauth.WithClaims(context.Background(), claims)
	resp, err := s.fs.MountVolume(ctx, req)
	if err != nil {
		s.sendStatusError(state, frame, err)
		return
	}
	s.sendPayload(state, frame, volproto.EncodeMountVolumeResponse(resp))
}

func (s *Server) handleHello(state *connState, frame volproto.Frame) {
	volumeID, sessionID, sessionSecret, err := volproto.DecodeHelloRequest(frame.Payload)
	if err != nil {
		s.sendError(state, frame, fserror.InvalidArgument, err.Error())
		return
	}
	claims, err := s.sessionResolver.ResolveVolumeSessionClaims(context.Background(), volumeID, sessionID, sessionSecret)
	if err != nil {
		s.sendError(state, frame, fserror.Unauthenticated, err.Error())
		return
	}
	state.volumeID = volumeID
	state.claims = claims
	if state.cancel != nil {
		state.cancel()
	}
	if s.eventHub != nil {
		ctx, cancel := context.WithCancel(context.Background())
		state.cancel = cancel
		_, ch, unsubscribe := s.eventHub.Subscribe(&pb.WatchRequest{
			VolumeId:    volumeID,
			Recursive:   true,
			IncludeSelf: true,
		})
		go func() {
			defer unsubscribe()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					payload, err := volproto.EncodeResponse(volproto.OpWatchEvent, event)
					if err != nil {
						continue
					}
					_ = s.writeFrame(state, volproto.Frame{Op: volproto.OpWatchEvent, Flags: volproto.FlagEvent, Payload: payload})
				}
			}
		}()
	}
	w := volproto.NewWriter()
	w.I64(time.Now().Unix())
	s.sendPayload(state, frame, w.Bytes())
}

func (s *Server) handleSessionFrame(state *connState, frame volproto.Frame) {
	if state.claims == nil || state.volumeID == "" {
		s.sendError(state, frame, fserror.Unauthenticated, "s0vp session is not authenticated")
		return
	}
	ctx := internalauth.WithClaims(context.Background(), state.claims)
	if frame.Op == volproto.OpHeartbeat {
		payload, err := volproto.EncodeResponse(volproto.OpHeartbeat, &pb.Empty{})
		if err != nil {
			s.sendError(state, frame, fserror.Internal, err.Error())
			return
		}
		s.sendPayload(state, frame, payload)
		return
	}
	if frame.Op == volproto.OpUnmountVolume {
		req, err := volproto.DecodeUnmountVolumeRequest(frame.Payload)
		if err != nil {
			s.sendError(state, frame, fserror.InvalidArgument, err.Error())
			return
		}
		resp, err := s.fs.UnmountVolume(ctx, req)
		s.sendResult(state, frame, resp, err)
		return
	}
	req, err := volproto.DecodeRequest(frame.Op, state.volumeID, frame.Payload)
	if err != nil {
		s.sendError(state, frame, fserror.InvalidArgument, err.Error())
		return
	}
	resp, err := s.dispatch(ctx, frame.Op, req)
	s.sendResult(state, frame, resp, err)
}

func (s *Server) dispatch(ctx context.Context, op volproto.Op, req any) (any, error) {
	switch op {
	case volproto.OpHeartbeat:
		return &pb.Empty{}, nil
	case volproto.OpLookup:
		return s.fs.Lookup(ctx, req.(*pb.LookupRequest))
	case volproto.OpGetAttr:
		return s.fs.GetAttr(ctx, req.(*pb.GetAttrRequest))
	case volproto.OpSetAttr:
		return s.fs.SetAttr(ctx, req.(*pb.SetAttrRequest))
	case volproto.OpMkdir:
		return s.fs.Mkdir(ctx, req.(*pb.MkdirRequest))
	case volproto.OpCreate:
		return s.fs.Create(ctx, req.(*pb.CreateRequest))
	case volproto.OpUnlink:
		return s.fs.Unlink(ctx, req.(*pb.UnlinkRequest))
	case volproto.OpRmdir:
		return s.fs.Rmdir(ctx, req.(*pb.RmdirRequest))
	case volproto.OpRename:
		return s.fs.Rename(ctx, req.(*pb.RenameRequest))
	case volproto.OpLink:
		return s.fs.Link(ctx, req.(*pb.LinkRequest))
	case volproto.OpSymlink:
		return s.fs.Symlink(ctx, req.(*pb.SymlinkRequest))
	case volproto.OpReadlink:
		return s.fs.Readlink(ctx, req.(*pb.ReadlinkRequest))
	case volproto.OpAccess:
		return s.fs.Access(ctx, req.(*pb.AccessRequest))
	case volproto.OpOpen:
		return s.fs.Open(ctx, req.(*pb.OpenRequest))
	case volproto.OpRead:
		return s.fs.Read(ctx, req.(*pb.ReadRequest))
	case volproto.OpWrite:
		return s.fs.Write(ctx, req.(*pb.WriteRequest))
	case volproto.OpRelease:
		return s.fs.Release(ctx, req.(*pb.ReleaseRequest))
	case volproto.OpFlush:
		return s.fs.Flush(ctx, req.(*pb.FlushRequest))
	case volproto.OpFsync:
		return s.fs.Fsync(ctx, req.(*pb.FsyncRequest))
	case volproto.OpFallocate:
		return s.fs.Fallocate(ctx, req.(*pb.FallocateRequest))
	case volproto.OpCopyFileRange:
		return s.fs.CopyFileRange(ctx, req.(*pb.CopyFileRangeRequest))
	case volproto.OpOpenDir:
		return s.fs.OpenDir(ctx, req.(*pb.OpenDirRequest))
	case volproto.OpReadDir:
		return s.fs.ReadDir(ctx, req.(*pb.ReadDirRequest))
	case volproto.OpReleaseDir:
		return s.fs.ReleaseDir(ctx, req.(*pb.ReleaseDirRequest))
	case volproto.OpStatFs:
		return s.fs.StatFs(ctx, req.(*pb.StatFsRequest))
	case volproto.OpGetXattr:
		return s.fs.GetXattr(ctx, req.(*pb.GetXattrRequest))
	case volproto.OpSetXattr:
		return s.fs.SetXattr(ctx, req.(*pb.SetXattrRequest))
	case volproto.OpListXattr:
		return s.fs.ListXattr(ctx, req.(*pb.ListXattrRequest))
	case volproto.OpRemoveXattr:
		return s.fs.RemoveXattr(ctx, req.(*pb.RemoveXattrRequest))
	case volproto.OpMknod:
		return s.fs.Mknod(ctx, req.(*pb.MknodRequest))
	case volproto.OpGetLk:
		return s.fs.GetLk(ctx, req.(*pb.GetLkRequest))
	case volproto.OpSetLk:
		return s.fs.SetLk(ctx, req.(*pb.SetLkRequest))
	case volproto.OpSetLkw:
		return s.fs.SetLkw(ctx, req.(*pb.SetLkRequest))
	case volproto.OpFlock:
		return s.fs.Flock(ctx, req.(*pb.FlockRequest))
	default:
		return nil, fserror.New(fserror.Unimplemented, "unsupported s0vp operation")
	}
}

func (s *Server) sendResult(state *connState, frame volproto.Frame, resp any, err error) {
	if err != nil {
		s.sendStatusError(state, frame, err)
		return
	}
	payload, err := volproto.EncodeResponse(frame.Op, resp)
	if err != nil {
		s.sendError(state, frame, fserror.Internal, err.Error())
		return
	}
	s.sendPayload(state, frame, payload)
}

func (s *Server) sendStatusError(state *connState, frame volproto.Frame, err error) {
	s.sendErrorWithRedirect(state, frame, fserror.CodeOf(err), fserror.MessageOf(err), fserror.RedirectOf(err))
}

func (s *Server) sendError(state *connState, frame volproto.Frame, code fserror.Code, msg string) {
	s.sendErrorWithRedirect(state, frame, code, msg, nil)
}

func (s *Server) sendErrorWithRedirect(state *connState, frame volproto.Frame, code fserror.Code, msg string, redirect *pb.PrimaryRedirect) {
	_ = s.writeFrame(state, volproto.Frame{
		RequestID: frame.RequestID,
		Op:        frame.Op,
		Flags:     volproto.FlagError,
		Payload:   volproto.WriteErrorWithRedirect(volproto.StatusCode(code), msg, redirect),
	})
}

func (s *Server) sendPayload(state *connState, frame volproto.Frame, payload []byte) {
	_ = s.writeFrame(state, volproto.Frame{
		RequestID: frame.RequestID,
		Op:        frame.Op,
		Payload:   payload,
	})
}

func (s *Server) writeFrame(state *connState, frame volproto.Frame) error {
	state.sendMu.Lock()
	defer state.sendMu.Unlock()
	return volproto.WriteFrame(state.conn, frame)
}
