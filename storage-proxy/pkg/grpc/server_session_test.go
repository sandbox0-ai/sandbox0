package grpc

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

func TestMountSessionFileLifecycle(t *testing.T) {
	t.Parallel()

	volCtx := newMountedS0FSVolumeContext(t, "vol-1", "team-a")
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, nil, nil)

	stream := newFakeMountSessionServer(authContext("team-a", ""))
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.MountSession(stream)
	}()

	stream.requests <- &pb.MountSessionRequest{
		VolumeId:  "vol-1",
		RequestId: 1,
		Payload: &pb.MountSessionRequest_Create{
			Create: &pb.CreateRequest{
				VolumeId: "vol-1",
				Parent:   1,
				Name:     "hello.txt",
				Mode:     0o644,
			},
		},
	}
	createResp := <-stream.responses
	if createResp.GetNode() == nil || createResp.GetNode().HandleId == 0 {
		t.Fatalf("create response = %+v", createResp)
	}

	stream.requests <- &pb.MountSessionRequest{
		VolumeId:  "vol-1",
		RequestId: 2,
		Payload: &pb.MountSessionRequest_Write{
			Write: &pb.WriteRequest{
				VolumeId: "vol-1",
				Inode:    createResp.GetNode().Inode,
				HandleId: createResp.GetNode().HandleId,
				Data:     []byte("hello"),
			},
		},
	}
	writeResp := <-stream.responses
	if writeResp.GetWrite() == nil || writeResp.GetWrite().BytesWritten != 5 {
		t.Fatalf("write response = %+v", writeResp)
	}

	stream.requests <- &pb.MountSessionRequest{
		VolumeId:  "vol-1",
		RequestId: 3,
		Payload: &pb.MountSessionRequest_Read{
			Read: &pb.ReadRequest{
				VolumeId: "vol-1",
				Inode:    createResp.GetNode().Inode,
				HandleId: createResp.GetNode().HandleId,
				Size:     16,
			},
		},
	}
	readResp := <-stream.responses
	if !bytes.Equal(readResp.GetRead().Data, []byte("hello")) {
		t.Fatalf("read response = %+v", readResp)
	}

	close(stream.requests)
	if err := <-errCh; err != nil {
		t.Fatalf("MountSession() error = %v", err)
	}
}

func TestMountSessionPushesInvalidateEvents(t *testing.T) {
	t.Parallel()

	hub := notify.NewHub(logrus.New(), 4)
	server := newTestFileSystemServer(&fakeVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": {VolumeID: "vol-1", TeamID: "team-a"},
		},
	}, nil, hub)

	ctx, cancel := context.WithCancel(authContext("team-a", ""))
	defer cancel()

	stream := newFakeMountSessionServer(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.MountSession(stream)
	}()

	stream.requests <- &pb.MountSessionRequest{
		VolumeId:  "vol-1",
		RequestId: 1,
		Payload: &pb.MountSessionRequest_Heartbeat{
			Heartbeat: &pb.MountSessionHeartbeat{},
		},
	}
	resp := <-stream.responses
	if resp.GetAck() == nil {
		t.Fatalf("heartbeat response = %+v", resp)
	}

	hub.Publish(&pb.WatchEvent{
		VolumeId:     "vol-1",
		EventType:    pb.WatchEventType_WATCH_EVENT_TYPE_INVALIDATE,
		InvalidateId: "invalidate-1",
	})
	eventResp := <-stream.responses
	if eventResp.GetWatchEvent() == nil || eventResp.GetWatchEvent().InvalidateId != "invalidate-1" {
		t.Fatalf("event response = %+v", eventResp)
	}

	close(stream.requests)
	if err := <-errCh; err != nil {
		t.Fatalf("MountSession() error = %v", err)
	}
}

type fakeMountSessionServer struct {
	ctx       context.Context
	requests  chan *pb.MountSessionRequest
	responses chan *pb.MountSessionResponse
}

func newFakeMountSessionServer(ctx context.Context) *fakeMountSessionServer {
	return &fakeMountSessionServer{
		ctx:       ctx,
		requests:  make(chan *pb.MountSessionRequest, 8),
		responses: make(chan *pb.MountSessionResponse, 8),
	}
}

func (s *fakeMountSessionServer) SetHeader(metadata.MD) error  { return nil }
func (s *fakeMountSessionServer) SendHeader(metadata.MD) error { return nil }
func (s *fakeMountSessionServer) SetTrailer(metadata.MD)       {}
func (s *fakeMountSessionServer) Context() context.Context     { return s.ctx }
func (s *fakeMountSessionServer) Send(resp *pb.MountSessionResponse) error {
	s.responses <- resp
	return nil
}
func (s *fakeMountSessionServer) SendMsg(any) error { return nil }
func (s *fakeMountSessionServer) RecvMsg(any) error { return nil }
func (s *fakeMountSessionServer) Recv() (*pb.MountSessionRequest, error) {
	req, ok := <-s.requests
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

var _ pb.FileSystem_MountSessionServer = (*fakeMountSessionServer)(nil)
