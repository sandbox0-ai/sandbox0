package volproto

import (
	"bytes"
	"fmt"
	"testing"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"google.golang.org/protobuf/proto"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := Frame{
		RequestID: 42,
		Op:        OpWrite,
		Flags:     FlagError,
		Payload:   []byte("payload"),
	}
	if err := WriteFrame(&buf, want); err != nil {
		t.Fatalf("WriteFrame() error = %v", err)
	}
	got, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("ReadFrame() error = %v", err)
	}
	if got.RequestID != want.RequestID || got.Op != want.Op || got.Flags != want.Flags || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("ReadFrame() = %+v, want %+v", got, want)
	}
}

func TestErrorRoundTrip(t *testing.T) {
	payload := WriteError(StatusNotFound, "missing")
	code, msg, redirect, err := ReadError(payload)
	if err != nil {
		t.Fatalf("ReadError() error = %v", err)
	}
	if redirect != nil {
		t.Fatalf("ReadError() redirect = %v, want nil", redirect)
	}
	if code != StatusNotFound || msg != "missing" {
		t.Fatalf("ReadError() = (%d, %q), want (%d, missing)", code, msg, StatusNotFound)
	}

	redirectPayload := WriteErrorWithRedirect(StatusFailedPrecondition, "redirect", &pb.PrimaryRedirect{
		VolumeId:      "vol-1",
		PrimaryNodeId: "node-a",
		PrimaryAddr:   "10.0.0.8:8082",
		Epoch:         3,
	})
	code, msg, redirect, err = ReadError(redirectPayload)
	if err != nil {
		t.Fatalf("ReadError(redirect) error = %v", err)
	}
	if code != StatusFailedPrecondition || msg != "redirect" {
		t.Fatalf("ReadError(redirect) = (%d, %q), want (%d, redirect)", code, msg, StatusFailedPrecondition)
	}
	if redirect == nil || redirect.PrimaryAddr != "10.0.0.8:8082" || redirect.Epoch != 3 {
		t.Fatalf("ReadError(redirect) redirect = %+v", redirect)
	}
}

func TestRequestRoundTrip(t *testing.T) {
	actor := &pb.PosixActor{Pid: 11, Uid: 12, Gids: []uint32{13, 14}}
	attr := &pb.GetAttrResponse{
		Ino:       2,
		Mode:      0o100644,
		Nlink:     1,
		Uid:       1000,
		Gid:       1000,
		Rdev:      9,
		Size:      123,
		Blocks:    1,
		AtimeSec:  20,
		AtimeNsec: 21,
		MtimeSec:  22,
		MtimeNsec: 23,
		CtimeSec:  24,
		CtimeNsec: 25,
	}
	lock := &pb.FileLock{Start: 1, End: 10, Typ: 2, Pid: 30}
	cases := []struct {
		op  Op
		req proto.Message
	}{
		{OpLookup, &pb.LookupRequest{Parent: 1, Name: "file", Actor: actor}},
		{OpGetAttr, &pb.GetAttrRequest{Inode: 2, Actor: actor}},
		{OpSetAttr, &pb.SetAttrRequest{Inode: 2, Valid: 7, Attr: attr, HandleId: 8, Actor: actor}},
		{OpMkdir, &pb.MkdirRequest{Parent: 1, Name: "dir", Mode: 0o755, Umask: 0o022, Actor: actor}},
		{OpCreate, &pb.CreateRequest{Parent: 1, Name: "new", Mode: 0o644, Flags: 65, Umask: 0o022, Actor: actor}},
		{OpUnlink, &pb.UnlinkRequest{Parent: 1, Name: "gone", Actor: actor}},
		{OpRmdir, &pb.RmdirRequest{Parent: 1, Name: "empty", Actor: actor}},
		{OpRename, &pb.RenameRequest{OldParent: 1, OldName: "old", NewParent: 2, NewName: "new", Flags: 3, Actor: actor}},
		{OpLink, &pb.LinkRequest{Inode: 3, NewParent: 4, NewName: "hard", Actor: actor}},
		{OpSymlink, &pb.SymlinkRequest{Parent: 1, Name: "link", Target: "/target", Actor: actor}},
		{OpReadlink, &pb.ReadlinkRequest{Inode: 3, Actor: actor}},
		{OpAccess, &pb.AccessRequest{Inode: 2, Mask: 7, Uid: 1000, Gids: []uint32{1000, 1001}, Actor: actor}},
		{OpOpen, &pb.OpenRequest{Inode: 2, Flags: 2, Actor: actor}},
		{OpRead, &pb.ReadRequest{Inode: 2, Offset: 3, Size: 4, HandleId: 5, Actor: actor}},
		{OpWrite, &pb.WriteRequest{Inode: 2, Offset: 3, Data: []byte("hello"), HandleId: 5, Actor: actor}},
		{OpRelease, &pb.ReleaseRequest{Inode: 2, HandleId: 5, Actor: actor}},
		{OpFlush, &pb.FlushRequest{HandleId: 5, Actor: actor}},
		{OpFsync, &pb.FsyncRequest{HandleId: 5, Datasync: true, Actor: actor}},
		{OpFallocate, &pb.FallocateRequest{Inode: 2, Mode: 1, Offset: 3, Length: 4, HandleId: 5, Actor: actor}},
		{OpCopyFileRange, &pb.CopyFileRangeRequest{InodeIn: 2, HandleIn: 3, OffsetIn: 4, InodeOut: 5, HandleOut: 6, OffsetOut: 7, Length: 8, Flags: 9, Actor: actor}},
		{OpOpenDir, &pb.OpenDirRequest{Inode: 2, Flags: 3, Actor: actor}},
		{OpReadDir, &pb.ReadDirRequest{Inode: 2, HandleId: 3, Offset: 4, Size: 5, Plus: true, Actor: actor}},
		{OpReleaseDir, &pb.ReleaseDirRequest{Inode: 2, HandleId: 3, Actor: actor}},
		{OpStatFs, &pb.StatFsRequest{Actor: actor}},
		{OpGetXattr, &pb.GetXattrRequest{Inode: 2, Name: "user.key", Size: 64, Actor: actor}},
		{OpSetXattr, &pb.SetXattrRequest{Inode: 2, Name: "user.key", Value: []byte("value"), Flags: 1, Actor: actor}},
		{OpListXattr, &pb.ListXattrRequest{Inode: 2, Size: 64, Actor: actor}},
		{OpRemoveXattr, &pb.RemoveXattrRequest{Inode: 2, Name: "user.key", Actor: actor}},
		{OpMknod, &pb.MknodRequest{Parent: 1, Name: "dev", Mode: 0o600, Rdev: 99, Umask: 0o022, Actor: actor}},
		{OpGetLk, &pb.GetLkRequest{Inode: 2, HandleId: 3, Owner: 4, Lock: lock, Actor: actor}},
		{OpSetLk, &pb.SetLkRequest{Inode: 2, HandleId: 3, Owner: 4, Lock: lock, Block: false, Actor: actor}},
		{OpSetLkw, &pb.SetLkRequest{Inode: 2, HandleId: 3, Owner: 4, Lock: lock, Block: true, Actor: actor}},
		{OpFlock, &pb.FlockRequest{Inode: 2, HandleId: 3, Owner: 4, Typ: 5, Block: true, Actor: actor}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("op_%d", tc.op), func(t *testing.T) {
			setVolumeID(tc.req, "vol-1")
			payload, err := EncodeRequest(tc.op, tc.req)
			if err != nil {
				t.Fatalf("EncodeRequest() error = %v", err)
			}
			got, err := DecodeRequest(tc.op, "vol-1", payload)
			if err != nil {
				t.Fatalf("DecodeRequest() error = %v", err)
			}
			if !proto.Equal(tc.req, got.(proto.Message)) {
				t.Fatalf("round trip mismatch\n got: %v\nwant: %v", got, tc.req)
			}
		})
	}
}

func TestResponseRoundTrip(t *testing.T) {
	attr := &pb.GetAttrResponse{Ino: 2, Mode: 0o100644, Nlink: 1, Uid: 1, Gid: 2, Size: 3}
	cases := []struct {
		op   Op
		resp proto.Message
	}{
		{OpHeartbeat, &pb.Empty{}},
		{OpLookup, &pb.NodeResponse{Inode: 2, Generation: 1, Attr: attr, HandleId: 9}},
		{OpGetAttr, attr},
		{OpSetAttr, &pb.SetAttrResponse{Attr: attr}},
		{OpReadlink, &pb.ReadlinkResponse{Target: "/target"}},
		{OpOpen, &pb.OpenResponse{HandleId: 7}},
		{OpRead, &pb.ReadResponse{Data: []byte("hello"), Eof: true}},
		{OpWrite, &pb.WriteResponse{BytesWritten: 5}},
		{OpCopyFileRange, &pb.CopyFileRangeResponse{BytesCopied: 11}},
		{OpOpenDir, &pb.OpenDirResponse{HandleId: 8}},
		{OpReadDir, &pb.ReadDirResponse{Entries: []*pb.DirEntry{{Inode: 2, Offset: 3, Name: "x", Type: 4, Attr: attr}}, Eof: true}},
		{OpStatFs, &pb.StatFsResponse{Blocks: 1, Bfree: 2, Bavail: 3, Files: 4, Ffree: 5, Bsize: 4096, Namelen: 255, Frsize: 4096}},
		{OpGetXattr, &pb.GetXattrResponse{Value: []byte("value")}},
		{OpListXattr, &pb.ListXattrResponse{Data: []byte("user.key\x00")}},
		{OpGetLk, &pb.GetLkResponse{Lock: &pb.FileLock{Start: 1, End: 2, Typ: 3, Pid: 4}}},
		{OpWatchEvent, &pb.WatchEvent{VolumeId: "vol-1", EventType: pb.WatchEventType_WATCH_EVENT_TYPE_WRITE, Path: "/x", Inode: 2, TimestampUnix: 123}},
		{OpUnlink, &pb.Empty{}},
		{OpRmdir, &pb.Empty{}},
		{OpRename, &pb.Empty{}},
		{OpAccess, &pb.Empty{}},
		{OpRelease, &pb.Empty{}},
		{OpFlush, &pb.Empty{}},
		{OpFsync, &pb.Empty{}},
		{OpFallocate, &pb.Empty{}},
		{OpReleaseDir, &pb.Empty{}},
		{OpSetXattr, &pb.Empty{}},
		{OpRemoveXattr, &pb.Empty{}},
		{OpSetLk, &pb.Empty{}},
		{OpSetLkw, &pb.Empty{}},
		{OpFlock, &pb.Empty{}},
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("op_%d", tc.op), func(t *testing.T) {
			payload, err := EncodeResponse(tc.op, tc.resp)
			if err != nil {
				t.Fatalf("EncodeResponse() error = %v", err)
			}
			got, err := DecodeResponse(tc.op, payload)
			if err != nil {
				t.Fatalf("DecodeResponse() error = %v", err)
			}
			if !proto.Equal(tc.resp, got.(proto.Message)) {
				t.Fatalf("round trip mismatch\n got: %v\nwant: %v", got, tc.resp)
			}
		})
	}
}

func setVolumeID(msg proto.Message, volumeID string) {
	switch v := msg.(type) {
	case *pb.LookupRequest:
		v.VolumeId = volumeID
	case *pb.GetAttrRequest:
		v.VolumeId = volumeID
	case *pb.SetAttrRequest:
		v.VolumeId = volumeID
	case *pb.MkdirRequest:
		v.VolumeId = volumeID
	case *pb.CreateRequest:
		v.VolumeId = volumeID
	case *pb.UnlinkRequest:
		v.VolumeId = volumeID
	case *pb.RmdirRequest:
		v.VolumeId = volumeID
	case *pb.RenameRequest:
		v.VolumeId = volumeID
	case *pb.LinkRequest:
		v.VolumeId = volumeID
	case *pb.SymlinkRequest:
		v.VolumeId = volumeID
	case *pb.ReadlinkRequest:
		v.VolumeId = volumeID
	case *pb.AccessRequest:
		v.VolumeId = volumeID
	case *pb.OpenRequest:
		v.VolumeId = volumeID
	case *pb.ReadRequest:
		v.VolumeId = volumeID
	case *pb.WriteRequest:
		v.VolumeId = volumeID
	case *pb.ReleaseRequest:
		v.VolumeId = volumeID
	case *pb.FlushRequest:
		v.VolumeId = volumeID
	case *pb.FsyncRequest:
		v.VolumeId = volumeID
	case *pb.FallocateRequest:
		v.VolumeId = volumeID
	case *pb.CopyFileRangeRequest:
		v.VolumeId = volumeID
	case *pb.OpenDirRequest:
		v.VolumeId = volumeID
	case *pb.ReadDirRequest:
		v.VolumeId = volumeID
	case *pb.ReleaseDirRequest:
		v.VolumeId = volumeID
	case *pb.StatFsRequest:
		v.VolumeId = volumeID
	case *pb.GetXattrRequest:
		v.VolumeId = volumeID
	case *pb.SetXattrRequest:
		v.VolumeId = volumeID
	case *pb.ListXattrRequest:
		v.VolumeId = volumeID
	case *pb.RemoveXattrRequest:
		v.VolumeId = volumeID
	case *pb.MknodRequest:
		v.VolumeId = volumeID
	case *pb.GetLkRequest:
		v.VolumeId = volumeID
	case *pb.SetLkRequest:
		v.VolumeId = volumeID
	case *pb.FlockRequest:
		v.VolumeId = volumeID
	}
}
