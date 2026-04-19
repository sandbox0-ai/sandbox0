package volumefuse

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type cacheEvent struct {
	op     string
	node   uint64
	parent uint64
	name   string
	off    int64
	length int64
	data   []byte
}

type recordingKernelCache struct {
	events      []cacheEvent
	inodeStatus fuse.Status
	storeStatus fuse.Status
	entryStatus fuse.Status
}

func (c *recordingKernelCache) InodeNotify(node uint64, off int64, length int64) fuse.Status {
	c.events = append(c.events, cacheEvent{op: "inode", node: node, off: off, length: length})
	if c.inodeStatus != fuse.OK {
		return c.inodeStatus
	}
	return fuse.OK
}

func (c *recordingKernelCache) InodeNotifyStoreCache(node uint64, offset int64, data []byte) fuse.Status {
	copied := append([]byte(nil), data...)
	c.events = append(c.events, cacheEvent{op: "store", node: node, off: offset, data: copied})
	if c.storeStatus != fuse.OK {
		return c.storeStatus
	}
	return fuse.OK
}

func (c *recordingKernelCache) EntryNotify(parent uint64, name string) fuse.Status {
	c.events = append(c.events, cacheEvent{op: "entry", parent: parent, name: name})
	if c.entryStatus != fuse.OK {
		return c.entryStatus
	}
	return fuse.OK
}

type writeSession struct {
	Session
	bytesWritten int64
	err          error
}

func (s *writeSession) Write(context.Context, *pb.WriteRequest) (*pb.WriteResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &pb.WriteResponse{BytesWritten: s.bytesWritten}, nil
}

type setattrSession struct {
	Session
	err error
}

func (s *setattrSession) SetAttr(context.Context, *pb.SetAttrRequest) (*pb.SetAttrResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &pb.SetAttrResponse{Attr: &pb.GetAttrResponse{Ino: 42, Size: 3, Mode: 0o100644}}, nil
}

type readIntoSession struct {
	Session
	calledReadInto bool
	calledRead     bool
}

func (s *readIntoSession) ReadInto(_ context.Context, req *pb.ReadRequest, dest []byte) (int, bool, error) {
	s.calledReadInto = true
	if req.Size != 4 {
		return 0, false, errors.New("unexpected read size")
	}
	return copy(dest, []byte("data")), true, nil
}

func (s *readIntoSession) Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.calledRead = true
	return &pb.ReadResponse{Data: []byte("slow")}, nil
}

func TestWriteStoresKernelCacheAfterSuccessfulWrite(t *testing.T) {
	fs := New("vol-1", time.Second, &writeSession{bytesWritten: 3})
	cache := &recordingKernelCache{}
	fs.setKernelCache(cache)

	written, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: 42},
		Offset:   5,
	}, []byte("abcdef"))
	if st != fuse.OK {
		t.Fatalf("Write() status = %v, want OK", st)
	}
	if written != 3 {
		t.Fatalf("Write() bytes = %d, want 3", written)
	}
	if len(cache.events) != 3 {
		t.Fatalf("cache events = %+v, want invalidate, attr invalidate, store", cache.events)
	}
	if got := cache.events[0]; got.op != "inode" || got.node != 42 || got.off != 5 || got.length != 6 {
		t.Fatalf("pre-write invalidate = %+v", got)
	}
	if got := cache.events[1]; got.op != "inode" || got.node != 42 || got.off != -1 || got.length != 0 {
		t.Fatalf("attr invalidate = %+v", got)
	}
	if got := cache.events[2]; got.op != "store" || got.node != 42 || got.off != 5 || !bytes.Equal(got.data, []byte("abc")) {
		t.Fatalf("store cache = %+v", got)
	}
}

func TestWriteErrorDoesNotStoreKernelCache(t *testing.T) {
	fs := New("vol-1", time.Second, &writeSession{err: errors.New("write failed")})
	cache := &recordingKernelCache{}
	fs.setKernelCache(cache)

	_, st := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}}, []byte("abc"))
	if st == fuse.OK {
		t.Fatal("Write() status = OK, want error")
	}
	for _, event := range cache.events {
		if event.op == "store" {
			t.Fatalf("store cache event on failed write: %+v", cache.events)
		}
	}
}

func TestStoreCacheENOSYSDisablesFutureStores(t *testing.T) {
	fs := New("vol-1", time.Second, &writeSession{bytesWritten: 3})
	cache := &recordingKernelCache{storeStatus: fuse.ENOSYS}
	fs.setKernelCache(cache)

	for i := 0; i < 2; i++ {
		if _, st := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 42}}, []byte("abc")); st != fuse.OK {
			t.Fatalf("Write(%d) status = %v, want OK", i, st)
		}
	}
	storeEvents := 0
	for _, event := range cache.events {
		if event.op == "store" {
			storeEvents++
		}
	}
	if storeEvents != 1 {
		t.Fatalf("store cache events = %d, want 1", storeEvents)
	}
}

func TestSetAttrSizeInvalidatesKernelInodeAroundMutation(t *testing.T) {
	fs := New("vol-1", time.Second, &setattrSession{})
	cache := &recordingKernelCache{}
	fs.setKernelCache(cache)

	st := fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: 42},
			Valid:    fuse.FATTR_SIZE,
			Size:     3,
		},
	}, &fuse.AttrOut{})
	if st != fuse.OK {
		t.Fatalf("SetAttr() status = %v, want OK", st)
	}
	if len(cache.events) != 2 {
		t.Fatalf("cache events = %+v, want two inode invalidates", cache.events)
	}
	for _, got := range cache.events {
		if got.op != "inode" || got.node != 42 || got.off != 0 || got.length != 0 {
			t.Fatalf("inode invalidate = %+v", got)
		}
	}
}

func TestReadUsesReadIntoSession(t *testing.T) {
	session := &readIntoSession{}
	fs := New("vol-1", time.Second, session)

	result, st := fs.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 42},
		Size:     4,
	}, make([]byte, 8))
	if st != fuse.OK {
		t.Fatalf("Read() status = %v, want OK", st)
	}
	data, st := result.Bytes(make([]byte, result.Size()))
	if st != fuse.OK {
		t.Fatalf("ReadResult.Bytes() status = %v, want OK", st)
	}
	if !bytes.Equal(data, []byte("data")) {
		t.Fatalf("Read() data = %q, want data", data)
	}
	if !session.calledReadInto {
		t.Fatal("ReadInto was not called")
	}
	if session.calledRead {
		t.Fatal("Read fallback was called")
	}
}
