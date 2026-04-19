package volumefuse

import (
	"context"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type Session interface {
	Close()
	Lookup(context.Context, *pb.LookupRequest) (*pb.NodeResponse, error)
	GetAttr(context.Context, *pb.GetAttrRequest) (*pb.GetAttrResponse, error)
	SetAttr(context.Context, *pb.SetAttrRequest) (*pb.SetAttrResponse, error)
	Mkdir(context.Context, *pb.MkdirRequest) (*pb.NodeResponse, error)
	Create(context.Context, *pb.CreateRequest) (*pb.NodeResponse, error)
	Unlink(context.Context, *pb.UnlinkRequest) (*pb.Empty, error)
	Rmdir(context.Context, *pb.RmdirRequest) (*pb.Empty, error)
	Rename(context.Context, *pb.RenameRequest) (*pb.Empty, error)
	Link(context.Context, *pb.LinkRequest) (*pb.NodeResponse, error)
	Symlink(context.Context, *pb.SymlinkRequest) (*pb.NodeResponse, error)
	Readlink(context.Context, *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error)
	Access(context.Context, *pb.AccessRequest) (*pb.Empty, error)
	Open(context.Context, *pb.OpenRequest) (*pb.OpenResponse, error)
	Read(context.Context, *pb.ReadRequest) (*pb.ReadResponse, error)
	Write(context.Context, *pb.WriteRequest) (*pb.WriteResponse, error)
	Release(context.Context, *pb.ReleaseRequest) (*pb.Empty, error)
	Flush(context.Context, *pb.FlushRequest) (*pb.Empty, error)
	Fsync(context.Context, *pb.FsyncRequest) (*pb.Empty, error)
	Fallocate(context.Context, *pb.FallocateRequest) (*pb.Empty, error)
	CopyFileRange(context.Context, *pb.CopyFileRangeRequest) (*pb.CopyFileRangeResponse, error)
	OpenDir(context.Context, *pb.OpenDirRequest) (*pb.OpenDirResponse, error)
	ReadDir(context.Context, *pb.ReadDirRequest) (*pb.ReadDirResponse, error)
	ReleaseDir(context.Context, *pb.ReleaseDirRequest) (*pb.Empty, error)
	StatFs(context.Context, *pb.StatFsRequest) (*pb.StatFsResponse, error)
	GetXattr(context.Context, *pb.GetXattrRequest) (*pb.GetXattrResponse, error)
	SetXattr(context.Context, *pb.SetXattrRequest) (*pb.Empty, error)
	ListXattr(context.Context, *pb.ListXattrRequest) (*pb.ListXattrResponse, error)
	RemoveXattr(context.Context, *pb.RemoveXattrRequest) (*pb.Empty, error)
	Mknod(context.Context, *pb.MknodRequest) (*pb.NodeResponse, error)
	GetLk(context.Context, *pb.GetLkRequest) (*pb.GetLkResponse, error)
	SetLk(context.Context, *pb.SetLkRequest) (*pb.Empty, error)
	SetLkw(context.Context, *pb.SetLkRequest) (*pb.Empty, error)
	Flock(context.Context, *pb.FlockRequest) (*pb.Empty, error)
}

type ReadIntoSession interface {
	ReadInto(context.Context, *pb.ReadRequest, []byte) (int, bool, error)
}
