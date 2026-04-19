package volproto

import (
	"fmt"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

func EncodeMountVolumeRequest(token string, req *pb.MountVolumeRequest) []byte {
	w := NewWriter()
	w.String(token)
	w.String(req.GetVolumeId())
	cfg := req.GetConfig()
	if cfg == nil {
		cfg = &pb.VolumeConfig{}
	}
	w.String(cfg.CacheSize)
	w.I32(cfg.Prefetch)
	w.String(cfg.BufferSize)
	w.Bool(cfg.Writeback)
	return w.Bytes()
}

func DecodeMountVolumeRequest(payload []byte) (string, *pb.MountVolumeRequest, error) {
	r := NewReader(payload)
	token, err := r.String()
	if err != nil {
		return "", nil, err
	}
	volumeID, err := r.String()
	if err != nil {
		return "", nil, err
	}
	cacheSize, err := r.String()
	if err != nil {
		return "", nil, err
	}
	prefetch, err := r.I32()
	if err != nil {
		return "", nil, err
	}
	bufferSize, err := r.String()
	if err != nil {
		return "", nil, err
	}
	writeback, err := r.Bool()
	if err != nil {
		return "", nil, err
	}
	return token, &pb.MountVolumeRequest{
		VolumeId: volumeID,
		Config: &pb.VolumeConfig{
			CacheSize:  cacheSize,
			Prefetch:   prefetch,
			BufferSize: bufferSize,
			Writeback:  writeback,
		},
	}, r.Err()
}

func EncodeMountVolumeResponse(resp *pb.MountVolumeResponse) []byte {
	w := NewWriter()
	w.String(resp.GetVolumeId())
	w.I64(resp.GetMountedAt())
	w.String(resp.GetMountSessionId())
	w.String(resp.GetMountSessionSecret())
	return w.Bytes()
}

func DecodeMountVolumeResponse(payload []byte) (*pb.MountVolumeResponse, error) {
	r := NewReader(payload)
	volumeID, err := r.String()
	if err != nil {
		return nil, err
	}
	mountedAt, err := r.I64()
	if err != nil {
		return nil, err
	}
	sessionID, err := r.String()
	if err != nil {
		return nil, err
	}
	sessionSecret, err := r.String()
	if err != nil {
		return nil, err
	}
	return &pb.MountVolumeResponse{
		VolumeId:           volumeID,
		MountedAt:          mountedAt,
		MountSessionId:     sessionID,
		MountSessionSecret: sessionSecret,
	}, r.Err()
}

func EncodeHelloRequest(volumeID, sessionID, sessionSecret string) []byte {
	w := NewWriter()
	w.String(volumeID)
	w.String(sessionID)
	w.String(sessionSecret)
	return w.Bytes()
}

func DecodeHelloRequest(payload []byte) (volumeID, sessionID, sessionSecret string, err error) {
	r := NewReader(payload)
	if volumeID, err = r.String(); err != nil {
		return
	}
	if sessionID, err = r.String(); err != nil {
		return
	}
	if sessionSecret, err = r.String(); err != nil {
		return
	}
	err = r.Err()
	return
}

func EncodeUnmountVolumeRequest(req *pb.UnmountVolumeRequest) []byte {
	w := NewWriter()
	w.String(req.GetVolumeId())
	w.String(req.GetMountSessionId())
	return w.Bytes()
}

func DecodeUnmountVolumeRequest(payload []byte) (*pb.UnmountVolumeRequest, error) {
	r := NewReader(payload)
	volumeID, err := r.String()
	if err != nil {
		return nil, err
	}
	sessionID, err := r.String()
	if err != nil {
		return nil, err
	}
	return &pb.UnmountVolumeRequest{VolumeId: volumeID, MountSessionId: sessionID}, r.Err()
}

func EncodeRequest(op Op, req any) ([]byte, error) {
	w := NewWriter()
	switch op {
	case OpLookup:
		v := req.(*pb.LookupRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		WriteActor(w, v.Actor)
	case OpGetAttr:
		v := req.(*pb.GetAttrRequest)
		w.U64(v.Inode)
		WriteActor(w, v.Actor)
	case OpSetAttr:
		v := req.(*pb.SetAttrRequest)
		w.U64(v.Inode)
		w.U32(v.Valid)
		WriteAttr(w, v.Attr)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpMkdir:
		v := req.(*pb.MkdirRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		w.U32(v.Mode)
		w.U32(v.Umask)
		WriteActor(w, v.Actor)
	case OpCreate:
		v := req.(*pb.CreateRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		w.U32(v.Mode)
		w.U32(v.Flags)
		w.U32(v.Umask)
		WriteActor(w, v.Actor)
	case OpUnlink:
		v := req.(*pb.UnlinkRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		WriteActor(w, v.Actor)
	case OpRmdir:
		v := req.(*pb.RmdirRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		WriteActor(w, v.Actor)
	case OpRename:
		v := req.(*pb.RenameRequest)
		w.U64(v.OldParent)
		w.String(v.OldName)
		w.U64(v.NewParent)
		w.String(v.NewName)
		w.U32(v.Flags)
		WriteActor(w, v.Actor)
	case OpLink:
		v := req.(*pb.LinkRequest)
		w.U64(v.Inode)
		w.U64(v.NewParent)
		w.String(v.NewName)
		WriteActor(w, v.Actor)
	case OpSymlink:
		v := req.(*pb.SymlinkRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		w.String(v.Target)
		WriteActor(w, v.Actor)
	case OpReadlink:
		v := req.(*pb.ReadlinkRequest)
		w.U64(v.Inode)
		WriteActor(w, v.Actor)
	case OpAccess:
		v := req.(*pb.AccessRequest)
		w.U64(v.Inode)
		w.U32(v.Mask)
		w.U32(v.Uid)
		w.U32(uint32(len(v.Gids)))
		for _, gid := range v.Gids {
			w.U32(gid)
		}
		WriteActor(w, v.Actor)
	case OpOpen:
		v := req.(*pb.OpenRequest)
		w.U64(v.Inode)
		w.U32(v.Flags)
		WriteActor(w, v.Actor)
	case OpRead:
		v := req.(*pb.ReadRequest)
		w.U64(v.Inode)
		w.I64(v.Offset)
		w.I64(v.Size)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpWrite:
		v := req.(*pb.WriteRequest)
		w.U64(v.Inode)
		w.I64(v.Offset)
		w.BytesRaw(v.Data)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpRelease:
		v := req.(*pb.ReleaseRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpFlush:
		v := req.(*pb.FlushRequest)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpFsync:
		v := req.(*pb.FsyncRequest)
		w.U64(v.HandleId)
		w.Bool(v.Datasync)
		WriteActor(w, v.Actor)
	case OpFallocate:
		v := req.(*pb.FallocateRequest)
		w.U64(v.Inode)
		w.U32(v.Mode)
		w.I64(v.Offset)
		w.I64(v.Length)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpCopyFileRange:
		v := req.(*pb.CopyFileRangeRequest)
		w.U64(v.InodeIn)
		w.U64(v.HandleIn)
		w.U64(v.OffsetIn)
		w.U64(v.InodeOut)
		w.U64(v.HandleOut)
		w.U64(v.OffsetOut)
		w.U64(v.Length)
		w.U32(v.Flags)
		WriteActor(w, v.Actor)
	case OpOpenDir:
		v := req.(*pb.OpenDirRequest)
		w.U64(v.Inode)
		w.U32(v.Flags)
		WriteActor(w, v.Actor)
	case OpReadDir:
		v := req.(*pb.ReadDirRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		w.I64(v.Offset)
		w.U32(v.Size)
		w.Bool(v.Plus)
		WriteActor(w, v.Actor)
	case OpReleaseDir:
		v := req.(*pb.ReleaseDirRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		WriteActor(w, v.Actor)
	case OpStatFs:
		v := req.(*pb.StatFsRequest)
		WriteActor(w, v.Actor)
	case OpGetXattr:
		v := req.(*pb.GetXattrRequest)
		w.U64(v.Inode)
		w.String(v.Name)
		w.U32(v.Size)
		WriteActor(w, v.Actor)
	case OpSetXattr:
		v := req.(*pb.SetXattrRequest)
		w.U64(v.Inode)
		w.String(v.Name)
		w.BytesRaw(v.Value)
		w.U32(v.Flags)
		WriteActor(w, v.Actor)
	case OpListXattr:
		v := req.(*pb.ListXattrRequest)
		w.U64(v.Inode)
		w.I32(v.Size)
		WriteActor(w, v.Actor)
	case OpRemoveXattr:
		v := req.(*pb.RemoveXattrRequest)
		w.U64(v.Inode)
		w.String(v.Name)
		WriteActor(w, v.Actor)
	case OpMknod:
		v := req.(*pb.MknodRequest)
		w.U64(v.Parent)
		w.String(v.Name)
		w.U32(v.Mode)
		w.U32(v.Rdev)
		w.U32(v.Umask)
		WriteActor(w, v.Actor)
	case OpGetLk:
		v := req.(*pb.GetLkRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		w.U64(v.Owner)
		WriteLock(w, v.Lock)
		WriteActor(w, v.Actor)
	case OpSetLk, OpSetLkw:
		v := req.(*pb.SetLkRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		w.U64(v.Owner)
		WriteLock(w, v.Lock)
		w.Bool(v.Block)
		WriteActor(w, v.Actor)
	case OpFlock:
		v := req.(*pb.FlockRequest)
		w.U64(v.Inode)
		w.U64(v.HandleId)
		w.U64(v.Owner)
		w.U32(v.Typ)
		w.Bool(v.Block)
		WriteActor(w, v.Actor)
	default:
		return nil, fmt.Errorf("unsupported request op %d", op)
	}
	return w.Bytes(), nil
}

func DecodeRequest(op Op, volumeID string, payload []byte) (any, error) {
	r := NewReader(payload)
	var out any
	var err error
	switch op {
	case OpLookup:
		v := &pb.LookupRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpGetAttr:
		v := &pb.GetAttrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpSetAttr:
		v := &pb.SetAttrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Valid, err = r.U32()
		}
		if err == nil {
			v.Attr, err = ReadAttr(r)
		}
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpMkdir:
		v := &pb.MkdirRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Mode, err = r.U32()
		}
		if err == nil {
			v.Umask, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpCreate:
		v := &pb.CreateRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Mode, err = r.U32()
		}
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Umask, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpUnlink:
		v := &pb.UnlinkRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpRmdir:
		v := &pb.RmdirRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpRename:
		v := &pb.RenameRequest{VolumeId: volumeID}
		v.OldParent, err = r.U64()
		if err == nil {
			v.OldName, err = r.String()
		}
		if err == nil {
			v.NewParent, err = r.U64()
		}
		if err == nil {
			v.NewName, err = r.String()
		}
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpLink:
		v := &pb.LinkRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.NewParent, err = r.U64()
		}
		if err == nil {
			v.NewName, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpSymlink:
		v := &pb.SymlinkRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Target, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpReadlink:
		v := &pb.ReadlinkRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpAccess:
		v := &pb.AccessRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Mask, err = r.U32()
		}
		if err == nil {
			v.Uid, err = r.U32()
		}
		var n uint32
		if err == nil {
			n, err = r.U32()
		}
		for i := uint32(0); err == nil && i < n; i++ {
			var gid uint32
			gid, err = r.U32()
			v.Gids = append(v.Gids, gid)
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpOpen:
		v := &pb.OpenRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpRead:
		v := &pb.ReadRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Offset, err = r.I64()
		}
		if err == nil {
			v.Size, err = r.I64()
		}
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpWrite:
		v := &pb.WriteRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Offset, err = r.I64()
		}
		if err == nil {
			v.Data, err = r.BytesRaw()
		}
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpRelease:
		v := &pb.ReleaseRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpFlush:
		v := &pb.FlushRequest{VolumeId: volumeID}
		v.HandleId, err = r.U64()
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpFsync:
		v := &pb.FsyncRequest{VolumeId: volumeID}
		v.HandleId, err = r.U64()
		if err == nil {
			v.Datasync, err = r.Bool()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpFallocate:
		v := &pb.FallocateRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Mode, err = r.U32()
		}
		if err == nil {
			v.Offset, err = r.I64()
		}
		if err == nil {
			v.Length, err = r.I64()
		}
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpCopyFileRange:
		v := &pb.CopyFileRangeRequest{VolumeId: volumeID}
		v.InodeIn, err = r.U64()
		if err == nil {
			v.HandleIn, err = r.U64()
		}
		if err == nil {
			v.OffsetIn, err = r.U64()
		}
		if err == nil {
			v.InodeOut, err = r.U64()
		}
		if err == nil {
			v.HandleOut, err = r.U64()
		}
		if err == nil {
			v.OffsetOut, err = r.U64()
		}
		if err == nil {
			v.Length, err = r.U64()
		}
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpOpenDir:
		v := &pb.OpenDirRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpReadDir:
		v := &pb.ReadDirRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Offset, err = r.I64()
		}
		if err == nil {
			v.Size, err = r.U32()
		}
		if err == nil {
			v.Plus, err = r.Bool()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpReleaseDir:
		v := &pb.ReleaseDirRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpStatFs:
		v := &pb.StatFsRequest{VolumeId: volumeID}
		v.Actor, err = ReadActor(r)
		out = v
	case OpGetXattr:
		v := &pb.GetXattrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Size, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpSetXattr:
		v := &pb.SetXattrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Value, err = r.BytesRaw()
		}
		if err == nil {
			v.Flags, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpListXattr:
		v := &pb.ListXattrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Size, err = r.I32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpRemoveXattr:
		v := &pb.RemoveXattrRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpMknod:
		v := &pb.MknodRequest{VolumeId: volumeID}
		v.Parent, err = r.U64()
		if err == nil {
			v.Name, err = r.String()
		}
		if err == nil {
			v.Mode, err = r.U32()
		}
		if err == nil {
			v.Rdev, err = r.U32()
		}
		if err == nil {
			v.Umask, err = r.U32()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpGetLk:
		v := &pb.GetLkRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Owner, err = r.U64()
		}
		if err == nil {
			v.Lock, err = ReadLock(r)
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpSetLk, OpSetLkw:
		v := &pb.SetLkRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Owner, err = r.U64()
		}
		if err == nil {
			v.Lock, err = ReadLock(r)
		}
		if err == nil {
			v.Block, err = r.Bool()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	case OpFlock:
		v := &pb.FlockRequest{VolumeId: volumeID}
		v.Inode, err = r.U64()
		if err == nil {
			v.HandleId, err = r.U64()
		}
		if err == nil {
			v.Owner, err = r.U64()
		}
		if err == nil {
			v.Typ, err = r.U32()
		}
		if err == nil {
			v.Block, err = r.Bool()
		}
		if err == nil {
			v.Actor, err = ReadActor(r)
		}
		out = v
	default:
		return nil, fmt.Errorf("unsupported request op %d", op)
	}
	if err != nil {
		return nil, err
	}
	return out, r.Err()
}

func EncodeResponse(op Op, resp any) ([]byte, error) {
	w := NewWriter()
	switch op {
	case OpHeartbeat, OpUnmountVolume, OpUnlink, OpRmdir, OpRename, OpRelease, OpFlush, OpFsync, OpReleaseDir, OpAccess, OpFallocate, OpSetXattr, OpRemoveXattr, OpSetLk, OpSetLkw, OpFlock:
	case OpLookup, OpMkdir, OpCreate, OpLink, OpSymlink, OpMknod:
		WriteNode(w, resp.(*pb.NodeResponse))
	case OpGetAttr:
		WriteAttr(w, resp.(*pb.GetAttrResponse))
	case OpSetAttr:
		v := resp.(*pb.SetAttrResponse)
		WriteAttr(w, v.Attr)
	case OpReadlink:
		v := resp.(*pb.ReadlinkResponse)
		w.String(v.GetTarget())
	case OpOpen:
		v := resp.(*pb.OpenResponse)
		w.U64(v.GetHandleId())
	case OpRead:
		v := resp.(*pb.ReadResponse)
		w.BytesRaw(v.GetData())
		w.Bool(v.GetEof())
	case OpWrite:
		v := resp.(*pb.WriteResponse)
		w.I64(v.GetBytesWritten())
	case OpCopyFileRange:
		v := resp.(*pb.CopyFileRangeResponse)
		w.U64(v.GetBytesCopied())
	case OpOpenDir:
		v := resp.(*pb.OpenDirResponse)
		w.U64(v.GetHandleId())
	case OpReadDir:
		v := resp.(*pb.ReadDirResponse)
		w.U32(uint32(len(v.Entries)))
		for _, entry := range v.Entries {
			w.U64(entry.Inode)
			w.U64(entry.Offset)
			w.String(entry.Name)
			w.U32(entry.Type)
			WriteAttr(w, entry.Attr)
		}
		w.Bool(v.Eof)
	case OpStatFs:
		v := resp.(*pb.StatFsResponse)
		w.U64(v.Blocks)
		w.U64(v.Bfree)
		w.U64(v.Bavail)
		w.U64(v.Files)
		w.U64(v.Ffree)
		w.U32(v.Bsize)
		w.U32(v.Namelen)
		w.U32(v.Frsize)
	case OpGetXattr:
		v := resp.(*pb.GetXattrResponse)
		w.BytesRaw(v.Value)
	case OpListXattr:
		v := resp.(*pb.ListXattrResponse)
		w.BytesRaw(v.Data)
	case OpGetLk:
		v := resp.(*pb.GetLkResponse)
		WriteLock(w, v.Lock)
	case OpWatchEvent:
		WriteWatchEvent(w, resp.(*pb.WatchEvent))
	default:
		return nil, fmt.Errorf("unsupported response op %d", op)
	}
	return w.Bytes(), nil
}

func DecodeResponse(op Op, payload []byte) (any, error) {
	r := NewReader(payload)
	var out any
	var err error
	switch op {
	case OpHeartbeat, OpUnmountVolume, OpUnlink, OpRmdir, OpRename, OpRelease, OpFlush, OpFsync, OpReleaseDir, OpAccess, OpFallocate, OpSetXattr, OpRemoveXattr, OpSetLk, OpSetLkw, OpFlock:
		out = &pb.Empty{}
	case OpLookup, OpMkdir, OpCreate, OpLink, OpSymlink, OpMknod:
		out, err = ReadNode(r)
	case OpGetAttr:
		out, err = ReadAttr(r)
	case OpSetAttr:
		var attr *pb.GetAttrResponse
		attr, err = ReadAttr(r)
		out = &pb.SetAttrResponse{Attr: attr}
	case OpReadlink:
		var target string
		target, err = r.String()
		out = &pb.ReadlinkResponse{Target: target}
	case OpOpen:
		v := &pb.OpenResponse{}
		v.HandleId, err = r.U64()
		out = v
	case OpRead:
		v := &pb.ReadResponse{}
		v.Data, err = r.BytesRaw()
		if err == nil {
			v.Eof, err = r.Bool()
		}
		out = v
	case OpWrite:
		v := &pb.WriteResponse{}
		v.BytesWritten, err = r.I64()
		out = v
	case OpCopyFileRange:
		v := &pb.CopyFileRangeResponse{}
		v.BytesCopied, err = r.U64()
		out = v
	case OpOpenDir:
		v := &pb.OpenDirResponse{}
		v.HandleId, err = r.U64()
		out = v
	case OpReadDir:
		v := &pb.ReadDirResponse{}
		var n uint32
		n, err = r.U32()
		for i := uint32(0); err == nil && i < n; i++ {
			entry := &pb.DirEntry{}
			entry.Inode, err = r.U64()
			if err == nil {
				entry.Offset, err = r.U64()
			}
			if err == nil {
				entry.Name, err = r.String()
			}
			if err == nil {
				entry.Type, err = r.U32()
			}
			if err == nil {
				entry.Attr, err = ReadAttr(r)
			}
			v.Entries = append(v.Entries, entry)
		}
		if err == nil {
			v.Eof, err = r.Bool()
		}
		out = v
	case OpStatFs:
		v := &pb.StatFsResponse{}
		v.Blocks, err = r.U64()
		if err == nil {
			v.Bfree, err = r.U64()
		}
		if err == nil {
			v.Bavail, err = r.U64()
		}
		if err == nil {
			v.Files, err = r.U64()
		}
		if err == nil {
			v.Ffree, err = r.U64()
		}
		if err == nil {
			v.Bsize, err = r.U32()
		}
		if err == nil {
			v.Namelen, err = r.U32()
		}
		if err == nil {
			v.Frsize, err = r.U32()
		}
		out = v
	case OpGetXattr:
		v := &pb.GetXattrResponse{}
		v.Value, err = r.BytesRaw()
		out = v
	case OpListXattr:
		v := &pb.ListXattrResponse{}
		v.Data, err = r.BytesRaw()
		out = v
	case OpGetLk:
		v := &pb.GetLkResponse{}
		v.Lock, err = ReadLock(r)
		out = v
	case OpWatchEvent:
		out, err = ReadWatchEvent(r)
	default:
		return nil, fmt.Errorf("unsupported response op %d", op)
	}
	if err != nil {
		return nil, err
	}
	return out, r.Err()
}

func WriteWatchEvent(w *Writer, event *pb.WatchEvent) {
	if event == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.String(event.VolumeId)
	w.I32(int32(event.EventType))
	w.String(event.Path)
	w.String(event.OldPath)
	w.U64(event.Inode)
	w.I64(event.TimestampUnix)
	w.String(event.OriginInstance)
	w.String(event.OriginSandboxId)
	w.String(event.InvalidateId)
}

func ReadWatchEvent(r *Reader) (*pb.WatchEvent, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	event := &pb.WatchEvent{}
	if event.VolumeId, err = r.String(); err != nil {
		return nil, err
	}
	var typ int32
	if typ, err = r.I32(); err != nil {
		return nil, err
	}
	event.EventType = pb.WatchEventType(typ)
	if event.Path, err = r.String(); err != nil {
		return nil, err
	}
	if event.OldPath, err = r.String(); err != nil {
		return nil, err
	}
	if event.Inode, err = r.U64(); err != nil {
		return nil, err
	}
	if event.TimestampUnix, err = r.I64(); err != nil {
		return nil, err
	}
	if event.OriginInstance, err = r.String(); err != nil {
		return nil, err
	}
	if event.OriginSandboxId, err = r.String(); err != nil {
		return nil, err
	}
	if event.InvalidateId, err = r.String(); err != nil {
		return nil, err
	}
	return event, nil
}
