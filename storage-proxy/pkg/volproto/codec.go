package volproto

import (
	"encoding/binary"
	"fmt"
	"math"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

type StatusCode int32

const (
	StatusOK                 StatusCode = 0
	StatusCanceled           StatusCode = 1
	StatusUnknown            StatusCode = 2
	StatusInvalidArgument    StatusCode = 3
	StatusDeadlineExceeded   StatusCode = 4
	StatusNotFound           StatusCode = 5
	StatusAlreadyExists      StatusCode = 6
	StatusPermissionDenied   StatusCode = 7
	StatusResourceExhausted  StatusCode = 8
	StatusFailedPrecondition StatusCode = 9
	StatusAborted            StatusCode = 10
	StatusOutOfRange         StatusCode = 11
	StatusUnimplemented      StatusCode = 12
	StatusInternal           StatusCode = 13
	StatusUnavailable        StatusCode = 14
	StatusDataLoss           StatusCode = 15
	StatusUnauthenticated    StatusCode = 16
)

type Writer struct {
	buf []byte
}

func NewWriter() *Writer {
	return &Writer{buf: make([]byte, 0, 256)}
}

func (w *Writer) Bytes() []byte {
	return w.buf
}

func (w *Writer) Bool(v bool) {
	if v {
		w.U8(1)
		return
	}
	w.U8(0)
}

func (w *Writer) U8(v uint8) {
	w.buf = append(w.buf, v)
}

func (w *Writer) U16(v uint16) {
	w.buf = binary.LittleEndian.AppendUint16(w.buf, v)
}

func (w *Writer) U32(v uint32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, v)
}

func (w *Writer) I32(v int32) {
	w.U32(uint32(v))
}

func (w *Writer) U64(v uint64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, v)
}

func (w *Writer) I64(v int64) {
	w.U64(uint64(v))
}

func (w *Writer) String(v string) {
	w.BytesRaw([]byte(v))
}

func (w *Writer) BytesRaw(v []byte) {
	w.U32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}

type Reader struct {
	buf []byte
	off int
}

func NewReader(payload []byte) *Reader {
	return &Reader{buf: payload}
}

func (r *Reader) Err() error {
	if r.off != len(r.buf) {
		return fmt.Errorf("s0vp payload has %d trailing bytes", len(r.buf)-r.off)
	}
	return nil
}

func (r *Reader) Bool() (bool, error) {
	v, err := r.U8()
	return v != 0, err
}

func (r *Reader) U8() (uint8, error) {
	if len(r.buf)-r.off < 1 {
		return 0, fmt.Errorf("short s0vp payload")
	}
	v := r.buf[r.off]
	r.off++
	return v, nil
}

func (r *Reader) U16() (uint16, error) {
	if len(r.buf)-r.off < 2 {
		return 0, fmt.Errorf("short s0vp payload")
	}
	v := binary.LittleEndian.Uint16(r.buf[r.off : r.off+2])
	r.off += 2
	return v, nil
}

func (r *Reader) U32() (uint32, error) {
	if len(r.buf)-r.off < 4 {
		return 0, fmt.Errorf("short s0vp payload")
	}
	v := binary.LittleEndian.Uint32(r.buf[r.off : r.off+4])
	r.off += 4
	return v, nil
}

func (r *Reader) I32() (int32, error) {
	v, err := r.U32()
	return int32(v), err
}

func (r *Reader) U64() (uint64, error) {
	if len(r.buf)-r.off < 8 {
		return 0, fmt.Errorf("short s0vp payload")
	}
	v := binary.LittleEndian.Uint64(r.buf[r.off : r.off+8])
	r.off += 8
	return v, nil
}

func (r *Reader) I64() (int64, error) {
	v, err := r.U64()
	return int64(v), err
}

func (r *Reader) String() (string, error) {
	v, err := r.BytesRaw()
	return string(v), err
}

func (r *Reader) BytesRaw() ([]byte, error) {
	n, err := r.U32()
	if err != nil {
		return nil, err
	}
	if n > math.MaxInt32 || int(n) > len(r.buf)-r.off {
		return nil, fmt.Errorf("invalid s0vp byte length: %d", n)
	}
	v := r.buf[r.off : r.off+int(n)]
	r.off += int(n)
	return v, nil
}

func WriteError(code StatusCode, message string) []byte {
	return WriteErrorWithRedirect(code, message, nil)
}

func WriteErrorWithRedirect(code StatusCode, message string, redirect *pb.PrimaryRedirect) []byte {
	w := NewWriter()
	w.I32(int32(code))
	w.String(message)
	WritePrimaryRedirect(w, redirect)
	return w.Bytes()
}

func ReadError(payload []byte) (StatusCode, string, *pb.PrimaryRedirect, error) {
	r := NewReader(payload)
	code, err := r.I32()
	if err != nil {
		return StatusInternal, "", nil, err
	}
	msg, err := r.String()
	if err != nil {
		return StatusInternal, "", nil, err
	}
	redirect, err := ReadPrimaryRedirect(r)
	if err != nil {
		return StatusInternal, "", nil, err
	}
	return StatusCode(code), msg, redirect, r.Err()
}

func WritePrimaryRedirect(w *Writer, redirect *pb.PrimaryRedirect) {
	if redirect == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.String(redirect.VolumeId)
	w.String(redirect.PrimaryNodeId)
	w.String(redirect.PrimaryAddr)
	w.U64(redirect.Epoch)
}

func ReadPrimaryRedirect(r *Reader) (*pb.PrimaryRedirect, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	redirect := &pb.PrimaryRedirect{}
	if redirect.VolumeId, err = r.String(); err != nil {
		return nil, err
	}
	if redirect.PrimaryNodeId, err = r.String(); err != nil {
		return nil, err
	}
	if redirect.PrimaryAddr, err = r.String(); err != nil {
		return nil, err
	}
	if redirect.Epoch, err = r.U64(); err != nil {
		return nil, err
	}
	return redirect, nil
}

func WriteActor(w *Writer, actor *pb.PosixActor) {
	if actor == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.U32(actor.Pid)
	w.U32(actor.Uid)
	w.U32(uint32(len(actor.Gids)))
	for _, gid := range actor.Gids {
		w.U32(gid)
	}
}

func ReadActor(r *Reader) (*pb.PosixActor, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	pid, err := r.U32()
	if err != nil {
		return nil, err
	}
	uid, err := r.U32()
	if err != nil {
		return nil, err
	}
	n, err := r.U32()
	if err != nil {
		return nil, err
	}
	if int(n) > (len(r.buf)-r.off)/4 {
		return nil, fmt.Errorf("invalid s0vp gid count: %d", n)
	}
	gids := make([]uint32, 0, n)
	for i := uint32(0); i < n; i++ {
		gid, err := r.U32()
		if err != nil {
			return nil, err
		}
		gids = append(gids, gid)
	}
	return &pb.PosixActor{Pid: pid, Uid: uid, Gids: gids}, nil
}

func WriteAttr(w *Writer, attr *pb.GetAttrResponse) {
	if attr == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.U64(attr.Ino)
	w.U32(attr.Mode)
	w.U32(attr.Nlink)
	w.U32(attr.Uid)
	w.U32(attr.Gid)
	w.U64(attr.Rdev)
	w.U64(attr.Size)
	w.U64(attr.Blocks)
	w.I64(attr.AtimeSec)
	w.I64(attr.AtimeNsec)
	w.I64(attr.MtimeSec)
	w.I64(attr.MtimeNsec)
	w.I64(attr.CtimeSec)
	w.I64(attr.CtimeNsec)
}

func ReadAttr(r *Reader) (*pb.GetAttrResponse, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	attr := &pb.GetAttrResponse{}
	if attr.Ino, err = r.U64(); err != nil {
		return nil, err
	}
	if attr.Mode, err = r.U32(); err != nil {
		return nil, err
	}
	if attr.Nlink, err = r.U32(); err != nil {
		return nil, err
	}
	if attr.Uid, err = r.U32(); err != nil {
		return nil, err
	}
	if attr.Gid, err = r.U32(); err != nil {
		return nil, err
	}
	if attr.Rdev, err = r.U64(); err != nil {
		return nil, err
	}
	if attr.Size, err = r.U64(); err != nil {
		return nil, err
	}
	if attr.Blocks, err = r.U64(); err != nil {
		return nil, err
	}
	if attr.AtimeSec, err = r.I64(); err != nil {
		return nil, err
	}
	if attr.AtimeNsec, err = r.I64(); err != nil {
		return nil, err
	}
	if attr.MtimeSec, err = r.I64(); err != nil {
		return nil, err
	}
	if attr.MtimeNsec, err = r.I64(); err != nil {
		return nil, err
	}
	if attr.CtimeSec, err = r.I64(); err != nil {
		return nil, err
	}
	if attr.CtimeNsec, err = r.I64(); err != nil {
		return nil, err
	}
	return attr, nil
}

func WriteNode(w *Writer, node *pb.NodeResponse) {
	if node == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.U64(node.Inode)
	w.U64(node.Generation)
	WriteAttr(w, node.Attr)
	w.U64(node.HandleId)
}

func ReadNode(r *Reader) (*pb.NodeResponse, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	node := &pb.NodeResponse{}
	if node.Inode, err = r.U64(); err != nil {
		return nil, err
	}
	if node.Generation, err = r.U64(); err != nil {
		return nil, err
	}
	if node.Attr, err = ReadAttr(r); err != nil {
		return nil, err
	}
	if node.HandleId, err = r.U64(); err != nil {
		return nil, err
	}
	return node, nil
}

func WriteLock(w *Writer, lock *pb.FileLock) {
	if lock == nil {
		w.Bool(false)
		return
	}
	w.Bool(true)
	w.U64(lock.Start)
	w.U64(lock.End)
	w.U32(lock.Typ)
	w.U32(lock.Pid)
}

func ReadLock(r *Reader) (*pb.FileLock, error) {
	ok, err := r.Bool()
	if err != nil || !ok {
		return nil, err
	}
	lock := &pb.FileLock{}
	if lock.Start, err = r.U64(); err != nil {
		return nil, err
	}
	if lock.End, err = r.U64(); err != nil {
		return nil, err
	}
	if lock.Typ, err = r.U32(); err != nil {
		return nil, err
	}
	if lock.Pid, err = r.U32(); err != nil {
		return nil, err
	}
	return lock, nil
}
