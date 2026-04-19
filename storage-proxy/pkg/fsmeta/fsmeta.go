package fsmeta

type Ino uint64

const (
	RootInode Ino = 1
)

const (
	TypeFile uint8 = iota + 1
	TypeDirectory
	TypeSymlink
)

const (
	SetAttrMode = 1 << iota
	SetAttrUID
	SetAttrGID
	SetAttrSize
)

const ENOATTR = 61

type Attr struct {
	Typ       uint8
	Mode      uint16
	Uid       uint32
	Gid       uint32
	Nlink     uint32
	Rdev      uint32
	Length    uint64
	Atime     int64
	Atimensec uint32
	Mtime     int64
	Mtimensec uint32
	Ctime     int64
	Ctimensec uint32
}

func (a *Attr) SMode() uint32 {
	if a == nil {
		return 0
	}
	var fileType uint32
	switch a.Typ {
	case TypeDirectory:
		fileType = 0o040000
	case TypeSymlink:
		fileType = 0o120000
	default:
		fileType = 0o100000
	}
	return fileType | uint32(a.Mode)
}

type Entry struct {
	Inode Ino
	Name  []byte
	Attr  *Attr
}

type Context interface {
	Pid() uint32
	Uid() uint32
	Gid() uint32
	Gids() []uint32
}

type basicContext struct {
	pid  uint32
	uid  uint32
	gids []uint32
}

func Background() Context {
	return basicContext{}
}

func NewContext(pid, uid uint32, gids []uint32) Context {
	copyGIDs := make([]uint32, len(gids))
	copy(copyGIDs, gids)
	return basicContext{
		pid:  pid,
		uid:  uid,
		gids: copyGIDs,
	}
}

func (c basicContext) Pid() uint32 {
	return c.pid
}

func (c basicContext) Uid() uint32 {
	return c.uid
}

func (c basicContext) Gid() uint32 {
	if len(c.gids) == 0 {
		return 0
	}
	return c.gids[0]
}

func (c basicContext) Gids() []uint32 {
	copyGIDs := make([]uint32, len(c.gids))
	copy(copyGIDs, c.gids)
	return copyGIDs
}
