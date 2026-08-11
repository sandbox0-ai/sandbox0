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
	SetAttrAtime
	SetAttrMtime
	SetAttrFH
	SetAttrAtimeNow
	SetAttrMtimeNow
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

type Entry struct {
	Inode Ino
	Name  []byte
	Attr  *Attr
}
