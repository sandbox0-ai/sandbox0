package legacyfs

import (
	"syscall"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
)

type LogContext struct {
	Meta fsmeta.Context
}

func NewLogContext(ctx fsmeta.Context) LogContext {
	return LogContext{Meta: ctx}
}

type Entry struct {
	Inode fsmeta.Ino
	Name  []byte
	Attr  *fsmeta.Attr
}

type Format struct {
	BlockSize uint32
}

type Config struct {
	Format Format
}

type VFS interface {
	Config() *Config
	GetAttr(LogContext, fsmeta.Ino, int) (*Entry, syscall.Errno)
	Lookup(LogContext, fsmeta.Ino, string) (*Entry, syscall.Errno)
	Open(LogContext, fsmeta.Ino, uint32) (*Entry, uint64, syscall.Errno)
	Read(LogContext, fsmeta.Ino, []byte, uint64, uint64) (int, syscall.Errno)
	Write(LogContext, fsmeta.Ino, []byte, uint64, uint64) syscall.Errno
	Create(LogContext, fsmeta.Ino, string, uint16, uint16, uint32) (*Entry, uint64, syscall.Errno)
	Mkdir(LogContext, fsmeta.Ino, string, uint16, uint16) (*Entry, syscall.Errno)
	Mknod(LogContext, fsmeta.Ino, string, uint16, uint16, uint32) (*Entry, syscall.Errno)
	Unlink(LogContext, fsmeta.Ino, string) syscall.Errno
	Readdir(LogContext, fsmeta.Ino, uint32, int, uint64, bool) ([]*Entry, bool, syscall.Errno)
	Opendir(LogContext, fsmeta.Ino, uint32) (uint64, syscall.Errno)
	Releasedir(LogContext, fsmeta.Ino, uint64) syscall.Errno
	Rename(LogContext, fsmeta.Ino, string, fsmeta.Ino, string, uint32) syscall.Errno
	SetAttr(LogContext, fsmeta.Ino, int, uint64, uint32, uint32, uint32, int64, int64, uint32, uint32, uint64) (*Entry, syscall.Errno)
	Flush(LogContext, fsmeta.Ino, uint64, uint64) syscall.Errno
	Fsync(LogContext, fsmeta.Ino, int, uint64) syscall.Errno
	Release(LogContext, fsmeta.Ino, uint64)
	Rmdir(LogContext, fsmeta.Ino, string) syscall.Errno
	Symlink(LogContext, string, fsmeta.Ino, string) (*Entry, syscall.Errno)
	Readlink(LogContext, fsmeta.Ino) ([]byte, syscall.Errno)
	Link(LogContext, fsmeta.Ino, fsmeta.Ino, string) (*Entry, syscall.Errno)
	Access(LogContext, fsmeta.Ino, int) syscall.Errno
	Fallocate(LogContext, fsmeta.Ino, uint8, int64, int64, uint64) syscall.Errno
	CopyFileRange(LogContext, fsmeta.Ino, uint64, uint64, fsmeta.Ino, uint64, uint64, uint64, uint64) (uint64, syscall.Errno)
	Getlk(LogContext, fsmeta.Ino, uint64, uint64, *uint64, *uint64, *uint32, *uint32) syscall.Errno
	Setlk(LogContext, fsmeta.Ino, uint64, uint64, uint64, uint64, uint32, uint32, bool) syscall.Errno
	Flock(LogContext, fsmeta.Ino, uint64, uint64, uint32, bool) syscall.Errno
	Ioctl(LogContext, fsmeta.Ino, uint32, uint64, []byte, []byte) syscall.Errno
	GetXattr(LogContext, fsmeta.Ino, string, uint32) ([]byte, syscall.Errno)
	SetXattr(LogContext, fsmeta.Ino, string, []byte, uint32) syscall.Errno
	ListXattr(LogContext, fsmeta.Ino, int) ([]byte, syscall.Errno)
	RemoveXattr(LogContext, fsmeta.Ino, string) syscall.Errno
	FlushAll(string) error
}
