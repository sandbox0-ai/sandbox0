package volume

import (
	"syscall"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/legacyfs"
)

type LegacyMeta interface {
	Lookup(fsmeta.Context, fsmeta.Ino, string, *fsmeta.Ino, *fsmeta.Attr, bool) syscall.Errno
	Mkdir(fsmeta.Context, fsmeta.Ino, string, uint16, uint16, uint8, *fsmeta.Ino, *fsmeta.Attr) syscall.Errno
	GetAttr(fsmeta.Context, fsmeta.Ino, *fsmeta.Attr) syscall.Errno
	SetAttr(fsmeta.Context, fsmeta.Ino, int, uint8, *fsmeta.Attr) syscall.Errno
	Remove(fsmeta.Context, fsmeta.Ino, string, bool, int, *uint64) syscall.Errno
	StatFS(legacyfs.LogContext, fsmeta.Ino, *uint64, *uint64, *uint64, *uint64) syscall.Errno
	GetPaths(fsmeta.Context, fsmeta.Ino) []string
}
