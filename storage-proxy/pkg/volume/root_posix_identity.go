package volume

import (
	"fmt"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
)

// EnsureLazyRootPosixIdentity lazily assigns the logical volume root to the
// first non-system actor that touches a fresh volume. It only updates roots
// still owned by 0:0, preserving roots that have already been claimed or
// explicitly managed by developers.
func EnsureLazyRootPosixIdentity(volCtx *VolumeContext, uid, gid uint32) error {
	if volCtx == nil || volCtx.Meta == nil {
		return fmt.Errorf("volume context is nil")
	}
	if uid == 0 && gid == 0 {
		return nil
	}

	rootInode := volCtx.RootInode
	if rootInode == 0 {
		rootInode = meta.RootInode
	}

	var attr meta.Attr
	if errno := volCtx.Meta.GetAttr(meta.Background(), rootInode, &attr); errno != 0 {
		return fmt.Errorf("get root attr: %w", syscall.Errno(errno))
	}
	if attr.Uid == uid && attr.Gid == gid {
		return nil
	}
	if attr.Uid != 0 || attr.Gid != 0 {
		return nil
	}

	updated := attr
	updated.Uid = uid
	updated.Gid = gid
	if errno := volCtx.Meta.SetAttr(meta.Background(), rootInode, meta.SetAttrUID|meta.SetAttrGID, 0, &updated); errno != 0 {
		return fmt.Errorf("set root posix identity: %w", syscall.Errno(errno))
	}
	return nil
}
