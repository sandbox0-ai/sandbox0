package volume

import (
	"fmt"
)

// EnsureLazyRootPosixIdentity lazily assigns the logical volume root to the
// first non-system actor that touches a fresh volume. It only updates roots
// still owned by 0:0, preserving roots that have already been claimed or
// explicitly managed by developers.
func EnsureLazyRootPosixIdentity(volCtx *VolumeContext, uid, gid uint32) error {
	if volCtx == nil || volCtx.S0FS == nil {
		return fmt.Errorf("volume context is nil")
	}
	if uid == 0 && gid == 0 {
		return nil
	}

	rootInode := volCtx.RootInode
	if rootInode == 0 {
		rootInode = 1
	}

	attr, err := volCtx.S0FS.GetAttr(uint64(rootInode))
	if err != nil {
		return fmt.Errorf("get root attr: %w", err)
	}
	if attr.UID == uid && attr.GID == gid {
		return nil
	}
	if attr.UID != 0 || attr.GID != 0 {
		return nil
	}

	if err := volCtx.S0FS.SetOwner(uint64(rootInode), uid, gid); err != nil {
		return fmt.Errorf("set root posix identity: %w", err)
	}
	return nil
}
