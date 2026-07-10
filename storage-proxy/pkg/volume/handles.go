package volume

// S0FS file handles are self-describing: the logical handle is the inode.
// This keeps Open and Release off the durable metadata path and lets a new
// ctld process resolve handles that the kernel retained across recovery.
func (v *VolumeContext) OpenFileHandle(inode uint64) uint64 {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	if v.openFileCount == nil {
		v.openFileCount = make(map[uint64]int)
	}
	v.openFileCount[inode]++
	return inode
}

// Directory handles use the same stable inode encoding. Directory releases
// do not need runtime state because no delayed unlink semantics depend on them.
func (v *VolumeContext) OpenDirHandle(inode uint64) uint64 {
	return inode
}

func (v *VolumeContext) ReleaseHandle(handleID uint64) (uint64, int, bool) {
	if handleID == 0 {
		return 0, 0, false
	}
	return handleID, 0, false
}

func (v *VolumeContext) MarkUnlinkedFileIfOpen(inode uint64) bool {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	if v.S0FS != nil && v.S0FS.RetainsUnlinked() {
		return true
	}
	if v.openFileCount[inode] == 0 {
		return false
	}
	if v.unlinkedFiles == nil {
		v.unlinkedFiles = make(map[uint64]struct{})
	}
	v.unlinkedFiles[inode] = struct{}{}
	return true
}

func (v *VolumeContext) FileOpenCount(inode uint64) int {
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	return v.openFileCount[inode]
}

func (v *VolumeContext) ReleaseFileHandle(handleID uint64) (uint64, int, bool, bool) {
	if handleID == 0 {
		return 0, 0, false, false
	}
	v.handleMu.Lock()
	defer v.handleMu.Unlock()
	inode := handleID
	count := v.openFileCount[inode]
	known := count > 0
	if count > 1 {
		v.openFileCount[inode] = count - 1
	} else if count == 1 {
		delete(v.openFileCount, inode)
	}
	remaining := v.openFileCount[inode]
	_, unlinked := v.unlinkedFiles[inode]
	if unlinked && remaining == 0 {
		delete(v.unlinkedFiles, inode)
	}
	return inode, remaining, unlinked, known
}

func (v *VolumeContext) HandleInode(handleID uint64) (uint64, bool) {
	return handleID, handleID != 0
}

// ResolveFileHandle validates that the request node and self-describing handle
// refer to the same S0FS inode. Requests without a handle use their node ID.
func (v *VolumeContext) ResolveFileHandle(handleID, requestInode uint64) (uint64, bool) {
	if handleID == 0 {
		return requestInode, requestInode != 0
	}
	if requestInode != 0 && requestInode != handleID {
		return 0, false
	}
	return handleID, true
}

// FinalizeRecoverableHandles reclaims nlink=0 inodes once the last portal for
// a recoverable volume has been unpublished.
func (v *VolumeContext) FinalizeRecoverableHandles() error {
	if v == nil || v.S0FS == nil || !v.S0FS.RetainsUnlinked() {
		return nil
	}
	if err := v.S0FS.CollectUnlinked(); err != nil {
		return err
	}
	v.handleMu.Lock()
	clear(v.openFileCount)
	clear(v.unlinkedFiles)
	v.handleMu.Unlock()
	return nil
}
