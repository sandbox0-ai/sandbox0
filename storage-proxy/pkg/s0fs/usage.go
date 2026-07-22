package s0fs

// FilesystemUsage describes the logical data and inode usage of a live S0FS
// engine. Hard links share an inode and are counted once. Retained unlinked
// inodes remain included until Forget removes them.
type FilesystemUsage struct {
	DataBytes uint64
	Inodes    uint64
}

// FilesystemUsage returns the current logical filesystem usage without cloning
// the engine state. Metadata and object-store encoding overhead are excluded.
func (e *Engine) FilesystemUsage() (FilesystemUsage, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return FilesystemUsage{}, err
	}

	var usage FilesystemUsage
	for _, node := range e.nodes {
		if node == nil {
			continue
		}
		usage.Inodes++
		usage.DataBytes = addUint64Saturating(usage.DataBytes, node.Size)
	}
	return usage, nil
}

func addUint64Saturating(left, right uint64) uint64 {
	const maxUint64 = ^uint64(0)
	if maxUint64-left < right {
		return maxUint64
	}
	return left + right
}
