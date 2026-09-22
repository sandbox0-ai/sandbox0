package runtimecheckpoint

import (
	"fmt"
	"path"
)

// StagingFootprint accounts for data at 4 KiB allocation granularity and every
// distinct file/directory inode, including the image root and empty files.
// One 4 KiB block per directory is admission headroom, not a guarantee of all
// filesystem metadata overhead. Kernel quotas remain authoritative while
// downloading, and the node must separately own durable capacity custody.
func (m Manifest) StagingFootprint() (bytes int64, inodes uint64, err error) {
	if err := m.Validate(MaxImageBytes); err != nil {
		return 0, 0, err
	}
	directories := map[string]bool{".": true}
	for _, file := range m.Files {
		bytes += (file.Size + 4095) / 4096 * 4096
		for parent := path.Dir(file.Path); parent != "."; parent = path.Dir(parent) {
			directories[parent] = true
		}
	}
	inodes = uint64(len(directories) + len(m.Files))
	if inodes > MaxFiles*8 {
		return 0, 0, fmt.Errorf("checkpoint image exceeds staging inode bound")
	}
	return bytes + int64(len(directories))*4096, inodes, nil
}
