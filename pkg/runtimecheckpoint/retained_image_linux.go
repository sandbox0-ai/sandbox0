//go:build linux

package runtimecheckpoint

import (
	"fmt"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

// Link the already-open source inode, not a pathname that could be exchanged
// between validation and link creation. This preserves its page-cache mapping.
func linkRetainedFile(input *os.File, destination *os.Root, name string) error {
	parent, err := destination.Open(path.Dir(name))
	if err != nil {
		return err
	}
	defer parent.Close()
	return unix.Linkat(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", input.Fd()), int(parent.Fd()), path.Base(name), unix.AT_SYMLINK_FOLLOW)
}
