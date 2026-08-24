//go:build unix

package rootfsblock

import "golang.org/x/sys/unix"

const syscallNoFollow = unix.O_NOFOLLOW
