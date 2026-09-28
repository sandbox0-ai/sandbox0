//go:build unix

package procdartifact

import (
	"fmt"
	"os"
	"os/user"
	"syscall"
)

func trustedOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

// The dedicated data mount is owned by the trusted host sandbox0 account on
// existing workers. Only this one ancestor may have that owner; the cache
// directory, digest directories, and executable remain root-owned.
func trustedCacheOwner(path, root string, info os.FileInfo) bool {
	if trustedOwner(info) {
		return true
	}
	if path != "/var/lib/sandbox0" || root != DefaultCacheDir || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	owner, err := user.LookupId(fmt.Sprint(stat.Uid))
	return err == nil && owner.Username == "sandbox0"
}
