//go:build linux

package rootfsblock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func (c *diskReadCache) cloneVerified(ctx context.Context, key readCacheKey, output *os.File, offset int64) (bool, error) {
	if c == nil {
		return false, nil
	}
	if output == nil || offset < 0 {
		return false, fmt.Errorf("checkpoint clone needs a private destination and nonnegative offset")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.readers.RLock()
	defer c.readers.RUnlock()
	c.mu.Lock()
	element, found := c.items[key]
	if !found || c.closed {
		c.mu.Unlock()
		return false, nil
	}
	name, _ := diskCacheName(key)
	input, err := c.root.OpenFile(name, os.O_RDONLY|diskCacheOpenFlags, 0)
	if err == nil {
		c.order.MoveToFront(element)
	}
	c.mu.Unlock()
	if err != nil {
		c.discardDamagedClone(key)
		return false, nil
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != key.length {
		c.discardDamagedClone(key)
		return false, nil
	}
	// Cache files are immutable after their atomic publication. Verify the
	// exact open inode before cloning it: reading the newly cloned destination
	// would turn every cache hit into a cold read of the full image on XFS.
	hash := sha256.New()
	read, err := io.CopyBuffer(hash, input, make([]byte, 256<<10))
	if err != nil || read != key.length || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != key.checksum {
		c.discardDamagedClone(key)
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := unix.IoctlFileCloneRange(int(output.Fd()), &unix.FileCloneRange{
		Src_fd: int64(input.Fd()), Src_offset: 0, Src_length: uint64(key.length), Dest_offset: uint64(offset),
	}); err != nil {
		if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) || errors.Is(err, unix.EXDEV) ||
			errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EPERM) || errors.Is(err, unix.ENOSYS) {
			return false, nil
		}
		return false, fmt.Errorf("clone checkpoint cache range: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.hits.Add(1)
	return true, nil
}

func (c *diskReadCache) discardDamagedClone(key readCacheKey) {
	c.errors.Add(1)
	c.mu.Lock()
	c.remove(key)
	c.mu.Unlock()
}
