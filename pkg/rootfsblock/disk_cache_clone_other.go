//go:build !linux

package rootfsblock

import (
	"context"
	"os"
)

func (c *diskReadCache) cloneVerified(context.Context, readCacheKey, *os.File, int64) (bool, error) {
	return false, nil
}
