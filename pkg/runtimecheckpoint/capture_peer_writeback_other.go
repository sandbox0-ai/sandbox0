//go:build !linux

package runtimecheckpoint

import "os"

// Non-Linux tests retain final fsync without an asynchronous writeback hint.
func capturePeerWriteback(*os.File, int64, int64) error { return nil }
