//go:build !linux

package runtimecheckpoint

import "os"

// Without a qualified kernel change guard, retain full content verification.
type imageVerificationGuard struct{}

func newImageVerificationGuard() *imageVerificationGuard       { return nil }
func (*imageVerificationGuard) seal(string, *os.File)          {}
func (*imageVerificationGuard) finish(*os.Root, Manifest) bool { return false }
func (*imageVerificationGuard) valid(*os.Root, Manifest) bool  { return false }
func (*imageVerificationGuard) close()                         {}
