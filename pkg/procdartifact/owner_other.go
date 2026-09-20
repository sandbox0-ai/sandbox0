//go:build !unix

package procdartifact

import "os"

func trustedOwner(os.FileInfo) bool { return false }
