//go:build !unix

package procdartifact

import "os"

func trustedOwner(os.FileInfo) bool { return false }

func trustedCacheOwner(string, string, os.FileInfo) bool { return false }
