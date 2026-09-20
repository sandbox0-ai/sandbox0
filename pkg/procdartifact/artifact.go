// Package procdartifact defines immutable, node-provided procd executables.
// Executable selection belongs to a runtime assignment, never a user RootFS head.
package procdartifact

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
)

const DefaultCacheDir = "/var/lib/sandbox0-procd"

// Artifact pins the executable and its wire/persistent-state compatibility contract.
// An existing runtime, including a memory checkpoint, must retain this exact pair.
type Artifact struct {
	Digest   string `json:"digest" yaml:"digest"`
	Protocol string `json:"protocol" yaml:"protocol"`
}

func (a Artifact) Validate() error {
	if a.Digest == PlaceholderDigest() {
		return fmt.Errorf("RootFS placeholder is not a runtime executable")
	}
	d, err := digest.Parse(a.Digest)
	if err != nil || d.Algorithm() != digest.SHA256 || d.Validate() != nil || d.String() != a.Digest {
		return fmt.Errorf("procd artifact requires a canonical SHA-256 digest")
	}
	if a.Protocol == "" || a.Protocol != strings.TrimSpace(a.Protocol) || len(a.Protocol) > 128 || strings.ContainsAny(a.Protocol, "\r\n\x00") {
		return fmt.Errorf("procd artifact protocol must be non-empty and canonical")
	}
	return nil
}

// Resolve verifies the digest-addressed executable before constructing a guest
// mount. It never downloads code, follows symlinks, or falls back to /procd in
// the user filesystem. Cache entries must be retained while runtimes use them.
func Resolve(root string, a Artifact) (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	if root == "" {
		root = DefaultCacheDir
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return "", fmt.Errorf("procd cache must be a canonical absolute directory")
	}
	path := filepath.Join(root, "sha256", strings.TrimPrefix(a.Digest, "sha256:"), "procd")
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return "", fmt.Errorf("inspect procd cache: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !trustedOwner(info) || info.Mode().Perm()&0022 != 0 {
			return "", fmt.Errorf("procd cache must be root-owned without symlinks or group/world write permissions: %s", current)
		}
		if current == path {
			if !info.Mode().IsRegular() || info.Mode().Perm()&0222 != 0 || info.Mode().Perm()&0111 == 0 || info.Size() <= 0 || info.Size() > 512<<20 {
				return "", fmt.Errorf("procd artifact must be an immutable executable regular file")
			}
		} else if !info.IsDir() {
			return "", fmt.Errorf("procd cache parent is not a directory")
		}
		if current == string(filepath.Separator) {
			break
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	actual, err := digest.FromReader(io.LimitReader(f, (512<<20)+1))
	if err != nil {
		return "", fmt.Errorf("hash procd artifact: %w", err)
	}
	if actual.String() != a.Digest {
		return "", fmt.Errorf("procd artifact digest mismatch")
	}
	return path, nil
}
