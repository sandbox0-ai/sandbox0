package procdartifact

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
)

// Install publishes a verified executable without ever replacing an existing
// inode. Runtime mounts can outlive a deployment; rollback must retain both
// versions. The caller supplies the digest from its authenticated release.
func Install(root, source, expected string) (string, error) {
	input, err := os.Open(source)
	if err != nil {
		return "", err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || !trustedOwner(info) || info.Mode().Perm()&0022 != 0 || info.Size() <= 0 || info.Size() > 512<<20 {
		return "", fmt.Errorf("untrusted procd source")
	}
	return installReader(root, input, expected)
}

// InstallEmbedded installs bytes authenticated as part of the ctld executable.
func InstallEmbedded(root string, executable string, expected string) (string, error) {
	if len(executable) == 0 || len(executable) > 512<<20 {
		return "", fmt.Errorf("invalid embedded procd size")
	}
	return installReader(root, strings.NewReader(executable), expected)
}

func installReader(root string, input io.Reader, expected string) (string, error) {
	a := Artifact{Digest: expected, Protocol: "install"}
	if err := a.Validate(); err != nil {
		return "", err
	}
	if root == "" {
		root = DefaultCacheDir
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return "", fmt.Errorf("invalid procd cache root")
	}
	directory := filepath.Join(root, "sha256", strings.TrimPrefix(expected, "sha256:"))
	// Walk from the trusted host root before creating each directory. In
	// particular, never MkdirAll through a symlink in a configurable cache path.
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(directory, current), string(filepath.Separator)) {
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, 0755); err != nil && !errors.Is(err, os.ErrExist) {
			return "", err
		}
		info, err := os.Lstat(current)
		if err != nil {
			return "", err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !trustedOwner(info) || info.Mode().Perm()&0022 != 0 {
			return "", fmt.Errorf("untrusted procd cache directory: %s", current)
		}
	}
	if _, err := os.Lstat(filepath.Join(directory, "procd")); err == nil {
		return Resolve(root, a)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	temp, err := os.CreateTemp(directory, ".install-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	hash := digest.SHA256.Digester()
	n, err := io.Copy(io.MultiWriter(temp, hash.Hash()), io.LimitReader(input, (512<<20)+1))
	if err != nil {
		return "", err
	}
	if n > 512<<20 || hash.Digest().String() != expected {
		return "", fmt.Errorf("procd source digest mismatch")
	}
	if err := temp.Chmod(0555); err != nil {
		return "", err
	}
	if err := temp.Sync(); err != nil {
		return "", err
	}
	if err := temp.Close(); err != nil {
		return "", err
	}
	if err := os.Link(temp.Name(), filepath.Join(directory, "procd")); err != nil && !errors.Is(err, os.ErrExist) {
		return "", err
	}
	dir, err := os.Open(directory)
	if err != nil {
		return "", err
	}
	err = dir.Sync()
	_ = dir.Close()
	if err != nil {
		return "", err
	}
	return Resolve(root, a)
}
