//go:build linux

package procdartifact

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestInstallPinsVersionsAndRejectsCacheTampering(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned cache ancestors")
	}
	root, err := os.MkdirTemp("/root", "procd-artifact-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cache := filepath.Join(root, "cache")
	source := filepath.Join(root, "source")
	first := []byte("first executable")
	second := []byte("second executable")
	require.NoError(t, os.WriteFile(source, first, 0755))
	a := Artifact{Digest: digest.FromBytes(first).String(), Protocol: "v3"}
	path, err := Install(cache, source, a.Digest)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0555), info.Mode().Perm())
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := Install(cache, source, a.Digest); require.NoError(t, err) }()
	}
	wg.Wait()
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(info, after))
	require.NoError(t, os.WriteFile(source, second, 0755))
	other, err := Install(cache, source, digest.FromBytes(second).String())
	require.NoError(t, err)
	require.NotEqual(t, path, other)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, first, data)
	require.NoError(t, os.Chmod(path, 0755))
	_, err = Resolve(cache, a)
	require.ErrorContains(t, err, "immutable")
	require.NoError(t, os.Chmod(path, 0555))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink(other, path))
	_, err = Resolve(cache, a)
	require.ErrorContains(t, err, "symlinks")
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, second, 0555))
	_, err = Resolve(cache, a)
	require.ErrorContains(t, err, "digest mismatch")
	_, err = Install(cache, source, digest.FromString("missing").String())
	require.ErrorContains(t, err, "digest mismatch")
	require.NoError(t, os.Symlink(cache, filepath.Join(root, "alias")))
	_, err = Install(filepath.Join(root, "alias"), source, a.Digest)
	require.ErrorContains(t, err, "untrusted")
}

func TestEmbeddedPublicationIsSafeAcrossABVersions(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned cache ancestors")
	}
	root, err := os.MkdirTemp("/root", "procd-embedded-test-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	var wg sync.WaitGroup
	for range 4 {
		for _, payload := range []string{"version-a", "version-b"} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				path, err := InstallEmbedded(root, payload, digest.FromString(payload).String())
				require.NoError(t, err)
				actual, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, payload, string(actual))
			}()
		}
	}
	wg.Wait()
	_, err = InstallEmbedded(root, "wrong", digest.FromString("not-installed").String())
	require.ErrorContains(t, err, "digest mismatch")
}
