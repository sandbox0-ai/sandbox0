//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeCPUBundleCompanions(t *testing.T, main string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(filepath.Dir(main), "gvisor-bin"), 0700))
	for _, name := range cpuBundleNames[1:] {
		require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(main), name), []byte("fixture:"+name), 0700))
	}
}

func TestCPUBundleIdentityCoversCompanionsAcrossInstallations(t *testing.T) {
	newBundle := func() *Command {
		main := filepath.Join(t.TempDir(), "runsc")
		require.NoError(t, os.WriteFile(main, []byte("runsc fixture"), 0700))
		writeCPUBundleCompanions(t, main)
		return New(Config{Path: main}).(*Command)
	}
	a, b := newBundle(), newBundle()
	first, err := a.ExecutableDigest(t.Context())
	require.NoError(t, err)
	second, err := b.ExecutableDigest(t.Context())
	require.NoError(t, err)
	require.Equal(t, first, second, "node-local installation paths are not executable identity")
	for _, name := range cpuBundleNames[1:] {
		t.Run(name, func(t *testing.T) {
			monitor, err := watchCPUBundle(b)
			require.NoError(t, err)
			defer monitor.Close()
			path := filepath.Join(filepath.Dir(b.config.Path), name)
			original, err := os.ReadFile(path)
			require.NoError(t, err)
			info, err := os.Stat(path)
			require.NoError(t, err)
			changed := append([]byte(nil), original...)
			changed[0] ^= 1
			require.NoError(t, os.WriteFile(path, changed, 0700))
			require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
			require.Error(t, monitor.Unchanged(), "sidecar writes invalidate fast launch witnesses")
			next, err := b.ExecutableDigest(t.Context())
			require.NoError(t, err)
			require.NotEqual(t, first, next)
			require.NoError(t, os.WriteFile(path, original, 0700))
			require.Error(t, monitor.Unchanged(), "restoring old bytes cannot revive a launch witness")
		})
	}
	path := filepath.Join(filepath.Dir(b.config.Path), cpuBundleNames[1])
	require.NoError(t, os.Remove(path))
	_, err = b.ExecutableDigest(t.Context())
	require.Error(t, err, "missing sidecar cannot fall back to main-binary identity")
	require.NoError(t, os.Symlink(filepath.Join(filepath.Dir(a.config.Path), cpuBundleNames[1]), path))
	_, err = b.ExecutableDigest(t.Context())
	require.Error(t, err, "ambiguous companion resolution is not qualified")
	for _, name := range []string{"GVISOR_SIDECAR_BINARIES_DIR", "GVISOR_ENFORCE_RELEASE"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "override")
			_, err := a.ExecutableDigest(t.Context())
			require.ErrorContains(t, err, name)
		})
	}
}
