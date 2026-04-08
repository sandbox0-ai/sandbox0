package cgroup

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreezeAndThawCgroupV2(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, freezeV2File), []byte("0\n"), 0o644))
	fs := &FS{SettleTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond}

	require.NoError(t, fs.Freeze(dir))
	data, err := os.ReadFile(filepath.Join(dir, freezeV2File))
	require.NoError(t, err)
	assert.Equal(t, "1", string(data))

	require.NoError(t, fs.Thaw(dir))
	data, err = os.ReadFile(filepath.Join(dir, freezeV2File))
	require.NoError(t, err)
	assert.Equal(t, "0", string(data))
}

func TestFreezeAndThawCgroupV1(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, freezeV1File), []byte("THAWED\n"), 0o644))
	fs := &FS{SettleTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond}

	require.NoError(t, fs.Freeze(dir))
	data, err := os.ReadFile(filepath.Join(dir, freezeV1File))
	require.NoError(t, err)
	assert.Equal(t, "FROZEN", string(data))

	require.NoError(t, fs.Thaw(dir))
	data, err = os.ReadFile(filepath.Join(dir, freezeV1File))
	require.NoError(t, err)
	assert.Equal(t, "THAWED", string(data))
}

func TestMemoryCurrentPrefersCgroupV2(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, memoryV2File), []byte("123\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, memoryV1File), []byte("999\n"), 0o644))

	value, err := NewFS().MemoryCurrent(dir)
	require.NoError(t, err)
	assert.Equal(t, int64(123), value)
}

func TestMemoryCurrentFallsBackToCgroupV1(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, memoryV1File), []byte("456\n"), 0o644))

	value, err := NewFS().MemoryCurrent(dir)
	require.NoError(t, err)
	assert.Equal(t, int64(456), value)
}
