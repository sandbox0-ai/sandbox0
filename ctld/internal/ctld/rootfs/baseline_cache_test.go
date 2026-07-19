package rootfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSBaselineCaptureRejectsHardEntryLimitWithoutPublishingTemp(t *testing.T) {
	runtime := newBaselineRuntimeForTest(t, 16<<10, 1<<20, 8, 1<<20, 8, time.Hour)
	source := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(source, "large"), []byte(strings.Repeat("x", 20<<10)), 0o600))

	err := runtime.captureRootFSBaseline(
		context.Background(),
		baselineInfoForTest(),
		"team-1",
		"sandbox-1",
		"layer-large",
		source,
		rootFSPathFilter{},
	)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrRootFSBaselineCacheCapacity)
	entries, readErr := os.ReadDir(runtime.rootFSBaselineRoot())
	require.NoError(t, readErr)
	for _, entry := range entries {
		assert.Equal(t, rootFSBaselineLockName, entry.Name())
	}
}

func TestRootFSBaselineRepeatedCaptureEvictsSameTeamBeforeOtherTeam(t *testing.T) {
	runtime := newBaselineRuntimeForTest(t, 64<<10, 1<<20, 8, 1<<20, 2, time.Hour)
	source := baselineSourceForTest(t, "payload")
	info := baselineInfoForTest()

	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-other", "sandbox-other", "other-1", source, rootFSPathFilter{}))
	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", "team-1-a", source, rootFSPathFilter{}))
	firstTeamEntry := filepath.Dir(runtime.rootFSBaselinePath(info, "team-1-a"))
	old := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(firstTeamEntry, old, old))
	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", "team-1-b", source, rootFSPathFilter{}))
	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", "team-1-c", source, rootFSPathFilter{}))

	_, err := os.Stat(firstTeamEntry)
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Dir(runtime.rootFSBaselinePath(info, "other-1")))
	assert.NoError(t, err)
	entries, _, err := runtime.rootFSBaselineEntriesLocked()
	require.NoError(t, err)
	var teamOne, other int
	for _, entry := range entries {
		switch entry.teamID {
		case "team-1":
			teamOne++
		case "team-other":
			other++
		}
	}
	assert.Equal(t, 2, teamOne)
	assert.Equal(t, 1, other)
}

func TestRootFSBaselineStartupSweepRemovesCrashAndInvalidLayouts(t *testing.T) {
	runtime := newBaselineRuntimeForTest(t, 64<<10, 1<<20, 8, 1<<20, 8, time.Hour)
	source := baselineSourceForTest(t, "payload")
	info := baselineInfoForTest()
	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", "valid", source, rootFSPathFilter{}))
	validEntry := filepath.Dir(runtime.rootFSBaselinePath(info, "valid"))

	require.NoError(t, os.MkdirAll(filepath.Join(runtime.rootFSBaselineRoot(), ".baseline-crash", "data"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(runtime.rootFSBaselineRoot(), "legacy", "data"), 0o700))

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, runtime.StartBaselineCache(ctx))
	cancel()

	_, err := os.Stat(filepath.Join(runtime.rootFSBaselineRoot(), ".baseline-crash"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(runtime.rootFSBaselineRoot(), "legacy"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(validEntry)
	assert.NoError(t, err)
}

func TestRootFSBaselineSweepExpiresIdleEntry(t *testing.T) {
	runtime := newBaselineRuntimeForTest(t, 64<<10, 1<<20, 8, 1<<20, 8, time.Minute)
	source := baselineSourceForTest(t, "payload")
	info := baselineInfoForTest()
	require.NoError(t, runtime.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", "expired", source, rootFSPathFilter{}))
	entry := filepath.Dir(runtime.rootFSBaselinePath(info, "expired"))
	old := time.Now().Add(-2 * time.Minute)
	require.NoError(t, os.Chtimes(entry, old, old))

	require.NoError(t, runtime.SweepRootFSBaselines())
	_, err := os.Stat(entry)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestRootFSBaselineSweepEnforcesPerTeamCountAfterRestart(t *testing.T) {
	root := t.TempDir()
	writer := NewContainerdRuntime(ContainerdRuntimeConfig{
		RootFSCacheDir:         root,
		BaselineMaxBytes:       64 << 10,
		BaselineMaxTotalBytes:  1 << 20,
		BaselineMaxEntries:     8,
		BaselineMaxTeamBytes:   1 << 20,
		BaselineMaxTeamEntries: 8,
		BaselineTTL:            time.Hour,
	})
	source := baselineSourceForTest(t, "payload")
	info := baselineInfoForTest()
	for _, layerID := range []string{"layer-a", "layer-b"} {
		require.NoError(t, writer.captureRootFSBaseline(context.Background(), info, "team-1", "sandbox-1", layerID, source, rootFSPathFilter{}))
	}
	sameTime := time.Now().Add(-time.Minute)
	for _, layerID := range []string{"layer-a", "layer-b"} {
		require.NoError(t, os.Chtimes(filepath.Dir(writer.rootFSBaselinePath(info, layerID)), sameTime, sameTime))
	}

	sweeper := NewContainerdRuntime(ContainerdRuntimeConfig{
		RootFSCacheDir:         root,
		BaselineMaxBytes:       64 << 10,
		BaselineMaxTotalBytes:  1 << 20,
		BaselineMaxEntries:     8,
		BaselineMaxTeamBytes:   1 << 20,
		BaselineMaxTeamEntries: 1,
		BaselineTTL:            time.Hour,
	})
	require.NoError(t, sweeper.SweepRootFSBaselines())

	_, err := os.Stat(filepath.Dir(sweeper.rootFSBaselinePath(info, "layer-a")))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Dir(sweeper.rootFSBaselinePath(info, "layer-b")))
	assert.NoError(t, err)
}

func newBaselineRuntimeForTest(
	t *testing.T,
	maxBytes, maxTotalBytes int64,
	maxEntries int,
	maxTeamBytes int64,
	maxTeamEntries int,
	ttl time.Duration,
) *ContainerdRuntime {
	t.Helper()
	return NewContainerdRuntime(ContainerdRuntimeConfig{
		RootFSCacheDir:         t.TempDir(),
		BaselineMaxBytes:       maxBytes,
		BaselineMaxTotalBytes:  maxTotalBytes,
		BaselineMaxEntries:     maxEntries,
		BaselineMaxTeamBytes:   maxTeamBytes,
		BaselineMaxTeamEntries: maxTeamEntries,
		BaselineTTL:            ttl,
	})
}

func baselineSourceForTest(t *testing.T, payload string) string {
	t.Helper()
	source := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(source, "dir"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(source, "dir", "file"), []byte(payload), 0o640))
	return source
}

func baselineInfoForTest() ctldapi.RootFSInfo {
	return ctldapi.RootFSInfo{
		ContainerID:   "container-1",
		ContainerName: "sandbox",
		PodNamespace:  "default",
		PodName:       "pod-1",
		PodUID:        "uid-1",
	}
}
