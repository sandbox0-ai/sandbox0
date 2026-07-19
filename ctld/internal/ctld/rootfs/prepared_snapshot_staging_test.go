package rootfs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPreparedSnapshotRejectsOversizedArtifactAndCleansFiles(t *testing.T) {
	dir := t.TempDir()
	runtime := &fakeRuntime{
		info:          rootFSInfo("runc"),
		createDesc:    rootFSDiffDescriptorForPayload("", "12345"),
		createContent: "12345",
	}
	controller := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    dir,
		PreparedSnapshotMaxBytes:       4,
		PreparedSnapshotMaxTotalBytes:  64,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   64,
		PreparedSnapshotMaxTeamEntries: 8,
	})

	resp, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("oversized", "team-1"),
	)

	assert.Equal(t, http.StatusRequestEntityTooLarge, status, resp.Error)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, preparedSnapshotLockName, entry.Name())
	}
}

func TestPreparedSnapshotRetainedTeamEntryLimitDoesNotBlockAnotherTeam(t *testing.T) {
	dir := t.TempDir()
	runtime := &fakeRuntime{
		info:          rootFSInfo("runc"),
		createDesc:    rootFSDiffDescriptorForPayload("", "x"),
		createContent: "x",
	}
	controller := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    dir,
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  8192,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 1,
		PreparedSnapshotMaxConcurrent:  2,
		PreparedSnapshotMaxPerTeam:     1,
	})

	first, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("team-a-first", "team-a"),
	)
	require.Equal(t, http.StatusOK, status, first.Error)

	second, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("team-a-second", "team-a"),
	)
	assert.Equal(t, http.StatusInsufficientStorage, status, second.Error)

	other, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("team-b-first", "team-b"),
	)
	assert.Equal(t, http.StatusOK, status, other.Error)
}

func TestPreparedSnapshotPerTeamConcurrencyIsIsolated(t *testing.T) {
	runtime := newBlockingPreparedSnapshotRuntime()
	controller := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    t.TempDir(),
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  8192,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 4,
		PreparedSnapshotMaxConcurrent:  2,
		PreparedSnapshotMaxPerTeam:     1,
	})
	firstDone := make(chan int, 1)
	go func() {
		_, status := controller.PrepareRootFSSnapshot(
			httptest.NewRequest(http.MethodPost, "/", nil),
			preparedSnapshotRequest("team-a-active", "team-a"),
		)
		firstDone <- status
	}()
	<-runtime.entered

	sameTeam, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("team-a-rejected", "team-a"),
	)
	assert.Equal(t, http.StatusTooManyRequests, status, sameTeam.Error)

	otherTeam, status := controller.PrepareRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		preparedSnapshotRequest("team-b-admitted", "team-b"),
	)
	assert.Equal(t, http.StatusOK, status, otherTeam.Error)

	close(runtime.release)
	assert.Equal(t, http.StatusOK, <-firstDone)
}

func TestPreparedSnapshotRetryUsesStableHandleWithoutRecreatingDiff(t *testing.T) {
	runtime := newBlockingPreparedSnapshotRuntime()
	close(runtime.release)
	controller := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    t.TempDir(),
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  8192,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 4,
	})
	req := preparedSnapshotRequest("stable-stage", "team-1")

	first, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), req)
	require.Equal(t, http.StatusOK, status, first.Error)
	second, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), req)
	require.Equal(t, http.StatusOK, status, second.Error)

	assert.Equal(t, first, second)
	assert.Equal(t, int32(1), runtime.createCalls.Load())
}

func TestPreparedSnapshotSweepEnforcesPerTeamRetainedLimitDeterministically(t *testing.T) {
	dir := t.TempDir()
	runtime := &fakeRuntime{
		info:          rootFSInfo("runc"),
		createDesc:    rootFSDiffDescriptorForPayload("", "x"),
		createContent: "x",
	}
	writer := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    dir,
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  8192,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 4,
	})
	for _, handle := range []string{"stage-a", "stage-b"} {
		resp, status := writer.PrepareRootFSSnapshot(
			httptest.NewRequest(http.MethodPost, "/", nil),
			preparedSnapshotRequest(handle, "team-1"),
		)
		require.Equal(t, http.StatusOK, status, resp.Error)
	}
	sameTime := time.Now().Add(-time.Minute)
	for _, handle := range []string{"stage-a", "stage-b"} {
		require.NoError(t, os.Chtimes(filepath.Join(dir, handle+".tar"), sameTime, sameTime))
		require.NoError(t, os.Chtimes(filepath.Join(dir, handle+".json"), sameTime, sameTime))
	}

	sweeper := NewController(Config{
		Runtime:                        runtime,
		SnapshotDir:                    dir,
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  8192,
		PreparedSnapshotMaxEntries:     8,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 1,
	})
	require.NoError(t, sweeper.SweepPreparedSnapshots())

	_, err := os.Stat(filepath.Join(dir, "stage-a.json"))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, "stage-b.json"))
	assert.NoError(t, err)
}

func TestPreparedSnapshotSweepRetainsActivelyLockedExpiredReservation(t *testing.T) {
	controller := NewController(Config{
		SnapshotDir:                    t.TempDir(),
		PreparedSnapshotMaxBytes:       1024,
		PreparedSnapshotMaxTotalBytes:  4096,
		PreparedSnapshotMaxEntries:     4,
		PreparedSnapshotMaxTeamBytes:   4096,
		PreparedSnapshotMaxTeamEntries: 4,
		PreparedSnapshotTTL:            time.Minute,
	})
	owner, err := validateRootFSCacheOwner("team-1", "sandbox-1")
	require.NoError(t, err)
	reservation, err := controller.reservePreparedSnapshot("active-stage", owner, time.Now().Add(time.Minute))
	require.NoError(t, err)

	require.NoError(t, controller.withPreparedSnapshotLock(func() error {
		return controller.sweepPreparedSnapshotsLocked(time.Now().Add(2 * time.Minute))
	}))
	_, err = os.Stat(controller.preparedSnapshotReservationPath("active-stage"))
	assert.NoError(t, err)
	require.NoError(t, controller.discardPreparedSnapshot("active-stage", reservation))
}

func TestPreparedSnapshotAbortRejectsOwnerMismatch(t *testing.T) {
	runtime := &fakeRuntime{
		info:          rootFSInfo("runc"),
		createDesc:    rootFSDiffDescriptorForPayload("", "x"),
		createContent: "x",
	}
	controller := NewController(Config{Runtime: runtime, SnapshotDir: t.TempDir()})
	req := preparedSnapshotRequest("owned-stage", "team-1")
	prepared, status := controller.PrepareRootFSSnapshot(httptest.NewRequest(http.MethodPost, "/", nil), req)
	require.Equal(t, http.StatusOK, status, prepared.Error)

	resp, status := controller.AbortRootFSSnapshot(
		httptest.NewRequest(http.MethodPost, "/", nil),
		ctldapi.AbortRootFSSnapshotRequest{
			Handle:    prepared.Handle,
			TeamID:    "team-other",
			SandboxID: req.SandboxID,
		},
	)
	assert.Equal(t, http.StatusConflict, status, resp.Error)
	_, err := os.Stat(controller.preparedSnapshotMetaPath(prepared.Handle))
	assert.NoError(t, err)
}

func preparedSnapshotRequest(stageID, teamID string) ctldapi.PrepareRootFSSnapshotRequest {
	return ctldapi.PrepareRootFSSnapshotRequest{
		Target:    rootFSTarget(),
		StageID:   stageID,
		TeamID:    teamID,
		SandboxID: "sandbox-1",
		ExpiresAt: time.Now().Add(time.Minute),
	}
}

type blockingPreparedSnapshotRuntime struct {
	createCalls atomic.Int32
	entered     chan struct{}
	release     chan struct{}
}

func newBlockingPreparedSnapshotRuntime() *blockingPreparedSnapshotRuntime {
	return &blockingPreparedSnapshotRuntime{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (r *blockingPreparedSnapshotRuntime) Inspect(context.Context, ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	return rootFSInfo("runc"), nil
}

func (r *blockingPreparedSnapshotRuntime) CreateDiff(ctx context.Context, _ ctldapi.RootFSInfo, _ []string, _ []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if r.createCalls.Add(1) == 1 {
		close(r.entered)
		select {
		case <-ctx.Done():
			return ctldapi.RootFSDiffDescriptor{}, nil, ctx.Err()
		case <-r.release:
		}
	}
	payload := "x"
	return rootFSDiffDescriptorForPayload("", payload), readSeekNopCloser{Reader: strings.NewReader(payload)}, nil
}

func (*blockingPreparedSnapshotRuntime) CreateDiffFromBaseline(context.Context, ctldapi.RootFSInfo, string, []string, []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	panic("unexpected CreateDiffFromBaseline")
}

func (*blockingPreparedSnapshotRuntime) ApplyDiff(context.Context, ctldapi.RootFSInfo, ctldapi.RootFSDiffDescriptor, io.Reader, []string, []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, error) {
	panic("unexpected ApplyDiff")
}

func (*blockingPreparedSnapshotRuntime) CaptureBaseline(context.Context, ctldapi.RootFSInfo, string, string, string, []string, []ctldapi.RootFSPortalPath) error {
	panic("unexpected CaptureBaseline")
}
