package rootfscow

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionInitialScanAndSeal(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "initial"), []byte("before"), 0o644))
	session, store, prefix := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))
	reconciliationsBeforeSeal := session.Status().Reconciliations

	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-1", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.True(t, session.Status().Sealed)
	assert.Greater(t, session.Status().Reconciliations, reconciliationsBeforeSeal, "seal must verify upper metadata")
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, head.Root, "initial")
	assert.Equal(t, []byte("before"), readFileEntry(t, store, prefix, entry))
}

func TestSessionInitialScanRetriesTransientReconciliationFailure(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "initial"), []byte("durable"), 0o644))
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-initial-retry")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	capture, err := NewCapture(CaptureConfig{
		Root: root, GenerationID: "generation-initial-retry", ChunkSize: 64 << 10,
		Editor: editor, Writer: writer,
	})
	require.NoError(t, err)
	var attempts atomic.Int32
	capture.scanOverride = func(ctx context.Context, visit func(string, FileVersion)) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient upper scan failure")
		}
		return capture.scan(ctx, visit)
	}
	session, err := StartSession(context.Background(), SessionConfig{
		Capture: capture, EventRoot: root, Protection: noopCaptureProtection{}, WatchFenceRoot: t.TempDir(),
		InitialScanRetryInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	require.Eventually(t, func() bool {
		return strings.Contains(session.Status().LastError, "transient upper scan failure")
	}, time.Second, time.Millisecond)
	require.NoError(t, session.WaitInitial(testContext(t)))
	assert.GreaterOrEqual(t, attempts.Load(), int32(2))
	assert.Empty(t, session.Status().LastError)

	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-initial-retry", Base: testBaseIdentity()})
	require.NoError(t, err)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	entry := mustFindEntry(t, store, writer.Prefix(), head.Root, "initial")
	assert.Equal(t, []byte("durable"), readFileEntry(t, store, writer.Prefix(), entry))
}

func TestSessionSealRejectsIncompleteInitialScan(t *testing.T) {
	session, _, _ := newTestSession(t, t.TempDir())
	require.NoError(t, session.WaitInitial(testContext(t)))
	session.mu.Lock()
	session.initialScanComplete = false
	session.mu.Unlock()

	_, err := session.Seal(testContext(t), SealRequest{HeadID: "head-too-early", Base: testBaseIdentity()})
	assert.ErrorIs(t, err, ErrInitialScanIncomplete)
}

func TestStartSessionRejectsCanceledParentBeforeCreatingFence(t *testing.T) {
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-canceled")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	capture, err := NewCapture(CaptureConfig{
		Root:         t.TempDir(),
		GenerationID: "generation-canceled",
		Editor:       editor,
		Writer:       writer,
	})
	require.NoError(t, err)
	fenceRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	session, err := StartSession(ctx, SessionConfig{Capture: capture, EventRoot: capture.Root(), WatchFenceRoot: fenceRoot})
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, session)
	entries, readErr := os.ReadDir(fenceRoot)
	require.NoError(t, readErr)
	assert.Empty(t, entries)
}

func TestStartSessionRequiresEventRoot(t *testing.T) {
	root := t.TempDir()
	capture, _, _, _ := newTestCapture(t, root, 1<<20)
	session, err := StartSession(context.Background(), SessionConfig{
		Capture: capture, Protection: noopCaptureProtection{}, WatchFenceRoot: t.TempDir(),
	})
	require.ErrorContains(t, err, "rootfs event root is required")
	assert.Nil(t, session)
}

func TestSessionSealFallsBackToFullReconciliationAfterWatcherUncertainty(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "value"), []byte("before"), 0o644))
	session, _, _ := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))
	reconciliationsBeforeSeal := session.Status().Reconciliations
	session.markWatcherUnhealthy(errors.New("simulated overflow"))
	assert.Empty(t, session.Status().LastError)

	_, err := session.Seal(testContext(t), SealRequest{HeadID: "head-fallback", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.Greater(t, session.Status().Reconciliations, reconciliationsBeforeSeal)
	assert.False(t, session.Status().NeedsFullReconcile)
}

func TestSealFinalReconciliationCapturesImmediateWriteAndDelete(t *testing.T) {
	root := t.TempDir()
	removed := filepath.Join(root, "removed")
	require.NoError(t, os.WriteFile(removed, []byte("remove me"), 0o644))
	session, store, prefix := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))

	require.NoError(t, os.Remove(removed))
	require.NoError(t, os.WriteFile(filepath.Join(root, "last-moment"), []byte("durable"), 0o600))
	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-final", Base: testBaseIdentity()})
	require.NoError(t, err)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	_, found := findEntry(t, store, prefix, head.Root, "removed")
	assert.False(t, found)
	entry := mustFindEntry(t, store, prefix, head.Root, "last-moment")
	assert.Equal(t, []byte("durable"), readFileEntry(t, store, prefix, entry))
}

func TestSessionSealIsIdempotentUntilAcknowledgedThenContinues(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "value"), []byte("one"), 0o644))
	session, _, _ := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))

	first, err := session.Seal(testContext(t), SealRequest{HeadID: "head-idempotent", Base: testBaseIdentity()})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "value"), []byte("two"), 0o644))
	second, err := session.Seal(testContext(t), SealRequest{HeadID: "head-idempotent", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.Equal(t, first.Reference, second.Reference)
	_, err = session.Seal(testContext(t), SealRequest{HeadID: "different", Base: testBaseIdentity()})
	assert.ErrorIs(t, err, ErrSessionSealed)
	require.NoError(t, session.Acknowledge(testContext(t), "head-idempotent", true, true))
	assert.True(t, session.Status().NeedsFullReconcile)
	require.Eventually(t, func() bool {
		status := session.Status()
		return status.DirtyPaths == 0 && status.ActiveCaptures == 0
	}, 5*time.Second, 10*time.Millisecond)
	third, err := session.Seal(testContext(t), SealRequest{HeadID: "different", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.NotEqual(t, first.Reference, third.Reference)
}

func TestSealRefreshesEveryHardlinkAliasAfterOnePathChanges(t *testing.T) {
	root := t.TempDir()
	firstPath := filepath.Join(root, "first")
	require.NoError(t, os.WriteFile(firstPath, []byte("before"), 0o644))
	require.NoError(t, os.Link(firstPath, filepath.Join(root, "second")))
	session, store, prefix := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))
	reconciliationsBeforeSeal := session.Status().Reconciliations

	require.NoError(t, os.WriteFile(firstPath, []byte("after"), 0o644))
	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-hardlinks", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.Greater(t, session.Status().Reconciliations, reconciliationsBeforeSeal)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	first := mustFindEntry(t, store, prefix, head.Root, "first")
	second := mustFindEntry(t, store, prefix, head.Root, "second")
	assert.Equal(t, first.Inode, second.Inode)
	assert.Equal(t, first.File, second.File)
	assert.Equal(t, []byte("after"), readFileEntry(t, store, prefix, first))
	assert.Equal(t, []byte("after"), readFileEntry(t, store, prefix, second))
}

func TestSealRefreshesRemainingHardlinkAfterAliasRemoval(t *testing.T) {
	root := t.TempDir()
	firstPath := filepath.Join(root, "first")
	secondPath := filepath.Join(root, "second")
	require.NoError(t, os.WriteFile(firstPath, []byte("linked"), 0o644))
	require.NoError(t, os.Link(firstPath, secondPath))
	session, store, prefix := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))
	reconciliationsBeforeSeal := session.Status().Reconciliations

	require.NoError(t, os.Remove(secondPath))
	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-hardlink-remove", Base: testBaseIdentity()})
	require.NoError(t, err)
	assert.Greater(t, session.Status().Reconciliations, reconciliationsBeforeSeal)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	first := mustFindEntry(t, store, prefix, head.Root, "first")
	assert.Equal(t, uint32(1), first.Nlink)
	_, found := findEntry(t, store, prefix, head.Root, "second")
	assert.False(t, found)
}

func TestSessionAbandonedSealCanBeRetriedWithoutMutation(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "value"), []byte("one"), 0o644))
	session, store, _ := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))

	first, err := session.Seal(testContext(t), SealRequest{HeadID: "head-abandoned", Base: testBaseIdentity()})
	require.NoError(t, err)
	require.NoError(t, session.Acknowledge(testContext(t), "head-abandoned", false, true))
	second, err := session.Seal(testContext(t), SealRequest{HeadID: "head-retry", Base: testBaseIdentity()})
	require.NoError(t, err)
	firstHead, err := rootfsstore.LoadHead(context.Background(), store, first.Reference)
	require.NoError(t, err)
	secondHead, err := rootfsstore.LoadHead(context.Background(), store, second.Reference)
	require.NoError(t, err)
	assert.Equal(t, firstHead.Root, secondHead.Root)
}

func TestSessionTracksWatcherChangesBeforeSeal(t *testing.T) {
	root := t.TempDir()
	session, _, _ := newTestSession(t, root)
	require.NoError(t, session.WaitInitial(testContext(t)))
	require.NoError(t, os.Mkdir(filepath.Join(root, "dir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "dir", "value"), bytes.Repeat([]byte("x"), 1<<20), 0o644))
	require.Eventually(t, func() bool {
		status := session.Status()
		return status.DirtyPaths == 0 && status.ActiveCaptures == 0
	}, 5*time.Second, 10*time.Millisecond)
	assert.Empty(t, session.Status().LastError)
}

func TestSessionCapturesMergedWriteWhenUpperWatcherMissesIt(t *testing.T) {
	upper := t.TempDir()
	merged := t.TempDir()
	path := ".bashrc"
	require.NoError(t, os.WriteFile(filepath.Join(merged, path), []byte("base"), 0o644))
	session, store, prefix := newTestSessionWithEventRoot(t, upper, merged)
	require.NoError(t, session.WaitInitial(testContext(t)))
	require.NoError(t, session.watcher.Remove(upper))

	require.NoError(t, os.WriteFile(filepath.Join(upper, path), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(merged, path), nil, 0o644))
	require.Eventually(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		version, ok := session.known[path]
		return ok && version.Size == 0
	}, 5*time.Second, 10*time.Millisecond)

	content := []byte("export ROOTFS_COW=durable\n")
	require.NoError(t, os.WriteFile(filepath.Join(upper, path), content, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(merged, path), content, 0o644))
	require.Eventually(t, func() bool {
		session.mu.Lock()
		defer session.mu.Unlock()
		version, ok := session.known[path]
		return ok && version.Size == int64(len(content))
	}, 5*time.Second, 10*time.Millisecond)

	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-merged-event", Base: testBaseIdentity()})
	require.NoError(t, err)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	entry := mustFindEntry(t, store, prefix, head.Root, path)
	assert.Equal(t, content, readFileEntry(t, store, prefix, entry))
}

func TestSessionInitialProtectionErrorRecoversBeforeCaptureStarts(t *testing.T) {
	root := t.TempDir()
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-protection-error")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	capture, err := NewCapture(CaptureConfig{
		Root: root, GenerationID: "generation-protection-error", ChunkSize: 64 << 10,
		Editor: editor, Writer: writer,
	})
	require.NoError(t, err)
	protection := &controlledCaptureProtection{checkpointErr: errors.New("database unavailable")}
	session, err := StartSession(context.Background(), SessionConfig{
		Capture: capture, EventRoot: root, Protection: protection, WatchFenceRoot: t.TempDir(),
		CaptureWorkers: 1, FlushInterval: time.Hour, ReconcileInterval: time.Hour,
		InitialScanRetryInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	require.Eventually(t, func() bool {
		return strings.Contains(session.Status().LastError, "protection: database unavailable")
	}, time.Second, time.Millisecond)
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, session.WaitInitial(waitCtx), context.DeadlineExceeded)

	protection.setCheckpointError(nil)
	require.NoError(t, session.WaitInitial(testContext(t)))
	assert.Empty(t, session.Status().LastError)

	require.NoError(t, os.WriteFile(filepath.Join(root, "later"), []byte("captured"), 0o644))
	require.Eventually(t, func() bool {
		status := session.Status()
		return status.DirtyPaths == 0 && status.ActiveCaptures == 0
	}, 5*time.Second, 10*time.Millisecond)
	assert.Empty(t, session.Status().LastError)
}

func TestCaptureRetryDelayIsExponentiallyBounded(t *testing.T) {
	assert.Equal(t, defaultCaptureRetryMin, captureRetryDelay(0))
	assert.Equal(t, defaultCaptureRetryMin, captureRetryDelay(1))
	assert.Equal(t, 2*defaultCaptureRetryMin, captureRetryDelay(2))
	assert.Equal(t, defaultCaptureRetryMax, captureRetryDelay(100))
}

func TestSessionDirtyBytesEstimatesNewAndRemovedFiles(t *testing.T) {
	root := t.TempDir()
	payload := bytes.Repeat([]byte("x"), 3<<20)
	require.NoError(t, os.WriteFile(filepath.Join(root, "new"), payload, 0o644))
	capture, _, _, _ := newTestCapture(t, root, 1<<20)
	session := &Session{
		capture:   capture,
		dirty:     make(map[string]dirtyPath),
		known:     make(map[string]FileVersion),
		accepting: true,
		wake:      make(chan struct{}, 1),
	}

	session.mark("new")
	assert.Equal(t, int64(len(payload)), session.Status().DirtyBytes)

	require.NoError(t, os.Remove(filepath.Join(root, "new")))
	session.mark("new")
	assert.Zero(t, session.Status().DirtyBytes)
}

func TestSessionExcludesOwnedPaths(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "tmp"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "tmp", "runtime"), []byte("excluded"), 0o644))
	session, store, prefix := newTestSessionWithExcludes(t, root, []string{"/tmp"})
	require.NoError(t, session.WaitInitial(testContext(t)))
	result, err := session.Seal(testContext(t), SealRequest{HeadID: "head-excludes", Base: testBaseIdentity()})
	require.NoError(t, err)
	head, err := rootfsstore.LoadHead(context.Background(), store, result.Reference)
	require.NoError(t, err)
	_, found := findEntry(t, store, prefix, head.Root, "tmp")
	assert.False(t, found)
}

func newTestSession(t *testing.T, root string) (*Session, objectstore.Store, string) {
	t.Helper()
	return newTestSessionWithExcludes(t, root, nil)
}

func newTestSessionWithExcludes(t *testing.T, root string, excludes []string) (*Session, objectstore.Store, string) {
	t.Helper()
	return newTestSessionWithExcludesAndEventRoot(t, root, root, excludes)
}

func newTestSessionWithEventRoot(t *testing.T, root, eventRoot string) (*Session, objectstore.Store, string) {
	t.Helper()
	return newTestSessionWithExcludesAndEventRoot(t, root, eventRoot, nil)
}

func newTestSessionWithExcludesAndEventRoot(t *testing.T, root, eventRoot string, excludes []string) (*Session, objectstore.Store, string) {
	t.Helper()
	store := objectstore.NewMemoryStore(t.Name())
	writer, err := rootfsstore.NewTeamWriter(store, "team-session")
	require.NoError(t, err)
	editor, err := NewEditor(store, writer, nil)
	require.NoError(t, err)
	capture, err := NewCapture(CaptureConfig{
		Root:          root,
		GenerationID:  "generation-session",
		ExcludedPaths: excludes,
		ChunkSize:     64 << 10,
		Editor:        editor,
		Writer:        writer,
	})
	require.NoError(t, err)
	session, err := StartSession(context.Background(), SessionConfig{
		Capture:           capture,
		EventRoot:         eventRoot,
		Protection:        noopCaptureProtection{},
		WatchFenceRoot:    t.TempDir(),
		CaptureWorkers:    2,
		FlushInterval:     20 * time.Millisecond,
		ReconcileInterval: time.Hour,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	return session, store, writer.Prefix()
}

type noopCaptureProtection struct{}

func (noopCaptureProtection) Begin(context.Context) error { return nil }

func (noopCaptureProtection) Checkpoint(context.Context, []rootfshead.Object) error { return nil }

func (noopCaptureProtection) Reset(context.Context) error { return nil }

type controlledCaptureProtection struct {
	mu            sync.Mutex
	checkpointErr error
}

func (*controlledCaptureProtection) Begin(context.Context) error { return nil }

func (p *controlledCaptureProtection) Checkpoint(context.Context, []rootfshead.Object) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkpointErr
}

func (*controlledCaptureProtection) Reset(context.Context) error { return nil }

func (p *controlledCaptureProtection) setCheckpointError(err error) {
	p.mu.Lock()
	p.checkpointErr = err
	p.mu.Unlock()
}

func testBaseIdentity() rootfshead.BaseIdentity {
	return rootfshead.BaseIdentity{
		ImageReference: "registry.example/base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ManifestDigest: digest.FromString("manifest").String(),
		ChainID:        digest.FromString("chain").String(),
		OS:             "linux",
		Architecture:   "amd64",
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
