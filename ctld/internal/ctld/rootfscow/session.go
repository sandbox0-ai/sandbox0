package rootfscow

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const (
	defaultFlushInterval     = 500 * time.Millisecond
	defaultReconcileInterval = 30 * time.Second
	defaultInitialRetry      = 100 * time.Millisecond
	defaultCaptureWorkers    = 4
	defaultCaptureRetryMin   = 25 * time.Millisecond
	defaultCaptureRetryMax   = time.Second
)

var (
	ErrSessionSealed         = errors.New("rootfs sync session is sealed")
	ErrInitialScanIncomplete = errors.New("rootfs sync initial scan is incomplete")
)

type SessionConfig struct {
	Capture                  *Capture
	EventRoot                string
	Protection               CaptureProtection
	WatchFenceRoot           string
	CaptureWorkers           int
	FlushInterval            time.Duration
	ReconcileInterval        time.Duration
	InitialScanRetryInterval time.Duration
}

// CaptureProtection closes object-deletion races while a generation writes
// CAS objects, then narrows protection to exact checkpointed references.
type CaptureProtection interface {
	Begin(context.Context) error
	Checkpoint(context.Context, []rootfshead.Object) error
	Reset(context.Context) error
}

type SyncStatus struct {
	InitialScanComplete bool
	DirtyPaths          int
	DirtyBytes          int64
	ActiveCaptures      int
	WatcherErrors       uint64
	Reconciliations     uint64
	NeedsFullReconcile  bool
	LastError           string
	Sealing             bool
	Sealed              bool
}

type SealRequest struct {
	HeadID string
	Base   rootfshead.BaseIdentity
}

type SealResult struct {
	Reference         rootfshead.HeadReference
	Head              rootfshead.Head
	CreatedBytes      int64
	CreatedObjects    int64
	ReconcileDuration time.Duration
	FlushDuration     time.Duration
	TotalDuration     time.Duration
}

type dirtyPath struct {
	sequence       uint64
	since          time.Time
	estimatedBytes int64
	failures       int
	retryAt        time.Time
}

type inodeIdentity struct {
	device uint64
	inode  uint64
}

// Session captures content from an active overlay upper while using the merged
// rootfs as its event source. Seal fences events and verifies upper metadata so
// watcher uncertainty cannot publish stale content.
type Session struct {
	capture   *Capture
	eventRoot string

	mu                  sync.Mutex
	dirty               map[string]dirtyPath
	inflight            map[string]bool
	known               map[string]FileVersion
	inodeAliases        map[inodeIdentity]map[string]struct{}
	sequence            uint64
	active              int
	initialScanComplete bool
	watcherErrors       uint64
	watcherStopped      bool
	reconciliations     uint64
	needsFullReconcile  bool
	captureErrors       map[string]error
	reconcileError      error
	flushError          error
	protectionError     error
	sealError           error
	accepting           bool
	sealing             bool
	sealed              bool
	sealHeadID          string
	sealResult          *SealResult
	backgroundStopped   bool

	wake              chan struct{}
	ctx               context.Context
	cancel            context.CancelFunc
	wg                sync.WaitGroup
	watcher           *fsnotify.Watcher
	watchFenceDir     string
	watchFenceWaiters map[string]chan struct{}
	watchFenceSeq     uint64
	workers           int
	flushInterval     time.Duration
	reconcileInterval time.Duration
	initialRetry      time.Duration
	reconcileMu       sync.Mutex
	sealMu            sync.Mutex
	protection        CaptureProtection
	protectionMu      sync.Mutex
	protectionCond    *sync.Cond
	protectionAll     bool
	protectionActive  int
	protectionSeal    bool
	checkpointing     bool
}

func StartSession(parent context.Context, cfg SessionConfig) (*Session, error) {
	if parent == nil {
		return nil, fmt.Errorf("rootfs sync parent context is required")
	}
	if err := parent.Err(); err != nil {
		return nil, fmt.Errorf("start rootfs sync session: %w", err)
	}
	if cfg.Capture == nil {
		return nil, fmt.Errorf("rootfs capture is required")
	}
	if cfg.Protection == nil {
		return nil, fmt.Errorf("rootfs capture protection is required")
	}
	eventRoot := filepath.Clean(strings.TrimSpace(cfg.EventRoot))
	if eventRoot == "" || eventRoot == "." || !filepath.IsAbs(eventRoot) {
		return nil, fmt.Errorf("rootfs event root is required")
	}
	eventRootInfo, err := os.Stat(eventRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect rootfs event root: %w", err)
	}
	if !eventRootInfo.IsDir() {
		return nil, fmt.Errorf("rootfs event root %s is not a directory", eventRoot)
	}
	workers := cfg.CaptureWorkers
	if workers <= 0 {
		workers = defaultCaptureWorkers
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}
	reconcileInterval := cfg.ReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = defaultReconcileInterval
	}
	initialRetry := cfg.InitialScanRetryInterval
	if initialRetry <= 0 {
		initialRetry = defaultInitialRetry
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create rootfs watcher: %w", err)
	}
	watchFenceRoot := filepath.Clean(strings.TrimSpace(cfg.WatchFenceRoot))
	if watchFenceRoot == "" || watchFenceRoot == "." || !filepath.IsAbs(watchFenceRoot) {
		_ = watcher.Close()
		return nil, fmt.Errorf("rootfs watcher fence root is required")
	}
	if err := os.MkdirAll(watchFenceRoot, 0o700); err != nil {
		_ = watcher.Close()
		return nil, fmt.Errorf("create rootfs watcher fence root: %w", err)
	}
	ctx, cancel := context.WithCancel(parent)
	watchFenceDir, err := os.MkdirTemp(watchFenceRoot, "session-")
	if err != nil {
		cancel()
		_ = watcher.Close()
		return nil, fmt.Errorf("create rootfs watcher fence: %w", err)
	}
	if err := watcher.Add(watchFenceDir); err != nil {
		cancel()
		_ = watcher.Close()
		_ = os.RemoveAll(watchFenceDir)
		return nil, fmt.Errorf("watch rootfs fence: %w", err)
	}
	session := &Session{
		capture:           cfg.Capture,
		eventRoot:         eventRoot,
		dirty:             make(map[string]dirtyPath),
		inflight:          make(map[string]bool),
		known:             make(map[string]FileVersion),
		inodeAliases:      make(map[inodeIdentity]map[string]struct{}),
		captureErrors:     make(map[string]error),
		accepting:         true,
		wake:              make(chan struct{}, 1),
		ctx:               ctx,
		cancel:            cancel,
		watcher:           watcher,
		watchFenceDir:     watchFenceDir,
		watchFenceWaiters: make(map[string]chan struct{}),
		workers:           workers,
		flushInterval:     flushInterval,
		reconcileInterval: reconcileInterval,
		initialRetry:      initialRetry,
		protection:        cfg.Protection,
		protectionAll:     true,
	}
	session.protectionCond = sync.NewCond(&session.protectionMu)
	if err := session.addWatchTree(session.capture.Root(), false); err != nil {
		cancel()
		_ = watcher.Close()
		_ = os.RemoveAll(watchFenceDir)
		return nil, err
	}
	session.wg.Add(workers + 3)
	for range workers {
		go session.worker()
	}
	go session.watch()
	go session.maintenance()
	go session.initialScan()
	return session, nil
}

func (s *Session) Status() SyncStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	status := SyncStatus{
		InitialScanComplete: s.initialScanComplete,
		DirtyPaths:          len(s.dirty),
		ActiveCaptures:      s.active,
		WatcherErrors:       s.watcherErrors,
		Reconciliations:     s.reconciliations,
		NeedsFullReconcile:  s.needsFullReconcile,
		Sealing:             s.sealing,
		Sealed:              s.sealed,
	}
	for _, item := range s.dirty {
		status.DirtyBytes += item.estimatedBytes
	}
	status.LastError = s.lastErrorLocked()
	return status
}

// PendingSealReference returns the immutable Head waiting for manager's
// publication decision, including a Head whose marker finalization failed.
func (s *Session) PendingSealReference() *rootfshead.HeadReference {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealResult == nil {
		return nil
	}
	reference := s.sealResult.Reference
	return &reference
}

func (s *Session) WaitInitial(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status := s.Status()
		if status.InitialScanComplete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Session) Seal(ctx context.Context, request SealRequest) (SealResult, error) {
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	started := time.Now()
	request.HeadID = strings.TrimSpace(request.HeadID)
	if request.HeadID == "" {
		return SealResult{}, fmt.Errorf("rootfs head id is required")
	}
	if err := request.Base.Validate(); err != nil {
		return SealResult{}, err
	}
	s.mu.Lock()
	if !s.initialScanComplete {
		s.mu.Unlock()
		return SealResult{}, ErrInitialScanIncomplete
	}
	if s.sealResult != nil {
		if s.sealHeadID != request.HeadID {
			s.mu.Unlock()
			return SealResult{}, ErrSessionSealed
		}
		result := cloneSealResult(*s.sealResult)
		s.mu.Unlock()
		return result, nil
	}
	if s.sealHeadID != "" && s.sealHeadID != request.HeadID {
		s.mu.Unlock()
		return SealResult{}, ErrSessionSealed
	}
	s.sealHeadID = request.HeadID
	s.sealing = true
	s.mu.Unlock()
	if err := s.beginProtection(ctx, true); err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	defer s.endProtection(true)

	if err := s.fenceWatcher(ctx); err != nil {
		s.markWatcherUnhealthy(err)
	}
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
	if err := s.waitIdle(ctx); err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	reconcileStarted := time.Now()
	if err := s.reconcileDirect(ctx); err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	s.mu.Lock()
	s.needsFullReconcile = false
	s.mu.Unlock()
	reconcileDuration := time.Since(reconcileStarted)
	flushStarted := time.Now()
	root, err := s.capture.editor.Flush(ctx)
	if err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	head := rootfshead.Head{Version: rootfshead.Version, HeadID: request.HeadID, Base: request.Base, Root: root}
	payload, err := rootfshead.EncodeHead(head)
	if err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	manifest, err := s.capture.writer.Put(ctx, rootfshead.HeadMediaType, payload)
	if err != nil {
		s.recordSealError(err)
		return SealResult{}, err
	}
	createdBytes, createdObjects := s.capture.writer.CreatedMetrics()
	result := SealResult{
		Reference:         rootfshead.HeadReference{Version: rootfshead.Version, HeadID: request.HeadID, Manifest: manifest},
		Head:              head,
		CreatedBytes:      createdBytes,
		CreatedObjects:    createdObjects,
		ReconcileDuration: reconcileDuration,
		FlushDuration:     time.Since(flushStarted),
		TotalDuration:     time.Since(started),
	}
	s.mu.Lock()
	s.sealing = false
	s.sealed = true
	s.flushError = nil
	s.sealError = nil
	cached := cloneSealResult(result)
	s.sealResult = &cached
	s.mu.Unlock()
	return result, nil
}

// Acknowledge clears one cached seal after manager has either published or
// abandoned it. A successful seal remains a capture accounting fence until
// this acknowledgement, so rotation cannot discard post-seal object refs.
func (s *Session) Acknowledge(ctx context.Context, headID string, published, continueCapture bool) error {
	if s == nil {
		return fmt.Errorf("rootfs sync session is required")
	}
	s.sealMu.Lock()
	defer s.sealMu.Unlock()
	headID = strings.TrimSpace(headID)
	s.mu.Lock()
	if s.sealResult == nil || s.sealHeadID != headID {
		s.mu.Unlock()
		return fmt.Errorf("rootfs sealed Head %q is not pending acknowledgement", headID)
	}
	s.mu.Unlock()
	if continueCapture {
		if err := s.checkpointProtection(ctx, published); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sealed = false
	s.sealHeadID = ""
	s.sealResult = nil
	if published {
		s.capture.writer.RotateGeneration()
	}
	if continueCapture && !s.backgroundStopped {
		// Watch events arriving between seal and publication were deliberately
		// ignored. Force the next seal to reconcile that bounded interval.
		s.needsFullReconcile = true
		s.accepting = true
		s.signalLocked()
	}
	return nil
}

func (s *Session) Close() error {
	s.stopBackground()
	s.mu.Lock()
	s.accepting = false
	s.mu.Unlock()
	return nil
}

func (s *Session) initialScan() {
	defer s.wg.Done()
	for {
		if err := s.enqueueReconciliation(s.ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.setReconcileError(err)
			if !s.waitInitialRetry() {
				return
			}
			continue
		}
		s.setReconcileError(nil)
		break
	}
	if err := s.waitIdle(s.ctx); err != nil {
		return
	}
	for {
		if err := s.checkpointProtection(s.ctx, false); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.setProtectionError(err)
			if !s.waitInitialRetry() {
				return
			}
			continue
		}
		break
	}
	s.mu.Lock()
	s.initialScanComplete = true
	s.mu.Unlock()
}

func (s *Session) waitInitialRetry() bool {
	timer := time.NewTimer(s.initialRetry)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Session) watch() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case event, ok := <-s.watcher.Events:
			if !ok {
				if s.ctx.Err() == nil {
					s.markWatcherStopped(fmt.Errorf("rootfs watcher event stream closed"))
				}
				return
			}
			if s.handleWatchFence(event.Name) {
				continue
			}
			if relative, ok := relativeToRoot(s.capture.Root(), event.Name); ok {
				s.handleUpperEvent(event, relative)
				continue
			}
			if relative, ok := relativeToRoot(s.eventRoot, event.Name); ok {
				s.handleMergedEvent(event, relative)
			}
		case _, ok := <-s.watcher.Errors:
			if !ok {
				if s.ctx.Err() == nil {
					s.markWatcherStopped(fmt.Errorf("rootfs watcher error stream closed"))
				}
				return
			}
			s.markWatcherUnhealthy(fmt.Errorf("rootfs watcher reported an event queue error"))
		}
	}
}

func (s *Session) handleUpperEvent(event fsnotify.Event, relative string) {
	if s.capture.Excludes(relative) {
		return
	}
	if event.Op&fsnotify.Create != 0 {
		if err := s.addWatchTree(event.Name, true); err != nil && !errors.Is(err, fs.ErrNotExist) {
			s.markWatcherUnhealthy(err)
		}
	}
	s.markWithParent(relative)
}

func (s *Session) handleMergedEvent(event fsnotify.Event, relative string) {
	if s.capture.Excludes(relative) {
		return
	}
	if event.Op&fsnotify.Create != 0 {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			if err := s.watcher.Add(event.Name); err != nil {
				s.markWatcherUnhealthy(fmt.Errorf("watch merged rootfs directory %s: %w", event.Name, err))
			}
		}
		upperPath := filepath.Join(s.capture.Root(), filepath.FromSlash(relative))
		if err := s.addWatchTree(upperPath, true); err != nil && !errors.Is(err, fs.ErrNotExist) {
			s.markWatcherUnhealthy(err)
		}
	}
	s.markWithParent(relative)
}

func (s *Session) markWithParent(relative string) {
	s.mark(relative)
	if relative != "" {
		s.mark(filepath.ToSlash(filepath.Dir(relative)))
	}
}

func (s *Session) maintenance() {
	defer s.wg.Done()
	flushTicker := time.NewTicker(s.flushInterval)
	reconcileTicker := time.NewTicker(s.reconcileInterval)
	defer flushTicker.Stop()
	defer reconcileTicker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-flushTicker.C:
			if !s.maintenanceAllowed() {
				continue
			}
			if err := s.beginProtection(s.ctx, false); err != nil {
				if !errors.Is(err, context.Canceled) {
					s.setProtectionError(err)
				}
				continue
			}
			_, err := s.capture.editor.Flush(s.ctx)
			s.endProtection(false)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.setFlushError(err)
				continue
			}
			s.setFlushError(nil)
			if err := s.checkpointProtection(s.ctx, false); err != nil && !errors.Is(err, context.Canceled) {
				s.setProtectionError(err)
			}
		case <-reconcileTicker.C:
			if !s.maintenanceAllowed() {
				continue
			}
			if err := s.enqueueReconciliation(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.setReconcileError(err)
			} else if err == nil {
				s.setReconcileError(nil)
			}
		}
	}
}

func (s *Session) worker() {
	defer s.wg.Done()
	for {
		relative, sequence, ok, retryAfter := s.nextDirty()
		if !ok {
			var retry <-chan time.Time
			var timer *time.Timer
			if retryAfter > 0 {
				timer = time.NewTimer(retryAfter)
				retry = timer.C
			}
			select {
			case <-s.ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case <-s.wake:
				if timer != nil {
					timer.Stop()
				}
			case <-retry:
			}
			continue
		}
		if err := s.beginProtection(s.ctx, false); err != nil {
			s.complete(relative, sequence, CaptureResult{}, err)
			continue
		}
		result, err := s.capture.Path(s.ctx, relative)
		s.endProtection(false)
		s.complete(relative, sequence, result, err)
	}
}

func (s *Session) nextDirty() (string, uint64, bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return "", 0, false, 0
	}
	now := time.Now()
	var selected string
	var item dirtyPath
	var earliestRetry time.Time
	found := false
	for relative, candidate := range s.dirty {
		if s.inflight[relative] {
			continue
		}
		if candidate.retryAt.After(now) {
			if earliestRetry.IsZero() || candidate.retryAt.Before(earliestRetry) {
				earliestRetry = candidate.retryAt
			}
			continue
		}
		if !found || pathLess(relative, selected) {
			selected, item, found = relative, candidate, true
		}
	}
	if !found {
		if earliestRetry.IsZero() {
			return "", 0, false, 0
		}
		return "", 0, false, time.Until(earliestRetry)
	}
	s.inflight[selected] = true
	s.active++
	return selected, item.sequence, true, 0
}

func (s *Session) complete(relative string, sequence uint64, result CaptureResult, captureErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, relative)
	s.active--
	current, exists := s.dirty[relative]
	if captureErr == nil {
		if result.Exists {
			previous, hadPrevious := s.known[relative]
			s.setKnownLocked(relative, result.Version)
			if !hadPrevious || previous != result.Version {
				s.markChangedAliasesLocked(relative, result.Version)
			}
		} else {
			s.removeKnownLocked(relative)
		}
		if exists && current.sequence == sequence {
			delete(s.dirty, relative)
		}
		delete(s.captureErrors, relative)
	} else if !errors.Is(captureErr, context.Canceled) {
		s.captureErrors[relative] = captureErr
		if exists && current.sequence == sequence {
			s.sequence++
			current.sequence = s.sequence
			current.failures++
			current.retryAt = time.Now().Add(captureRetryDelay(current.failures))
			s.dirty[relative] = current
		}
	}
	s.signalLocked()
}

func (s *Session) mark(relative string) {
	relative = cleanRelativePath(relative)
	version, pathExists, statErr := s.capture.Version(relative)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting || s.capture.Excludes(relative) {
		return
	}
	s.sequence++
	item, itemExists := s.dirty[relative]
	if !itemExists {
		item.since = time.Now()
	}
	item.sequence = s.sequence
	if pathExists {
		item.estimatedBytes = max(version.Size, 0)
	} else if statErr != nil {
		known := s.known[relative]
		item.estimatedBytes = max(known.Size, 0)
	} else {
		item.estimatedBytes = 0
	}
	item.failures = 0
	item.retryAt = time.Time{}
	s.dirty[relative] = item
	s.signalLocked()
}

func (s *Session) enqueueReconciliation(ctx context.Context) error {
	s.reconcileMu.Lock()
	defer s.reconcileMu.Unlock()
	watcherRepaired := false
	s.mu.Lock()
	watcherStopped := s.watcherStopped
	s.mu.Unlock()
	if !watcherStopped {
		if err := s.addWatchTree(s.capture.Root(), false); err != nil {
			s.markWatcherUnhealthy(err)
		} else {
			watcherRepaired = true
		}
	}
	current, err := s.scanVersions(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return nil
	}
	for relative, version := range current {
		if known, ok := s.known[relative]; !ok || known != version {
			s.sequence++
			item := s.dirty[relative]
			if item.since.IsZero() {
				item.since = time.Now()
			}
			item.sequence = s.sequence
			item.estimatedBytes = max(version.Size, 0)
			item.failures = 0
			item.retryAt = time.Time{}
			s.dirty[relative] = item
		}
	}
	for relative := range s.known {
		if _, ok := current[relative]; !ok {
			s.sequence++
			item := s.dirty[relative]
			if item.since.IsZero() {
				item.since = time.Now()
			}
			item.sequence = s.sequence
			item.estimatedBytes = 0
			item.failures = 0
			item.retryAt = time.Time{}
			s.dirty[relative] = item
		}
	}
	s.reconciliations++
	if watcherRepaired && !s.watcherStopped {
		s.needsFullReconcile = false
	}
	s.signalLocked()
	s.mu.Unlock()
	return nil
}

func (s *Session) reconcileDirect(ctx context.Context) error {
	for {
		current, err := s.scanVersions(ctx)
		if err != nil {
			return err
		}
		s.mu.Lock()
		known := cloneVersions(s.known)
		s.mu.Unlock()
		var changed []string
		for relative, version := range current {
			if previous, ok := known[relative]; !ok || previous != version {
				changed = append(changed, relative)
			}
		}
		sort.Slice(changed, func(i, j int) bool { return pathLess(changed[i], changed[j]) })
		for _, relative := range changed {
			result, err := s.capture.Path(ctx, relative)
			if errors.Is(err, ErrUnstable) {
				continue
			}
			if err != nil {
				return err
			}
			s.mu.Lock()
			if result.Exists {
				s.setKnownLocked(relative, result.Version)
			} else {
				s.removeKnownLocked(relative)
			}
			s.mu.Unlock()
		}
		var removed []string
		for relative := range known {
			if _, ok := current[relative]; !ok {
				removed = append(removed, relative)
			}
		}
		sort.Slice(removed, func(i, j int) bool { return pathLess(removed[j], removed[i]) })
		for _, relative := range removed {
			if _, err := s.capture.Path(ctx, relative); err != nil {
				return err
			}
			s.mu.Lock()
			s.removeKnownLocked(relative)
			s.mu.Unlock()
		}
		s.mu.Lock()
		s.reconciliations++
		s.mu.Unlock()
		verified, err := s.scanVersions(ctx)
		if err != nil {
			return err
		}
		s.mu.Lock()
		stable := versionsEqual(verified, s.known)
		s.mu.Unlock()
		if stable {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (s *Session) scanVersions(ctx context.Context) (map[string]FileVersion, error) {
	current := make(map[string]FileVersion)
	err := s.capture.Scan(ctx, func(relative string, version FileVersion) { current[relative] = version })
	return current, err
}

func (s *Session) waitIdle(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		idle := len(s.dirty) == 0 && s.active == 0
		s.mu.Unlock()
		if idle {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Session) beginProtection(ctx context.Context, seal bool) error {
	s.protectionMu.Lock()
	defer s.protectionMu.Unlock()
	for s.checkpointing {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.protectionCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.protectionAll {
		if err := s.protection.Begin(ctx); err != nil {
			return err
		}
		s.protectionAll = true
	}
	s.protectionActive++
	if seal {
		s.protectionSeal = true
	}
	return nil
}

func (s *Session) endProtection(seal bool) {
	s.protectionMu.Lock()
	s.protectionActive--
	if s.protectionActive < 0 {
		panic("rootfs capture protection underflow")
	}
	if seal {
		s.protectionSeal = false
	}
	s.protectionCond.Broadcast()
	s.protectionMu.Unlock()
}

func (s *Session) checkpointProtection(ctx context.Context, reset bool) error {
	s.protectionMu.Lock()
	for s.checkpointing {
		if err := ctx.Err(); err != nil {
			s.protectionMu.Unlock()
			return err
		}
		s.protectionCond.Wait()
	}
	if s.protectionSeal {
		s.protectionMu.Unlock()
		return nil
	}
	s.checkpointing = true
	for s.protectionActive > 0 {
		if err := ctx.Err(); err != nil {
			s.checkpointing = false
			s.protectionCond.Broadcast()
			s.protectionMu.Unlock()
			return err
		}
		s.protectionCond.Wait()
	}
	var objects []rootfshead.Object
	if !reset {
		objects = s.capture.writer.PendingProtection()
	}
	var err error
	if reset {
		err = s.protection.Reset(ctx)
	} else {
		err = s.protection.Checkpoint(ctx, objects)
	}
	if err == nil {
		if !reset {
			s.capture.writer.MarkProtected(objects)
		}
		s.protectionAll = false
	}
	s.checkpointing = false
	s.protectionCond.Broadcast()
	s.protectionMu.Unlock()
	if err == nil {
		s.setProtectionError(nil)
	}
	return err
}

func (s *Session) stopBackground() {
	s.mu.Lock()
	if s.backgroundStopped {
		s.mu.Unlock()
		return
	}
	s.backgroundStopped = true
	s.accepting = false
	s.mu.Unlock()
	s.cancel()
	_ = s.watcher.Close()
	s.wg.Wait()
	_ = os.RemoveAll(s.watchFenceDir)
}

func (s *Session) addWatchTree(root string, markEntries bool) error {
	return filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(s.capture.Root(), current)
		if err != nil {
			return err
		}
		if s.capture.Excludes(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if err := s.addMergedDirectoryWatch(relative); err != nil {
				return err
			}
			if filepath.Clean(current) != filepath.Clean(filepath.Join(s.eventRoot, relative)) {
				if err := s.watcher.Add(current); err != nil {
					return fmt.Errorf("watch rootfs upper directory %s: %w", current, err)
				}
			}
		}
		if markEntries {
			s.mark(relative)
		}
		return nil
	})
}

func (s *Session) addMergedDirectoryWatch(relative string) error {
	path := filepath.Join(s.eventRoot, relative)
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect merged rootfs directory %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("merged rootfs path %s is not a directory", path)
	}
	if err := s.watcher.Add(path); err != nil {
		return fmt.Errorf("watch merged rootfs directory %s: %w", path, err)
	}
	return nil
}

func relativeToRoot(root, path string) (string, bool) {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return cleanRelativePath(relative), true
}

func (s *Session) fenceWatcher(ctx context.Context) error {
	s.mu.Lock()
	if s.watcherStopped {
		s.mu.Unlock()
		return fmt.Errorf("rootfs watcher is stopped")
	}
	s.watchFenceSeq++
	name := fmt.Sprintf("%d", s.watchFenceSeq)
	waiter := make(chan struct{})
	s.watchFenceWaiters[name] = waiter
	directory := s.watchFenceDir
	s.mu.Unlock()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		s.mu.Lock()
		delete(s.watchFenceWaiters, name)
		s.mu.Unlock()
		return fmt.Errorf("write rootfs watcher fence: %w", err)
	}
	defer os.Remove(path)
	select {
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.watchFenceWaiters, name)
		s.mu.Unlock()
		return ctx.Err()
	case <-waiter:
		return nil
	}
}

func (s *Session) handleWatchFence(path string) bool {
	if filepath.Clean(filepath.Dir(path)) != filepath.Clean(s.watchFenceDir) {
		return false
	}
	name := filepath.Base(path)
	s.mu.Lock()
	waiter := s.watchFenceWaiters[name]
	if waiter != nil {
		delete(s.watchFenceWaiters, name)
		close(waiter)
	}
	s.mu.Unlock()
	return true
}

func (s *Session) markWatcherUnhealthy(err error) {
	s.mu.Lock()
	s.watcherErrors++
	s.needsFullReconcile = true
	s.mu.Unlock()
}

func (s *Session) markWatcherStopped(_ error) {
	s.mu.Lock()
	s.watcherErrors++
	s.watcherStopped = true
	s.needsFullReconcile = true
	s.mu.Unlock()
}

func (s *Session) maintenanceAllowed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accepting && !s.sealing && !s.backgroundStopped
}

func (s *Session) removeKnownLocked(relative string) {
	s.markRemovedAliasesLocked(relative)
	s.removeKnownEntryLocked(relative)
	if relative == "" {
		for candidate := range s.known {
			s.markRemovedAliasesLocked(candidate)
			s.removeKnownEntryLocked(candidate)
		}
		return
	}
	prefix := relative + "/"
	for candidate := range s.known {
		if strings.HasPrefix(candidate, prefix) {
			s.markRemovedAliasesLocked(candidate)
			s.removeKnownEntryLocked(candidate)
		}
	}
}

func (s *Session) markRemovedAliasesLocked(relative string) {
	version, ok := s.known[relative]
	if !ok {
		return
	}
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	for alias := range s.inodeAliases[identity] {
		if alias == relative || s.capture.Excludes(alias) {
			continue
		}
		s.sequence++
		item := s.dirty[alias]
		if item.since.IsZero() {
			item.since = time.Now()
		}
		item.sequence = s.sequence
		item.estimatedBytes = max(s.known[alias].Size, 0)
		item.failures = 0
		item.retryAt = time.Time{}
		s.dirty[alias] = item
	}
}

func (s *Session) setKnownLocked(relative string, version FileVersion) {
	if previous, ok := s.known[relative]; ok {
		s.removeAliasLocked(relative, previous)
	}
	s.known[relative] = version
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	aliases := s.inodeAliases[identity]
	if aliases == nil {
		aliases = make(map[string]struct{})
		s.inodeAliases[identity] = aliases
	}
	aliases[relative] = struct{}{}
}

func (s *Session) removeKnownEntryLocked(relative string) {
	version, ok := s.known[relative]
	if !ok {
		return
	}
	delete(s.known, relative)
	s.removeAliasLocked(relative, version)
}

func (s *Session) removeAliasLocked(relative string, version FileVersion) {
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	aliases := s.inodeAliases[identity]
	delete(aliases, relative)
	if len(aliases) == 0 {
		s.capture.ForgetFile(version)
		delete(s.inodeAliases, identity)
	}
}

// markChangedAliasesLocked keeps all names of one hardlinked inode on the
// same immutable content and metadata. fsnotify reports the path used for a
// write, not every alias, so a healthy watcher fence alone cannot discover
// the other names.
func (s *Session) markChangedAliasesLocked(relative string, version FileVersion) {
	identity := inodeIdentity{device: version.Device, inode: version.Inode}
	for alias := range s.inodeAliases[identity] {
		if alias == relative || s.known[alias] == version || s.capture.Excludes(alias) {
			continue
		}
		s.sequence++
		item := s.dirty[alias]
		if item.since.IsZero() {
			item.since = time.Now()
		}
		item.sequence = s.sequence
		item.estimatedBytes = max(s.known[alias].Size, 0)
		item.failures = 0
		item.retryAt = time.Time{}
		s.dirty[alias] = item
	}
}

func captureRetryDelay(failures int) time.Duration {
	if failures <= 1 {
		return defaultCaptureRetryMin
	}
	delay := defaultCaptureRetryMin
	for i := 1; i < failures && delay < defaultCaptureRetryMax; i++ {
		delay *= 2
	}
	return min(delay, defaultCaptureRetryMax)
}

func (s *Session) signalLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *Session) setReconcileError(err error) {
	s.mu.Lock()
	s.reconcileError = err
	s.mu.Unlock()
}

func (s *Session) setFlushError(err error) {
	s.mu.Lock()
	s.flushError = err
	s.mu.Unlock()
}

func (s *Session) setProtectionError(err error) {
	s.mu.Lock()
	s.protectionError = err
	s.mu.Unlock()
}

func (s *Session) recordSealError(err error) {
	s.mu.Lock()
	s.sealing = false
	s.sealError = err
	if !s.backgroundStopped {
		s.accepting = true
		s.signalLocked()
	}
	s.mu.Unlock()
}

func (s *Session) lastErrorLocked() string {
	var messages []string
	appendError := func(kind string, err error) {
		if err != nil {
			messages = append(messages, kind+": "+err.Error())
		}
	}
	appendError("reconcile", s.reconcileError)
	appendError("flush", s.flushError)
	appendError("protection", s.protectionError)
	appendError("seal", s.sealError)
	if len(s.captureErrors) > 0 {
		paths := make([]string, 0, len(s.captureErrors))
		for relative := range s.captureErrors {
			paths = append(paths, relative)
		}
		sort.Strings(paths)
		first := paths[0]
		message := fmt.Sprintf("capture %q: %v", first, s.captureErrors[first])
		if len(paths) > 1 {
			message += fmt.Sprintf(" (and %d more)", len(paths)-1)
		}
		messages = append(messages, message)
	}
	return strings.Join(messages, "; ")
}

func pathLess(left, right string) bool {
	leftDepth := strings.Count(left, "/")
	rightDepth := strings.Count(right, "/")
	if leftDepth != rightDepth {
		return leftDepth < rightDepth
	}
	return left < right
}

func cloneVersions(values map[string]FileVersion) map[string]FileVersion {
	cloned := make(map[string]FileVersion, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func versionsEqual(left, right map[string]FileVersion) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func cloneSealResult(value SealResult) SealResult {
	return value
}
