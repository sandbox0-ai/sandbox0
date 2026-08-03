package rootfscow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

const (
	defaultSyncWorkers   = 4
	backgroundFlushEvery = 500 * time.Millisecond
	pathSyncDebounce     = 100 * time.Millisecond
	sealQuiescenceWindow = 15 * time.Millisecond
)

type SessionConfig struct {
	Root            string
	GenerationID    string
	TeamID          string
	FilesystemID    string
	BaseImageDigest string
	BaseSnapshotKey string
	Parent          *rootfshead.Head
	ExcludedPaths   []string
	PortalPaths     []ctldapi.RootFSPortalPath
	Store           objectstore.Store
	Workers         int
	ChunkSize       int
}

type SealResult struct {
	Head               rootfshead.Head
	Reference          rootfshead.HeadReference
	Objects            []rootfshead.Object
	CreatedBytes       int64
	CreatedObjectCount int64
	DirtyPaths         int
	Duration           time.Duration
}

type dirtyPath struct {
	version    uint64
	processing bool
}

// Session continuously drains one active overlay upper into immutable CAS
// objects. Pause only waits for the bounded dirty tail and seals a new root.
type Session struct {
	root            string
	baseImageDigest string
	baseSnapshotKey string
	capture         *Capture
	portalCaptures  []*Capture
	editor          *Editor
	writer          *ObjectWriter
	watcher         *fsnotify.Watcher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu            sync.Mutex
	cond          *sync.Cond
	dirty         map[string]*dirtyPath
	known         map[string]struct{}
	revision      uint64
	active        int
	overflow      bool
	fatalErr      error
	backgroundErr error
	initialDone   bool
	closed        bool
}

func NewSession(parentContext context.Context, cfg SessionConfig) (*Session, error) {
	if parentContext == nil {
		parentContext = context.Background()
	}
	if strings.TrimSpace(cfg.TeamID) == "" || strings.TrimSpace(cfg.FilesystemID) == "" {
		return nil, fmt.Errorf("rootfs team and filesystem ids are required")
	}
	baseImageDigest := strings.TrimSpace(cfg.BaseImageDigest)
	baseSnapshotKey := strings.TrimSpace(cfg.BaseSnapshotKey)
	if cfg.Parent != nil {
		if err := cfg.Parent.Validate(); err != nil {
			return nil, err
		}
		baseImageDigest = cfg.Parent.BaseImageDigest
		baseSnapshotKey = cfg.Parent.BaseSnapshotKey
	}
	if baseImageDigest == "" || baseSnapshotKey == "" {
		return nil, fmt.Errorf("rootfs base image digest and snapshot key are required")
	}
	prefix := path.Join(
		"sandbox-rootfs", "cow-v2", "teams", opaqueComponent(cfg.TeamID),
		"filesystems", opaqueComponent(cfg.FilesystemID),
	)
	writer, err := NewObjectWriter(cfg.Store, prefix)
	if err != nil {
		return nil, err
	}
	editor, err := NewEditor(cfg.Store, writer, cfg.Parent)
	if err != nil {
		return nil, err
	}
	capture, err := NewCapture(CaptureConfig{
		Root:          cfg.Root,
		GenerationID:  cfg.GenerationID,
		ExcludedPaths: cfg.ExcludedPaths,
		ChunkSize:     cfg.ChunkSize,
		Editor:        editor,
		Writer:        writer,
	})
	if err != nil {
		return nil, err
	}
	portalCaptures := make([]*Capture, 0, len(cfg.PortalPaths))
	for _, portal := range cfg.PortalPaths {
		backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
		mountPath := strings.Trim(strings.TrimSpace(portal.MountPath), "/")
		if backingPath == "" || backingPath == "." || mountPath == "" {
			continue
		}
		if err := os.MkdirAll(backingPath, 0o755); err != nil {
			return nil, fmt.Errorf("prepare rootfs portal %s: %w", backingPath, err)
		}
		portalCapture, err := NewCapture(CaptureConfig{
			Root:         backingPath,
			Prefix:       mountPath,
			GenerationID: cfg.GenerationID,
			ChunkSize:    cfg.ChunkSize,
			OpaqueRoot:   true,
			Editor:       editor,
			Writer:       writer,
		})
		if err != nil {
			return nil, err
		}
		portalCaptures = append(portalCaptures, portalCapture)
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parentContext)
	session := &Session{
		root:            capture.Root(),
		baseImageDigest: baseImageDigest,
		baseSnapshotKey: baseSnapshotKey,
		capture:         capture,
		portalCaptures:  portalCaptures,
		editor:          editor,
		writer:          writer,
		watcher:         watcher,
		ctx:             ctx,
		cancel:          cancel,
		dirty:           make(map[string]*dirtyPath),
		known:           make(map[string]struct{}),
	}
	session.cond = sync.NewCond(&session.mu)
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultSyncWorkers
	}
	if err := watcher.Add(session.root); err != nil {
		_ = watcher.Close()
		cancel()
		return nil, fmt.Errorf("watch rootfs upper %s: %w", session.root, err)
	}
	session.wg.Add(3 + workers)
	go session.watchLoop()
	go session.initialScan()
	go session.flushLoop()
	for range workers {
		go session.worker()
	}
	return session, nil
}

// Seal drains the dirty tail, commits one immutable head, and terminally stops
// the generation before returning its conservative object inventory.
func (s *Session) Seal(ctx context.Context, headID string) (*SealResult, error) {
	started := time.Now()
	headID = strings.TrimSpace(headID)
	if headID == "" {
		return nil, fmt.Errorf("rootfs head id is required")
	}
	dirtyAtStart := s.dirtyCount()
	if err := s.waitClean(ctx); err != nil {
		return nil, err
	}
	for _, portalCapture := range s.portalCaptures {
		if err := portalCapture.CaptureTree(ctx); err != nil {
			return nil, fmt.Errorf("capture rootfs portal %s: %w", portalCapture.Root(), err)
		}
	}
	root, err := s.editor.Flush(ctx)
	if err != nil {
		return nil, fmt.Errorf("flush rootfs metadata: %w", err)
	}
	head := rootfshead.Head{
		Version:         rootfshead.Version,
		HeadID:          headID,
		BaseImageDigest: s.baseImageDigest,
		BaseSnapshotKey: s.baseSnapshotKey,
		Root:            root,
	}
	payload, err := rootfshead.EncodeHead(head)
	if err != nil {
		return nil, err
	}
	manifest, err := s.writer.Put(ctx, rootfshead.HeadMediaType, payload)
	if err != nil {
		return nil, err
	}
	reference := rootfshead.HeadReference{Version: rootfshead.Version, HeadID: headID, Manifest: manifest}
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	// A sealed generation must stop writing before its conservative inventory is
	// observed. Pod teardown can mutate the containerd upper after the head is
	// committed; leaving the watcher alive would upload objects after they can no
	// longer be attached to a layer, making them invisible to manager GC.
	if err := s.Close(); err != nil {
		return nil, fmt.Errorf("close sealed rootfs generation: %w", err)
	}
	bytes, count := s.writer.CreatedMetrics()
	return &SealResult{
		Head:               head,
		Reference:          reference,
		Objects:            s.writer.Referenced(),
		CreatedBytes:       bytes,
		CreatedObjectCount: count,
		DirtyPaths:         dirtyAtStart,
		Duration:           time.Since(started),
	}, nil
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.cancel()
	s.cond.Broadcast()
	s.mu.Unlock()
	watchErr := s.watcher.Close()
	s.wg.Wait()
	return watchErr
}

func (s *Session) initialScan() {
	defer s.wg.Done()
	err := s.scanAll(false)
	s.mu.Lock()
	if err != nil && !errors.Is(err, context.Canceled) {
		s.fatalErr = err
	}
	s.initialDone = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Session) scanAll(resetMissing bool) error {
	seen := make(map[string]struct{})
	err := filepath.WalkDir(s.root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(s.root, current)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "." {
			relative = ""
		}
		if s.capture.Excludes(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if err := s.watcher.Add(current); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
		}
		seen[relative] = struct{}{}
		s.mark(relative)
		return nil
	})
	if err != nil {
		return err
	}
	if resetMissing {
		s.mu.Lock()
		for known := range s.known {
			if _, ok := seen[known]; !ok {
				s.markLocked(known)
			}
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *Session) watchLoop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case event, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			s.handleEvent(event)
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			s.mu.Lock()
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				s.overflow = true
			} else if s.fatalErr == nil {
				s.fatalErr = err
			}
			s.cond.Broadcast()
			s.mu.Unlock()
		}
	}
}

func (s *Session) handleEvent(event fsnotify.Event) {
	relative, err := filepath.Rel(s.root, event.Name)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return
	}
	relative = filepath.ToSlash(relative)
	s.markWithParents(relative)
	if event.Has(fsnotify.Create) {
		if info, err := os.Lstat(event.Name); err == nil && info.IsDir() {
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				if err := s.scanSubtree(event.Name); err != nil && !errors.Is(err, context.Canceled) {
					s.setFatal(err)
				}
			}()
		}
	}
}

func (s *Session) scanSubtree(root string) error {
	return filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			if err := s.watcher.Add(current); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
		}
		relative, err := filepath.Rel(s.root, current)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if s.capture.Excludes(relative) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		s.mark(relative)
		return s.ctx.Err()
	})
}

func (s *Session) worker() {
	defer s.wg.Done()
	for {
		relative, version, ok := s.nextDirty()
		if !ok {
			return
		}
		if !s.waitForStablePath(relative, version) {
			continue
		}
		exists, err := s.capture.Path(s.ctx, relative)
		s.finishDirty(relative, version, exists, err)
	}
}

func (s *Session) waitForStablePath(relative string, version uint64) bool {
	timer := time.NewTimer(pathSyncDebounce)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
	case <-timer.C:
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.dirty[relative]
	if state != nil && state.version == version && s.ctx.Err() == nil && !s.closed {
		return true
	}
	s.active--
	if state != nil {
		state.processing = false
	}
	s.cond.Broadcast()
	return false
}

func (s *Session) nextDirty() (string, uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed || s.ctx.Err() != nil {
			return "", 0, false
		}
		for relative, state := range s.dirty {
			if state.processing {
				continue
			}
			state.processing = true
			s.active++
			return relative, state.version, true
		}
		s.cond.Wait()
	}
}

func (s *Session) finishDirty(relative string, version uint64, exists bool, captureErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	state := s.dirty[relative]
	if state == nil {
		s.cond.Broadcast()
		return
	}
	if errors.Is(captureErr, ErrUnstable) {
		state.processing = false
		state.version++
		s.revision++
		s.cond.Broadcast()
		return
	}
	if captureErr != nil {
		if !errors.Is(captureErr, context.Canceled) && s.fatalErr == nil {
			s.fatalErr = fmt.Errorf("persist rootfs path %q: %w", relative, captureErr)
		}
		state.processing = false
		s.cond.Broadcast()
		return
	}
	if exists {
		s.known[relative] = struct{}{}
	} else {
		delete(s.known, relative)
	}
	if state.version == version {
		delete(s.dirty, relative)
	} else {
		state.processing = false
	}
	s.cond.Broadcast()
}

func (s *Session) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(backgroundFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(s.ctx, backgroundFlushEvery)
			_, err := s.editor.Flush(flushCtx)
			cancel()
			s.mu.Lock()
			if err == nil {
				s.backgroundErr = nil
			} else if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.backgroundErr = err
			}
			s.mu.Unlock()
		}
	}
}

func (s *Session) waitClean(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	wakeDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-wakeDone:
		}
	}()
	defer close(wakeDone)
	for {
		s.mu.Lock()
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		if s.fatalErr != nil {
			err := s.fatalErr
			s.mu.Unlock()
			return err
		}
		if s.overflow && s.initialDone {
			s.overflow = false
			s.mu.Unlock()
			if err := s.scanAll(true); err != nil {
				return fmt.Errorf("rescan rootfs upper after watcher overflow: %w", err)
			}
			continue
		}
		clean := s.initialDone && len(s.dirty) == 0 && s.active == 0
		revision := s.revision
		if !clean {
			s.cond.Wait()
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
		timer := time.NewTimer(sealQuiescenceWindow)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		s.mu.Lock()
		stable := s.initialDone && len(s.dirty) == 0 && s.active == 0 && revision == s.revision && !s.overflow
		s.mu.Unlock()
		if stable {
			return nil
		}
	}
}

func (s *Session) mark(relative string) {
	s.mu.Lock()
	s.markLocked(relative)
	s.mu.Unlock()
}

func (s *Session) markWithParents(relative string) {
	relative = cleanRelativePath(relative)
	s.mu.Lock()
	s.markLocked(relative)
	for parent := path.Dir(relative); parent != "." && parent != "/"; parent = path.Dir(parent) {
		s.markLocked(parent)
	}
	if relative != "" {
		s.markLocked("")
	}
	s.mu.Unlock()
}

func (s *Session) markLocked(relative string) {
	relative = cleanRelativePath(relative)
	s.revision++
	state := s.dirty[relative]
	if state == nil {
		state = &dirtyPath{}
		s.dirty[relative] = state
	}
	state.version++
	s.cond.Broadcast()
}

func (s *Session) setFatal(err error) {
	s.mu.Lock()
	if s.fatalErr == nil {
		s.fatalErr = err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *Session) dirtyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirty)
}

func opaqueComponent(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:16])
}
