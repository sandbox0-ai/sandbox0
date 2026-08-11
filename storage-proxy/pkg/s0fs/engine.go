package s0fs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

type Engine struct {
	mu            sync.RWMutex
	mutationMu    sync.Mutex
	materializeMu sync.Mutex
	volumeID      string
	wal           *wal
	closed        bool

	nextSeq   uint64
	nextInode uint64
	metadata  metadataStore

	materializer            *Materializer
	encryption              *EncryptionConfig
	stateFormatVersion      int
	localDiskGuard          *LocalDiskGuard
	retainUnlinked          bool
	metadataPath            string
	metadataCacheBytes      int64
	mutationVersion         uint64
	lastCommittedManifest   uint64
	lastCommittedHead       *CommittedHead
	lastMaterializedVersion uint64
	pendingMaterialization  *pendingMaterialization
	dirty                   bool
	dirtyAt                 time.Time
	failure                 atomic.Pointer[engineFailure]
}

type engineFailure struct {
	err error
}

type pendingMaterialization struct {
	manifestSeq     uint64
	head            *CommittedHead
	stateDigest     string
	mutationVersion uint64
	state           *SnapshotState
	walCheckpoint   *walCheckpoint
}

type loadedEngineState struct {
	state        *SnapshotState
	metadata     metadataStore
	nextSeq      uint64
	nextInode    uint64
	metadataPath string
	stateDigest  string
}

func loadedEngineStateFromSnapshot(state *SnapshotState) *loadedEngineState {
	if state == nil {
		return nil
	}
	normalizeState(state)
	return &loadedEngineState{state: state, nextSeq: state.NextSeq, nextInode: state.NextInode}
}

func (s *loadedEngineState) store() metadataStore {
	if s == nil {
		return nil
	}
	if s.metadata != nil {
		return s.metadata
	}
	if s.state == nil {
		return nil
	}
	return newEagerMetadataStore(s.state)
}

func (s *loadedEngineState) checkpointSequence() uint64 {
	if s == nil || s.nextSeq == 0 {
		return 0
	}
	return s.nextSeq - 1
}

func (s *loadedEngineState) digest() (string, error) {
	if s == nil {
		return "", nil
	}
	if s.stateDigest != "" {
		return s.stateDigest, nil
	}
	state := s.state
	if state == nil && s.metadata != nil {
		state = s.metadata.Snapshot(s.nextSeq, s.nextInode)
	}
	digest, err := snapshotStateDigest(state)
	if err != nil {
		return "", err
	}
	s.stateDigest = digest
	return digest, nil
}

func (s *loadedEngineState) close() {
	if s != nil && s.metadata != nil {
		_ = s.metadata.Close()
		s.metadata = nil
	}
}

func Open(ctx context.Context, cfg Config) (engine *Engine, retErr error) {
	ctx = nonNilContext(ctx)
	openedAt := time.Now()
	selectedSource := "new"
	selectedFormat := 0
	walRecordsApplied := 0
	walRecordsSkipped := 0
	var replayStats walReplayStats
	defer func() {
		if cfg.OpenObserver == nil {
			return
		}
		observation := OpenObservation{
			VolumeID:           cfg.VolumeID,
			Phase:              "complete",
			Source:             selectedSource,
			Format:             selectedFormat,
			Duration:           time.Since(openedAt),
			Bytes:              -1,
			WALRecords:         walRecordsApplied,
			WALRecordsScanned:  replayStats.RecordsScanned,
			WALRecordsSkipped:  walRecordsSkipped,
			WALMaxRecordBytes:  replayStats.MaxRecordBytes,
			WALMaxDecodedBytes: replayStats.MaxDecodedBytes,
			Err:                retErr,
		}
		if engine != nil {
			engine.mu.RLock()
			observation.Nodes = engine.metadata.NodeCount()
			observation.DirectoryEntries = engine.metadata.DirectoryEntryCount()
			observation.Segments = engine.metadata.SegmentCount()
			engine.mu.RUnlock()
		}
		emitOpenObservation(cfg.OpenObserver, observation)
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.VolumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}
	if err := validateStateFormatVersion(cfg.StateFormatVersion); err != nil {
		return nil, err
	}

	phaseStarted := time.Now()
	replay, err := openWALReplay(cfg.WALPath, cfg.VolumeID, cfg.Encryption)
	if err != nil {
		emitWALOpenPhase(cfg, "wal_open", 0, phaseStarted, replayStats, 0, 0, nil, err)
		return nil, err
	}
	defer replay.Close()
	_, hasWALRecords, err := replay.Peek(ctx)
	replayStats = replay.Stats()
	emitWALOpenPhase(cfg, "wal_open", 0, phaseStarted, replayStats, 0, 0, nil, err)
	if err != nil {
		return nil, err
	}

	phaseStarted = time.Now()
	state, localFormat, localBytes, err := loadCurrentEngineState(ctx, cfg)
	localState := state
	defer func() {
		if localState != nil {
			localState.close()
		}
	}()
	localStateErr := err
	emitOpenPhase(cfg, "state_load", "local", localFormat, phaseStarted, localBytes, replayStats.RecordsScanned, state, err)
	localBinding, localBindingErr := loadRecoveryBinding(localHeadBindingPath(cfg.WALPath), cfg.VolumeID)
	walBinding, walBindingErr := loadRecoveryBinding(walBaseBindingPath(cfg.WALPath), cfg.VolumeID)
	materializer := NewMaterializer(cfg.VolumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	if materializer != nil {
		materializer.SetEncryption(cfg.Encryption)
		materializer.SetSegmentTargetSize(cfg.SegmentTargetSize)
		materializer.SetStateFormatVersion(cfg.StateFormatVersion)
		materializer.SetOpenObserver(cfg.OpenObserver)
	}
	var latestManifest *Manifest
	var committedHead *CommittedHead
	var remoteState *loadedEngineState
	replayTrusted := true
	defer func() {
		if remoteState != nil {
			remoteState.close()
		}
	}()
	if materializer != nil {
		if materializer.headStore != nil {
			head, headErr := materializer.loadCommittedHead(ctx)
			if headErr == nil {
				committedHead = head
			} else if !errors.Is(headErr, ErrCommittedHeadNotFound) {
				return nil, headErr
			}
		}

		localCommitted := false
		if committedHead != nil && committedHead.ManifestDigest != "" && state != nil && localStateErr == nil && localBindingErr == nil {
			reuseStarted := time.Now()
			stateDigest, digestErr := state.digest()
			if digestErr != nil {
				return nil, digestErr
			}
			if committedHeadMatchesCheckpoint(committedHead, cfg.VolumeID, state.checkpointSequence()) &&
				recoveryBindingMatches(localBinding, committedHead, stateDigest) {
				if err := materializer.validateCommittedLoadedStateSegments(ctx, state); err != nil {
					return nil, err
				}
				latestManifest = manifestFromCommittedHead(committedHead, stateDigest)
				localCommitted = true
				selectedSource = "local"
				selectedFormat = localFormat
				emitOpenPhase(cfg, "state_reuse", "local", localFormat, reuseStarted, localBytes, replayStats.RecordsScanned, state, nil)
			}
		}

		if !localCommitted {
			phaseStarted = time.Now()
			remoteMetadataPath := ""
			if strings.TrimSpace(cfg.MetadataPath) != "" {
				remoteMetadataPath = cfg.MetadataPath + ".remote"
			}
			latestState, manifest, latestErr := materializer.loadLatestEngineState(ctx, remoteMetadataPath, cfg.MetadataCacheBytes)
			remoteState = latestState
			remoteFormat := 0
			if manifest != nil && (manifest.Version == StateFormatV1 || manifest.Version == StateFormatV2) {
				remoteFormat = manifest.Version
			}
			emitOpenPhase(cfg, "state_load", "remote", remoteFormat, phaseStarted, -1, replayStats.RecordsScanned, latestState, latestErr)
			if latestErr == nil {
				if err := materializer.validateCommittedLoadedStateSegments(ctx, latestState); err != nil {
					return nil, err
				}
			}
			if latestErr == nil {
				state = latestState
				err = nil
				latestManifest = manifest
				selectedSource = "remote"
				selectedFormat = remoteFormat
				if materializer.headStore != nil {
					head, headErr := materializer.loadCommittedHead(ctx)
					if headErr != nil {
						return nil, headErr
					}
					committedHead = head
					if err := validateManifestHead(committedHead, manifest); err != nil {
						return nil, err
					}
				} else {
					committedHead = committedHeadForManifest(manifest, manifestLatestKey, nil)
				}
			} else if committedHead != nil {
				if errors.Is(latestErr, ErrMaterializedManifestNotFound) || objectstore.IsNotFound(latestErr) {
					return nil, fmt.Errorf("%w: committed manifest %s is missing", ErrCommittedStateIntegrity, committedHead.ManifestKey)
				}
				return nil, latestErr
			} else if materializer.headStore != nil {
				state = nil
				err = latestErr
			} else if localStateErr != nil && !errors.Is(latestErr, ErrMaterializedManifestNotFound) {
				err = latestErr
			}
		}
		trusted, lineageErr := validateCommittedWALLineage(
			committedHead, latestManifest, localState, localStateErr,
			localBinding, localBindingErr, walBinding, walBindingErr,
			hasWALRecords,
		)
		if lineageErr != nil {
			return nil, lineageErr
		}
		replayTrusted = trusted
		materializer.SetOpenObserver(nil)
	}
	if err != nil && !errors.Is(err, ErrSnapshotNotFound) && !errors.Is(err, ErrMaterializedManifestNotFound) {
		return nil, err
	}
	if state == nil {
		now := time.Now().UTC()
		state = loadedEngineStateFromSnapshot(&SnapshotState{
			NextSeq:   1,
			NextInode: RootInode + 1,
			Nodes: map[uint64]*Node{
				RootInode: {
					Inode: RootInode,
					Type:  TypeDirectory,
					Mode:  0o755,
					Nlink: 1,
					Atime: now,
					Mtime: now,
					Ctime: now,
				},
			},
			Children: map[uint64]map[string]uint64{
				RootInode: {},
			},
			Data:      make(map[uint64][]byte),
			ColdFiles: make(map[uint64][]FileExtent),
			Segments:  make(map[string]*Segment),
		})
		selectedSource = "new"
		selectedFormat = 0
	} else if selectedSource == "new" && localStateErr == nil {
		selectedSource = "local"
		selectedFormat = localFormat
	}
	if state == remoteState && state != nil && state.metadata != nil && strings.TrimSpace(cfg.MetadataPath) != "" && state.metadataPath != cfg.MetadataPath {
		if localState != nil {
			localState.close()
			localState = nil
		}
		if sqliteState, ok := state.metadata.(*sqliteMetadataStore); ok {
			if err := sqliteState.relocate(cfg.MetadataPath); err != nil {
				return nil, err
			}
			state.metadataPath = cfg.MetadataPath
		}
	}
	if selectedFormat == StateFormatV1 && normalizedStateFormatVersion(cfg.StateFormatVersion) == StateFormatV2 && state.state != nil {
		migrationStarted := time.Now()
		migrationErr := saveSnapshotState(headStatePath(cfg.WALPath), cfg.VolumeID, "head", state.state, cfg.Encryption, StateFormatV2)
		emitOpenPhase(cfg, "state_migrate", selectedSource, StateFormatV1, migrationStarted, fileSize(headStatePath(cfg.WALPath)), replayStats.RecordsScanned, state, migrationErr)
		if migrationErr != nil {
			return nil, fmt.Errorf("migrate state v1 to v2: %w", migrationErr)
		}
	}

	var metadata metadataStore
	metadataPath := cfg.MetadataPath
	if state.metadata != nil {
		metadata = state.metadata
		state.metadata = nil
		metadataPath = state.metadataPath
	} else {
		metadata, err = newEngineMetadataStore(ctx, cfg, state.state)
		if err != nil {
			return nil, err
		}
	}
	e := &Engine{
		volumeID:           cfg.VolumeID,
		nextSeq:            state.nextSeq,
		nextInode:          state.nextInode,
		metadata:           metadata,
		materializer:       materializer,
		encryption:         cfg.Encryption,
		stateFormatVersion: normalizedStateFormatVersion(cfg.StateFormatVersion),
		localDiskGuard:     cfg.LocalDiskGuard,
		retainUnlinked:     cfg.RetainUnlinked,
		metadataPath:       metadataPath,
		metadataCacheBytes: cfg.MetadataCacheBytes,
	}
	defer func() {
		if engine != nil {
			return
		}
		if e.wal != nil {
			_ = e.wal.close()
		}
		if e.metadata != nil {
			_ = e.metadata.Close()
		}
	}()
	if latestManifest != nil {
		e.lastCommittedManifest = latestManifest.ManifestSeq
		e.lastCommittedHead = cloneCommittedHead(committedHead)
	}

	phaseStarted = time.Now()
	appliedRecords := 0
	if replayTrusted {
		for {
			record, ok, replayErr := replay.Next(ctx)
			replayStats = replay.Stats()
			if replayErr != nil {
				emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, replayErr)
				return nil, replayErr
			}
			if !ok {
				break
			}
			if record.Seq < e.nextSeq {
				walRecordsSkipped++
				continue
			}
			if record.Seq > e.nextSeq {
				replayErr := fmt.Errorf("replay wal seq %d: %w: missing wal seq %d", record.Seq, ErrInvalidInput, e.nextSeq)
				emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, replayErr)
				return nil, replayErr
			}
			if err := e.apply(record); err != nil {
				replayErr := fmt.Errorf("replay wal seq %d: %w", record.Seq, err)
				emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, replayErr)
				return nil, replayErr
			}
			appliedRecords++
			walRecordsApplied = appliedRecords
			if record.Seq >= e.nextSeq {
				e.nextSeq = record.Seq + 1
			}
			if record.Inode >= e.nextInode {
				e.nextInode = record.Inode + 1
			}
		}
	}
	if err := replay.Close(); err != nil {
		emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, err)
		return nil, err
	}
	if !replayTrusted {
		if _, err := quarantineRecoveryEvidence(cfg.WALPath); err != nil {
			return nil, fmt.Errorf("quarantine untrusted s0fs recovery evidence: %w", err)
		}
		replayStats = walReplayStats{}
	}
	walFile, err := openWAL(cfg.WALPath, cfg.VolumeID, cfg.Encryption, cfg.WALSyncHook, replayStats)
	if err != nil {
		emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, err)
		return nil, err
	}
	e.wal = walFile
	if !e.retainUnlinked {
		if err := e.collectUnlinkedLocked(ctx); err != nil {
			return nil, err
		}
	}
	if appliedRecords > 0 {
		e.dirty = true
		e.dirtyAt = time.Now().UTC()
		e.mutationVersion = 1
	}
	if err := saveRecoveryBinding(walBaseBindingPath(cfg.WALPath), cfg.VolumeID, e.lastCommittedHead, ""); err != nil {
		return nil, fmt.Errorf("persist wal committed base: %w", err)
	}
	walRecordsApplied = appliedRecords
	emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.metadata, nil)

	engine = e
	return engine, nil
}

func emitWALOpenPhase(cfg Config, phase string, format int, started time.Time, stats walReplayStats, skipped, applied int, metadata metadataStore, err error) {
	bytes := stats.BytesScanned
	if phase == "wal_open" {
		bytes = walFileSize(cfg.WALPath)
	}
	observation := OpenObservation{
		VolumeID:           cfg.VolumeID,
		Phase:              phase,
		Source:             "local",
		Format:             format,
		Duration:           time.Since(started),
		Bytes:              bytes,
		WALRecords:         applied,
		WALRecordsScanned:  stats.RecordsScanned,
		WALRecordsSkipped:  skipped,
		WALMaxRecordBytes:  stats.MaxRecordBytes,
		WALMaxDecodedBytes: stats.MaxDecodedBytes,
		Err:                err,
	}
	if metadata != nil {
		observation.Nodes = metadata.NodeCount()
		observation.DirectoryEntries = metadata.DirectoryEntryCount()
		observation.Segments = metadata.SegmentCount()
	}
	emitOpenObservation(cfg.OpenObserver, observation)
}

func emitOpenPhase(cfg Config, phase, source string, format int, started time.Time, bytes int64, walRecords int, state *loadedEngineState, err error) {
	observation := OpenObservation{
		VolumeID:   cfg.VolumeID,
		Phase:      phase,
		Source:     source,
		Format:     format,
		Duration:   time.Since(started),
		Bytes:      bytes,
		WALRecords: walRecords,
		Err:        err,
	}
	if phase == "complete" && state != nil {
		metadata := state.store()
		if metadata != nil {
			observation.Nodes = metadata.NodeCount()
			observation.DirectoryEntries = metadata.DirectoryEntryCount()
			observation.Segments = metadata.SegmentCount()
		}
	}
	emitOpenObservation(cfg.OpenObserver, observation)
}

func emitOpenObservation(observer OpenObserver, observation OpenObservation) {
	if observer != nil {
		observer(observation)
	}
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func walFileSize(path string) int64 {
	return fileSize(path)
}

func directoryEntryCount(children map[uint64]map[string]uint64) int {
	total := 0
	for _, entries := range children {
		total += len(entries)
	}
	return total
}

func (e *Engine) Close() error {
	e.materializeMu.Lock()
	defer e.materializeMu.Unlock()

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	terminalFailure := e.failure.Load() != nil
	if terminalFailure || (e.dirty && e.materializer != nil && e.materializer.Enabled()) {
		wait, err := e.wal.beginSyncCurrent()
		if err != nil {
			return err
		}
		if wait != nil {
			if err := wait(); err != nil {
				return err
			}
		}
	} else {
		if err := e.persistCurrentStateLocked(); err != nil {
			return err
		}
		if e.lastCommittedHead != nil {
			stateDigest, err := snapshotStateDigest(e.currentStateLocked())
			if err != nil {
				return err
			}
			if err := saveRecoveryBinding(localHeadBindingPath(e.wal.path), e.volumeID, e.lastCommittedHead, stateDigest); err != nil {
				return err
			}
			if err := saveRecoveryBinding(walBaseBindingPath(e.wal.path), e.volumeID, e.lastCommittedHead, ""); err != nil {
				return err
			}
		}
		if err := e.wal.reset(); err != nil {
			return err
		}
	}
	e.refreshLocalDiskGuardLocked()
	e.closed = true
	if err := e.metadata.Close(); err != nil {
		return err
	}
	return e.wal.close()
}

// PruneUnlinked removes recovered zero-link inodes that have no restored open
// file handle. It is used after an HA owner restores its handle table.
func (e *Engine) PruneUnlinked(retain map[uint64]struct{}) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.metadata.PruneUnlinked(context.Background(), retain)
}

func (e *Engine) Lookup(parent uint64, name string) (*Node, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	inode, err := e.lookupLocked(parent, name)
	if err != nil {
		return nil, err
	}
	node, ok := e.metadata.Node(inode)
	if !ok {
		return nil, ErrNotFound
	}
	return node, nil
}

func (e *Engine) GetAttr(inode uint64) (*Node, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	node, ok := e.metadata.Node(inode)
	if !ok {
		return nil, ErrNotFound
	}
	return node, nil
}

func (e *Engine) Mkdir(parent uint64, name string, mode uint32) (*Node, error) {
	return e.create(parent, name, TypeDirectory, mode, "", CreateOptions{})
}

func (e *Engine) ReadDir(inode uint64) ([]DirEntry, error) {
	entries, _, err := e.ReadDirPage(inode, 0, 0)
	return entries, err
}

// ReadDirPage returns directory entries in stable name order. Offset is the
// number of entries already consumed; a zero limit preserves the legacy
// unbounded helper behavior for in-process callers.
func (e *Engine) ReadDirPage(inode, offset uint64, limit uint32) ([]DirEntry, bool, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, false, err
	}
	if err := e.ensureDirLocked(inode); err != nil {
		return nil, false, err
	}

	page, eof, ok := e.metadata.DirectoryPage(inode, offset, limit)
	if err := e.metadata.Err(); err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, ErrNotDir
	}
	entries := make([]DirEntry, 0, len(page))
	for _, entry := range page {
		childInode := entry.Inode
		node, _ := e.metadata.Node(childInode)
		if node == nil {
			continue
		}
		entries = append(entries, DirEntry{
			Name:  entry.Name,
			Inode: childInode,
			Type:  node.Type,
		})
	}
	return entries, eof, e.metadata.Err()
}

func (e *Engine) Path(inode uint64) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return "", false
	}
	return e.pathLocked(inode)
}

func (e *Engine) ChildPath(parent uint64, name string) (string, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return "", false
	}
	parentPath, ok := e.pathLocked(parent)
	if !ok || name == "" {
		return "", false
	}
	if parentPath == "/" {
		return "/" + name, true
	}
	return parentPath + "/" + name, true
}

func (e *Engine) CreateFile(parent uint64, name string, mode uint32) (*Node, error) {
	return e.create(parent, name, TypeFile, mode, "", CreateOptions{})
}

func (e *Engine) CreateFileWithOwner(parent uint64, name string, mode uint32, uid, gid uint32) (*Node, error) {
	return e.create(parent, name, TypeFile, mode, "", CreateOptions{UID: uid, GID: gid})
}

func (e *Engine) Symlink(parent uint64, name, target string, mode uint32) (*Node, error) {
	if target == "" {
		return nil, fmt.Errorf("%w: symlink target is required", ErrInvalidInput)
	}
	return e.create(parent, name, TypeSymlink, mode, target, CreateOptions{})
}

func (e *Engine) Link(inode uint64, newParent uint64, newName string) (*Node, error) {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if newName == "" {
		return nil, fmt.Errorf("%w: empty link name", ErrInvalidInput)
	}
	node, ok := e.metadata.Node(inode)
	if !ok || node == nil {
		return nil, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	if err := e.ensureDirLocked(newParent); err != nil {
		return nil, err
	}
	if _, exists := e.metadata.Child(newParent, newName); exists {
		return nil, ErrExists
	}
	record := e.newRecord("link")
	record.Inode = inode
	record.NewParent = newParent
	record.NewName = newName
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return nil, err
	}
	linked, ok := e.metadata.Node(inode)
	if !ok {
		return nil, ErrNotFound
	}
	return linked, nil
}

func (e *Engine) Write(inode uint64, offset uint64, payload []byte) (int, error) {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	if err := e.checkOpen(); err != nil {
		e.mu.Unlock()
		return 0, err
	}
	node, err := e.fileNodeLocked(inode)
	if err != nil {
		e.mu.Unlock()
		return 0, err
	}
	record := e.newRecord("write")
	record.Inode = inode
	record.Offset = offset
	record.Data = payload
	projectedBytes := estimatedWALRecordBytes(record)
	end := offset + uint64(len(payload))
	if end > node.Size {
		projectedBytes += int64(end - node.Size)
	}
	if err := e.reserveLocalDiskLocked(projectedBytes); err != nil {
		e.mu.Unlock()
		return 0, err
	}
	e.mu.Unlock()

	walPayload, err := e.wal.prepare(record)
	if err != nil {
		return 0, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return 0, err
	}
	if err := e.appendPreparedAndApplyLocked(record, walPayload); err != nil {
		return 0, err
	}
	return len(payload), nil
}

func (e *Engine) Read(inode uint64, offset uint64, size uint64) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	node, err := e.fileNodeLocked(inode)
	if err != nil {
		return nil, err
	}
	payload, err := e.readFileLocked(node, inode, offset, size)
	if err != nil {
		e.failClosed(err)
	}
	return payload, err
}

func (e *Engine) ReadInto(inode uint64, offset uint64, dest []byte) (int, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return 0, err
	}
	node, err := e.fileNodeLocked(inode)
	if err != nil {
		return 0, err
	}
	n, err := e.readFileIntoLocked(node, inode, offset, dest)
	if err != nil {
		e.failClosed(err)
	}
	return n, err
}

func (e *Engine) Rename(oldParent uint64, oldName string, newParent uint64, newName string) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if oldName == "" || newName == "" {
		return fmt.Errorf("%w: empty rename component", ErrInvalidInput)
	}
	if _, err := e.lookupLocked(oldParent, oldName); err != nil {
		return err
	}
	if err := e.ensureDirLocked(newParent); err != nil {
		return err
	}
	record := e.newRecord("rename")
	record.Parent = oldParent
	record.Name = oldName
	record.NewParent = newParent
	record.NewName = newName
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Unlink(parent uint64, name string) error {
	_, err := e.UnlinkWithInode(parent, name)
	return err
}

// UnlinkWithInode removes a file entry and returns the inode that was unlinked.
func (e *Engine) UnlinkWithInode(parent uint64, name string) (uint64, error) {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return 0, err
	}
	inode, err := e.lookupLocked(parent, name)
	if err != nil {
		return 0, err
	}
	node, ok := e.metadata.Node(inode)
	if !ok {
		return 0, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return 0, ErrIsDir
	}
	record := e.newRecord("unlink")
	record.Parent = parent
	record.Name = name
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return 0, err
	}
	return inode, nil
}

func (e *Engine) Forget(inode uint64) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	node, ok := e.metadata.Node(inode)
	if !ok || node == nil || node.Nlink != 0 {
		return nil
	}
	e.metadata.DeleteDirectory(inode)
	e.metadata.DeleteNode(inode)
	e.metadata.DeleteData(inode)
	e.metadata.DeleteColdFile(inode)
	return nil
}

func (e *Engine) RemoveDir(parent uint64, name string) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	inode, err := e.lookupLocked(parent, name)
	if err != nil {
		return err
	}
	node, _ := e.metadata.Node(inode)
	if node == nil {
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	children, _, _ := e.metadata.DirectoryPage(inode, 0, 1)
	if err := e.metadata.Err(); err != nil {
		return err
	}
	if len(children) != 0 {
		return ErrNotEmpty
	}
	record := e.newRecord("rmdir")
	record.Parent = parent
	record.Name = name
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) SetMode(inode uint64, mode uint32) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if _, ok := e.metadata.Node(inode); !ok {
		return ErrNotFound
	}
	record := e.newRecord("chmod")
	record.Inode = inode
	record.Mode = mode
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) SetOwner(inode uint64, uid, gid uint32) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if _, ok := e.metadata.Node(inode); !ok {
		return ErrNotFound
	}
	record := e.newRecord("chown")
	record.Inode = inode
	record.Mode = uid
	record.Offset = uint64(gid)
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Truncate(inode uint64, size uint64) error {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	node, err := e.fileNodeLocked(inode)
	if err != nil {
		return err
	}
	record := e.newRecord("truncate")
	record.Inode = inode
	record.Offset = size
	projectedBytes := estimatedWALRecordBytes(record)
	if size > node.Size {
		projectedBytes += int64(size - node.Size)
	}
	if err := e.appendAndApplyLocked(record, projectedBytes); err != nil {
		return err
	}
	return nil
}

func (e *Engine) Fsync(_ uint64) error {
	e.mu.RLock()
	if err := e.checkOpen(); err != nil {
		e.mu.RUnlock()
		return err
	}
	wait, err := e.wal.beginSyncCurrent()
	e.mu.RUnlock()
	if err != nil || wait == nil {
		return err
	}
	return wait()
}

func (e *Engine) SnapshotState() *SnapshotState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentStateLocked()
}

// SnapshotReferenceState returns a metadata snapshot for retaining live object
// references during GC. Inline payload bytes may be shared with the engine and
// must be treated as read-only by callers.
func (e *Engine) SnapshotReferenceState() *SnapshotState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.metadata.ReferenceSnapshot(e.nextSeq, e.nextInode)
}

// CommittedHeadCurrent reports whether this clean in-memory engine still
// matches the authoritative committed manifest pointer. It is used before a
// node-local hot engine is made visible again after relinquishing ownership.
func (e *Engine) CommittedHeadCurrent(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	e.mu.RLock()
	if err := e.checkOpen(); err != nil {
		e.mu.RUnlock()
		return false, err
	}
	dirty := e.dirty
	expectedHead := cloneCommittedHead(e.lastCommittedHead)
	checkpointSeq := uint64(0)
	if e.nextSeq > 0 {
		checkpointSeq = e.nextSeq - 1
	}
	materializer := e.materializer
	e.mu.RUnlock()
	if dirty || materializer == nil || materializer.headStore == nil {
		return false, nil
	}
	head, err := materializer.headStore.LoadCommittedHead(ctx, e.volumeID)
	if errors.Is(err, ErrCommittedHeadNotFound) {
		return expectedHead == nil, nil
	}
	if err != nil {
		return false, err
	}
	return committedHeadMatchesCheckpoint(head, e.volumeID, checkpointSeq) && sameCommittedHeadIdentity(head, expectedHead), nil
}

func committedHeadMatchesCheckpoint(head *CommittedHead, volumeID string, checkpointSeq uint64) bool {
	return head != nil &&
		head.VolumeID == volumeID &&
		head.ManifestSeq != 0 &&
		head.ManifestSeq == checkpointSeq &&
		head.CheckpointSeq == checkpointSeq &&
		strings.TrimSpace(head.ManifestKey) != ""
}

// EstimatedMemoryBytes returns a conservative size estimate for charging the
// shared active-and-detached node budget. It does not clone engine state.
func (e *Engine) EstimatedMemoryBytes() int64 {
	if e == nil {
		return 0
	}
	e.mu.RLock()
	total := e.metadata.EstimatedMemoryBytes()
	materializer := e.materializer
	e.mu.RUnlock()
	if materializer != nil {
		total += materializer.estimatedCacheMemoryBytes()
	}
	return total
}

// EngineMemoryReservationBytes returns the fixed upper-bound charge used
// before opening a disk-indexed engine. It reserves both the SQLite page cache
// and the segment cache so concurrent cold opens cannot bypass node admission.
func EngineMemoryReservationBytes(metadataCacheBytes int64) int64 {
	if metadataCacheBytes <= 0 {
		metadataCacheBytes = defaultMetadataCacheBytes
	}
	return metadataCacheBytes + (1 << 20) + defaultSegmentCacheMaxBytes + (1 << 20)
}

func (e *Engine) ExportState() (*SnapshotState, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	return e.exportStateLocked()
}

func (e *Engine) SyncMaterialize(ctx context.Context) (*Manifest, error) {
	return e.syncMaterialize(ctx, false)
}

// EnsureMaterialized writes any inline file data to immutable segments even if
// the engine was reopened from a persisted head and is not currently dirty.
func (e *Engine) EnsureMaterialized(ctx context.Context) (*Manifest, error) {
	return e.syncMaterialize(ctx, true)
}

func (e *Engine) syncMaterialize(ctx context.Context, force bool) (*Manifest, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	e.materializeMu.Lock()
	defer e.materializeMu.Unlock()

	e.mutationMu.Lock()
	e.mu.Lock()
	if err := e.checkOpen(); err != nil {
		e.mu.Unlock()
		e.mutationMu.Unlock()
		return nil, err
	}
	if err := e.finalizePendingMaterializationLocked(); err != nil {
		e.mu.Unlock()
		e.mutationMu.Unlock()
		return nil, err
	}
	needsMaterialization := false
	if force {
		needsMaterialization = e.needsMaterializationLocked()
		if err := e.metadata.Err(); err != nil {
			e.mu.Unlock()
			e.mutationMu.Unlock()
			return nil, err
		}
	}
	if e.materializer == nil || !e.materializer.Enabled() || (!e.dirty && (!force || !needsMaterialization)) {
		e.mu.Unlock()
		e.mutationMu.Unlock()
		return nil, nil
	}
	version := e.mutationVersion
	state, err := e.materializeStateLocked()
	if err != nil {
		e.mu.Unlock()
		e.mutationMu.Unlock()
		return nil, err
	}
	expected := cloneCommittedHead(e.lastCommittedHead)
	e.mu.Unlock()
	checkpoint, err := e.wal.checkpoint(checkpointSequence(state))
	if err != nil {
		e.mutationMu.Unlock()
		return nil, err
	}
	e.mutationMu.Unlock()

	manifest, err := e.materializer.materializeOwned(ctx, state, expected)
	if err != nil || manifest == nil {
		e.failClosed(err)
		return manifest, err
	}

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if manifest.ManifestSeq > e.lastCommittedManifest {
		e.lastCommittedManifest = manifest.ManifestSeq
	}
	committedHead := committedHeadForManifest(manifest, manifestKey(manifest.ManifestSeq, manifest.CommitID), expected)
	e.lastCommittedHead = cloneCommittedHead(committedHead)
	e.pendingMaterialization = &pendingMaterialization{
		manifestSeq:     manifest.ManifestSeq,
		head:            committedHead,
		stateDigest:     manifest.StateDigest,
		mutationVersion: version,
		state:           manifest.State,
		walCheckpoint:   checkpoint,
	}
	if err := e.finalizePendingMaterializationLocked(); err != nil {
		return nil, err
	}
	return manifest, nil
}

// finalizePendingMaterializationLocked makes a committed remote checkpoint
// durable locally before reclaiming its WAL prefix. The caller holds
// mutationMu and e.mu so Close, restore, and new mutations cannot cross the
// local checkpoint boundary.
func (e *Engine) finalizePendingMaterializationLocked() error {
	pending := e.pendingMaterialization
	if pending == nil {
		return nil
	}
	if pending.state == nil || checkpointSequence(pending.state) != pending.manifestSeq {
		return fmt.Errorf("%w: pending materialization checkpoint is invalid", ErrInvalidInput)
	}
	if err := e.persistStateLocked(pending.state, true); err != nil {
		return err
	}
	if pending.head == nil || pending.head.ManifestSeq != pending.manifestSeq || pending.stateDigest == "" {
		return fmt.Errorf("%w: pending materialization identity is invalid", ErrInvalidInput)
	}
	if err := saveRecoveryBinding(localHeadBindingPath(e.wal.path), e.volumeID, pending.head, pending.stateDigest); err != nil {
		return fmt.Errorf("persist local committed binding: %w", err)
	}
	if err := saveRecoveryBinding(walBaseBindingPath(e.wal.path), e.volumeID, pending.head, ""); err != nil {
		return fmt.Errorf("persist wal committed base: %w", err)
	}
	if pending.walCheckpoint == nil || pending.walCheckpoint.throughSeq != pending.manifestSeq {
		return fmt.Errorf("%w: pending materialization wal checkpoint is invalid", ErrInvalidInput)
	}
	if err := e.wal.discardThrough(pending.walCheckpoint); err != nil {
		return err
	}
	if e.mutationVersion == pending.mutationVersion {
		if err := e.replaceStateLocked(cloneState(pending.state)); err != nil {
			return err
		}
		e.lastMaterializedVersion = pending.mutationVersion
		e.dirty = false
	}
	e.pendingMaterialization = nil
	e.refreshLocalDiskGuardLocked()
	return nil
}

func (e *Engine) RefreshMaterialized(ctx context.Context) (bool, error) {
	e.materializeMu.Lock()
	defer e.materializeMu.Unlock()

	e.mu.RLock()
	if err := e.checkOpen(); err != nil {
		e.mu.RUnlock()
		return false, err
	}
	if e.materializer == nil || !e.materializer.Enabled() || e.dirty {
		e.mu.RUnlock()
		return false, nil
	}
	currentNextSeq := e.nextSeq
	e.mu.RUnlock()

	state, manifest, err := e.materializer.loadLatestStateOwned(ctx)
	if errors.Is(err, ErrMaterializedManifestNotFound) {
		return false, nil
	}
	if err != nil {
		e.failClosed(err)
		return false, err
	}
	if state == nil || state.NextSeq <= currentNextSeq {
		return false, nil
	}
	if err := e.materializer.validateCommittedStateSegments(ctx, state); err != nil {
		e.failClosed(err)
		return false, err
	}

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return false, err
	}
	if e.dirty || state.NextSeq <= e.nextSeq {
		return false, nil
	}
	var refreshedHead *CommittedHead
	if manifest != nil {
		if e.materializer.headStore != nil {
			head, headErr := e.materializer.loadCommittedHead(ctx)
			if headErr != nil {
				return false, headErr
			}
			if head.ManifestKey != manifestKey(manifest.ManifestSeq, manifest.CommitID) || validateManifestHead(head, manifest) != nil {
				// The committed head advanced while this state was loading. Leave the
				// current engine untouched and let the caller retry the newer head.
				return false, nil
			}
			refreshedHead = head
		} else {
			refreshedHead = committedHeadForManifest(manifest, manifestLatestKey, nil)
		}
	}
	if err := e.persistStateLocked(state, true); err != nil {
		err = fmt.Errorf("%w: persist refreshed committed state: %w", ErrCommittedHeadConflict, err)
		e.failClosed(err)
		return false, err
	}
	if refreshedHead != nil {
		if err := saveRecoveryBinding(localHeadBindingPath(e.wal.path), e.volumeID, refreshedHead, manifest.StateDigest); err != nil {
			err = fmt.Errorf("%w: persist refreshed committed binding: %w", ErrCommittedHeadConflict, err)
			e.failClosed(err)
			return false, err
		}
		if err := saveRecoveryBinding(walBaseBindingPath(e.wal.path), e.volumeID, refreshedHead, ""); err != nil {
			err = fmt.Errorf("%w: persist refreshed wal base: %w", ErrCommittedHeadConflict, err)
			e.failClosed(err)
			return false, err
		}
	}
	if err := e.wal.reset(); err != nil {
		err = fmt.Errorf("%w: reset refreshed wal: %w", ErrCommittedHeadConflict, err)
		e.failClosed(err)
		return false, err
	}
	if err := e.replaceStateLocked(state); err != nil {
		err = fmt.Errorf("%w: install refreshed committed state: %w", ErrCommittedHeadConflict, err)
		e.failClosed(err)
		return false, err
	}
	e.refreshLocalDiskGuardLocked()
	if manifest != nil {
		e.lastCommittedManifest = manifest.ManifestSeq
		e.lastCommittedHead = cloneCommittedHead(refreshedHead)
	}
	return true, nil
}

func (e *Engine) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateSnapshotID(snapshotID); err != nil {
		return nil, err
	}
	state := e.snapshotStateLocked()
	if err := e.reserveLocalDiskLocked(estimatedStateBytes(state)); err != nil {
		return nil, err
	}
	if err := saveSnapshotState(snapshotFilePath(e.wal.path, snapshotID), e.volumeID, "snapshot:"+snapshotID, state, e.encryption, e.stateFormatVersion); err != nil {
		return nil, err
	}
	e.refreshLocalDiskGuardLocked()
	return state, nil
}

func (e *Engine) RestoreSnapshot(snapshotID string) error {
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	state, err := loadSnapshotState(snapshotFilePath(e.wal.path, snapshotID), e.volumeID, "snapshot:"+snapshotID, e.encryption)
	if err != nil {
		return err
	}
	return e.restoreStateLocked(state)
}

// RestoreState replaces the current filesystem state with an immutable
// snapshot loaded independently from local engine storage.
func (e *Engine) RestoreState(state *SnapshotState) error {
	if state == nil {
		return fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}
	state = cloneState(state)
	normalizeState(state)

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	return e.restoreStateLocked(state)
}

func (e *Engine) restoreStateLocked(state *SnapshotState) error {
	minNextSeq := e.nextSeq + 1
	if committedNext := e.lastCommittedManifest + 2; committedNext > minNextSeq {
		minNextSeq = committedNext
	}
	if state.NextSeq < minNextSeq {
		state.NextSeq = minNextSeq
	}
	if err := e.reserveLocalDiskLocked(estimatedStateBytes(state)); err != nil {
		return err
	}
	if err := e.replaceStateLocked(state); err != nil {
		return err
	}
	e.refreshLocalDiskGuardLocked()
	e.markDirtyLocked()
	return nil
}

func (e *Engine) ReplaceState(state *SnapshotState) error {
	state = cloneState(state)
	ensureMaterializableSequence(state)

	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if err := e.reserveLocalDiskLocked(estimatedStateBytes(state)); err != nil {
		return err
	}
	if err := e.replaceStateLocked(state); err != nil {
		return err
	}
	e.refreshLocalDiskGuardLocked()
	e.markDirtyLocked()
	return nil
}

func (e *Engine) DeleteSnapshot(snapshotID string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if err := validateSnapshotID(snapshotID); err != nil {
		return err
	}
	if err := os.Remove(snapshotFilePath(e.wal.path, snapshotID)); err != nil {
		if os.IsNotExist(err) {
			return ErrSnapshotNotFound
		}
		return fmt.Errorf("delete snapshot state: %w", err)
	}
	e.refreshLocalDiskGuardLocked()
	return nil
}

func (e *Engine) create(parent uint64, name string, typ FileType, mode uint32, target string, opts CreateOptions) (*Node, error) {
	e.mutationMu.Lock()
	defer e.mutationMu.Unlock()

	e.mu.Lock()
	if err := e.checkOpen(); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if name == "" {
		e.mu.Unlock()
		return nil, fmt.Errorf("%w: empty name", ErrInvalidInput)
	}
	if err := e.ensureDirLocked(parent); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if _, exists := e.metadata.Child(parent, name); exists {
		e.mu.Unlock()
		return nil, ErrExists
	}

	record := e.newRecord("create")
	record.Inode = e.nextInode
	record.Parent = parent
	record.Name = name
	record.Type = typ
	record.Mode = mode
	record.UID = opts.UID
	record.GID = opts.GID
	record.Target = target
	if err := e.reserveLocalDiskLocked(estimatedWALRecordBytes(record)); err != nil {
		e.mu.Unlock()
		return nil, err
	}
	e.mu.Unlock()

	walPayload, err := e.wal.prepare(record)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if err := e.appendPreparedAndApplyLocked(record, walPayload); err != nil {
		return nil, err
	}
	created, ok := e.metadata.Node(record.Inode)
	if !ok {
		return nil, ErrNotFound
	}
	return created, nil
}

func (e *Engine) newRecord(op string) walRecord {
	return walRecord{
		Seq:      e.nextSeq,
		Op:       op,
		TimeUnix: time.Now().UTC().UnixNano(),
	}
}

func (e *Engine) appendAndApplyLocked(record walRecord, projectedBytes int64) error {
	if err := e.reserveLocalDiskLocked(projectedBytes); err != nil {
		return err
	}
	walPayload, err := e.wal.prepare(record)
	if err != nil {
		return err
	}
	return e.appendPreparedAndApplyLocked(record, walPayload)
}

func (e *Engine) appendPreparedAndApplyLocked(record walRecord, walPayload []byte) error {
	if err := e.wal.appendPrepared(record, walPayload); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.advanceSeqLocked(record)
	e.markDirtyLocked()
	return nil
}

func (e *Engine) advanceSeqLocked(record walRecord) {
	if record.Seq >= e.nextSeq {
		e.nextSeq = record.Seq + 1
	}
}

func (e *Engine) reserveLocalDiskLocked(projectedBytes int64) error {
	if e == nil || e.localDiskGuard == nil {
		return nil
	}
	return e.localDiskGuard.Reserve(projectedBytes)
}

func (e *Engine) refreshLocalDiskGuardLocked() {
	if e != nil && e.localDiskGuard != nil {
		e.localDiskGuard.Refresh()
	}
}

func estimatedWALRecordBytes(record walRecord) int64 {
	size := int64(512 + len(record.Name) + len(record.NewName) + len(record.Target))
	if len(record.Data) > 0 {
		size += int64(len(record.Data) * 2)
	}
	return size
}

func estimatedStateBytes(state *SnapshotState) int64 {
	if state == nil {
		return 0
	}
	var total int64 = 4096
	for _, data := range state.Data {
		total += int64(len(data) * 2)
	}
	for _, node := range state.Nodes {
		if node != nil {
			total += int64(256 + len(node.Target))
		}
	}
	for _, children := range state.Children {
		total += int64(128 + len(children)*128)
	}
	return total
}

func estimatedStateMemoryBytes(state *SnapshotState) int64 {
	if state == nil {
		return 0
	}
	total := estimatedStateBytes(state)
	for _, children := range state.Children {
		for name := range children {
			total += int64(len(name))
		}
	}
	for _, extents := range state.ColdFiles {
		total += int64(64 + len(extents)*64)
		for _, extent := range extents {
			total += int64(len(extent.SegmentID))
		}
	}
	for id, segment := range state.Segments {
		total += int64(256 + len(id))
		if segment == nil {
			continue
		}
		total += int64(len(segment.ID) + len(segment.VolumeID) + len(segment.Key) + len(segment.SHA256) + len(segment.InlineData))
		if segment.Encryption != nil {
			total += int64(len(segment.Encryption.Algorithm) + len(segment.Encryption.WrappedKey) + len(segment.Encryption.NoncePrefix) + 128)
		}
	}
	return total
}

func (e *Engine) currentStateLocked() *SnapshotState {
	return e.metadata.Snapshot(e.nextSeq, e.nextInode)
}

func (e *Engine) replaceStateLocked(state *SnapshotState) error {
	normalizeState(state)
	metadata, err := newEngineMetadataStore(context.Background(), Config{
		MetadataPath:       e.metadataPath,
		MetadataCacheBytes: e.metadataCacheBytes,
		Encryption:         e.encryption,
	}, state)
	if err != nil {
		return err
	}
	previous := e.metadata
	e.nextSeq = state.NextSeq
	if e.nextSeq == 0 {
		e.nextSeq = 1
	}
	e.nextInode = state.NextInode
	if e.nextInode <= RootInode {
		e.nextInode = RootInode + 1
	}
	e.metadata = metadata
	if previous != nil {
		if err := previous.Close(); err != nil {
			return err
		}
	}
	if !e.retainUnlinked {
		if err := e.collectUnlinkedLocked(context.Background()); err != nil {
			return err
		}
	}
	return e.metadata.Err()
}

func newEngineMetadataStore(ctx context.Context, cfg Config, state *SnapshotState) (metadataStore, error) {
	if strings.TrimSpace(cfg.MetadataPath) == "" {
		return newEagerMetadataStore(state), nil
	}
	return newSQLiteMetadataStoreWithEncryption(ctx, cfg.MetadataPath, state, cfg.MetadataCacheBytes, cfg.Encryption)
}

func (e *Engine) persistCurrentStateLocked() error {
	return e.persistCurrentStateLockedWithReserve(true)
}

func (e *Engine) persistCurrentStateLockedWithReserve(reserve bool) error {
	if e.stateFormatVersion == StateFormatV2 {
		if reserve {
			if err := e.reserveLocalDiskLocked(e.metadata.EstimatedPersistentBytes()); err != nil {
				return err
			}
		}
		return saveMetadataStateV2(context.Background(), headStatePath(e.wal.path), e.volumeID, "head", e.metadata, e.nextSeq, e.nextInode, e.encryption)
	}
	return e.persistStateLocked(e.currentStateLocked(), reserve)
}

func (e *Engine) persistStateLocked(source *SnapshotState, reserve bool) error {
	if e.stateFormatVersion == StateFormatV2 {
		pruneUnreferencedSegments(source)
		if reserve {
			if err := e.reserveLocalDiskLocked(estimatedStateBytes(source)); err != nil {
				return err
			}
		}
		return saveMetadataStateV2(context.Background(), headStatePath(e.wal.path), e.volumeID, "head", newEagerMetadataStore(source), source.NextSeq, source.NextInode, e.encryption)
	}
	state := cloneState(source)
	pruneUnreferencedSegments(state)
	if reserve {
		if err := e.reserveLocalDiskLocked(estimatedStateBytes(state)); err != nil {
			return err
		}
	}
	return saveSnapshotState(headStatePath(e.wal.path), e.volumeID, "head", state, e.encryption, e.stateFormatVersion)
}

func loadCurrentState(ctx context.Context, cfg Config) (*SnapshotState, int, int64, error) {
	if cfg.WALPath == "" {
		return nil, 0, -1, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	return loadSnapshotStateWithFormatContext(ctx, headStatePath(cfg.WALPath), cfg.VolumeID, "head", cfg.Encryption)
}

func loadCurrentEngineState(ctx context.Context, cfg Config) (*loadedEngineState, int, int64, error) {
	if cfg.WALPath == "" {
		return nil, 0, -1, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	if strings.TrimSpace(cfg.MetadataPath) != "" {
		path := headStatePath(cfg.WALPath)
		file, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, 0, -1, ErrSnapshotNotFound
			}
			return nil, 0, -1, fmt.Errorf("read snapshot state: %w", err)
		}
		buffered := bufio.NewReader(&contextReader{ctx: ctx, reader: file})
		v2, inspectErr := hasStateV2Magic(buffered)
		if inspectErr != nil {
			_ = file.Close()
			return nil, 0, -1, fmt.Errorf("inspect snapshot state: %w", inspectErr)
		}
		if v2 {
			metadataPath := cfg.MetadataPath
			metadata, stream, decodeErr := newSQLiteMetadataStoreFromStateV2(ctx, metadataPath, buffered, cfg.VolumeID, stateBlobAAD(cfg.VolumeID, "head"), StateV2Role_STATE_V2_ROLE_HEAD, cfg.Encryption, cfg.MetadataCacheBytes)
			closeErr := file.Close()
			if decodeErr != nil {
				return nil, StateFormatV2, -1, fmt.Errorf("decode snapshot state v2: %w", decodeErr)
			}
			if closeErr != nil {
				_ = metadata.Close()
				return nil, StateFormatV2, stream.Bytes, closeErr
			}
			return &loadedEngineState{
				metadata: metadata, nextSeq: stream.Header.NextSeq, nextInode: stream.Header.NextInode,
				metadataPath: metadataPath, stateDigest: stream.Metadata.StateDigest,
			}, StateFormatV2, stream.Bytes, nil
		}
		_ = file.Close()
	}
	state, format, bytes, err := loadCurrentState(ctx, cfg)
	return loadedEngineStateFromSnapshot(state), format, bytes, err
}

func manifestFromCommittedHead(head *CommittedHead, stateDigest string) *Manifest {
	if head == nil {
		return nil
	}
	return &Manifest{
		VolumeID: head.VolumeID, ManifestSeq: head.ManifestSeq, CheckpointSeq: head.CheckpointSeq,
		CommitID: head.CommitID, StateDigest: stateDigest, ManifestDigest: head.ManifestDigest,
		CreatedAt: head.UpdatedAt,
	}
}

func validateCommittedWALLineage(
	head *CommittedHead,
	manifest *Manifest,
	local *loadedEngineState,
	localErr error,
	localBinding *recoveryBinding,
	localBindingErr error,
	walBinding *recoveryBinding,
	walBindingErr error,
	hasRecords bool,
) (bool, error) {
	if !hasRecords || head == nil {
		return true, nil
	}
	if walBindingErr == nil && recoveryBindingMatches(walBinding, head, "") {
		return true, nil
	}
	if localErr == nil && local != nil && local.checkpointSequence() == head.CheckpointSeq {
		localDigest, err := local.digest()
		if err != nil {
			return false, err
		}
		if localBindingErr == nil && recoveryBindingMatches(localBinding, head, localDigest) {
			return true, nil
		}
		if manifest != nil && manifest.StateDigest != "" && localDigest == manifest.StateDigest {
			return true, nil
		}
	}
	return false, nil
}

func (e *Engine) apply(record walRecord) error {
	var err error
	switch record.Op {
	case "create":
		err = e.applyCreate(record)
	case "write":
		err = e.applyWrite(record)
	case "link":
		err = e.applyLink(record)
	case "rmdir":
		err = e.applyRemoveDir(record)
	case "rename":
		err = e.applyRename(record)
	case "chmod":
		err = e.applySetMode(record)
	case "chown":
		err = e.applySetOwner(record)
	case "truncate":
		err = e.applyTruncate(record)
	case "unlink":
		err = e.applyUnlink(record)
	default:
		return fmt.Errorf("unknown wal op %q", record.Op)
	}
	if err != nil {
		return err
	}
	return e.metadata.Err()
}

func (e *Engine) applyCreate(record walRecord) error {
	if record.Inode == 0 || record.Parent == 0 || record.Name == "" {
		return fmt.Errorf("%w: invalid create record", ErrInvalidInput)
	}
	if err := e.ensureDirLocked(record.Parent); err != nil {
		return err
	}
	if _, exists := e.metadata.Child(record.Parent, record.Name); exists {
		return ErrExists
	}
	now := time.Unix(0, record.TimeUnix).UTC()
	node := &Node{
		Inode:  record.Inode,
		Type:   record.Type,
		Mode:   record.Mode,
		UID:    record.UID,
		GID:    record.GID,
		Nlink:  1,
		Target: record.Target,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
	}
	if node.Type == "" {
		node.Type = TypeFile
	}
	if node.Type == TypeDirectory {
		e.metadata.EnsureDirectory(node.Inode)
	}
	e.metadata.PutNode(node.Inode, node)
	e.metadata.PutChild(record.Parent, record.Name, node.Inode)
	if record.Inode >= e.nextInode {
		e.nextInode = record.Inode + 1
	}
	return nil
}

func (e *Engine) applyLink(record walRecord) error {
	if record.Inode == 0 || record.NewParent == 0 || record.NewName == "" {
		return fmt.Errorf("%w: invalid link record", ErrInvalidInput)
	}
	node, ok := e.metadata.Node(record.Inode)
	if !ok {
		return ErrNotFound
	}
	if node.Type == TypeDirectory {
		return ErrIsDir
	}
	if err := e.ensureDirLocked(record.NewParent); err != nil {
		return err
	}
	if _, exists := e.metadata.Child(record.NewParent, record.NewName); exists {
		return ErrExists
	}
	e.metadata.PutChild(record.NewParent, record.NewName, record.Inode)
	node.Nlink++
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyWrite(record walRecord) error {
	if e.usesExtentLayoutLocked() {
		return e.applyExtentWrite(record)
	}
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	current, err := e.mutableFileDataLocked(record.Inode)
	if err != nil {
		return err
	}
	end := record.Offset + uint64(len(record.Data))
	if end > uint64(len(current)) {
		grown := make([]byte, end)
		copy(grown, current)
		current = grown
	}
	copy(current[record.Offset:end], record.Data)
	e.metadata.PutData(record.Inode, current)
	node.Size = uint64(len(current))
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyRename(record walRecord) error {
	inode, err := e.lookupLocked(record.Parent, record.Name)
	if err != nil {
		return err
	}
	if err := e.ensureDirLocked(record.NewParent); err != nil {
		return err
	}
	e.metadata.DeleteChild(record.Parent, record.Name)
	e.metadata.PutChild(record.NewParent, record.NewName, inode)
	if node, ok := e.metadata.Node(inode); ok {
		node.Ctime = time.Unix(0, record.TimeUnix).UTC()
		e.metadata.PutNode(inode, node)
	}
	return nil
}

func (e *Engine) applyRemoveDir(record walRecord) error {
	inode, err := e.lookupLocked(record.Parent, record.Name)
	if err != nil {
		return err
	}
	node, ok := e.metadata.Node(inode)
	if !ok {
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	children, _, _ := e.metadata.DirectoryPage(inode, 0, 1)
	if err := e.metadata.Err(); err != nil {
		return err
	}
	if len(children) != 0 {
		return ErrNotEmpty
	}
	e.metadata.DeleteChild(record.Parent, record.Name)
	e.metadata.DeleteDirectory(inode)
	e.metadata.DeleteNode(inode)
	return nil
}

func (e *Engine) applySetMode(record walRecord) error {
	node, ok := e.metadata.Node(record.Inode)
	if !ok {
		return ErrNotFound
	}
	node.Mode = record.Mode
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applySetOwner(record walRecord) error {
	node, ok := e.metadata.Node(record.Inode)
	if !ok {
		return ErrNotFound
	}
	node.UID = record.Mode
	node.GID = uint32(record.Offset)
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyTruncate(record walRecord) error {
	if e.usesExtentLayoutLocked() {
		return e.applyExtentTruncate(record)
	}
	node, err := e.fileNodeLocked(record.Inode)
	if err != nil {
		return err
	}
	current, err := e.mutableFileDataLocked(record.Inode)
	if err != nil {
		return err
	}
	target := int(record.Offset)
	switch {
	case target < len(current):
		current = slices.Clone(current[:target])
	case target > len(current):
		grown := make([]byte, target)
		copy(grown, current)
		current = grown
	default:
		current = slices.Clone(current)
	}
	e.metadata.PutData(record.Inode, current)
	node.Size = uint64(len(current))
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	e.metadata.PutNode(record.Inode, node)
	return nil
}

func (e *Engine) applyUnlink(record walRecord) error {
	inode, err := e.lookupLocked(record.Parent, record.Name)
	if err != nil {
		return err
	}
	node, _ := e.metadata.Node(inode)
	if node != nil && node.Type == TypeDirectory {
		return ErrIsDir
	}
	e.metadata.DeleteChild(record.Parent, record.Name)
	if node != nil && node.Nlink > 0 {
		node.Nlink--
		node.Ctime = time.Unix(0, record.TimeUnix).UTC()
		e.metadata.PutNode(inode, node)
	}
	return nil
}

func (e *Engine) markDirtyLocked() {
	e.mutationVersion++
	e.dirty = true
	e.dirtyAt = time.Now().UTC()
}

func (e *Engine) needsMaterializationLocked() bool {
	return e.metadata.NeedsMaterialization()
}

func (e *Engine) snapshotStateLocked() *SnapshotState {
	state := e.currentStateLocked()
	for inode := range state.ColdFiles {
		if state.Nodes[inode] == nil {
			delete(state.ColdFiles, inode)
		}
	}
	pruneUnreferencedSegments(state)
	return state
}

func (e *Engine) exportStateLocked() (*SnapshotState, error) {
	state := e.currentStateLocked()
	if len(state.ColdFiles) == 0 {
		state.Segments = make(map[string]*Segment)
		return state, nil
	}
	for inode := range state.ColdFiles {
		node := state.Nodes[inode]
		if node == nil {
			continue
		}
		payload, err := e.readColdRangeLocked(inode, 0, node.Size)
		if err != nil {
			return nil, err
		}
		state.Data[inode] = payload
	}
	state.ColdFiles = make(map[uint64][]FileExtent)
	state.Segments = make(map[string]*Segment)
	return state, nil
}

func (e *Engine) materializeStateLocked() (*SnapshotState, error) {
	state := e.currentStateLocked()
	if len(state.ColdFiles) == 0 {
		pruneUnreferencedSegments(state)
		return state, nil
	}
	for inode := range state.ColdFiles {
		if state.Nodes[inode] == nil {
			delete(state.ColdFiles, inode)
		}
	}
	pruneUnreferencedSegments(state)
	return state, nil
}

func (e *Engine) readFileLocked(node *Node, inode uint64, offset uint64, size uint64) ([]byte, error) {
	if node == nil {
		return nil, ErrNotFound
	}
	if offset >= node.Size {
		return nil, nil
	}
	payload, hasData := e.metadata.Data(inode)
	extents, _ := e.metadata.ColdFile(inode)
	if len(payload) > 0 || (hasData && len(extents) == 0) || len(extents) == 0 {
		if offset >= uint64(len(payload)) {
			return nil, nil
		}
		end := offset + size
		if end > uint64(len(payload)) {
			end = uint64(len(payload))
		}
		return slices.Clone(payload[offset:end]), nil
	}
	return e.readColdRangeLocked(inode, offset, size)
}

func (e *Engine) readFileIntoLocked(node *Node, inode uint64, offset uint64, dest []byte) (int, error) {
	if node == nil {
		return 0, ErrNotFound
	}
	if len(dest) == 0 || offset >= node.Size {
		return 0, nil
	}
	payload, hasData := e.metadata.Data(inode)
	extents, _ := e.metadata.ColdFile(inode)
	if len(payload) > 0 || (hasData && len(extents) == 0) || len(extents) == 0 {
		if offset >= uint64(len(payload)) {
			return 0, nil
		}
		end := offset + uint64(len(dest))
		if end > uint64(len(payload)) {
			end = uint64(len(payload))
		}
		return copy(dest, payload[offset:end]), nil
	}
	payload, err := e.readColdRangeLocked(inode, offset, uint64(len(dest)))
	if err != nil {
		return 0, err
	}
	return copy(dest, payload), nil
}

func (e *Engine) mutableFileDataLocked(inode uint64) ([]byte, error) {
	payload, hasData := e.metadata.Data(inode)
	extents, _ := e.metadata.ColdFile(inode)
	if len(payload) > 0 || (hasData && len(extents) == 0) || len(extents) == 0 {
		return payload, nil
	}
	node, ok := e.metadata.Node(inode)
	if !ok {
		return nil, ErrNotFound
	}
	payload, err := e.readColdRangeLocked(inode, 0, node.Size)
	if err != nil {
		return nil, err
	}
	e.metadata.PutData(inode, payload)
	e.metadata.DeleteColdFile(inode)
	return payload, nil
}

func (e *Engine) readColdRangeLocked(inode uint64, offset uint64, size uint64) ([]byte, error) {
	extents, _ := e.metadata.ColdFile(inode)
	segments := make(map[string]*Segment)
	for _, extent := range extents {
		if extent.SegmentID == "" {
			continue
		}
		if segment, ok := e.metadata.Segment(extent.SegmentID); ok {
			segments[extent.SegmentID] = segment
		}
	}
	return readColdRange(e.materializer, extents, segments, offset, size)
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (e *Engine) checkOpen() error {
	if e == nil || e.closed {
		return ErrClosed
	}
	if failure := e.failure.Load(); failure != nil {
		return failure.err
	}
	if e.metadata != nil {
		return e.metadata.Err()
	}
	return nil
}

func (e *Engine) failClosed(err error) {
	if e == nil || err == nil || (!errors.Is(err, ErrCommittedStateIntegrity) && !errors.Is(err, ErrCommittedHeadConflict)) {
		return
	}
	e.failure.CompareAndSwap(nil, &engineFailure{err: err})
}

func (e *Engine) ensureDirLocked(inode uint64) error {
	node, ok := e.metadata.Node(inode)
	if !ok {
		if err := e.metadata.Err(); err != nil {
			return err
		}
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	e.metadata.EnsureDirectory(inode)
	return nil
}

func (e *Engine) lookupLocked(parent uint64, name string) (uint64, error) {
	if err := e.ensureDirLocked(parent); err != nil {
		return 0, err
	}
	inode, ok := e.metadata.Child(parent, name)
	if !ok {
		if err := e.metadata.Err(); err != nil {
			return 0, err
		}
		return 0, ErrNotFound
	}
	return inode, nil
}

func (e *Engine) fileNodeLocked(inode uint64) (*Node, error) {
	node, ok := e.metadata.Node(inode)
	if !ok {
		if err := e.metadata.Err(); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	return node, nil
}

func (e *Engine) pathLocked(target uint64) (string, bool) {
	return e.metadata.Path(target)
}

func (e *Engine) collectUnlinkedLocked(ctx context.Context) error {
	return e.metadata.PruneUnlinked(ctx, nil)
}
