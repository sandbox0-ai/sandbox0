package s0fs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"
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
	nodes     map[uint64]*Node
	children  map[uint64]map[string]uint64
	data      map[uint64][]byte
	coldFiles map[uint64][]FileExtent
	segments  map[string]*Segment

	materializer            *Materializer
	encryption              *EncryptionConfig
	stateFormatVersion      int
	localDiskGuard          *LocalDiskGuard
	retainUnlinked          bool
	mutationVersion         uint64
	lastCommittedManifest   uint64
	lastMaterializedVersion uint64
	pendingMaterialization  *pendingMaterialization
	dirty                   bool
	dirtyAt                 time.Time
}

type pendingMaterialization struct {
	manifestSeq     uint64
	mutationVersion uint64
	state           *SnapshotState
	walCheckpoint   *walCheckpoint
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
			observation.Nodes = len(engine.nodes)
			observation.DirectoryEntries = directoryEntryCount(engine.children)
			observation.Segments = len(engine.segments)
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
	firstWALRecord, hasWALRecords, err := replay.Peek(ctx)
	replayStats = replay.Stats()
	emitWALOpenPhase(cfg, "wal_open", 0, phaseStarted, replayStats, 0, 0, nil, err)
	if err != nil {
		return nil, err
	}

	phaseStarted = time.Now()
	state, localFormat, localBytes, err := loadCurrentState(cfg)
	localStateErr := err
	emitOpenPhase(cfg, "state_load", "local", localFormat, phaseStarted, localBytes, replayStats.RecordsScanned, state, err)
	materializer := NewMaterializer(cfg.VolumeID, cfg.ObjectStore, cfg.HeadStore, cfg.ObjectStoreForVolume)
	if materializer != nil {
		materializer.SetEncryption(cfg.Encryption)
		materializer.SetSegmentTargetSize(cfg.SegmentTargetSize)
		materializer.SetStateFormatVersion(cfg.StateFormatVersion)
		materializer.SetOpenObserver(cfg.OpenObserver)
	}
	var latestManifest *Manifest
	if materializer != nil {
		localCommitted := false
		if state != nil && localStateErr == nil && materializer.headStore != nil {
			head, headErr := materializer.loadCommittedHead(ctx)
			reuseStarted := time.Now()
			if headErr == nil && committedHeadMatchesCheckpoint(head, cfg.VolumeID, checkpointSequence(state)) {
				latestManifest = &Manifest{
					VolumeID:      head.VolumeID,
					ManifestSeq:   head.ManifestSeq,
					CheckpointSeq: head.CheckpointSeq,
					CreatedAt:     head.UpdatedAt,
				}
				localCommitted = true
				selectedSource = "local"
				selectedFormat = localFormat
				emitOpenPhase(cfg, "state_reuse", "local", localFormat, reuseStarted, localBytes, replayStats.RecordsScanned, state, nil)
			} else if headErr != nil && !errors.Is(headErr, ErrCommittedHeadNotFound) {
				err = headErr
			}
		}
		if !localCommitted && (err == nil || errors.Is(err, ErrSnapshotNotFound)) {
			phaseStarted = time.Now()
			latestState, manifest, latestErr := materializer.loadLatestStateOwned(ctx)
			remoteFormat := 0
			if manifest != nil && (manifest.Version == StateFormatV1 || manifest.Version == StateFormatV2) {
				remoteFormat = manifest.Version
			}
			emitOpenPhase(cfg, "state_load", "remote", remoteFormat, phaseStarted, -1, replayStats.RecordsScanned, latestState, latestErr)
			if latestErr == nil {
				if baseErr := validateCommittedWALBase(localStateErr, latestState, firstWALRecord, hasWALRecords); baseErr != nil {
					return nil, baseErr
				}
			}
			switch {
			case latestErr == nil && shouldUseMaterializedState(state, err, latestState, hasWALRecords):
				state = latestState
				err = nil
				latestManifest = manifest
				selectedSource = "remote"
				selectedFormat = remoteFormat
			case latestErr == nil:
				latestManifest = manifest
				if localStateErr == nil {
					selectedSource = "local"
					selectedFormat = localFormat
				}
			case errors.Is(err, ErrSnapshotNotFound) && latestErr != nil && !errors.Is(latestErr, ErrMaterializedManifestNotFound):
				err = latestErr
			}
		}
		materializer.SetOpenObserver(nil)
	}
	if err != nil && !errors.Is(err, ErrSnapshotNotFound) && !errors.Is(err, ErrMaterializedManifestNotFound) {
		return nil, err
	}
	if state == nil {
		now := time.Now().UTC()
		state = &SnapshotState{
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
		}
		selectedSource = "new"
		selectedFormat = 0
	} else if selectedSource == "new" && localStateErr == nil {
		selectedSource = "local"
		selectedFormat = localFormat
	}

	e := &Engine{
		volumeID:           cfg.VolumeID,
		nextSeq:            state.NextSeq,
		nextInode:          state.NextInode,
		nodes:              state.Nodes,
		children:           state.Children,
		data:               state.Data,
		coldFiles:          state.ColdFiles,
		segments:           state.Segments,
		materializer:       materializer,
		encryption:         cfg.Encryption,
		stateFormatVersion: normalizedStateFormatVersion(cfg.StateFormatVersion),
		localDiskGuard:     cfg.LocalDiskGuard,
		retainUnlinked:     cfg.RetainUnlinked,
	}
	if latestManifest != nil {
		e.lastCommittedManifest = latestManifest.ManifestSeq
	}

	phaseStarted = time.Now()
	appliedRecords := 0
	for {
		record, ok, replayErr := replay.Next(ctx)
		replayStats = replay.Stats()
		if replayErr != nil {
			emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), replayErr)
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
			emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), replayErr)
			return nil, replayErr
		}
		if err := e.apply(record); err != nil {
			replayErr := fmt.Errorf("replay wal seq %d: %w", record.Seq, err)
			emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), replayErr)
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
	if err := replay.Close(); err != nil {
		emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), err)
		return nil, err
	}
	walFile, err := openWAL(cfg.WALPath, cfg.VolumeID, cfg.Encryption, cfg.WALSyncHook, replayStats)
	if err != nil {
		emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), err)
		return nil, err
	}
	e.wal = walFile
	if !e.retainUnlinked {
		e.collectUnlinkedLocked()
	}
	if appliedRecords > 0 {
		e.dirty = true
		e.dirtyAt = time.Now().UTC()
		e.mutationVersion = 1
	}
	walRecordsApplied = appliedRecords
	emitWALOpenPhase(cfg, "wal_replay", selectedFormat, phaseStarted, replayStats, walRecordsSkipped, appliedRecords, e.currentStateLocked(), nil)

	engine = e
	return engine, nil
}

func emitWALOpenPhase(cfg Config, phase string, format int, started time.Time, stats walReplayStats, skipped, applied int, state *SnapshotState, err error) {
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
	if state != nil {
		observation.Nodes = len(state.Nodes)
		observation.DirectoryEntries = directoryEntryCount(state.Children)
		observation.Segments = len(state.Segments)
	}
	emitOpenObservation(cfg.OpenObserver, observation)
}

func emitOpenPhase(cfg Config, phase, source string, format int, started time.Time, bytes int64, walRecords int, state *SnapshotState, err error) {
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
		observation.Nodes = len(state.Nodes)
		observation.DirectoryEntries = directoryEntryCount(state.Children)
		observation.Segments = len(state.Segments)
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
	if err := e.persistCurrentStateLocked(); err != nil {
		return err
	}
	if err := e.wal.reset(); err != nil {
		return err
	}
	e.refreshLocalDiskGuardLocked()
	e.closed = true
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
	for inode, node := range e.nodes {
		if inode == RootInode || node == nil || node.Nlink != 0 {
			continue
		}
		if _, ok := retain[inode]; ok {
			continue
		}
		delete(e.children, inode)
		delete(e.nodes, inode)
		delete(e.data, inode)
		delete(e.coldFiles, inode)
	}
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
	return cloneNode(e.nodes[inode]), nil
}

func (e *Engine) GetAttr(inode uint64) (*Node, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	node, ok := e.nodes[inode]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneNode(node), nil
}

func (e *Engine) Mkdir(parent uint64, name string, mode uint32) (*Node, error) {
	return e.create(parent, name, TypeDirectory, mode, "", CreateOptions{})
}

func (e *Engine) ReadDir(inode uint64) ([]DirEntry, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if err := e.ensureDirLocked(inode); err != nil {
		return nil, err
	}

	entries := make([]DirEntry, 0, len(e.children[inode]))
	for name, childInode := range e.children[inode] {
		node := e.nodes[childInode]
		if node == nil {
			continue
		}
		entries = append(entries, DirEntry{
			Name:  name,
			Inode: childInode,
			Type:  node.Type,
		})
	}
	slices.SortFunc(entries, func(a, b DirEntry) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return entries, nil
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
	node, ok := e.nodes[inode]
	if !ok || node == nil {
		return nil, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	if err := e.ensureDirLocked(newParent); err != nil {
		return nil, err
	}
	if _, exists := e.children[newParent][newName]; exists {
		return nil, ErrExists
	}
	record := e.newRecord("link")
	record.Inode = inode
	record.NewParent = newParent
	record.NewName = newName
	if err := e.appendAndApplyLocked(record, estimatedWALRecordBytes(record)); err != nil {
		return nil, err
	}
	return cloneNode(e.nodes[inode]), nil
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
	return e.readFileLocked(node, inode, offset, size)
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
	return e.readFileIntoLocked(node, inode, offset, dest)
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
	node := e.nodes[inode]
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
	node, ok := e.nodes[inode]
	if !ok || node == nil || node.Nlink != 0 {
		return nil
	}
	delete(e.children, inode)
	delete(e.nodes, inode)
	delete(e.data, inode)
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
	node := e.nodes[inode]
	if node == nil {
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	if len(e.children[inode]) > 0 {
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
	if _, ok := e.nodes[inode]; !ok {
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
	if _, ok := e.nodes[inode]; !ok {
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
	return cloneState(e.currentStateLocked())
}

// SnapshotReferenceState returns a metadata snapshot for retaining live object
// references during GC. Inline payload bytes may be shared with the engine and
// must be treated as read-only by callers.
func (e *Engine) SnapshotReferenceState() *SnapshotState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneStateForMaterialization(e.currentStateLocked())
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
	manifestSeq := e.lastCommittedManifest
	checkpointSeq := checkpointSequence(e.currentStateLocked())
	materializer := e.materializer
	e.mu.RUnlock()
	if dirty || materializer == nil || materializer.headStore == nil {
		return false, nil
	}
	head, err := materializer.headStore.LoadCommittedHead(ctx, e.volumeID)
	if errors.Is(err, ErrCommittedHeadNotFound) {
		return manifestSeq == 0, nil
	}
	if err != nil {
		return false, err
	}
	return committedHeadMatchesCheckpoint(head, e.volumeID, checkpointSeq) && head.ManifestSeq == manifestSeq, nil
}

func committedHeadMatchesCheckpoint(head *CommittedHead, volumeID string, checkpointSeq uint64) bool {
	return head != nil &&
		head.VolumeID == volumeID &&
		head.ManifestSeq != 0 &&
		head.ManifestSeq == checkpointSeq &&
		head.CheckpointSeq == checkpointSeq &&
		head.ManifestKey == manifestKey(head.ManifestSeq)
}

// EstimatedMemoryBytes returns a conservative size estimate for bounding the
// node-local hot engine cache. It does not mutate or clone engine state.
func (e *Engine) EstimatedMemoryBytes() int64 {
	if e == nil {
		return 0
	}
	e.mu.RLock()
	total := estimatedStateMemoryBytes(e.currentStateLocked())
	materializer := e.materializer
	e.mu.RUnlock()
	if materializer != nil {
		total += materializer.estimatedCacheMemoryBytes()
	}
	return total
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
	if e.materializer == nil || !e.materializer.Enabled() || (!e.dirty && (!force || !e.needsMaterializationLocked())) {
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
	expectedManifestSeq := e.lastCommittedManifest
	e.mu.Unlock()
	checkpoint, err := e.wal.checkpoint(checkpointSequence(state))
	if err != nil {
		e.mutationMu.Unlock()
		return nil, err
	}
	e.mutationMu.Unlock()

	manifest, err := e.materializer.Materialize(ctx, state, expectedManifestSeq)
	if err != nil || manifest == nil {
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
	e.pendingMaterialization = &pendingMaterialization{
		manifestSeq:     manifest.ManifestSeq,
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
	if pending.walCheckpoint == nil || pending.walCheckpoint.throughSeq != pending.manifestSeq {
		return fmt.Errorf("%w: pending materialization wal checkpoint is invalid", ErrInvalidInput)
	}
	if err := e.wal.discardThrough(pending.walCheckpoint); err != nil {
		return err
	}
	if e.mutationVersion == pending.mutationVersion {
		e.replaceStateLocked(cloneState(pending.state))
		e.lastMaterializedVersion = pending.mutationVersion
		e.dirty = false
	}
	e.pendingMaterialization = nil
	e.refreshLocalDiskGuardLocked()
	return nil
}

func (e *Engine) RefreshMaterialized(ctx context.Context) (bool, error) {
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
		return false, err
	}
	if state == nil || state.NextSeq <= currentNextSeq {
		return false, nil
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
	e.replaceStateLocked(state)
	if err := e.persistCurrentStateLocked(); err != nil {
		return false, err
	}
	if err := e.wal.reset(); err != nil {
		return false, err
	}
	e.refreshLocalDiskGuardLocked()
	if manifest != nil {
		e.lastCommittedManifest = manifest.ManifestSeq
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
	e.replaceStateLocked(state)
	if err := e.persistCurrentStateLockedWithReserve(false); err != nil {
		return err
	}
	if err := e.wal.reset(); err != nil {
		return err
	}
	e.refreshLocalDiskGuardLocked()
	e.markDirtyLocked()
	return nil
}

func (e *Engine) ReplaceState(state *SnapshotState) error {
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
	e.replaceStateLocked(cloneState(state))
	if err := e.persistCurrentStateLockedWithReserve(false); err != nil {
		return err
	}
	if err := e.wal.reset(); err != nil {
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
	if _, exists := e.children[parent][name]; exists {
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
	return cloneNode(e.nodes[record.Inode]), nil
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
	return &SnapshotState{
		NextSeq:   e.nextSeq,
		NextInode: e.nextInode,
		Nodes:     e.nodes,
		Children:  e.children,
		Data:      e.data,
		ColdFiles: e.coldFiles,
		Segments:  e.segments,
	}
}

func (e *Engine) replaceStateLocked(state *SnapshotState) {
	normalizeState(state)
	e.nextSeq = state.NextSeq
	if e.nextSeq == 0 {
		e.nextSeq = 1
	}
	e.nextInode = state.NextInode
	if e.nextInode <= RootInode {
		e.nextInode = RootInode + 1
	}
	e.nodes = state.Nodes
	e.children = state.Children
	e.data = state.Data
	e.coldFiles = state.ColdFiles
	e.segments = state.Segments
	if !e.retainUnlinked {
		e.collectUnlinkedLocked()
	}
}

func (e *Engine) persistCurrentStateLocked() error {
	return e.persistCurrentStateLockedWithReserve(true)
}

func (e *Engine) persistCurrentStateLockedWithReserve(reserve bool) error {
	return e.persistStateLocked(e.currentStateLocked(), reserve)
}

func (e *Engine) persistStateLocked(source *SnapshotState, reserve bool) error {
	state := cloneState(source)
	pruneUnreferencedSegments(state)
	if reserve {
		if err := e.reserveLocalDiskLocked(estimatedStateBytes(state)); err != nil {
			return err
		}
	}
	return saveSnapshotState(headStatePath(e.wal.path), e.volumeID, "head", state, e.encryption, e.stateFormatVersion)
}

func loadCurrentState(cfg Config) (*SnapshotState, int, int64, error) {
	if cfg.WALPath == "" {
		return nil, 0, -1, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	return loadSnapshotStateWithFormat(headStatePath(cfg.WALPath), cfg.VolumeID, "head", cfg.Encryption)
}

func shouldUseMaterializedState(current *SnapshotState, currentErr error, latest *SnapshotState, hasWALRecords bool) bool {
	if latest == nil {
		return false
	}
	if errors.Is(currentErr, ErrSnapshotNotFound) {
		return true
	}
	if currentErr != nil {
		return false
	}
	if current == nil {
		return true
	}
	if hasWALRecords {
		return false
	}
	return latest.NextSeq > current.NextSeq
}

func validateCommittedWALBase(localStateErr error, committed *SnapshotState, firstWALRecord walRecord, hasWALRecords bool) error {
	if !hasWALRecords || !errors.Is(localStateErr, ErrSnapshotNotFound) || committed == nil {
		return nil
	}
	if firstWALRecord.Seq == committed.NextSeq {
		return nil
	}
	return fmt.Errorf(
		"%w: local head is missing, committed state expects wal seq %d, and local wal starts at seq %d",
		ErrCommittedHeadConflict,
		committed.NextSeq,
		firstWALRecord.Seq,
	)
}

func (e *Engine) apply(record walRecord) error {
	switch record.Op {
	case "create":
		return e.applyCreate(record)
	case "write":
		return e.applyWrite(record)
	case "link":
		return e.applyLink(record)
	case "rmdir":
		return e.applyRemoveDir(record)
	case "rename":
		return e.applyRename(record)
	case "chmod":
		return e.applySetMode(record)
	case "chown":
		return e.applySetOwner(record)
	case "truncate":
		return e.applyTruncate(record)
	case "unlink":
		return e.applyUnlink(record)
	default:
		return fmt.Errorf("unknown wal op %q", record.Op)
	}
}

func (e *Engine) applyCreate(record walRecord) error {
	if record.Inode == 0 || record.Parent == 0 || record.Name == "" {
		return fmt.Errorf("%w: invalid create record", ErrInvalidInput)
	}
	if err := e.ensureDirLocked(record.Parent); err != nil {
		return err
	}
	if _, exists := e.children[record.Parent][record.Name]; exists {
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
		e.children[node.Inode] = map[string]uint64{}
	}
	e.nodes[node.Inode] = node
	e.children[record.Parent][record.Name] = node.Inode
	if record.Inode >= e.nextInode {
		e.nextInode = record.Inode + 1
	}
	return nil
}

func (e *Engine) applyLink(record walRecord) error {
	if record.Inode == 0 || record.NewParent == 0 || record.NewName == "" {
		return fmt.Errorf("%w: invalid link record", ErrInvalidInput)
	}
	node := e.nodes[record.Inode]
	if node == nil {
		return ErrNotFound
	}
	if node.Type == TypeDirectory {
		return ErrIsDir
	}
	if err := e.ensureDirLocked(record.NewParent); err != nil {
		return err
	}
	if _, exists := e.children[record.NewParent][record.NewName]; exists {
		return ErrExists
	}
	e.children[record.NewParent][record.NewName] = record.Inode
	node.Nlink++
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
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
	e.data[record.Inode] = current
	node.Size = uint64(len(current))
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
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
	delete(e.children[record.Parent], record.Name)
	e.children[record.NewParent][record.NewName] = inode
	if node := e.nodes[inode]; node != nil {
		node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	}
	return nil
}

func (e *Engine) applyRemoveDir(record walRecord) error {
	inode, err := e.lookupLocked(record.Parent, record.Name)
	if err != nil {
		return err
	}
	node := e.nodes[inode]
	if node == nil {
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	if len(e.children[inode]) > 0 {
		return ErrNotEmpty
	}
	delete(e.children[record.Parent], record.Name)
	delete(e.children, inode)
	delete(e.nodes, inode)
	return nil
}

func (e *Engine) applySetMode(record walRecord) error {
	node, ok := e.nodes[record.Inode]
	if !ok {
		return ErrNotFound
	}
	node.Mode = record.Mode
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	return nil
}

func (e *Engine) applySetOwner(record walRecord) error {
	node, ok := e.nodes[record.Inode]
	if !ok {
		return ErrNotFound
	}
	node.UID = record.Mode
	node.GID = uint32(record.Offset)
	node.Ctime = time.Unix(0, record.TimeUnix).UTC()
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
	e.data[record.Inode] = current
	node.Size = uint64(len(current))
	now := time.Unix(0, record.TimeUnix).UTC()
	node.Mtime = now
	node.Ctime = now
	return nil
}

func (e *Engine) applyUnlink(record walRecord) error {
	inode, err := e.lookupLocked(record.Parent, record.Name)
	if err != nil {
		return err
	}
	node := e.nodes[inode]
	if node != nil && node.Type == TypeDirectory {
		return ErrIsDir
	}
	delete(e.children[record.Parent], record.Name)
	if node != nil && node.Nlink > 0 {
		node.Nlink--
		node.Ctime = time.Unix(0, record.TimeUnix).UTC()
	}
	return nil
}

func (e *Engine) markDirtyLocked() {
	e.mutationVersion++
	e.dirty = true
	e.dirtyAt = time.Now().UTC()
}

func (e *Engine) needsMaterializationLocked() bool {
	for _, payload := range e.data {
		if len(payload) > 0 {
			return true
		}
	}
	for _, extents := range e.coldFiles {
		for _, extent := range extents {
			if segment := e.segments[extent.SegmentID]; isInlineSegment(segment) {
				return true
			}
		}
	}
	return false
}

func (e *Engine) snapshotStateLocked() *SnapshotState {
	state := cloneState(e.currentStateLocked())
	for inode := range state.ColdFiles {
		if state.Nodes[inode] == nil {
			delete(state.ColdFiles, inode)
		}
	}
	pruneUnreferencedSegments(state)
	return state
}

func (e *Engine) exportStateLocked() (*SnapshotState, error) {
	state := cloneState(e.currentStateLocked())
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
	state := cloneStateForMaterialization(e.currentStateLocked())
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
	if payload := e.data[inode]; len(payload) > 0 || len(e.coldFiles[inode]) == 0 {
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
	if payload := e.data[inode]; len(payload) > 0 || len(e.coldFiles[inode]) == 0 {
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
	if payload := e.data[inode]; len(payload) > 0 || len(e.coldFiles[inode]) == 0 {
		return payload, nil
	}
	node := e.nodes[inode]
	if node == nil {
		return nil, ErrNotFound
	}
	payload, err := e.readColdRangeLocked(inode, 0, node.Size)
	if err != nil {
		return nil, err
	}
	e.data[inode] = payload
	delete(e.coldFiles, inode)
	return payload, nil
}

func (e *Engine) readColdRangeLocked(inode uint64, offset uint64, size uint64) ([]byte, error) {
	return readColdRange(e.materializer, e.coldFiles[inode], e.segments, offset, size)
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
	return nil
}

func (e *Engine) ensureDirLocked(inode uint64) error {
	node, ok := e.nodes[inode]
	if !ok {
		return ErrNotFound
	}
	if node.Type != TypeDirectory {
		return ErrNotDir
	}
	if e.children[inode] == nil {
		e.children[inode] = map[string]uint64{}
	}
	return nil
}

func (e *Engine) lookupLocked(parent uint64, name string) (uint64, error) {
	if err := e.ensureDirLocked(parent); err != nil {
		return 0, err
	}
	inode, ok := e.children[parent][name]
	if !ok {
		return 0, ErrNotFound
	}
	return inode, nil
}

func (e *Engine) fileNodeLocked(inode uint64) (*Node, error) {
	node, ok := e.nodes[inode]
	if !ok {
		return nil, ErrNotFound
	}
	if node.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	return node, nil
}

func (e *Engine) pathLocked(target uint64) (string, bool) {
	if target == RootInode {
		return "/", true
	}
	if e.nodes[target] == nil {
		return "", false
	}

	type frame struct {
		inode uint64
		path  string
	}
	queue := []frame{{inode: RootInode, path: "/"}}
	seen := map[uint64]struct{}{RootInode: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		children := e.children[current.inode]
		if len(children) == 0 {
			continue
		}
		names := make([]string, 0, len(children))
		for name := range children {
			names = append(names, name)
		}
		slices.Sort(names)
		for _, name := range names {
			child := children[name]
			childPath := current.path + name
			if current.path != "/" {
				childPath = current.path + "/" + name
			}
			if child == target {
				return childPath, true
			}
			if _, ok := seen[child]; ok {
				continue
			}
			seen[child] = struct{}{}
			if node := e.nodes[child]; node != nil && node.Type == TypeDirectory {
				queue = append(queue, frame{inode: child, path: childPath})
			}
		}
	}
	return "", false
}

func (e *Engine) collectUnlinkedLocked() {
	for inode, node := range e.nodes {
		if inode == RootInode || node == nil || node.Nlink != 0 {
			continue
		}
		delete(e.children, inode)
		delete(e.nodes, inode)
		delete(e.data, inode)
		delete(e.coldFiles, inode)
	}
}
