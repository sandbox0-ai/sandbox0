package s0fs

import (
	"bytes"
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
	mu       sync.RWMutex
	volumeID string
	wal      *wal
	closed   bool

	nextSeq   uint64
	nextInode uint64
	nodes     map[uint64]*Node
	children  map[uint64]map[string]uint64
	data      map[uint64][]byte
	coldFiles map[uint64][]FileExtent
	segments  map[string]*Segment

	materializer            *Materializer
	mutationVersion         uint64
	lastMaterializedVersion uint64
	dirty                   bool
	dirtyAt                 time.Time
}

func Open(ctx context.Context, cfg Config) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.VolumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}

	walFile, records, err := openWAL(cfg.WALPath, cfg.WALSyncHook)
	if err != nil {
		return nil, err
	}

	state, err := loadCurrentState(cfg)
	if cfg.ObjectStore != nil {
		materializer := NewMaterializer(cfg.ObjectStore)
		if materializer != nil {
			latestState, _, latestErr := materializer.LoadLatestState(ctx)
			switch {
			case latestErr == nil && shouldUseMaterializedState(state, err, latestState, len(records)):
				state = latestState
				err = nil
			case errors.Is(err, ErrSnapshotNotFound) && latestErr != nil && !errors.Is(latestErr, ErrMaterializedManifestNotFound):
				err = latestErr
			}
		}
	}
	if err != nil && !errors.Is(err, ErrSnapshotNotFound) && !errors.Is(err, ErrMaterializedManifestNotFound) {
		_ = walFile.close()
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
	}

	e := &Engine{
		volumeID:     cfg.VolumeID,
		wal:          walFile,
		nextSeq:      state.NextSeq,
		nextInode:    state.NextInode,
		nodes:        state.Nodes,
		children:     state.Children,
		data:         state.Data,
		coldFiles:    state.ColdFiles,
		segments:     state.Segments,
		materializer: NewMaterializer(cfg.ObjectStore),
	}

	for _, record := range records {
		if err := e.apply(record); err != nil {
			_ = walFile.close()
			return nil, fmt.Errorf("replay wal seq %d: %w", record.Seq, err)
		}
		if record.Seq >= e.nextSeq {
			e.nextSeq = record.Seq + 1
		}
		if record.Inode >= e.nextInode {
			e.nextInode = record.Inode + 1
		}
	}
	e.collectUnlinkedLocked()
	if len(records) > 0 {
		e.dirty = true
		e.dirtyAt = time.Now().UTC()
		e.mutationVersion = 1
	}

	return e, nil
}

func (e *Engine) Close() error {
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
	e.closed = true
	return e.wal.close()
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
	return e.create(parent, name, TypeDirectory, mode, "")
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
	return e.create(parent, name, TypeFile, mode, "")
}

func (e *Engine) Symlink(parent uint64, name, target string, mode uint32) (*Node, error) {
	if target == "" {
		return nil, fmt.Errorf("%w: symlink target is required", ErrInvalidInput)
	}
	return e.create(parent, name, TypeSymlink, mode, target)
}

func (e *Engine) Write(inode uint64, offset uint64, payload []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return 0, err
	}
	if _, err := e.fileNodeLocked(inode); err != nil {
		return 0, err
	}
	record := e.newRecord("write")
	record.Inode = inode
	record.Offset = offset
	record.Data = slices.Clone(payload)
	if err := e.wal.append(record); err != nil {
		return 0, err
	}
	if err := e.apply(record); err != nil {
		return 0, err
	}
	e.markDirtyLocked()
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
	if err := e.wal.append(record); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) Unlink(parent uint64, name string) error {
	_, err := e.UnlinkWithInode(parent, name)
	return err
}

// UnlinkWithInode removes a file entry and returns the inode that was unlinked.
func (e *Engine) UnlinkWithInode(parent uint64, name string) (uint64, error) {
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
	if err := e.wal.append(record); err != nil {
		return 0, err
	}
	if err := e.apply(record); err != nil {
		return 0, err
	}
	e.markDirtyLocked()
	return inode, nil
}

func (e *Engine) Forget(inode uint64) error {
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
	if err := e.wal.append(record); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) SetMode(inode uint64, mode uint32) error {
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
	if err := e.wal.append(record); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) SetOwner(inode uint64, uid, gid uint32) error {
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
	if err := e.wal.append(record); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) Truncate(inode uint64, size uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if _, err := e.fileNodeLocked(inode); err != nil {
		return err
	}
	record := e.newRecord("truncate")
	record.Inode = inode
	record.Offset = size
	if err := e.wal.append(record); err != nil {
		return err
	}
	if err := e.apply(record); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) Fsync(_ uint64) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	return e.wal.sync()
}

func (e *Engine) SnapshotState() *SnapshotState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneState(e.currentStateLocked())
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
	e.mu.RLock()
	if err := e.checkOpen(); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	if e.materializer == nil || !e.materializer.Enabled() || !e.dirty {
		e.mu.RUnlock()
		return nil, nil
	}
	version := e.mutationVersion
	state, err := e.materializeStateLocked()
	if err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	volumeID := e.volumeID
	e.mu.RUnlock()

	manifest, err := e.materializer.Materialize(ctx, volumeID, state)
	if err != nil || manifest == nil {
		return manifest, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.mutationVersion == version {
		if manifest.State != nil {
			e.replaceStateLocked(cloneState(manifest.State))
		}
		if err := e.persistCurrentStateLocked(); err != nil {
			return nil, err
		}
		if err := e.wal.reset(); err != nil {
			return nil, err
		}
		e.lastMaterializedVersion = version
		e.dirty = false
	}
	return manifest, nil
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

	state, _, err := e.materializer.LoadLatestState(ctx)
	if errors.Is(err, ErrMaterializedManifestNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if state == nil || state.NextSeq <= currentNextSeq {
		return false, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return false, err
	}
	if e.dirty || state.NextSeq <= e.nextSeq {
		return false, nil
	}
	e.replaceStateLocked(cloneState(state))
	if err := e.persistCurrentStateLocked(); err != nil {
		return false, err
	}
	if err := e.wal.reset(); err != nil {
		return false, err
	}
	return true, nil
}

func (e *Engine) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: snapshot id is required", ErrInvalidInput)
	}
	state, err := e.exportStateLocked()
	if err != nil {
		return nil, err
	}
	if err := saveSnapshotState(snapshotFilePath(e.wal.path, snapshotID), state); err != nil {
		return nil, err
	}
	return state, nil
}

func (e *Engine) RestoreSnapshot(snapshotID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	state, err := loadSnapshotState(snapshotFilePath(e.wal.path, snapshotID))
	if err != nil {
		return err
	}
	e.replaceStateLocked(state)
	if err := e.persistCurrentStateLocked(); err != nil {
		return err
	}
	if err := e.wal.reset(); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) ReplaceState(state *SnapshotState) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	e.replaceStateLocked(cloneState(state))
	if err := e.persistCurrentStateLocked(); err != nil {
		return err
	}
	if err := e.wal.reset(); err != nil {
		return err
	}
	e.markDirtyLocked()
	return nil
}

func (e *Engine) DeleteSnapshot(snapshotID string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return err
	}
	if snapshotID == "" {
		return fmt.Errorf("%w: snapshot id is required", ErrInvalidInput)
	}
	if err := os.Remove(snapshotFilePath(e.wal.path, snapshotID)); err != nil {
		if os.IsNotExist(err) {
			return ErrSnapshotNotFound
		}
		return fmt.Errorf("delete snapshot state: %w", err)
	}
	return nil
}

func (e *Engine) create(parent uint64, name string, typ FileType, mode uint32, target string) (*Node, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("%w: empty name", ErrInvalidInput)
	}
	if err := e.ensureDirLocked(parent); err != nil {
		return nil, err
	}
	if _, exists := e.children[parent][name]; exists {
		return nil, ErrExists
	}

	record := e.newRecord("create")
	record.Inode = e.nextInode
	record.Parent = parent
	record.Name = name
	record.Type = typ
	record.Mode = mode
	record.Target = target
	if err := e.wal.append(record); err != nil {
		return nil, err
	}
	if err := e.apply(record); err != nil {
		return nil, err
	}
	e.markDirtyLocked()
	return cloneNode(e.nodes[record.Inode]), nil
}

func (e *Engine) newRecord(op string) walRecord {
	record := walRecord{
		Seq:      e.nextSeq,
		Op:       op,
		TimeUnix: time.Now().UTC().UnixNano(),
	}
	e.nextSeq++
	return record
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
	e.collectUnlinkedLocked()
}

func (e *Engine) persistCurrentStateLocked() error {
	return saveSnapshotState(headStatePath(e.wal.path), cloneState(e.currentStateLocked()))
}

func loadCurrentState(cfg Config) (*SnapshotState, error) {
	if cfg.WALPath == "" {
		return nil, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	return loadSnapshotState(headStatePath(cfg.WALPath))
}

func shouldUseMaterializedState(current *SnapshotState, currentErr error, latest *SnapshotState, walRecords int) bool {
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
	if walRecords > 0 {
		return false
	}
	return latest.NextSeq > current.NextSeq
}

func (e *Engine) apply(record walRecord) error {
	switch record.Op {
	case "create":
		return e.applyCreate(record)
	case "write":
		return e.applyWrite(record)
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

func (e *Engine) applyWrite(record walRecord) error {
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
	state := cloneState(e.currentStateLocked())
	if len(state.ColdFiles) == 0 {
		return state, nil
	}
	for inode := range state.ColdFiles {
		if state.Nodes[inode] == nil {
			delete(state.ColdFiles, inode)
		}
	}
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
	if e.materializer == nil || !e.materializer.Enabled() {
		return nil, fmt.Errorf("%w: cold data resolver is not configured", ErrInvalidInput)
	}
	extents := e.coldFiles[inode]
	if len(extents) == 0 {
		return nil, nil
	}
	var out bytes.Buffer
	remaining := size
	rangeStart := offset
	rangeEnd := offset + size
	fileOffset := uint64(0)

	for _, extent := range extents {
		extentStart := fileOffset
		extentEnd := fileOffset + extent.Length
		if rangeEnd <= extentStart || rangeStart >= extentEnd {
			fileOffset = extentEnd
			continue
		}

		readStart := maxUint64(rangeStart, extentStart)
		readEnd := minUint64(rangeEnd, extentEnd)
		segment := e.segments[extent.SegmentID]
		if segment == nil {
			return nil, fmt.Errorf("%w: missing segment %s", ErrInvalidInput, extent.SegmentID)
		}
		segmentOffset := extent.Offset + (readStart - extentStart)
		chunk, err := e.materializer.ReadSegmentRange(segment, int64(segmentOffset), int64(readEnd-readStart))
		if err != nil {
			return nil, fmt.Errorf("read cold segment %s: %w", segment.Key, err)
		}
		if _, err := out.Write(chunk); err != nil {
			return nil, fmt.Errorf("assemble cold file data: %w", err)
		}
		remaining -= uint64(len(chunk))
		if remaining == 0 {
			break
		}
		fileOffset = extentEnd
	}
	return out.Bytes(), nil
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
