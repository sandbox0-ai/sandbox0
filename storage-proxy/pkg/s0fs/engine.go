package s0fs

import (
	"cmp"
	"context"
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
}

func Open(ctx context.Context, cfg Config) (*Engine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cfg.VolumeID == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}

	walFile, records, err := openWAL(cfg.WALPath)
	if err != nil {
		return nil, err
	}

	state, err := loadCurrentState(cfg)
	if err != nil && err != ErrSnapshotNotFound {
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
			Data: make(map[uint64][]byte),
		}
	}

	e := &Engine{
		volumeID:  cfg.VolumeID,
		wal:       walFile,
		nextSeq:   state.NextSeq,
		nextInode: state.NextInode,
		nodes:     state.Nodes,
		children:  state.Children,
		data:      state.Data,
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
	return len(payload), nil
}

func (e *Engine) Read(inode uint64, offset uint64, size uint64) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if _, err := e.fileNodeLocked(inode); err != nil {
		return nil, err
	}
	fileData := e.data[inode]
	if offset >= uint64(len(fileData)) {
		return nil, nil
	}
	end := offset + size
	if end > uint64(len(fileData)) {
		end = uint64(len(fileData))
	}
	return slices.Clone(fileData[offset:end]), nil
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
	return e.apply(record)
}

func (e *Engine) Unlink(parent uint64, name string) error {
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
	if node.Type == TypeDirectory {
		return ErrIsDir
	}
	record := e.newRecord("unlink")
	record.Parent = parent
	record.Name = name
	if err := e.wal.append(record); err != nil {
		return err
	}
	return e.apply(record)
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
	return e.apply(record)
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
	return e.apply(record)
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
	return e.apply(record)
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
	return e.apply(record)
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

func (e *Engine) CreateSnapshot(snapshotID string) (*SnapshotState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.checkOpen(); err != nil {
		return nil, err
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: snapshot id is required", ErrInvalidInput)
	}
	state := cloneState(e.currentStateLocked())
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
	return e.wal.reset()
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
	return e.wal.reset()
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
	current := e.data[record.Inode]
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
	current := e.data[record.Inode]
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

func (e *Engine) collectUnlinkedLocked() {
	for inode, node := range e.nodes {
		if inode == RootInode || node == nil || node.Nlink != 0 {
			continue
		}
		delete(e.children, inode)
		delete(e.nodes, inode)
		delete(e.data, inode)
	}
}
