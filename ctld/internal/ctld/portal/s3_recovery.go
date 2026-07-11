package portal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
)

const s3RecoveryStateVersion = 2

type s3RecoveryState struct {
	Version      int                         `json:"version"`
	VolumeID     string                      `json:"volume_id"`
	NextInode    uint64                      `json:"next_inode"`
	NextHandleID uint64                      `json:"next_handle_id"`
	Nodes        []s3RecoveryNode            `json:"nodes"`
	Handles      map[uint64]s3RecoveryHandle `json:"handles,omitempty"`
	Implicit     map[uint64]s3RecoveryHandle `json:"implicit,omitempty"`
}

type s3RecoveryNode struct {
	Inode     uint64     `json:"inode"`
	Path      string     `json:"path"`
	Kind      s3NodeKind `json:"kind"`
	Size      int64      `json:"size"`
	Modified  int64      `json:"modified_unix_nano"`
	LocalOnly bool       `json:"local_only,omitempty"`
}

type s3RecoveryHandle struct {
	Inode       uint64   `json:"inode"`
	Path        string   `json:"path"`
	Writable    bool     `json:"writable,omitempty"`
	ActorPID    uint32   `json:"actor_pid,omitempty"`
	ActorUID    uint32   `json:"actor_uid,omitempty"`
	ActorGIDs   []uint32 `json:"actor_gids,omitempty"`
	Read        bool     `json:"read,omitempty"`
	RecoveryKey string   `json:"recovery_key,omitempty"`
	BufferSize  int64    `json:"buffer_size,omitempty"`
	Committed   bool     `json:"committed,omitempty"`
	Closed      bool     `json:"closed,omitempty"`
	Failed      bool     `json:"failed,omitempty"`
}

func (s *s3Session) Handoff() error {
	return s.persistRecoveryState()
}

func (s *s3Session) RecoveryError() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateErr
}

func (s *s3Session) markRecoveryDirty() {
	if s == nil || s.statePath == "" {
		return
	}
	s.mu.Lock()
	s.stateDirty = true
	s.mu.Unlock()
}

func (s *s3Session) setRecoveryError(err error) {
	if s == nil || err == nil {
		return
	}
	s.mu.Lock()
	s.stateErr = err
	s.mu.Unlock()
}

func (s *s3Session) persistRecoveryState() error {
	if s == nil || s.statePath == "" {
		return nil
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.mu.Lock()
	if !s.stateDirty {
		err := s.stateErr
		s.mu.Unlock()
		return err
	}
	state := s.snapshotRecoveryStateLocked()
	s.stateDirty = false
	s.mu.Unlock()
	payload, err := json.Marshal(state)
	if err == nil {
		err = writeAtomicRecoveryState(s.statePath, ".s3-*.tmp", payload)
	}
	s.mu.Lock()
	if err != nil {
		s.stateDirty = true
		s.stateErr = err
	} else {
		s.stateErr = nil
	}
	s.mu.Unlock()
	return err
}

func (s *s3Session) snapshotRecoveryStateLocked() *s3RecoveryState {
	state := &s3RecoveryState{
		Version:      s3RecoveryStateVersion,
		VolumeID:     s.volumeID,
		NextInode:    s.nextInode,
		NextHandleID: s.nextHandleID,
		Nodes:        make([]s3RecoveryNode, 0, len(s.nodesByInode)),
		Handles:      make(map[uint64]s3RecoveryHandle, len(s.handles)),
		Implicit:     make(map[uint64]s3RecoveryHandle, len(s.implicit)),
	}
	for _, node := range s.nodesByInode {
		if node == nil {
			continue
		}
		state.Nodes = append(state.Nodes, s3RecoveryNode{
			Inode: node.inode, Path: node.path, Kind: node.kind, Size: node.size,
			Modified: node.modified.UnixNano(), LocalOnly: node.localOnly,
		})
	}
	for id, handle := range s.handles {
		if handle != nil {
			state.Handles[id] = snapshotS3Handle(handle)
		}
	}
	for inode, handle := range s.implicit {
		if handle != nil {
			state.Implicit[inode] = snapshotS3Handle(handle)
		}
	}
	return state
}

func snapshotS3Handle(handle *s3Handle) s3RecoveryHandle {
	handle.mu.Lock()
	defer handle.mu.Unlock()
	state := s3RecoveryHandle{
		Inode: handle.inode, Path: handle.path, Writable: handle.writable,
		Read: handle.read, RecoveryKey: handle.recoveryKey, BufferSize: int64(handle.buffer.Len()),
		Committed: handle.committed, Closed: handle.closed, Failed: handle.failed,
	}
	if handle.actor != nil {
		state.ActorPID = handle.actor.Pid
		state.ActorUID = handle.actor.Uid
		state.ActorGIDs = append([]uint32(nil), handle.actor.Gids...)
	}
	return state
}

func (s *s3Session) restoreRecoveryState(state *s3RecoveryState) error {
	if state == nil {
		return nil
	}
	if state.Version != s3RecoveryStateVersion {
		return fmt.Errorf("unsupported s3 recovery state version %d", state.Version)
	}
	if state.VolumeID != s.volumeID {
		return fmt.Errorf("s3 recovery state belongs to volume %q", state.VolumeID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextInode = state.NextInode
	s.nextHandleID = state.NextHandleID
	s.nodesByInode = make(map[uint64]*s3Node, len(state.Nodes))
	s.inodeByPath = make(map[string]uint64, len(state.Nodes))
	for _, saved := range state.Nodes {
		node := &s3Node{
			inode: saved.Inode, path: saved.Path, kind: saved.Kind, size: saved.Size,
			modified: time.Unix(0, saved.Modified).UTC(), localOnly: saved.LocalOnly,
		}
		s.nodesByInode[node.inode] = node
		s.inodeByPath[node.path] = node.inode
	}
	if s.nodesByInode[s3RootInode] == nil {
		return fmt.Errorf("s3 recovery state is missing the root inode")
	}
	s.handles = make(map[uint64]*s3Handle, len(state.Handles))
	for id, saved := range state.Handles {
		handle, err := s.restoreS3Handle(saved)
		if err != nil {
			return fmt.Errorf("restore s3 handle %d: %w", id, err)
		}
		s.handles[id] = handle
	}
	s.implicit = make(map[uint64]*s3Handle, len(state.Implicit))
	for inode, saved := range state.Implicit {
		handle, err := s.restoreS3Handle(saved)
		if err != nil {
			return fmt.Errorf("restore implicit s3 handle for inode %d: %w", inode, err)
		}
		s.implicit[inode] = handle
	}
	s.stateDirty = false
	s.stateErr = nil
	return nil
}

func (s *s3Session) restoreS3Handle(saved s3RecoveryHandle) (*s3Handle, error) {
	handle := &s3Handle{
		inode: saved.Inode, path: saved.Path, writable: saved.Writable,
		read: saved.Read, recoveryKey: saved.RecoveryKey,
		committed: saved.Committed, closed: saved.Closed, failed: saved.Failed,
	}
	if saved.ActorPID != 0 || saved.ActorUID != 0 || len(saved.ActorGIDs) > 0 {
		handle.actor = &pb.PosixActor{Pid: saved.ActorPID, Uid: saved.ActorUID, Gids: append([]uint32(nil), saved.ActorGIDs...)}
	}
	if saved.BufferSize < 0 {
		return nil, fmt.Errorf("negative recovery buffer size %d", saved.BufferSize)
	}
	if saved.BufferSize > 0 {
		dataPath, err := s.recoveryDataPath(saved.RecoveryKey)
		if err != nil {
			return nil, err
		}
		file, err := os.Open(dataPath)
		if err != nil {
			return nil, fmt.Errorf("open recovery data: %w", err)
		}
		defer file.Close()
		if _, err := io.CopyN(&handle.buffer, file, saved.BufferSize); err != nil {
			return nil, fmt.Errorf("read recovery data: %w", err)
		}
	}
	return handle, nil
}

func (s *s3Session) appendRecoveryData(handle *s3Handle, data []byte, offset int64) error {
	if s.statePath == "" || len(data) == 0 {
		return nil
	}
	dataPath, err := s.recoveryDataPath(handle.recoveryKey)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o700); err != nil {
		return fmt.Errorf("create s3 recovery data directory: %w", err)
	}
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open s3 recovery data: %w", err)
	}
	defer file.Close()
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate s3 recovery data: %w", err)
	}
	if _, err := file.WriteAt(data, offset); err != nil {
		return fmt.Errorf("append s3 recovery data: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync s3 recovery data: %w", err)
	}
	return nil
}

func (s *s3Session) resetRecoveryData(handle *s3Handle) error {
	if s.statePath == "" || handle.recoveryKey == "" {
		return nil
	}
	dataPath, err := s.recoveryDataPath(handle.recoveryKey)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dataPath), 0o700); err != nil {
		return fmt.Errorf("create s3 recovery data directory: %w", err)
	}
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("reset s3 recovery data: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync reset s3 recovery data: %w", err)
	}
	return nil
}

func (s *s3Session) recoveryDataPath(key string) (string, error) {
	if key == "" || filepath.Base(key) != key || key == "." {
		return "", fmt.Errorf("invalid s3 recovery key %q", key)
	}
	return filepath.Join(s.statePath+".data", key), nil
}

func (s *s3Session) pruneRecoveryData(state *s3RecoveryState) {
	dir := s.statePath + ".data"
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		s.logger.WithError(err).Warn("Failed to inspect s3 recovery data directory")
		return
	}
	keep := make(map[string]struct{}, len(state.Handles)+len(state.Implicit))
	for _, handle := range state.Handles {
		if handle.RecoveryKey != "" {
			keep[handle.RecoveryKey] = struct{}{}
		}
	}
	for _, handle := range state.Implicit {
		if handle.RecoveryKey != "" {
			keep[handle.RecoveryKey] = struct{}{}
		}
	}
	for _, entry := range entries {
		if _, ok := keep[entry.Name()]; ok || entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.logger.WithError(err).WithField("file", entry.Name()).Warn("Failed to prune s3 recovery data")
		}
	}
}

func loadS3RecoveryState(path string) (*s3RecoveryState, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read s3 recovery state: %w", err)
	}
	var state s3RecoveryState
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, fmt.Errorf("decode s3 recovery state: %w", err)
	}
	return &state, nil
}

func writeAtomicRecoveryState(path, pattern string, payload []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create recovery state directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), pattern)
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}
