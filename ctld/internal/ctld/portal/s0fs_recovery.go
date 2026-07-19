package portal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const (
	s0fsHandleStateVersion          = 1
	s0fsHandleJournalVersion        = 1
	s0fsHandleJournalCompactEvents  = 4096
	s0fsHandleJournalOperationOpen  = "open"
	s0fsHandleJournalOperationClose = "close"
)

type s0fsHandleRecoveryState struct {
	Version  int                `json:"version"`
	VolumeID string             `json:"volume_id"`
	Handles  volume.HandleState `json:"handles"`
}

type s0fsHandleJournalEvent struct {
	Version   int    `json:"version"`
	VolumeID  string `json:"volume_id"`
	Operation string `json:"operation"`
	HandleID  uint64 `json:"handle_id"`
	Inode     uint64 `json:"inode,omitempty"`
}

type s0fsHandleStateJournal struct {
	file   *os.File
	events int
}

func loadS0FSHandleState(path, volumeID string) (volume.HandleState, error) {
	state, err := loadS0FSHandleSnapshot(path, volumeID)
	if err != nil {
		return volume.HandleState{}, err
	}
	return replayS0FSHandleJournal(s0fsHandleJournalPath(path), volumeID, state)
}

func loadS0FSHandleSnapshot(path, volumeID string) (volume.HandleState, error) {
	if strings.TrimSpace(path) == "" {
		return volume.HandleState{}, nil
	}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return volume.HandleState{}, nil
	}
	if err != nil {
		return volume.HandleState{}, fmt.Errorf("read s0fs handle recovery state: %w", err)
	}
	var state s0fsHandleRecoveryState
	if err := json.Unmarshal(payload, &state); err != nil {
		return volume.HandleState{}, fmt.Errorf("decode s0fs handle recovery state: %w", err)
	}
	if state.Version != s0fsHandleStateVersion {
		return volume.HandleState{}, fmt.Errorf("unsupported s0fs handle recovery state version %d", state.Version)
	}
	if state.VolumeID != volumeID {
		return volume.HandleState{}, fmt.Errorf("s0fs handle recovery state belongs to volume %q", state.VolumeID)
	}
	return normalizeS0FSHandleState(state.Handles), nil
}

func replayS0FSHandleJournal(path, volumeID string, state volume.HandleState) (volume.HandleState, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return normalizeS0FSHandleState(state), nil
	}
	if err != nil {
		return volume.HandleState{}, fmt.Errorf("read s0fs handle recovery journal: %w", err)
	}
	replayer := newS0FSHandleJournalReplayer(state)
	lines := bytes.Split(payload, []byte{'\n'})
	for index, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var event s0fsHandleJournalEvent
		if err := json.Unmarshal(line, &event); err != nil {
			if index == len(lines)-1 && len(payload) > 0 && payload[len(payload)-1] != '\n' {
				break
			}
			return volume.HandleState{}, fmt.Errorf("decode s0fs handle recovery journal event: %w", err)
		}
		if err := validateS0FSHandleJournalEvent(event, volumeID); err != nil {
			return volume.HandleState{}, err
		}
		replayer.apply(event)
	}
	return replayer.state(), nil
}

type s0fsHandleJournalReplayer struct {
	handles       volume.HandleState
	openByInode   map[uint64]int
	unlinkedFiles map[uint64]struct{}
}

func newS0FSHandleJournalReplayer(state volume.HandleState) *s0fsHandleJournalReplayer {
	state = normalizeS0FSHandleState(state)
	replayer := &s0fsHandleJournalReplayer{
		handles:       state,
		openByInode:   make(map[uint64]int),
		unlinkedFiles: make(map[uint64]struct{}, len(state.UnlinkedFiles)),
	}
	for _, inode := range state.FileHandles {
		replayer.openByInode[inode]++
	}
	for _, inode := range state.UnlinkedFiles {
		replayer.unlinkedFiles[inode] = struct{}{}
	}
	return replayer
}

func (r *s0fsHandleJournalReplayer) apply(event s0fsHandleJournalEvent) {
	if event.HandleID > r.handles.NextHandleID {
		r.handles.NextHandleID = event.HandleID
	}
	switch event.Operation {
	case s0fsHandleJournalOperationOpen:
		if previous, ok := r.handles.FileHandles[event.HandleID]; ok {
			if previous == event.Inode {
				return
			}
			r.decrement(previous)
		}
		r.handles.FileHandles[event.HandleID] = event.Inode
		r.openByInode[event.Inode]++
	case s0fsHandleJournalOperationClose:
		inode, ok := r.handles.FileHandles[event.HandleID]
		if !ok {
			return
		}
		delete(r.handles.FileHandles, event.HandleID)
		r.decrement(inode)
	}
}

func (r *s0fsHandleJournalReplayer) decrement(inode uint64) {
	if r.openByInode[inode] > 1 {
		r.openByInode[inode]--
		return
	}
	delete(r.openByInode, inode)
	delete(r.unlinkedFiles, inode)
}

func (r *s0fsHandleJournalReplayer) state() volume.HandleState {
	r.handles.UnlinkedFiles = nil
	for inode := range r.unlinkedFiles {
		r.handles.UnlinkedFiles = append(r.handles.UnlinkedFiles, inode)
	}
	sort.Slice(r.handles.UnlinkedFiles, func(i, j int) bool {
		return r.handles.UnlinkedFiles[i] < r.handles.UnlinkedFiles[j]
	})
	return r.handles
}

func validateS0FSHandleJournalEvent(event s0fsHandleJournalEvent, volumeID string) error {
	if event.Version != s0fsHandleJournalVersion {
		return fmt.Errorf("unsupported s0fs handle recovery journal version %d", event.Version)
	}
	if event.VolumeID != volumeID {
		return fmt.Errorf("s0fs handle recovery journal belongs to volume %q", event.VolumeID)
	}
	if event.HandleID == 0 {
		return fmt.Errorf("s0fs handle recovery journal event is missing handle_id")
	}
	switch event.Operation {
	case s0fsHandleJournalOperationOpen:
		if event.Inode == 0 {
			return fmt.Errorf("s0fs handle open recovery event is missing inode")
		}
	case s0fsHandleJournalOperationClose:
	default:
		return fmt.Errorf("unsupported s0fs handle recovery journal operation %q", event.Operation)
	}
	return nil
}

func normalizeS0FSHandleState(state volume.HandleState) volume.HandleState {
	if state.FileHandles == nil {
		state.FileHandles = make(map[uint64]uint64)
	}
	for handle := range state.FileHandles {
		if handle > state.NextHandleID {
			state.NextHandleID = handle
		}
	}
	for handle := range state.DirHandles {
		if handle > state.NextHandleID {
			state.NextHandleID = handle
		}
	}
	state.DirHandles = nil
	return state
}

func persistS0FSHandleState(path, volumeID string, handles volume.HandleState) error {
	return persistS0FSHandleStateWithDurability(path, volumeID, handles, true)
}

// persistProcessLocalS0FSHandleState makes the latest handle snapshot visible
// to a standby process without forcing it to stable storage. S0FS portal HA
// protects against a process or Pod failure on the same node, so the shared
// kernel page cache remains available to the promoted process.
func persistProcessLocalS0FSHandleState(path, volumeID string, handles volume.HandleState) error {
	return persistS0FSHandleStateWithDurability(path, volumeID, handles, false)
}

func persistS0FSHandleStateWithDurability(path, volumeID string, handles volume.HandleState, durable bool) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	payload, err := json.Marshal(s0fsHandleRecoveryState{
		Version:  s0fsHandleStateVersion,
		VolumeID: volumeID,
		Handles:  normalizeS0FSHandleState(handles),
	})
	if err != nil {
		return err
	}
	return writeAtomicRecoveryStateWithDurability(path, ".s0fs-*.tmp", payload, durable)
}

// compactS0FSHandleState replaces the snapshot before clearing the journal.
// Journal events use unique handle IDs and are idempotent against the newer
// snapshot, so a crash between those steps can safely replay the old events.
func compactS0FSHandleState(
	path, volumeID string,
	handles volume.HandleState,
	durable bool,
	journal *s0fsHandleStateJournal,
) error {
	if err := persistS0FSHandleStateWithDurability(path, volumeID, handles, durable); err != nil {
		return err
	}
	if journal != nil {
		return journal.Reset()
	}
	if err := os.Remove(s0fsHandleJournalPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove s0fs handle recovery journal: %w", err)
	}
	return nil
}

func openS0FSHandleStateJournal(path string) (*s0fsHandleStateJournal, error) {
	if strings.TrimSpace(path) == "" {
		return &s0fsHandleStateJournal{}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create s0fs handle recovery directory: %w", err)
	}
	file, err := os.OpenFile(s0fsHandleJournalPath(path), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open s0fs handle recovery journal: %w", err)
	}
	return &s0fsHandleStateJournal{file: file}, nil
}

func (j *s0fsHandleStateJournal) Append(event s0fsHandleJournalEvent) error {
	if j == nil || j.file == nil {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	written, err := j.file.Write(payload)
	if err != nil {
		return fmt.Errorf("append s0fs handle recovery journal: %w", err)
	}
	if written != len(payload) {
		return fmt.Errorf("append s0fs handle recovery journal: %w", io.ErrShortWrite)
	}
	j.events++
	return nil
}

func (j *s0fsHandleStateJournal) Reset() error {
	if j == nil || j.file == nil {
		return nil
	}
	if err := j.file.Truncate(0); err != nil {
		return fmt.Errorf("reset s0fs handle recovery journal: %w", err)
	}
	j.events = 0
	return nil
}

func (j *s0fsHandleStateJournal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	return err
}

func (j *s0fsHandleStateJournal) ShouldCompact() bool {
	return j != nil && j.events >= s0fsHandleJournalCompactEvents
}

func s0fsHandleJournalPath(statePath string) string {
	return statePath + ".journal"
}

func removeS0FSHandleRecoveryState(path string) error {
	var firstErr error
	for _, candidate := range []string{path, s0fsHandleJournalPath(path)} {
		if err := os.Remove(candidate); err != nil && !errors.Is(err, os.ErrNotExist) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func retainedUnlinkedInodes(state volume.HandleState) map[uint64]struct{} {
	retained := make(map[uint64]struct{}, len(state.UnlinkedFiles))
	for _, inode := range state.UnlinkedFiles {
		retained[inode] = struct{}{}
	}
	return retained
}
