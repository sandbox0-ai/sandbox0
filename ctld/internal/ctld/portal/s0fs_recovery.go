package portal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const s0fsHandleStateVersion = 1

type s0fsHandleRecoveryState struct {
	Version  int                `json:"version"`
	VolumeID string             `json:"volume_id"`
	Handles  volume.HandleState `json:"handles"`
}

func loadS0FSHandleState(path, volumeID string) (volume.HandleState, error) {
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
	return state.Handles, nil
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
		Handles:  handles,
	})
	if err != nil {
		return err
	}
	return writeAtomicRecoveryStateWithDurability(path, ".s0fs-*.tmp", payload, durable)
}

func retainedUnlinkedInodes(state volume.HandleState) map[uint64]struct{} {
	retained := make(map[uint64]struct{}, len(state.UnlinkedFiles))
	for _, inode := range state.UnlinkedFiles {
		retained[inode] = struct{}{}
	}
	return retained
}
