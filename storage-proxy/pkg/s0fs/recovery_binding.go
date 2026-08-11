package s0fs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

const recoveryBindingVersion = 1

type recoveryBinding struct {
	Version     int            `json:"version"`
	VolumeID    string         `json:"volume_id"`
	Head        *CommittedHead `json:"head,omitempty"`
	StateDigest string         `json:"state_digest,omitempty"`
	Checksum    string         `json:"checksum"`
}

type recoveryBindingChecksum struct {
	Version     int            `json:"version"`
	VolumeID    string         `json:"volume_id"`
	Head        *CommittedHead `json:"head,omitempty"`
	StateDigest string         `json:"state_digest,omitempty"`
}

func localHeadBindingPath(walPath string) string {
	return headStatePath(walPath) + ".commit"
}

func walBaseBindingPath(walPath string) string {
	return walPath + ".base"
}

func loadRecoveryBinding(path, volumeID string) (*recoveryBinding, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read s0fs recovery binding: %w", err)
	}
	var binding recoveryBinding
	if err := json.Unmarshal(payload, &binding); err != nil {
		return nil, fmt.Errorf("decode s0fs recovery binding: %w", err)
	}
	if binding.Version != recoveryBindingVersion || binding.VolumeID != volumeID || strings.TrimSpace(binding.Checksum) == "" {
		return nil, fmt.Errorf("%w: invalid s0fs recovery binding", ErrCommittedStateIntegrity)
	}
	want, err := recoveryBindingDigest(&binding)
	if err != nil {
		return nil, err
	}
	if binding.Checksum != want {
		return nil, fmt.Errorf("%w: s0fs recovery binding checksum mismatch", ErrCommittedStateIntegrity)
	}
	binding.Head = cloneCommittedHead(binding.Head)
	return &binding, nil
}

func saveRecoveryBinding(path, volumeID string, head *CommittedHead, stateDigest string) error {
	binding := &recoveryBinding{
		Version: recoveryBindingVersion, VolumeID: volumeID,
		Head: cloneCommittedHead(head), StateDigest: stateDigest,
	}
	checksum, err := recoveryBindingDigest(binding)
	if err != nil {
		return err
	}
	binding.Checksum = checksum
	payload, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	writeErr := error(nil)
	if _, err := file.Write(payload); err != nil {
		writeErr = err
	} else if err := file.Sync(); err != nil {
		writeErr = err
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		_ = os.Remove(tempPath)
		return errors.Join(writeErr, closeErr)
	}
	if err := os.Rename(tempPath, path); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func recoveryBindingDigest(binding *recoveryBinding) (string, error) {
	identity := recoveryBindingChecksum{
		Version: binding.Version, VolumeID: binding.VolumeID,
		Head: cloneCommittedHead(binding.Head), StateDigest: binding.StateDigest,
	}
	if identity.Head != nil {
		identity.Head.UpdatedAt = identity.Head.UpdatedAt.UTC()
	}
	payload, err := json.Marshal(identity)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func recoveryBindingMatches(binding *recoveryBinding, head *CommittedHead, stateDigest string) bool {
	return binding != nil && sameCommittedHeadIdentity(binding.Head, head) &&
		(stateDigest == "" || binding.StateDigest == stateDigest)
}

// quarantineRecoveryEvidence removes an unproven WAL from the active recovery
// path without deleting it. The committed manifest can then be mounted while
// the old WAL, local checkpoint, and bindings remain available for diagnosis
// or an explicit repair workflow.
func quarantineRecoveryEvidence(walPath string) (string, error) {
	evidencePath := walPath + ".untrusted-" + uuid.NewString()
	moves := [][2]string{
		{walPath, evidencePath},
		{walBaseBindingPath(walPath), evidencePath + ".base"},
		{headStatePath(walPath), evidencePath + ".head"},
		{localHeadBindingPath(walPath), evidencePath + ".head.commit"},
	}
	directories := make(map[string]struct{})
	for _, move := range moves {
		if err := os.Rename(move[0], move[1]); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return evidencePath, err
		}
		directories[filepath.Dir(move[0])] = struct{}{}
		directories[filepath.Dir(move[1])] = struct{}{}
	}
	for directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return evidencePath, err
		}
	}
	return evidencePath, nil
}
