// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package soakstate provides a durable, hash-chained checkpoint log for
// opt-in endurance gates. Elapsed time is supplied by the caller so host
// downtime never silently counts toward an active-duration acceptance gate.
package soakstate

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	LogVersion        = 1
	defaultBootIDPath = "/proc/sys/kernel/random/boot_id"
)

type Mode string

const (
	ModeCreate Mode = "create"
	ModeResume Mode = "resume"
	ModeAuto   Mode = "auto"
)

func ParseMode(raw string) (Mode, error) {
	mode := Mode(strings.TrimSpace(raw))
	switch mode {
	case ModeCreate, ModeResume, ModeAuto:
		return mode, nil
	default:
		return "", fmt.Errorf("soak state mode must be create, resume, or auto")
	}
}

type OpenOptions struct {
	Path       string
	Mode       Mode
	Config     any
	Initial    any
	BootIDPath string
}

type ResumeInfo struct {
	Resumed               bool      `json:"resumed"`
	PreviousAt            time.Time `json:"previous_at"`
	OpenedAt              time.Time `json:"opened_at"`
	PreviousBootID        string    `json:"previous_boot_id"`
	CurrentBootID         string    `json:"current_boot_id"`
	BootChanged           bool      `json:"boot_changed"`
	WallGapNS             int64     `json:"wall_gap_ns"`
	TruncatedPartialBytes int64     `json:"truncated_partial_bytes"`
}

type Log struct {
	file             *os.File
	runID            string
	configSHA256     string
	executableSHA256 string
	bootID           string
	sequence         uint64
	previousSHA256   string
	lastAt           time.Time
	lastBootID       string
	activeElapsed    time.Duration
	checkpoint       json.RawMessage
	resume           ResumeInfo
}

type record struct {
	Version          int             `json:"version"`
	Sequence         uint64          `json:"sequence"`
	RunID            string          `json:"run_id"`
	ConfigSHA256     string          `json:"config_sha256"`
	ExecutableSHA256 string          `json:"executable_sha256"`
	BootID           string          `json:"boot_id"`
	Type             string          `json:"type"`
	At               time.Time       `json:"at"`
	ActiveElapsedNS  int64           `json:"active_elapsed_ns"`
	PreviousSHA256   string          `json:"previous_sha256,omitempty"`
	Data             json.RawMessage `json:"data"`
	Checkpoint       json.RawMessage `json:"checkpoint"`
	EventSHA256      string          `json:"event_sha256,omitempty"`
}

func Open(options OpenOptions) (*Log, error) {
	path := filepath.Clean(strings.TrimSpace(options.Path))
	if !filepath.IsAbs(path) || path == string(filepath.Separator) {
		return nil, fmt.Errorf("soak evidence path must be a non-root absolute path")
	}
	mode, err := ParseMode(string(options.Mode))
	if err != nil {
		return nil, err
	}
	config, err := json.Marshal(options.Config)
	if err != nil {
		return nil, fmt.Errorf("marshal soak configuration: %w", err)
	}
	initial, err := json.Marshal(options.Initial)
	if err != nil {
		return nil, fmt.Errorf("marshal initial soak checkpoint: %w", err)
	}
	configDigest := sha256.Sum256(config)
	executableDigest, err := executableSHA256()
	if err != nil {
		return nil, err
	}
	bootIDPath := strings.TrimSpace(options.BootIDPath)
	if bootIDPath == "" {
		bootIDPath = defaultBootIDPath
	}
	bootID, err := readCanonicalFile(bootIDPath, "boot ID")
	if err != nil {
		return nil, err
	}

	exists, err := regularFileExists(path)
	if err != nil {
		return nil, err
	}
	create := mode == ModeCreate || (mode == ModeAuto && !exists)
	if mode == ModeCreate && exists {
		return nil, fmt.Errorf("create exclusive soak evidence file: %w", os.ErrExist)
	}
	if mode == ModeResume && !exists {
		return nil, fmt.Errorf("resume soak evidence file: %w", os.ErrNotExist)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create soak evidence directory: %w", err)
	}
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open soak evidence: %w", err)
	}
	closeOnError := func(openErr error) (*Log, error) {
		_ = file.Close()
		return nil, openErr
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return closeOnError(fmt.Errorf("lock soak evidence: %w", err))
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return closeOnError(fmt.Errorf("secure soak evidence: %w", err))
	}

	log := &Log{
		file: file, configSHA256: hex.EncodeToString(configDigest[:]),
		executableSHA256: executableDigest, bootID: bootID,
	}
	if create {
		log.runID, err = randomRunID()
		if err != nil {
			return closeOnError(err)
		}
		if err := log.commitRaw("configuration", 0, config, initial); err != nil {
			return closeOnError(err)
		}
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return closeOnError(fmt.Errorf("sync soak evidence directory: %w", err))
		}
		return log, nil
	}
	if err := log.load(); err != nil {
		return closeOnError(err)
	}
	if log.configSHA256 != hex.EncodeToString(configDigest[:]) {
		return closeOnError(fmt.Errorf("soak configuration digest changed across resume"))
	}
	if log.executableSHA256 != executableDigest {
		return closeOnError(fmt.Errorf("soak executable digest changed across resume"))
	}
	openedAt := time.Now().UTC()
	log.resume = ResumeInfo{
		Resumed: true, PreviousAt: log.lastAt, PreviousBootID: log.lastBootID,
		OpenedAt: openedAt, CurrentBootID: bootID, BootChanged: log.lastBootID != bootID,
		WallGapNS:             openedAt.Sub(log.lastAt).Nanoseconds(),
		TruncatedPartialBytes: log.resume.TruncatedPartialBytes,
	}
	log.bootID = bootID
	return log, nil
}

func (l *Log) RunID() string { return l.runID }

func (l *Log) ConfigSHA256() string { return l.configSHA256 }

func (l *Log) ExecutableSHA256() string { return l.executableSHA256 }

func (l *Log) ActiveElapsed() time.Duration { return l.activeElapsed }

func (l *Log) ResumeInfo() ResumeInfo { return l.resume }

func (l *Log) DecodeCheckpoint(destination any) error {
	if l == nil || len(l.checkpoint) == 0 {
		return fmt.Errorf("soak checkpoint is unavailable")
	}
	if err := json.Unmarshal(l.checkpoint, destination); err != nil {
		return fmt.Errorf("decode soak checkpoint: %w", err)
	}
	return nil
}

func (l *Log) Commit(eventType string, activeElapsed time.Duration, data, checkpoint any) error {
	dataPayload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal soak event data: %w", err)
	}
	checkpointPayload, err := json.Marshal(checkpoint)
	if err != nil {
		return fmt.Errorf("marshal soak checkpoint: %w", err)
	}
	return l.commitRaw(eventType, activeElapsed, dataPayload, checkpointPayload)
}

func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}

func (l *Log) commitRaw(eventType string, activeElapsed time.Duration, data, checkpoint json.RawMessage) error {
	if l == nil || l.file == nil {
		return fmt.Errorf("soak evidence is closed")
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" || len(eventType) > 128 || strings.ContainsAny(eventType, "\r\n\t ") {
		return fmt.Errorf("soak event type must be a canonical token of at most 128 bytes")
	}
	if activeElapsed < l.activeElapsed {
		return fmt.Errorf("soak active elapsed time cannot decrease")
	}
	now := time.Now().UTC()
	rec := record{
		Version: LogVersion, Sequence: l.sequence + 1, RunID: l.runID,
		ConfigSHA256: l.configSHA256, ExecutableSHA256: l.executableSHA256,
		BootID: l.bootID, Type: eventType, At: now,
		ActiveElapsedNS: activeElapsed.Nanoseconds(), PreviousSHA256: l.previousSHA256,
		Data: append(json.RawMessage(nil), data...), Checkpoint: append(json.RawMessage(nil), checkpoint...),
	}
	digest, payload, err := encodeRecord(rec)
	if err != nil {
		return err
	}
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek soak evidence: %w", err)
	}
	if _, err := l.file.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("append soak evidence: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync soak evidence: %w", err)
	}
	l.sequence = rec.Sequence
	l.previousSHA256 = digest
	l.lastAt = now
	l.lastBootID = l.bootID
	l.activeElapsed = activeElapsed
	l.checkpoint = append(l.checkpoint[:0], checkpoint...)
	return nil
}

func (l *Log) load() error {
	content, err := io.ReadAll(l.file)
	if err != nil {
		return fmt.Errorf("read soak evidence: %w", err)
	}
	if len(content) == 0 {
		return fmt.Errorf("resume soak evidence is empty")
	}
	if content[len(content)-1] != '\n' {
		lastNewline := bytes.LastIndexByte(content, '\n')
		if lastNewline < 0 {
			return fmt.Errorf("resume soak evidence has no complete event")
		}
		l.resume.TruncatedPartialBytes = int64(len(content) - lastNewline - 1)
		content = content[:lastNewline+1]
		if err := l.file.Truncate(int64(len(content))); err != nil {
			return fmt.Errorf("truncate partial soak event: %w", err)
		}
		if err := l.file.Sync(); err != nil {
			return fmt.Errorf("sync truncated soak evidence: %w", err)
		}
	}
	lines := bytes.Split(content[:len(content)-1], []byte{'\n'})
	for index, line := range lines {
		if len(line) == 0 {
			return fmt.Errorf("soak evidence contains an empty event at line %d", index+1)
		}
		var rec record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("decode soak evidence line %d: %w", index+1, err)
		}
		if err := validateRecord(rec, l, uint64(index+1)); err != nil {
			return fmt.Errorf("validate soak evidence line %d: %w", index+1, err)
		}
		digest := rec.EventSHA256
		l.sequence = rec.Sequence
		l.runID = rec.RunID
		l.configSHA256 = rec.ConfigSHA256
		l.executableSHA256 = rec.ExecutableSHA256
		l.previousSHA256 = digest
		l.lastAt = rec.At
		l.lastBootID = rec.BootID
		l.activeElapsed = time.Duration(rec.ActiveElapsedNS)
		l.checkpoint = append(l.checkpoint[:0], rec.Checkpoint...)
	}
	if lines[0] == nil || l.sequence == 0 {
		return fmt.Errorf("resume soak evidence has no events")
	}
	return nil
}

func validateRecord(rec record, log *Log, sequence uint64) error {
	if rec.Version != LogVersion || rec.Sequence != sequence {
		return fmt.Errorf("unexpected version/sequence %d/%d", rec.Version, rec.Sequence)
	}
	if rec.RunID == "" || rec.ConfigSHA256 == "" || rec.ExecutableSHA256 == "" ||
		rec.BootID == "" || rec.Type == "" || rec.At.IsZero() || len(rec.Checkpoint) == 0 {
		return fmt.Errorf("required event identity is absent")
	}
	if sequence == 1 {
		if rec.Type != "configuration" || rec.PreviousSHA256 != "" || rec.ActiveElapsedNS != 0 {
			return fmt.Errorf("first event is not the initial configuration")
		}
	} else {
		if rec.RunID != log.runID || rec.ConfigSHA256 != log.configSHA256 ||
			rec.ExecutableSHA256 != log.executableSHA256 || rec.PreviousSHA256 != log.previousSHA256 {
			return fmt.Errorf("event identity or hash chain changed")
		}
		if rec.ActiveElapsedNS < log.activeElapsed.Nanoseconds() {
			return fmt.Errorf("active elapsed time moved backward")
		}
	}
	want, _, err := encodeRecord(recordWithoutDigest(rec))
	if err != nil {
		return err
	}
	if rec.EventSHA256 != want {
		return fmt.Errorf("event digest mismatch")
	}
	return nil
}

func recordWithoutDigest(rec record) record {
	rec.EventSHA256 = ""
	return rec
}

func encodeRecord(rec record) (string, []byte, error) {
	rec.EventSHA256 = ""
	canonical, err := json.Marshal(rec)
	if err != nil {
		return "", nil, fmt.Errorf("marshal soak event for digest: %w", err)
	}
	digest := sha256.Sum256(canonical)
	rec.EventSHA256 = hex.EncodeToString(digest[:])
	payload, err := json.Marshal(rec)
	if err != nil {
		return "", nil, fmt.Errorf("marshal soak event: %w", err)
	}
	return rec.EventSHA256, payload, nil
}

func executableSHA256() (string, error) {
	path, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve soak executable: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open soak executable: %w", err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", fmt.Errorf("hash soak executable: %w", err)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func randomRunID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate soak run ID: %w", err)
	}
	return "soak-" + hex.EncodeToString(value), nil
}

func readCanonicalFile(path, name string) (string, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", name, err)
	}
	value := strings.TrimSpace(string(payload))
	if value == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\t ") {
		return "", fmt.Errorf("%s is not canonical", name)
	}
	return value, nil
}

func regularFileExists(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect soak evidence: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("soak evidence must be a regular file")
	}
	return true, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
