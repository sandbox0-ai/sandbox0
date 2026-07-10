package s0fs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const walSyncCoalesceDelay = time.Millisecond

type wal struct {
	path       string
	mu         sync.Mutex
	syncCond   *sync.Cond
	file       *os.File
	volumeID   string
	encryption *EncryptionConfig
	onSync     func()
	writeGen   uint64
	syncedGen  uint64
	syncing    bool
}

func openWAL(path, volumeID string, encryption *EncryptionConfig, onSync func()) (*wal, []walRecord, error) {
	if path == "" {
		return nil, nil, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, nil, fmt.Errorf("create wal directory: %w", err)
	}

	records, err := readWAL(path, volumeID, encryption)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open wal: %w", err)
	}
	w := &wal{path: path, file: file, volumeID: volumeID, encryption: encryption, onSync: onSync}
	w.syncCond = sync.NewCond(&w.mu)
	return w, records, nil
}

func readWAL(path, volumeID string, encryption *EncryptionConfig) ([]walRecord, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read wal: %w", err)
	}
	defer file.Close()

	var records []walRecord
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if errors.Is(err, io.EOF) && line[len(line)-1] != '\n' {
				return records, nil
			}
			payload := line
			if plaintext, encrypted, err := encryption.decryptBlobIfEncrypted(line, walRecordAAD(volumeID)); encrypted || err != nil {
				if err != nil {
					return nil, fmt.Errorf("decrypt wal record: %w", err)
				}
				payload = plaintext
			}
			var record walRecord
			if decodeErr := json.Unmarshal(payload, &record); decodeErr != nil {
				return nil, fmt.Errorf("decode wal record: %w", decodeErr)
			}
			records = append(records, record)
		}
		if errors.Is(err, io.EOF) {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("scan wal: %w", err)
		}
	}
}

func (w *wal) prepare(record walRecord) ([]byte, error) {
	if w == nil {
		return nil, ErrClosed
	}
	w.mu.Lock()
	closed := w.file == nil
	w.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	return w.encode(record)
}

func (w *wal) encode(record walRecord) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal wal record: %w", err)
	}
	payload, err = w.encryption.encryptBlob(payload, walRecordAAD(w.volumeID))
	if err != nil {
		return nil, fmt.Errorf("encrypt wal record: %w", err)
	}
	payload = append(payload, '\n')
	return payload, nil
}

func (w *wal) appendPrepared(payload []byte) error {
	if w == nil {
		return ErrClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrClosed
	}
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("append wal record: %w", err)
	}
	w.writeGen++
	return nil
}

func (w *wal) beginSyncCurrent() (func() error, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil, ErrClosed
	}
	target := w.writeGen
	if target <= w.syncedGen {
		return nil, nil
	}
	if w.syncing {
		return func() error {
			return w.waitSync(target)
		}, nil
	}
	w.syncing = true
	return func() error {
		return w.runSync(target)
	}, nil
}

func (w *wal) waitSync(target uint64) error {
	for {
		w.mu.Lock()
		if w.file == nil {
			w.mu.Unlock()
			return ErrClosed
		}
		if target <= w.syncedGen {
			w.mu.Unlock()
			return nil
		}
		if !w.syncing {
			w.syncing = true
			w.mu.Unlock()
			return w.runSync(target)
		}
		w.syncCond.Wait()
		w.mu.Unlock()
	}
}

func (w *wal) runSync(target uint64) error {
	if walSyncCoalesceDelay > 0 {
		time.Sleep(walSyncCoalesceDelay)
	}

	w.mu.Lock()
	if w.file == nil {
		w.syncing = false
		w.syncCond.Broadcast()
		w.mu.Unlock()
		return ErrClosed
	}
	if w.writeGen > target {
		target = w.writeGen
	}
	file := w.file
	w.mu.Unlock()

	err := file.Sync()

	w.mu.Lock()
	if err == nil && target > w.syncedGen {
		w.syncedGen = target
	}
	w.syncing = false
	w.syncCond.Broadcast()
	onSync := w.onSync
	w.mu.Unlock()

	if err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	if onSync != nil {
		onSync()
	}
	return nil
}

func (w *wal) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *wal) reset(records ...walRecord) error {
	if w == nil {
		return ErrClosed
	}
	payloads := make([][]byte, 0, len(records))
	for _, record := range records {
		payload, err := w.encode(record)
		if err != nil {
			return err
		}
		payloads = append(payloads, payload)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
	if w.file == nil {
		return ErrClosed
	}
	temp, err := os.CreateTemp(filepath.Dir(w.path), ".wal-reset-*")
	if err != nil {
		return fmt.Errorf("create replacement wal: %w", err)
	}
	tempPath := temp.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()
	for _, payload := range payloads {
		if _, err := temp.Write(payload); err != nil {
			_ = temp.Close()
			return fmt.Errorf("rewrite retained wal record: %w", err)
		}
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync replacement wal: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close replacement wal: %w", err)
	}
	replacement, err := os.OpenFile(tempPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("reopen replacement wal: %w", err)
	}
	if err := os.Rename(tempPath, w.path); err != nil {
		_ = replacement.Close()
		return fmt.Errorf("replace wal: %w", err)
	}
	cleanupTemp = false
	oldFile := w.file
	w.file = replacement
	// Generations must remain monotonic across replacement. A delayed fsync
	// waiter may still carry a target from the previous file generation.
	w.writeGen++
	w.syncedGen = w.writeGen
	if err := oldFile.Close(); err != nil {
		return fmt.Errorf("close replaced wal: %w", err)
	}
	if err := syncWALParent(w.path); err != nil {
		return err
	}
	return nil
}

func syncWALParent(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open wal directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync wal directory: %w", err)
	}
	return nil
}
