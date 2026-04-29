package s0fs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type wal struct {
	path       string
	file       *os.File
	volumeID   string
	encryption *EncryptionConfig
	onSync     func()
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
	return &wal{path: path, file: file, volumeID: volumeID, encryption: encryption, onSync: onSync}, records, nil
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

func (w *wal) append(record walRecord) error {
	if w == nil || w.file == nil {
		return ErrClosed
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal wal record: %w", err)
	}
	payload, err = w.encryption.encryptBlob(payload, walRecordAAD(w.volumeID))
	if err != nil {
		return fmt.Errorf("encrypt wal record: %w", err)
	}
	payload = append(payload, '\n')
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("append wal record: %w", err)
	}
	return nil
}

func (w *wal) sync() error {
	if w == nil || w.file == nil {
		return ErrClosed
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}
	if w.onSync != nil {
		w.onSync()
	}
	return nil
}

func (w *wal) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *wal) reset() error {
	if w == nil {
		return ErrClosed
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("reset wal: %w", err)
	}
	w.file = file
	return nil
}
