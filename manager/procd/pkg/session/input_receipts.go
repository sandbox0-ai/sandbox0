package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const inputReceiptsFile = "input-receipts.jsonl"

// Receipts contain IDs/digests, never input bytes. Append + sync is independent
// of managedSession.mu, so durable acknowledgements do not block PTY output.
// Compaction is amortized across 1024 inputs instead of rewriting state per key.
type inputReceiptJournal struct {
	mu         sync.Mutex
	loaded     bool
	receipts   []InputReceipt
	validBytes int64
}

func (s *FileStore) inputJournal(id string) *inputReceiptJournal {
	s.inputJournalsMu.Lock()
	defer s.inputJournalsMu.Unlock()
	j := s.inputJournals[id]
	if j == nil {
		j = &inputReceiptJournal{}
		s.inputJournals[id] = j
	}
	return j
}

func readInputReceipts(path string) ([]InputReceipt, int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	// An interrupted last append is not acknowledged. Ignore only an incomplete
	// tail; a complete corrupt record fails closed instead of permitting replay.
	valid := bytes.LastIndexByte(data, '\n') + 1
	var receipts []InputReceipt
	for _, line := range bytes.Split(data[:valid], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var receipt InputReceipt
		if err := json.Unmarshal(line, &receipt); err != nil {
			return nil, 0, fmt.Errorf("decode input receipt: %w", err)
		}
		if receipt.InputID == "" || receipt.AttemptID == "" || receipt.Digest == "" || receipt.AcceptedAt.IsZero() || receipt.Sequence < 0 {
			return nil, 0, errors.New("invalid input receipt")
		}
		receipts = append(receipts, receipt)
	}
	return receipts, int64(valid), nil
}

func (s *FileStore) loadInputReceipts(record *Session) error {
	dir, err := s.sessionDir(record.ID)
	if err != nil {
		return err
	}
	receipts, _, err := readInputReceipts(filepath.Join(dir, inputReceiptsFile))
	if err != nil {
		return err
	}
	seen := make(map[InputReceipt]bool)
	merged := append(append([]InputReceipt(nil), record.InputReceipts...), receipts...)
	unique := make([]InputReceipt, 0, len(merged))
	for _, receipt := range merged {
		receipt.AcceptedAt = receipt.AcceptedAt.UTC()
		if !seen[receipt] {
			seen[receipt] = true
			unique = append(unique, receipt)
		}
	}
	// Preserve acceptance order even if a newer full state checkpoint already
	// includes receipts also present in the append journal.
	merged = unique
	sort.SliceStable(merged, func(i, j int) bool {
		// Legacy receipts have no sequence and precede new v2 receipts.
		// Keep their stored order; wall time is not an acceptance-order fence.
		return merged[i].Sequence < merged[j].Sequence
	})
	if len(merged) > maxInputReceipts {
		merged = merged[len(merged)-maxInputReceipts:]
	}
	byID := make(map[string]bool)
	for _, receipt := range merged {
		if byID[receipt.InputID] {
			return errors.New("conflicting persisted input receipts")
		}
		byID[receipt.InputID] = true
	}
	record.InputReceipts = merged
	if len(merged) > 0 {
		latest := merged[len(merged)-1].AcceptedAt
		if record.LastActivityAt.Before(latest) {
			record.LastActivityAt = latest
		}
		if record.UpdatedAt.Before(latest) {
			record.UpdatedAt = latest
		}
	}
	return nil
}

func (s *FileStore) saveInputReceipt(id string, receipt InputReceipt) (err error) {
	dir, err := s.sessionDir(id)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, inputReceiptsFile)
	j := s.inputJournal(id)
	j.mu.Lock()
	defer j.mu.Unlock()
	defer func() {
		// A failed sync/rename can leave a complete but uncertain record on disk.
		// Reload and sync it before a retry can acknowledge that input ID.
		if err != nil {
			j.loaded = false
		}
	}()
	if !j.loaded {
		j.receipts, j.validBytes, err = readInputReceipts(path)
		if err != nil {
			return err
		}
		if j.validBytes > 0 {
			file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
			if err != nil {
				return err
			}
			syncErr := file.Sync()
			closeErr := file.Close()
			if err := errors.Join(syncErr, closeErr, syncReceiptDirectory(dir)); err != nil {
				return err
			}
		}
		j.loaded = true
	}
	recent := j.receipts
	if len(recent) > maxInputReceipts {
		recent = recent[len(recent)-maxInputReceipts:]
	}
	for _, old := range recent {
		if receipt.Sequence > 0 && old.Sequence <= receipt.Sequence-maxInputReceipts {
			continue
		}
		if old.InputID == receipt.InputID {
			if old.Digest != receipt.Digest || old.AttemptID != receipt.AttemptID {
				return ErrInputAlreadyExists
			}
			return nil // This receipt was successfully synced by an earlier input.
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if len(j.receipts) >= 2*maxInputReceipts {
		receipts := append(append([]InputReceipt(nil), j.receipts[len(j.receipts)-maxInputReceipts+1:]...), receipt)
		var compact bytes.Buffer
		encoder := json.NewEncoder(&compact)
		for _, value := range receipts {
			if err := encoder.Encode(value); err != nil {
				return err
			}
		}
		if err := writeFileAtomic(path, compact.Bytes(), 0o600); err != nil {
			return err
		}
		if err := syncReceiptDirectory(dir); err != nil {
			return err
		}
		j.receipts = receipts
		j.validBytes = int64(compact.Len())
		return nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	// Repair an incomplete tail before append; retry persistence errors using
	// the same receipt without sending bytes to the PTY a second time.
	if err := file.Truncate(j.validBytes); err != nil {
		return err
	}
	if _, err := file.Seek(j.validBytes, io.SeekStart); err != nil {
		return err
	}
	if n, err := file.Write(data); err != nil || n != len(data) {
		return errors.Join(err, io.ErrShortWrite)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if j.validBytes == 0 {
		if err := syncReceiptDirectory(dir); err != nil {
			return err
		}
	}
	j.receipts = append(j.receipts, receipt)
	j.validBytes += int64(len(data))
	return nil
}

func syncReceiptDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
