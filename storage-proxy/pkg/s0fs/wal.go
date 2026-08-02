package s0fs

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const (
	walSyncCoalesceDelay = time.Millisecond
	// Recovery checks cancellation between 64 KiB scan chunks and before and
	// after decrypting and decoding each bounded record.
	walReplayReadBufferBytes = 64 << 10

	// Direct file APIs currently accept writes up to 100 MiB. Encrypted WAL
	// records wrap the base64-encoded JSON payload in a second JSON envelope, so
	// the on-disk limit leaves enough room for the supported write size while
	// still bounding recovery memory and cancellation latency per record.
	maximumWALRecordBytes int64 = 256 << 20

	walRewriteSuffix = ".rewrite"
)

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
	firstSeq   uint64
	lastSeq    uint64
}

type walReplayStats struct {
	BytesScanned     int64
	RecordsScanned   int
	MaxRecordBytes   int64
	MaxDecodedBytes  int64
	ActiveFirstSeq   uint64
	ActiveLastSeq    uint64
	ActiveValidBytes int64
}

type walReplay struct {
	path       string
	volumeID   string
	encryption *EncryptionConfig
	file       *os.File
	reader     *bufio.Reader
	bytesRead  int64
	peeked     *walRecord
	finished   bool
	stats      walReplayStats
}

// walCheckpoint identifies the byte prefix represented by a materialization
// snapshot. It remains valid while later mutations append to the same WAL.
type walCheckpoint struct {
	throughSeq uint64
	offset     int64
	applied    bool
}

// openWALReplay builds a bounded iterator over the legacy-compatible active
// WAL. It does not create or modify the WAL file.
func openWALReplay(path, volumeID string, encryption *EncryptionConfig) (*walReplay, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	if _, err := os.Stat(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	return &walReplay{
		path:       path,
		volumeID:   volumeID,
		encryption: encryption,
	}, nil
}

func (r *walReplay) Peek(ctx context.Context) (walRecord, bool, error) {
	if r == nil {
		return walRecord{}, false, nil
	}
	if r.peeked != nil {
		return *r.peeked, true, nil
	}
	record, ok, err := r.Next(ctx)
	if err != nil || !ok {
		return walRecord{}, ok, err
	}
	r.peeked = &record
	return record, true, nil
}

// Next decrypts and decodes at most one complete record. The encoded record is
// no longer retained by the iterator after the call returns.
func (r *walReplay) Next(ctx context.Context) (walRecord, bool, error) {
	if r == nil || r.finished {
		return walRecord{}, false, nil
	}
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return walRecord{}, false, err
	}
	if r.peeked != nil {
		record := *r.peeked
		r.peeked = nil
		return record, true, nil
	}
	if r.file == nil {
		file, err := os.Open(r.path)
		if errors.Is(err, os.ErrNotExist) {
			r.finished = true
			return walRecord{}, false, nil
		}
		if err != nil {
			return walRecord{}, false, fmt.Errorf("open wal replay source: %w", err)
		}
		r.file = file
		r.reader = bufio.NewReaderSize(file, walReplayReadBufferBytes)
	}

	line, complete, bytesRead, err := readBoundedWALLine(ctx, r.reader)
	r.bytesRead += bytesRead
	r.stats.BytesScanned += bytesRead
	if err != nil {
		return walRecord{}, false, err
	}
	if !complete {
		if err := r.finish(); err != nil {
			return walRecord{}, false, err
		}
		return walRecord{}, false, nil
	}

	record, err := decodeWALRecord(ctx, line, r.volumeID, r.encryption)
	if err != nil {
		return walRecord{}, false, err
	}
	r.stats.RecordsScanned++
	if size := int64(len(line)); size > r.stats.MaxRecordBytes {
		r.stats.MaxRecordBytes = size
	}
	if size := estimatedDecodedWALRecordBytes(record); size > r.stats.MaxDecodedBytes {
		r.stats.MaxDecodedBytes = size
	}
	if r.stats.ActiveFirstSeq == 0 {
		r.stats.ActiveFirstSeq = record.Seq
	}
	r.stats.ActiveLastSeq = record.Seq
	r.stats.ActiveValidBytes = r.bytesRead
	return record, true, nil
}

func (r *walReplay) finish() error {
	if r == nil || r.finished {
		return nil
	}
	r.finished = true
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	r.reader = nil
	return err
}

func (r *walReplay) Stats() walReplayStats {
	if r == nil {
		return walReplayStats{}
	}
	return r.stats
}

func (r *walReplay) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	r.reader = nil
	return err
}

func readBoundedWALLine(ctx context.Context, reader *bufio.Reader) ([]byte, bool, int64, error) {
	return readBoundedWALLineLimit(ctx, reader, maximumWALRecordBytes)
}

func readBoundedWALLineLimit(ctx context.Context, reader *bufio.Reader, maximumBytes int64) ([]byte, bool, int64, error) {
	ctx = nonNilContext(ctx)
	if reader == nil {
		return nil, false, 0, io.EOF
	}
	if maximumBytes <= 0 {
		return nil, false, 0, fmt.Errorf("%w: wal record limit must be positive", ErrInvalidInput)
	}
	var line []byte
	var bytesRead int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, bytesRead, err
		}
		fragment, err := reader.ReadSlice('\n')
		bytesRead += int64(len(fragment))
		if int64(len(line))+int64(len(fragment)) > maximumBytes {
			return nil, false, bytesRead, fmt.Errorf("%w: wal record exceeds %d bytes", ErrInvalidInput, maximumBytes)
		}
		if err == nil && len(line) == 0 {
			return fragment, true, bytesRead, nil
		}
		line = append(line, fragment...)
		switch {
		case err == nil:
			return line, true, bytesRead, nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			// A final record is durable only after its newline is appended. Preserve
			// the existing truncated-tail behavior and let openWAL remove the tail
			// before accepting new writes.
			return line, false, bytesRead, nil
		default:
			return nil, false, bytesRead, fmt.Errorf("scan wal: %w", err)
		}
	}
}

func decodeWALRecord(ctx context.Context, line []byte, volumeID string, encryption *EncryptionConfig) (walRecord, error) {
	if err := nonNilContext(ctx).Err(); err != nil {
		return walRecord{}, err
	}
	payload := line
	if plaintext, encrypted, err := encryption.decryptBlobIfEncrypted(line, walRecordAAD(volumeID)); encrypted || err != nil {
		if err != nil {
			return walRecord{}, fmt.Errorf("decrypt wal record: %w", err)
		}
		payload = plaintext
	}
	if err := nonNilContext(ctx).Err(); err != nil {
		return walRecord{}, err
	}
	var record walRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return walRecord{}, fmt.Errorf("decode wal record: %w", err)
	}
	if err := nonNilContext(ctx).Err(); err != nil {
		return walRecord{}, err
	}
	return record, nil
}

func estimatedDecodedWALRecordBytes(record walRecord) int64 {
	// The fixed allowance covers the decoded struct, string headers, and slice
	// header; dynamic payloads are counted exactly.
	const fixedRecordBytes = 256
	return fixedRecordBytes + int64(len(record.Op)+len(record.Name)+len(record.NewName)+len(record.Target)+len(record.Data))
}

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func openWAL(path, volumeID string, encryption *EncryptionConfig, onSync func(), replay walReplayStats) (*wal, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: wal path is required", ErrInvalidInput)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat wal: %w", err)
	}
	if replay.ActiveValidBytes < 0 || replay.ActiveValidBytes > info.Size() {
		_ = file.Close()
		return nil, fmt.Errorf("%w: invalid active wal length %d for file size %d", ErrInvalidInput, replay.ActiveValidBytes, info.Size())
	}
	if info.Size() != replay.ActiveValidBytes {
		if err := file.Truncate(replay.ActiveValidBytes); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("truncate partial wal tail: %w", err)
		}
	}
	if (replay.ActiveFirstSeq == 0) != (replay.ActiveLastSeq == 0) || replay.ActiveLastSeq < replay.ActiveFirstSeq {
		_ = file.Close()
		return nil, fmt.Errorf("%w: invalid active wal sequence range", ErrInvalidInput)
	}
	if err := removeStaleWALRewrite(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	w := &wal{
		path:       path,
		file:       file,
		volumeID:   volumeID,
		encryption: encryption,
		onSync:     onSync,
		firstSeq:   replay.ActiveFirstSeq,
		lastSeq:    replay.ActiveLastSeq,
	}
	w.syncCond = sync.NewCond(&w.mu)
	return w, nil
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
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal wal record: %w", err)
	}
	if int64(len(payload)) > maximumWALRecordBytes {
		return nil, fmt.Errorf("%w: wal record exceeds %d bytes", ErrInvalidInput, maximumWALRecordBytes)
	}
	payload, err = w.encryption.encryptBlob(payload, walRecordAAD(w.volumeID))
	if err != nil {
		return nil, fmt.Errorf("encrypt wal record: %w", err)
	}
	payload = append(payload, '\n')
	if int64(len(payload)) > maximumWALRecordBytes {
		return nil, fmt.Errorf("%w: encoded wal record exceeds %d bytes", ErrInvalidInput, maximumWALRecordBytes)
	}
	return payload, nil
}

func (w *wal) appendPrepared(record walRecord, payload []byte) error {
	if w == nil {
		return ErrClosed
	}
	if int64(len(payload)) > maximumWALRecordBytes {
		return fmt.Errorf("%w: encoded wal record exceeds %d bytes", ErrInvalidInput, maximumWALRecordBytes)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return ErrClosed
	}
	if w.lastSeq != 0 && record.Seq <= w.lastSeq {
		return fmt.Errorf("%w: wal seq %d does not advance beyond %d", ErrInvalidInput, record.Seq, w.lastSeq)
	}
	if _, err := w.file.Write(payload); err != nil {
		return fmt.Errorf("append wal record: %w", err)
	}
	if w.firstSeq == 0 {
		w.firstSeq = record.Seq
	}
	w.lastSeq = record.Seq
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

// checkpoint captures the current append boundary without changing the WAL
// format or blocking mutations while the snapshot is committed remotely.
func (w *wal) checkpoint(throughSeq uint64) (*walCheckpoint, error) {
	if w == nil {
		return nil, ErrClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
	if w.file == nil {
		return nil, ErrClosed
	}
	if w.lastSeq > throughSeq {
		return nil, fmt.Errorf("%w: active wal ends at %d beyond checkpoint %d", ErrInvalidInput, w.lastSeq, throughSeq)
	}
	info, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat wal checkpoint: %w", err)
	}
	return &walCheckpoint{throughSeq: throughSeq, offset: info.Size()}, nil
}

// discardThrough atomically rewrites the WAL to the suffix appended after the
// checkpoint. The on-disk path and record format remain readable by older CTLD
// versions throughout rolling upgrades and rollback.
func (w *wal) discardThrough(checkpoint *walCheckpoint) error {
	if w == nil {
		return ErrClosed
	}
	if checkpoint == nil {
		return fmt.Errorf("%w: wal checkpoint is required", ErrInvalidInput)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
	if w.file == nil {
		return ErrClosed
	}
	if checkpoint.applied {
		return nil
	}
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("stat wal before checkpoint rewrite: %w", err)
	}
	if checkpoint.offset < 0 || checkpoint.offset > info.Size() {
		return fmt.Errorf("%w: wal checkpoint offset %d exceeds file size %d", ErrInvalidInput, checkpoint.offset, info.Size())
	}
	if checkpoint.offset == info.Size() {
		if err := w.file.Truncate(0); err != nil {
			return fmt.Errorf("truncate committed wal checkpoint: %w", err)
		}
		checkpoint.applied = true
		w.firstSeq = 0
		w.lastSeq = 0
		w.writeGen = 0
		w.syncedGen = 0
		return nil
	}

	rewritePath := walRewritePath(w.path)
	rewrite, err := os.OpenFile(rewritePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("create wal checkpoint rewrite: %w", err)
	}
	cleanupRewrite := true
	defer func() {
		if cleanupRewrite {
			_ = os.Remove(rewritePath)
		}
	}()
	suffixBytes := info.Size() - checkpoint.offset
	if _, err := io.CopyN(rewrite, io.NewSectionReader(w.file, checkpoint.offset, suffixBytes), suffixBytes); err != nil {
		_ = rewrite.Close()
		return fmt.Errorf("copy wal checkpoint suffix: %w", err)
	}
	if err := rewrite.Sync(); err != nil {
		_ = rewrite.Close()
		return fmt.Errorf("sync wal checkpoint suffix: %w", err)
	}
	if err := rewrite.Close(); err != nil {
		return fmt.Errorf("close wal checkpoint suffix: %w", err)
	}
	if err := os.Rename(rewritePath, w.path); err != nil {
		return fmt.Errorf("replace wal with checkpoint suffix: %w", err)
	}
	cleanupRewrite = false
	checkpoint.applied = true

	reopened, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		_ = w.file.Close()
		w.file = nil
		return fmt.Errorf("reopen wal checkpoint suffix: %w", err)
	}
	oldFile := w.file
	w.file = reopened
	if err := oldFile.Close(); err != nil {
		return fmt.Errorf("close replaced wal: %w", err)
	}
	if w.lastSeq > checkpoint.throughSeq {
		w.firstSeq = checkpoint.throughSeq + 1
	} else {
		w.firstSeq = 0
		w.lastSeq = 0
	}
	w.writeGen = 0
	w.syncedGen = 0
	if err := syncDirectory(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("sync wal checkpoint rewrite: %w", err)
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

func (w *wal) reset() error {
	if w == nil {
		return ErrClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.syncing {
		w.syncCond.Wait()
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reset wal: %w", err)
	}
	w.file = file
	w.writeGen = 0
	w.syncedGen = 0
	w.firstSeq = 0
	w.lastSeq = 0
	return removeStaleWALRewrite(w.path)
}

func walRewritePath(path string) string {
	return path + walRewriteSuffix
}

func removeStaleWALRewrite(path string) error {
	if err := os.Remove(walRewritePath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale wal checkpoint rewrite: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil && !errors.Is(syncErr, syscall.ENOSYS) && !errors.Is(syncErr, syscall.EINVAL) {
		return errors.Join(syncErr, closeErr)
	}
	return closeErr
}
