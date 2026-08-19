package rootfsblock

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
)

const (
	branchHeaderFixedBytes = 48
	branchRecordBytes      = 64 + LogicalBlockSize
	branchHeaderMaxBytes   = 16 << 10
	BranchFormatVersion    = 1
)

var (
	branchHeaderMagic = [8]byte{'S', '0', 'B', 'C', 'O', 'W', '0', '1'}
	branchRecordMagic = [8]byte{'S', '0', 'B', 'L', 'K', 'W', '0', '1'}
)

type BranchIdentity struct {
	Version          int    `json:"version"`
	RootFSID         string `json:"rootfs_id"`
	GenerationID     string `json:"generation_id"`
	WriterEpoch      int64  `json:"writer_epoch"`
	LogicalSizeBytes int64  `json:"logical_size_bytes"`
	BaseRootDigest   string `json:"base_root_digest"`
}

func (i BranchIdentity) Validate() error {
	if i.Version != BranchFormatVersion {
		return fmt.Errorf("unsupported branch format version %d", i.Version)
	}
	if strings.TrimSpace(i.RootFSID) == "" || strings.TrimSpace(i.GenerationID) == "" || i.WriterEpoch <= 0 {
		return fmt.Errorf("rootfs_id, generation_id, and positive writer_epoch are required")
	}
	if i.LogicalSizeBytes <= 0 || i.LogicalSizeBytes%LogicalBlockSize != 0 {
		return fmt.Errorf("logical size must be a positive multiple of %d", LogicalBlockSize)
	}
	if _, err := digestBytes(i.BaseRootDigest); err != nil {
		return fmt.Errorf("base_root_digest: %w", err)
	}
	return nil
}

type branchRecord struct {
	sequence uint64
	block    uint64
	offset   int64
}

const branchRecordChunkCapacity = 4096

// branchRecordLog keeps the append index in immutable chunks. Checkpoint can
// rotate the current chunk while holding the branch lock and retain the old
// chunks without copying every record during a guest filesystem freeze.
type branchRecordLog struct {
	chunks []*branchRecordChunk
	count  int
}

type branchRecordChunk struct {
	records []branchRecord
}

func (l *branchRecordLog) append(record branchRecord) {
	if len(l.chunks) == 0 || len(l.chunks[len(l.chunks)-1].records) == branchRecordChunkCapacity {
		l.chunks = append(l.chunks, &branchRecordChunk{records: make([]branchRecord, 0, branchRecordChunkCapacity)})
	}
	last := l.chunks[len(l.chunks)-1]
	last.records = append(last.records, record)
	l.count++
}

func (l *branchRecordLog) snapshot() []*branchRecordChunk {
	if l.count == 0 {
		return nil
	}
	result := make([]*branchRecordChunk, 0, len(l.chunks))
	for _, chunk := range l.chunks {
		if len(chunk.records) > 0 {
			result = append(result, chunk)
		}
	}
	last := l.chunks[len(l.chunks)-1]
	if len(last.records) > 0 && len(last.records) < branchRecordChunkCapacity {
		// Future appends must not mutate a chunk retained by the checkpoint.
		l.chunks = append(l.chunks, &branchRecordChunk{records: make([]branchRecord, 0, branchRecordChunkCapacity)})
	}
	return result
}

func (l *branchRecordLog) forEach(visit func(branchRecord) error) error {
	for _, chunk := range l.chunks {
		for _, record := range chunk.records {
			if err := visit(record); err != nil {
				return err
			}
		}
	}
	return nil
}

// BlockUpdate is one final block value in a flushed local branch.
type BlockUpdate struct {
	Sequence uint64
	Block    uint64
	Data     []byte
}

// BranchOptions bounds one writer's unpublished local dirty tail. The limit
// counts logical 4 KiB record payloads, including repeated overwrites. Zero
// leaves the branch unbounded and is intended only for compatibility tests;
// production session owners must set an explicit positive limit.
type BranchOptions struct {
	MaxDirtyTailBytes int64
}

// DirtyTailUsage describes the exact local unpublished branch occupancy.
// JournalBytes includes the record framing but excludes the bounded identity
// header. DirtyBytes is the admission value compared with MaxBytes.
type DirtyTailUsage struct {
	DirtyBytes   int64
	JournalBytes int64
	MaxBytes     int64
}

// DirtyTailCapacityError rejects a complete block request before appending any
// of its records. It unwraps to ENOSPC so the Linux NBD client and guest
// filesystem receive a stable capacity error instead of a generic EIO.
type DirtyTailCapacityError struct {
	UsedBytes      int64
	RequestedBytes int64
	LimitBytes     int64
}

func (e *DirtyTailCapacityError) Error() string {
	return fmt.Sprintf(
		"rootfs dirty tail capacity exhausted: used %d bytes, request %d bytes, limit %d bytes",
		e.UsedBytes, e.RequestedBytes, e.LimitBytes,
	)
}

func (*DirtyTailCapacityError) Unwrap() error { return syscall.ENOSPC }

// Branch is a local append-only writable COW layer over one immutable
// generation. A successful Flush makes every preceding block record durable
// on the current node. It does not claim region durability.
type Branch struct {
	mu       sync.RWMutex
	file     *os.File
	base     io.ReaderAt
	identity BranchIdentity
	header   int64
	end      int64
	sequence uint64
	durable  uint64
	blocks   map[uint64]branchRecord
	records  branchRecordLog
	maxDirty int64
	closed   bool
}

func OpenBranch(path string, identity BranchIdentity, base io.ReaderAt) (*Branch, error) {
	return OpenBranchWithOptions(path, identity, base, BranchOptions{})
}

// OpenBranchWithOptions opens one branch and applies an immutable admission
// limit for the lifetime of this handle. Lowering the limit below recovered
// usage is allowed so retirement can still read and publish the branch, but
// every subsequent write is rejected until it is retired.
func OpenBranchWithOptions(path string, identity BranchIdentity, base io.ReaderAt, options BranchOptions) (*Branch, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if base == nil {
		return nil, fmt.Errorf("base generation reader is required")
	}
	if options.MaxDirtyTailBytes < 0 {
		return nil, fmt.Errorf("maximum dirty tail bytes must be non-negative")
	}
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return nil, fmt.Errorf("branch path must be a non-root absolute path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create branch directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscallNoFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open branch journal: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("protect branch journal: %w", err)
	}
	branch := &Branch{
		file: file, base: base, identity: identity, blocks: make(map[uint64]branchRecord),
		maxDirty: options.MaxDirtyTailBytes,
	}
	if err := branch.open(); err != nil {
		file.Close()
		return nil, err
	}
	return branch, nil
}

func (b *Branch) Identity() BranchIdentity { return b.identity }
func (b *Branch) Size() int64              { return b.identity.LogicalSizeBytes }

func (b *Branch) ReadAt(target []byte, offset int64) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	if offset < 0 {
		return 0, fmt.Errorf("read offset must be non-negative")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return 0, os.ErrClosed
	}
	return b.readAtLocked(target, offset)
}

func (b *Branch) WriteAt(payload []byte, offset int64) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	if offset < 0 || offset >= b.Size() || int64(len(payload)) > b.Size()-offset {
		return 0, fmt.Errorf("write range exceeds the logical device")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, os.ErrClosed
	}
	records := mappingBlocksForRange(offset, int64(len(payload)))
	if err := b.admitRecordsLocked(records); err != nil {
		return 0, err
	}
	return b.writeAtLocked(payload, offset)
}

func (b *Branch) writeAtLocked(payload []byte, offset int64) (int, error) {
	written := 0
	for written < len(payload) {
		absolute := offset + int64(written)
		block := uint64(absolute / LogicalBlockSize)
		inBlock := int(absolute % LogicalBlockSize)
		length := min(len(payload)-written, LogicalBlockSize-inBlock)
		data := make([]byte, LogicalBlockSize)
		if inBlock != 0 || length != LogicalBlockSize {
			if _, err := b.readBlockLocked(data, block); err != nil {
				return written, err
			}
		}
		copy(data[inBlock:inBlock+length], payload[written:written+length])
		if err := b.appendBlockLocked(block, data); err != nil {
			return written, err
		}
		written += length
	}
	return written, nil
}

// WriteZeroes records explicit zero data and therefore masks any immutable
// base blocks in the requested range.
func (b *Branch) WriteZeroes(offset, length int64) error {
	if offset < 0 || length < 0 || offset > b.Size() || length > b.Size()-offset {
		return fmt.Errorf("zero range must be within the logical device")
	}
	if length == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return os.ErrClosed
	}
	if err := b.admitRecordsLocked(mappingBlocksForRange(offset, length)); err != nil {
		return err
	}
	zero := make([]byte, LogicalBlockSize)
	for current := int64(0); current < length; {
		absolute := offset + current
		chunk := min(length-current, int64(LogicalBlockSize)-absolute%LogicalBlockSize)
		if _, err := b.writeAtLocked(zero[:chunk], absolute); err != nil {
			return err
		}
		current += chunk
	}
	return nil
}

// DirtyTailUsage returns a race-free snapshot suitable for metrics and
// admission diagnostics. Repeated writes count separately because every
// record consumes durable local journal capacity until publication.
func (b *Branch) DirtyTailUsage() DirtyTailUsage {
	b.mu.RLock()
	defer b.mu.RUnlock()
	records := int64(b.records.count)
	return DirtyTailUsage{
		DirtyBytes: records * LogicalBlockSize, JournalBytes: records * branchRecordBytes,
		MaxBytes: b.maxDirty,
	}
}

func (b *Branch) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return os.ErrClosed
	}
	return b.flushLocked()
}

func (b *Branch) flushLocked() error {
	if b.durable == b.sequence {
		return nil
	}
	if err := b.file.Sync(); err != nil {
		return fmt.Errorf("flush branch journal: %w", err)
	}
	b.durable = b.sequence
	return nil
}

func (b *Branch) DirtyBlocks() []uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	blocks := make([]uint64, 0, len(b.blocks))
	for block := range b.blocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	return blocks
}

// DurableUpdates returns the final value of every dirty block. Callers must
// Flush first so a seal can never publish a branch state that was not durable
// on the current node.
func (b *Branch) DurableUpdates() ([]BlockUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, os.ErrClosed
	}
	if b.durable != b.sequence {
		return nil, fmt.Errorf("branch has unflushed writes")
	}
	blocks := make([]uint64, 0, len(b.blocks))
	for block := range b.blocks {
		blocks = append(blocks, block)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	updates := make([]BlockUpdate, 0, len(blocks))
	for _, block := range blocks {
		payload := make([]byte, LogicalBlockSize)
		record := b.blocks[block]
		n, err := b.file.ReadAt(payload, record.offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read durable block %d: %w", block, err)
		}
		if n != len(payload) {
			return nil, fmt.Errorf("read durable block %d: %w", block, io.ErrUnexpectedEOF)
		}
		updates = append(updates, BlockUpdate{Sequence: record.sequence, Block: block, Data: payload})
	}
	return updates, nil
}

// DurableRecords returns the complete flushed write order for a composite
// tail. Unlike DurableUpdates it intentionally preserves repeated writes to
// the same block.
func (b *Branch) DurableRecords() ([]BlockUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, os.ErrClosed
	}
	if b.durable != b.sequence {
		return nil, fmt.Errorf("branch has unflushed writes")
	}
	required := compositeTailHeaderBytes + b.records.count*compositeTailRecordBytes
	if required > MaxCompositeTailBytes {
		return nil, &CompositeTailTooLargeError{Required: required, Limit: MaxCompositeTailBytes}
	}
	updates := make([]BlockUpdate, 0, b.records.count)
	err := b.records.forEach(func(record branchRecord) error {
		payload := make([]byte, LogicalBlockSize)
		n, err := b.file.ReadAt(payload, record.offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf("read durable record %d: %w", record.sequence, err)
		}
		if n != len(payload) {
			return fmt.Errorf("read durable record %d: %w", record.sequence, io.ErrUnexpectedEOF)
		}
		updates = append(updates, BlockUpdate{Sequence: record.sequence, Block: record.block, Data: payload})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return updates, nil
}

func (b *Branch) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	return b.file.Close()
}

func (b *Branch) open() error {
	info, err := b.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		return b.createHeader()
	}
	headerPayload, headerEnd, err := readBranchHeader(b.file)
	if err != nil {
		return err
	}
	var stored BranchIdentity
	decoder := json.NewDecoder(bytes.NewReader(headerPayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&stored); err != nil {
		return fmt.Errorf("decode branch identity: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("branch identity contains trailing data")
	}
	if stored != b.identity {
		return fmt.Errorf("branch journal belongs to a different immutable writer binding")
	}
	b.header = headerEnd
	b.end = headerEnd
	return b.scanRecords(info.Size())
}

func (b *Branch) createHeader() error {
	payload, err := json.Marshal(b.identity)
	if err != nil {
		return err
	}
	if len(payload) > branchHeaderMaxBytes {
		return fmt.Errorf("branch identity exceeds %d bytes", branchHeaderMaxBytes)
	}
	header := make([]byte, branchHeaderFixedBytes+len(payload))
	copy(header[:8], branchHeaderMagic[:])
	binary.BigEndian.PutUint32(header[8:12], BranchFormatVersion)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(payload)))
	checksum := sha256.Sum256(payload)
	copy(header[16:48], checksum[:])
	copy(header[48:], payload)
	if _, err := b.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write branch header: %w", err)
	}
	if err := b.file.Sync(); err != nil {
		return fmt.Errorf("flush branch header: %w", err)
	}
	b.header = int64(len(header))
	b.end = b.header
	return nil
}

func readBranchHeader(file *os.File) ([]byte, int64, error) {
	fixed := make([]byte, branchHeaderFixedBytes)
	if _, err := file.ReadAt(fixed, 0); err != nil {
		return nil, 0, fmt.Errorf("read branch header: %w", err)
	}
	if !bytes.Equal(fixed[:8], branchHeaderMagic[:]) || binary.BigEndian.Uint32(fixed[8:12]) != BranchFormatVersion {
		return nil, 0, fmt.Errorf("branch header format is invalid")
	}
	length := int(binary.BigEndian.Uint32(fixed[12:16]))
	if length <= 0 || length > branchHeaderMaxBytes {
		return nil, 0, fmt.Errorf("branch identity length is invalid")
	}
	payload := make([]byte, length)
	if _, err := file.ReadAt(payload, branchHeaderFixedBytes); err != nil {
		return nil, 0, fmt.Errorf("read branch identity: %w", err)
	}
	checksum := sha256.Sum256(payload)
	if !bytes.Equal(fixed[16:48], checksum[:]) {
		return nil, 0, fmt.Errorf("branch identity checksum mismatch")
	}
	return payload, int64(branchHeaderFixedBytes + length), nil
}

func (b *Branch) scanRecords(size int64) error {
	offset := b.header
	var previous uint64
	for size-offset >= 64 {
		header := make([]byte, 64)
		if _, err := b.file.ReadAt(header, offset); err != nil {
			return fmt.Errorf("read branch record: %w", err)
		}
		if !bytes.Equal(header[:8], branchRecordMagic[:]) || binary.BigEndian.Uint32(header[24:28]) != LogicalBlockSize ||
			binary.BigEndian.Uint32(header[28:32]) != 0 {
			return fmt.Errorf("branch record at %d is corrupt", offset)
		}
		sequence := binary.BigEndian.Uint64(header[8:16])
		block := binary.BigEndian.Uint64(header[16:24])
		if size-offset < branchRecordBytes {
			break
		}
		payload := make([]byte, LogicalBlockSize)
		if _, err := b.file.ReadAt(payload, offset+64); err != nil {
			return fmt.Errorf("read branch record payload: %w", err)
		}
		checksum := branchRecordChecksum(header[8:32], payload)
		if !bytes.Equal(header[32:64], checksum[:]) {
			return fmt.Errorf("branch record at %d checksum mismatch", offset)
		}
		if sequence != previous+1 || block >= uint64(b.Size()/LogicalBlockSize) {
			return fmt.Errorf("branch record at %d has invalid sequence or block", offset)
		}
		b.blocks[block] = branchRecord{sequence: sequence, block: block, offset: offset + 64}
		b.records.append(branchRecord{sequence: sequence, block: block, offset: offset + 64})
		previous = sequence
		offset += branchRecordBytes
	}
	if offset != size {
		if err := b.file.Truncate(offset); err != nil {
			return fmt.Errorf("truncate incomplete branch tail: %w", err)
		}
		if err := b.file.Sync(); err != nil {
			return fmt.Errorf("flush recovered branch tail: %w", err)
		}
	}
	b.end = offset
	b.sequence = previous
	b.durable = previous
	return nil
}

func (b *Branch) appendBlockLocked(block uint64, payload []byte) error {
	b.sequence++
	header := make([]byte, 64)
	copy(header[:8], branchRecordMagic[:])
	binary.BigEndian.PutUint64(header[8:16], b.sequence)
	binary.BigEndian.PutUint64(header[16:24], block)
	binary.BigEndian.PutUint32(header[24:28], LogicalBlockSize)
	checksum := branchRecordChecksum(header[8:32], payload)
	copy(header[32:64], checksum[:])
	record := append(header, payload...)
	if _, err := b.file.WriteAt(record, b.end); err != nil {
		b.sequence--
		return fmt.Errorf("append branch block: %w", err)
	}
	b.blocks[block] = branchRecord{sequence: b.sequence, block: block, offset: b.end + 64}
	b.records.append(branchRecord{sequence: b.sequence, block: block, offset: b.end + 64})
	b.end += int64(len(record))
	return nil
}

func (b *Branch) admitRecordsLocked(records int64) error {
	if records <= 0 || b.maxDirty == 0 {
		return nil
	}
	used := int64(b.records.count) * LogicalBlockSize
	requested := records * LogicalBlockSize
	if used > b.maxDirty || requested > b.maxDirty-used {
		return &DirtyTailCapacityError{
			UsedBytes: used, RequestedBytes: requested, LimitBytes: b.maxDirty,
		}
	}
	return nil
}

func mappingBlocksForRange(offset, length int64) int64 {
	if length <= 0 {
		return 0
	}
	first := offset / LogicalBlockSize
	last := (offset + length - 1) / LogicalBlockSize
	return last - first + 1
}

func branchRecordChecksum(metadata, payload []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(metadata)
	_, _ = hash.Write(payload)
	var checksum [sha256.Size]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

func (b *Branch) readAtLocked(target []byte, offset int64) (int, error) {
	if offset >= b.Size() {
		return 0, io.EOF
	}
	wanted := len(target)
	if remaining := b.Size() - offset; int64(wanted) > remaining {
		wanted = int(remaining)
	}
	written := 0
	for written < wanted {
		absolute := offset + int64(written)
		block := uint64(absolute / LogicalBlockSize)
		inBlock := int(absolute % LogicalBlockSize)
		length := min(wanted-written, LogicalBlockSize-inBlock)
		data := make([]byte, LogicalBlockSize)
		if _, err := b.readBlockLocked(data, block); err != nil {
			return written, err
		}
		copy(target[written:written+length], data[inBlock:inBlock+length])
		written += length
	}
	if written < len(target) {
		return written, io.EOF
	}
	return written, nil
}

func (b *Branch) readBlockLocked(target []byte, block uint64) (int, error) {
	if record, ok := b.blocks[block]; ok {
		n, err := b.file.ReadAt(target, record.offset)
		if err != nil && err != io.EOF {
			return n, err
		}
		if n != len(target) {
			return n, io.ErrUnexpectedEOF
		}
		return n, nil
	}
	n, err := b.base.ReadAt(target, int64(block)*LogicalBlockSize)
	if err == io.EOF && n == len(target) {
		err = nil
	}
	return n, err
}
