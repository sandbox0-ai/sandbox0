package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// BlockUpdateReader exposes a stable final value for each dirty logical
// block. Blocks may return any order, but it must return the same set for the
// lifetime of the reader. ReadBlock must fill exactly one logical block.
type BlockUpdateReader interface {
	Blocks() ([]uint64, error)
	ReadBlock(block uint64, target []byte) (int, error)
}

// BranchCheckpoint is an immutable journal boundary. Creating it flushes the
// branch, duplicates the exact journal inode, and rotates the append index so
// the active writer can continue immediately. Payloads are read lazily after
// the caller releases its guest filesystem freeze.
type BranchCheckpoint struct {
	mu       sync.RWMutex
	file     *os.File
	identity BranchIdentity
	end      int64
	sequence uint64
	count    int
	chunks   []*branchRecordChunk
	latest   map[uint64]branchRecord
	blocks   []uint64
	closed   bool
}

// Checkpoint flushes and captures the exact current branch boundary. It does
// not stop later writes and does not copy any 4 KiB record payload.
func (b *Branch) Checkpoint() (*BranchCheckpoint, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, os.ErrClosed
	}
	if err := b.flushLocked(); err != nil {
		return nil, err
	}
	file, err := duplicateBranchFile(b.file)
	if err != nil {
		return nil, fmt.Errorf("duplicate branch journal for checkpoint: %w", err)
	}
	return &BranchCheckpoint{
		file: file, identity: b.identity, end: b.end, sequence: b.sequence,
		count: b.records.count, chunks: b.records.snapshot(),
	}, nil
}

func (c *BranchCheckpoint) Identity() BranchIdentity { return c.identity }
func (c *BranchCheckpoint) Sequence() uint64         { return c.sequence }
func (c *BranchCheckpoint) RecordCount() int         { return c.count }

// Blocks returns the sorted final dirty-block set at the captured boundary.
// Resolving repeated writes happens outside the branch lock.
func (c *BranchCheckpoint) Blocks() ([]uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	c.prepareFinalIndexLocked()
	return append([]uint64(nil), c.blocks...), nil
}

// ReadBlock reads and verifies one final dirty block at the checkpoint.
func (c *BranchCheckpoint) ReadBlock(block uint64, target []byte) (int, error) {
	if len(target) != LogicalBlockSize {
		return 0, fmt.Errorf("checkpoint target must contain exactly %d bytes", LogicalBlockSize)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, os.ErrClosed
	}
	c.prepareFinalIndexLocked()
	record, ok := c.latest[block]
	if !ok {
		return 0, fmt.Errorf("logical block %d is not dirty in this checkpoint", block)
	}
	if err := c.readRecordLocked(record, target); err != nil {
		return 0, err
	}
	return len(target), nil
}

// DurableRecords returns the ordered checkpoint records only when they can
// fit in one PostgreSQL composite descriptor. Large journals fail before any
// payload allocation so callers can stream final blocks to object storage.
func (c *BranchCheckpoint) DurableRecords() ([]BlockUpdate, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, os.ErrClosed
	}
	required := compositeTailHeaderBytes + c.count*compositeTailRecordBytes
	if required > MaxCompositeTailBytes {
		return nil, &CompositeTailTooLargeError{Required: required, Limit: MaxCompositeTailBytes}
	}
	updates := make([]BlockUpdate, 0, c.count)
	for _, chunk := range c.chunks {
		for _, record := range chunk.records {
			payload := make([]byte, LogicalBlockSize)
			if err := c.readRecordLocked(record, payload); err != nil {
				return nil, err
			}
			updates = append(updates, BlockUpdate{Sequence: record.sequence, Block: record.block, Data: payload})
		}
	}
	if len(updates) != c.count {
		return nil, fmt.Errorf("checkpoint record index contains %d records, expected %d", len(updates), c.count)
	}
	return updates, nil
}

func (c *BranchCheckpoint) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.chunks = nil
	c.latest = nil
	c.blocks = nil
	return c.file.Close()
}

func (c *BranchCheckpoint) prepareFinalIndexLocked() {
	if c.latest != nil {
		return
	}
	c.latest = make(map[uint64]branchRecord)
	for _, chunk := range c.chunks {
		for _, record := range chunk.records {
			c.latest[record.block] = record
		}
	}
	c.blocks = make([]uint64, 0, len(c.latest))
	for block := range c.latest {
		c.blocks = append(c.blocks, block)
	}
	sort.Slice(c.blocks, func(i, j int) bool { return c.blocks[i] < c.blocks[j] })
}

func (c *BranchCheckpoint) readRecordLocked(record branchRecord, target []byte) error {
	if record.offset < 64 || record.offset+LogicalBlockSize > c.end {
		return fmt.Errorf("checkpoint record %d is outside the captured journal", record.sequence)
	}
	header := make([]byte, 64)
	if _, err := c.file.ReadAt(header, record.offset-64); err != nil {
		return fmt.Errorf("read checkpoint record %d header: %w", record.sequence, err)
	}
	n, err := c.file.ReadAt(target, record.offset)
	if err != nil && err != io.EOF {
		return fmt.Errorf("read checkpoint record %d payload: %w", record.sequence, err)
	}
	if n != len(target) {
		return fmt.Errorf("read checkpoint record %d payload: %w", record.sequence, io.ErrUnexpectedEOF)
	}
	if !bytes.Equal(header[:8], branchRecordMagic[:]) ||
		binary.BigEndian.Uint64(header[8:16]) != record.sequence ||
		binary.BigEndian.Uint64(header[16:24]) != record.block ||
		binary.BigEndian.Uint32(header[24:28]) != LogicalBlockSize ||
		binary.BigEndian.Uint32(header[28:32]) != 0 {
		return fmt.Errorf("checkpoint record %d metadata is invalid", record.sequence)
	}
	checksum := branchRecordChecksum(header[8:32], target)
	if !bytes.Equal(header[32:64], checksum[:]) {
		return fmt.Errorf("checkpoint record %d checksum mismatch", record.sequence)
	}
	return nil
}
