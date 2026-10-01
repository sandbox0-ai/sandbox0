package rootfsblock

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
)

type liveBranchIndex struct {
	Version  int            `json:"version"`
	Identity BranchIdentity `json:"identity"`
	Header   int64          `json:"header"`
	End      int64          `json:"end"`
	Sequence uint64         `json:"sequence"`
	Device   uint64         `json:"device"`
	Inode    uint64         `json:"inode"`
}

// ExportLiveIndex copies only the source's already verified journal index.
// The device must be paused and flushed first. This file is private and
// ephemeral; it is never a substitute for crash recovery or a durable WAL.
func (b *Branch) ExportLiveIndex() (_ *os.File, result error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed || b.sequence != b.durable {
		return nil, fmt.Errorf("branch is not quiescent and durable")
	}
	file, err := newLiveIndexFile(b.file.Name())
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			_ = file.Close()
		}
	}()
	device, inode, err := liveJournalIdentity(b.file)
	if err != nil {
		return nil, err
	}
	metadata, err := json.Marshal(liveBranchIndex{Version: 1, Identity: b.identity, Header: b.header, End: b.end, Sequence: b.sequence, Device: device, Inode: inode})
	if err != nil {
		return nil, err
	}
	buffer := bufio.NewWriterSize(file, 1<<20)
	hash := sha256.New()
	writer := io.MultiWriter(buffer, hash)
	header := make([]byte, 12)
	copy(header, []byte("S0LIVEI1"))
	binary.BigEndian.PutUint32(header[8:], uint32(len(metadata)))
	if _, err := writer.Write(header); err != nil {
		return nil, err
	}
	if _, err := writer.Write(metadata); err != nil {
		return nil, err
	}
	entry := make([]byte, 24)
	if err := b.records.forEach(func(record branchRecord) error {
		binary.BigEndian.PutUint64(entry, record.sequence)
		binary.BigEndian.PutUint64(entry[8:], record.block)
		binary.BigEndian.PutUint64(entry[16:], uint64(record.offset))
		_, err := writer.Write(entry)
		return err
	}); err != nil {
		return nil, err
	}
	if _, err := buffer.Write(hash.Sum(nil)); err != nil {
		return nil, err
	}
	if err := buffer.Flush(); err != nil {
		return nil, err
	}
	if err := sealLiveIndex(file); err != nil {
		return nil, err
	}
	return file, nil
}

func (b *Branch) openLiveIndex(index *os.File) error {
	if err := validateLiveIndexSeal(index); err != nil {
		return err
	}
	info, err := b.file.Stat()
	if err != nil {
		return err
	}
	identityData, headerEnd, err := readBranchHeader(b.file)
	if err != nil {
		return err
	}
	var identity BranchIdentity
	if err := json.Unmarshal(identityData, &identity); err != nil {
		return err
	}
	if identity != b.identity {
		return fmt.Errorf("live branch journal has another writer identity")
	}
	indexInfo, err := index.Stat()
	if err != nil {
		return err
	}
	reader := bufio.NewReaderSize(io.NewSectionReader(index, 0, indexInfo.Size()), 1<<20)
	hash := sha256.New()
	checked := io.TeeReader(reader, hash)
	header := make([]byte, 12)
	if _, err := io.ReadFull(checked, header); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(header[8:])
	if !bytes.Equal(header[:8], []byte("S0LIVEI1")) || length == 0 || length > branchHeaderMaxBytes {
		return fmt.Errorf("invalid live branch index header")
	}
	metadata := make([]byte, int(length))
	if _, err := io.ReadFull(checked, metadata); err != nil {
		return err
	}
	var cut liveBranchIndex
	if err := json.Unmarshal(metadata, &cut); err != nil {
		return err
	}
	device, inode, err := liveJournalIdentity(b.file)
	if err != nil || device != cut.Device || inode != cut.Inode {
		return fmt.Errorf("live branch journal inode changed: %w", err)
	}
	if cut.Version != 1 || cut.Identity != b.identity || cut.Header != headerEnd || cut.End != info.Size() || cut.Sequence > uint64(math.MaxInt64/branchRecordBytes) || cut.End-cut.Header != int64(cut.Sequence)*branchRecordBytes || indexInfo.Size() != 12+int64(length)+int64(cut.Sequence)*24+sha256.Size {
		return fmt.Errorf("live branch index differs from its exact journal cut")
	}
	entry := make([]byte, 24)
	for sequence := uint64(1); sequence <= cut.Sequence; sequence++ {
		if _, err := io.ReadFull(checked, entry); err != nil {
			return err
		}
		record := branchRecord{sequence: binary.BigEndian.Uint64(entry), block: binary.BigEndian.Uint64(entry[8:]), offset: int64(binary.BigEndian.Uint64(entry[16:]))}
		if record.sequence != sequence || record.block >= uint64(b.Size()/LogicalBlockSize) || record.offset != cut.Header+int64(sequence-1)*branchRecordBytes+64 {
			return fmt.Errorf("invalid live branch index record")
		}
		b.blocks[record.block] = record
		b.records.append(record)
	}
	checksum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(reader, checksum); err != nil {
		return err
	}
	if !bytes.Equal(checksum, hash.Sum(nil)) {
		return fmt.Errorf("live branch index checksum mismatch")
	}
	b.header, b.end, b.sequence, b.durable = cut.Header, cut.End, cut.Sequence, cut.Sequence
	return nil
}
