package session

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/google/uuid"
)

const (
	journalFormatVersion        = 2
	journalDirectoryName        = "journal-v2"
	journalManifestName         = "manifest.json"
	journalLegacyIndexName      = "legacy.idx"
	defaultJournalSegmentBytes  = int64(4 << 20)
	minimumJournalSegmentBytes  = int64(64 << 10)
	segmentHeaderSize           = int64(16)
	segmentFrameHeaderSize      = int64(32)
	segmentFooterPrefixSize     = int64(8)
	segmentFooterTrailerSize    = int64(20)
	segmentSparseIndexStride    = 64
	maximumJournalFramePayload  = int64(64 << 20)
	maximumJournalFooterPayload = int64(16 << 20)
	legacyIndexHeaderSize       = int64(32)
	legacyIndexEntrySize        = int64(32)
)

var (
	segmentHeaderMagic = [8]byte{'S', '0', 'J', 'S', 'E', 'G', '2', '\n'}
	segmentFooterMagic = [8]byte{'S', '0', 'J', 'E', 'N', 'D', '2', '\n'}
	legacyIndexMagic   = [8]byte{'S', '0', 'J', 'I', 'D', 'X', '2', '\n'}
	journalCRCTable    = crc32.MakeTable(crc32.Castagnoli)
)

// A segment starts with segmentHeaderMagic, followed by independently
// checksummed frames. Sealing appends segmentFooterMagic, JSON metadata, a
// metadata checksum, and the magic again. DataEnd excludes the footer so
// snapshot readers never interpret index bytes as events.

type journalSparseEntry struct {
	Seq            int64 `json:"seq"`
	Offset         int64 `json:"offset"`
	OccurredAtNano int64 `json:"occurred_at_nano"`
	LogicalBefore  int64 `json:"logical_before"`
}

type journalSegmentMeta struct {
	File            string               `json:"file"`
	FirstSeq        int64                `json:"first_seq"`
	LastSeq         int64                `json:"last_seq"`
	FirstOccurredAt int64                `json:"first_occurred_at_nano"`
	LastOccurredAt  int64                `json:"last_occurred_at_nano"`
	DataEnd         int64                `json:"data_end"`
	LogicalBytes    int64                `json:"logical_bytes"`
	EventCount      int64                `json:"event_count"`
	Sparse          []journalSparseEntry `json:"sparse,omitempty"`
}

type journalLegacyMeta struct {
	File                 string `json:"file"`
	IndexFile            string `json:"index_file,omitempty"`
	ValidBytes           int64  `json:"valid_bytes"`
	EarliestSeq          int64  `json:"earliest_seq"`
	LatestSeq            int64  `json:"latest_seq"`
	RetainedLogicalBytes int64  `json:"retained_logical_bytes"`
}

type journalManifest struct {
	Version              int                  `json:"version"`
	Generation           int64                `json:"generation"`
	SegmentTargetBytes   int64                `json:"segment_target_bytes"`
	HeadSeq              int64                `json:"head_seq"`
	NextSeq              int64                `json:"next_seq"`
	RetainedLogicalBytes int64                `json:"retained_logical_bytes"`
	Segments             []journalSegmentMeta `json:"segments,omitempty"`
	ActiveFile           string               `json:"active_file"`
	ActiveDataEnd        int64                `json:"active_data_end"`
	ActiveLogicalBytes   int64                `json:"active_logical_bytes"`
	Legacy               *journalLegacyMeta   `json:"legacy,omitempty"`
}

type journalRecordMeta struct {
	seq            int64
	offset         int64
	frameSize      int64
	payloadSize    int64
	logicalSize    int64
	occurredAtNano int64
}

func normalizedJournalSegmentTarget(retention EventRetentionSpec, requested int64) int64 {
	if requested <= 0 {
		requested = defaultJournalSegmentBytes
	}
	if retention.MaxBytes > 0 && retention.MaxBytes < requested {
		requested = retention.MaxBytes
	}
	if requested < minimumJournalSegmentBytes {
		requested = minimumJournalSegmentBytes
	}
	return requested
}

func journalDirectoryForPath(path string) string {
	return filepath.Join(filepath.Dir(path), journalDirectoryName)
}

func journalManifestPath(path string) string {
	return filepath.Join(journalDirectoryForPath(path), journalManifestName)
}

func validateJournalFileName(name string) error {
	if name == "" || filepath.Base(name) != name || !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".sjr") {
		return fmt.Errorf("invalid journal segment file %q", name)
	}
	return nil
}

func createJournalSegment(dir string) (string, *os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create journal directory: %w", err)
	}
	name := "seg-" + uuid.NewString() + ".sjr"
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return "", nil, fmt.Errorf("create journal segment: %w", err)
	}
	header := make([]byte, segmentHeaderSize)
	copy(header, segmentHeaderMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], journalFormatVersion)
	if _, err := file.Write(header); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("write journal segment header: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("sync journal segment header: %w", err)
	}
	return name, file, nil
}

func validateJournalSegmentHeader(file *os.File) error {
	header := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("read journal segment header: %w", err)
	}
	if string(header[:8]) != string(segmentHeaderMagic[:]) {
		return errors.New("journal segment header magic is invalid")
	}
	if version := binary.LittleEndian.Uint32(header[8:12]); version != journalFormatVersion {
		return fmt.Errorf("journal segment version %d is unsupported", version)
	}
	return nil
}

func encodeJournalFrame(event Event) ([]byte, journalRecordMeta, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, journalRecordMeta{}, fmt.Errorf("encode event: %w", err)
	}
	if int64(len(payload)) > maximumJournalFramePayload {
		return nil, journalRecordMeta{}, fmt.Errorf("event payload is %d bytes, limit is %d", len(payload), maximumJournalFramePayload)
	}
	header := make([]byte, segmentFrameHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(header[8:16], uint64(event.Seq))
	binary.LittleEndian.PutUint64(header[16:24], uint64(event.OccurredAt.UnixNano()))
	logicalSize := int64(len(payload) + 1)
	binary.LittleEndian.PutUint64(header[24:32], uint64(logicalSize))
	checksum := crc32.New(journalCRCTable)
	_, _ = checksum.Write(header[:4])
	_, _ = checksum.Write(header[8:])
	_, _ = checksum.Write(payload)
	binary.LittleEndian.PutUint32(header[4:8], checksum.Sum32())
	frame := make([]byte, 0, len(header)+len(payload))
	frame = append(frame, header...)
	frame = append(frame, payload...)
	return frame, journalRecordMeta{
		seq:            event.Seq,
		frameSize:      int64(len(frame)),
		payloadSize:    int64(len(payload)),
		logicalSize:    logicalSize,
		occurredAtNano: event.OccurredAt.UnixNano(),
	}, nil
}

func readJournalFrameMeta(file *os.File, offset, end int64) (journalRecordMeta, error) {
	if offset < segmentHeaderSize || end-offset < segmentFrameHeaderSize {
		return journalRecordMeta{}, io.ErrUnexpectedEOF
	}
	header := make([]byte, segmentFrameHeaderSize)
	if _, err := file.ReadAt(header, offset); err != nil {
		return journalRecordMeta{}, err
	}
	payloadSize := int64(binary.LittleEndian.Uint32(header[0:4]))
	if payloadSize < 0 || payloadSize > maximumJournalFramePayload {
		return journalRecordMeta{}, fmt.Errorf("invalid journal frame payload size %d", payloadSize)
	}
	frameSize := segmentFrameHeaderSize + payloadSize
	if frameSize > end-offset {
		return journalRecordMeta{}, io.ErrUnexpectedEOF
	}
	seq := int64(binary.LittleEndian.Uint64(header[8:16]))
	logicalSize := int64(binary.LittleEndian.Uint64(header[24:32]))
	if seq <= 0 || logicalSize <= 0 {
		return journalRecordMeta{}, errors.New("journal frame metadata is invalid")
	}
	return journalRecordMeta{
		seq:            seq,
		offset:         offset,
		frameSize:      frameSize,
		payloadSize:    payloadSize,
		logicalSize:    logicalSize,
		occurredAtNano: int64(binary.LittleEndian.Uint64(header[16:24])),
	}, nil
}

func readJournalFrame(file *os.File, meta journalRecordMeta) (Event, error) {
	if meta.payloadSize < 0 || meta.payloadSize > maximumJournalFramePayload {
		return Event{}, fmt.Errorf("invalid journal frame payload size %d", meta.payloadSize)
	}
	header := make([]byte, segmentFrameHeaderSize)
	if _, err := file.ReadAt(header, meta.offset); err != nil {
		return Event{}, fmt.Errorf("read journal frame header: %w", err)
	}
	payload := make([]byte, int(meta.payloadSize))
	if _, err := file.ReadAt(payload, meta.offset+segmentFrameHeaderSize); err != nil {
		return Event{}, fmt.Errorf("read journal frame payload: %w", err)
	}
	checksum := crc32.New(journalCRCTable)
	_, _ = checksum.Write(header[:4])
	_, _ = checksum.Write(header[8:])
	_, _ = checksum.Write(payload)
	if got, want := checksum.Sum32(), binary.LittleEndian.Uint32(header[4:8]); got != want {
		return Event{}, fmt.Errorf("journal frame checksum %08x does not match %08x", got, want)
	}
	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return Event{}, fmt.Errorf("decode journal frame: %w", err)
	}
	if event.Seq != meta.seq {
		return Event{}, fmt.Errorf("journal frame sequence %d does not match index %d", event.Seq, meta.seq)
	}
	return event, nil
}

func verifyJournalFrame(file *os.File, meta journalRecordMeta, scratch []byte) error {
	header := make([]byte, segmentFrameHeaderSize)
	if _, err := file.ReadAt(header, meta.offset); err != nil {
		return fmt.Errorf("read journal frame header: %w", err)
	}
	checksum := crc32.New(journalCRCTable)
	_, _ = checksum.Write(header[:4])
	_, _ = checksum.Write(header[8:])
	if len(scratch) == 0 {
		scratch = make([]byte, 64<<10)
	}
	readOffset := meta.offset + segmentFrameHeaderSize
	remaining := meta.payloadSize
	for remaining > 0 {
		chunkSize := int64(len(scratch))
		if chunkSize > remaining {
			chunkSize = remaining
		}
		chunk := scratch[:int(chunkSize)]
		if _, err := file.ReadAt(chunk, readOffset); err != nil {
			return fmt.Errorf("checksum journal frame payload: %w", err)
		}
		_, _ = checksum.Write(chunk)
		readOffset += chunkSize
		remaining -= chunkSize
	}
	if got, want := checksum.Sum32(), binary.LittleEndian.Uint32(header[4:8]); got != want {
		return fmt.Errorf("journal frame checksum %08x does not match %08x", got, want)
	}
	return nil
}

func scanActiveJournalSegment(file *os.File) ([]journalRecordMeta, int64, error) {
	if err := validateJournalSegmentHeader(file); err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, fmt.Errorf("stat active journal segment: %w", err)
	}
	end := info.Size()
	offset := segmentHeaderSize
	entries := make([]journalRecordMeta, 0)
	checksumScratch := make([]byte, 64<<10)
	previous := int64(0)
	for offset < end {
		if end-offset >= segmentFooterPrefixSize {
			prefix := make([]byte, segmentFooterPrefixSize)
			if _, err := file.ReadAt(prefix, offset); err != nil {
				return nil, 0, fmt.Errorf("read active journal tail prefix: %w", err)
			}
			if string(prefix) == string(segmentFooterMagic[:]) {
				break
			}
		}
		meta, metaErr := readJournalFrameMeta(file, offset, end)
		if metaErr != nil {
			break
		}
		if meta.seq <= previous {
			return nil, 0, fmt.Errorf("journal sequence %d is not greater than %d", meta.seq, previous)
		}
		if err := verifyJournalFrame(file, meta, checksumScratch); err != nil {
			if offset+meta.frameSize == end {
				break
			}
			return nil, 0, fmt.Errorf("verify active journal frame at offset %d: %w", offset, err)
		}
		entries = append(entries, meta)
		previous = meta.seq
		offset += meta.frameSize
	}
	if offset < end {
		if err := file.Truncate(offset); err != nil {
			return nil, 0, fmt.Errorf("truncate incomplete active journal tail: %w", err)
		}
	}
	return entries, offset, nil
}

func segmentMetaFromEntries(file string, entries []journalRecordMeta, dataEnd int64) journalSegmentMeta {
	meta := journalSegmentMeta{File: file, DataEnd: dataEnd, EventCount: int64(len(entries))}
	if len(entries) == 0 {
		return meta
	}
	meta.FirstSeq = entries[0].seq
	meta.LastSeq = entries[len(entries)-1].seq
	meta.FirstOccurredAt = entries[0].occurredAtNano
	meta.LastOccurredAt = entries[len(entries)-1].occurredAtNano
	logicalBefore := int64(0)
	for i, entry := range entries {
		if i%segmentSparseIndexStride == 0 {
			meta.Sparse = append(meta.Sparse, journalSparseEntry{
				Seq:            entry.seq,
				Offset:         entry.offset,
				OccurredAtNano: entry.occurredAtNano,
				LogicalBefore:  logicalBefore,
			})
		}
		logicalBefore += entry.logicalSize
	}
	meta.LogicalBytes = logicalBefore
	return meta
}

func writeJournalSegmentFooter(file *os.File, meta journalSegmentMeta) error {
	payload, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encode journal segment footer: %w", err)
	}
	trailer := make([]byte, segmentFooterTrailerSize)
	binary.LittleEndian.PutUint64(trailer[0:8], uint64(len(payload)))
	binary.LittleEndian.PutUint32(trailer[8:12], crc32.Checksum(payload, journalCRCTable))
	copy(trailer[12:20], segmentFooterMagic[:])
	if _, err := file.Write(segmentFooterMagic[:]); err != nil {
		return fmt.Errorf("write journal segment footer prefix: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("write journal segment footer: %w", err)
	}
	if _, err := file.Write(trailer); err != nil {
		return fmt.Errorf("write journal segment footer trailer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync sealed journal segment: %w", err)
	}
	return nil
}

func readJournalSegmentFooter(file *os.File) (journalSegmentMeta, bool, error) {
	info, err := file.Stat()
	if err != nil {
		return journalSegmentMeta{}, false, err
	}
	if info.Size() < segmentHeaderSize+segmentFooterPrefixSize+segmentFooterTrailerSize {
		return journalSegmentMeta{}, false, nil
	}
	trailer := make([]byte, segmentFooterTrailerSize)
	if _, err := file.ReadAt(trailer, info.Size()-segmentFooterTrailerSize); err != nil {
		return journalSegmentMeta{}, false, err
	}
	if string(trailer[12:20]) != string(segmentFooterMagic[:]) {
		return journalSegmentMeta{}, false, nil
	}
	payloadSize := int64(binary.LittleEndian.Uint64(trailer[0:8]))
	footerOffset := info.Size() - segmentFooterTrailerSize - payloadSize - segmentFooterPrefixSize
	if payloadSize <= 0 || payloadSize > maximumJournalFooterPayload || footerOffset < segmentHeaderSize {
		return journalSegmentMeta{}, false, errors.New("journal segment footer size is invalid")
	}
	prefix := make([]byte, segmentFooterPrefixSize)
	if _, err := file.ReadAt(prefix, footerOffset); err != nil {
		return journalSegmentMeta{}, false, err
	}
	if string(prefix) != string(segmentFooterMagic[:]) {
		return journalSegmentMeta{}, false, errors.New("journal segment footer prefix is invalid")
	}
	payload := make([]byte, int(payloadSize))
	if _, err := file.ReadAt(payload, footerOffset+segmentFooterPrefixSize); err != nil {
		return journalSegmentMeta{}, false, err
	}
	if got, want := crc32.Checksum(payload, journalCRCTable), binary.LittleEndian.Uint32(trailer[8:12]); got != want {
		return journalSegmentMeta{}, false, fmt.Errorf("journal segment footer checksum %08x does not match %08x", got, want)
	}
	var meta journalSegmentMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		return journalSegmentMeta{}, false, fmt.Errorf("decode journal segment footer: %w", err)
	}
	if meta.DataEnd != footerOffset {
		return journalSegmentMeta{}, false, fmt.Errorf("journal segment data end %d does not match footer offset %d", meta.DataEnd, footerOffset)
	}
	return meta, true, nil
}

func journalSegmentStartOffset(meta journalSegmentMeta, seq int64) int64 {
	offset := segmentHeaderSize
	index := sort.Search(len(meta.Sparse), func(i int) bool { return meta.Sparse[i].Seq > seq })
	if index > 0 {
		offset = meta.Sparse[index-1].Offset
	}
	return offset
}

func scanJournalSegmentMetadata(file *os.File, start, end int64) ([]journalRecordMeta, error) {
	entries := make([]journalRecordMeta, 0)
	offset := start
	for offset < end {
		meta, err := readJournalFrameMeta(file, offset, end)
		if err != nil {
			return nil, fmt.Errorf("scan journal segment metadata at offset %d: %w", offset, err)
		}
		entries = append(entries, meta)
		offset += meta.frameSize
	}
	if offset != end {
		return nil, fmt.Errorf("journal segment metadata ended at %d, want %d", offset, end)
	}
	return entries, nil
}

func readJournalManifest(path string) (journalManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return journalManifest{}, err
	}
	var manifest journalManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return journalManifest{}, fmt.Errorf("decode journal manifest: %w", err)
	}
	if manifest.Version != journalFormatVersion {
		return journalManifest{}, fmt.Errorf("journal manifest version %d is unsupported", manifest.Version)
	}
	if manifest.NextSeq <= 0 {
		manifest.NextSeq = 1
	}
	if manifest.SegmentTargetBytes <= 0 {
		manifest.SegmentTargetBytes = defaultJournalSegmentBytes
	}
	if err := validateJournalFileName(manifest.ActiveFile); err != nil {
		return journalManifest{}, err
	}
	for i := range manifest.Segments {
		if err := validateJournalFileName(manifest.Segments[i].File); err != nil {
			return journalManifest{}, err
		}
		segment := manifest.Segments[i]
		if segment.EventCount <= 0 || segment.FirstSeq <= 0 || segment.LastSeq < segment.FirstSeq || segment.DataEnd < segmentHeaderSize || segment.LogicalBytes <= 0 {
			return journalManifest{}, fmt.Errorf("journal segment %s metadata is invalid", segment.File)
		}
		previousOffset := int64(0)
		for _, sparse := range segment.Sparse {
			if sparse.Seq < segment.FirstSeq || sparse.Seq > segment.LastSeq || sparse.Offset < segmentHeaderSize || sparse.Offset >= segment.DataEnd || sparse.Offset <= previousOffset {
				return journalManifest{}, fmt.Errorf("journal segment %s sparse index is invalid", segment.File)
			}
			previousOffset = sparse.Offset
		}
	}
	if manifest.Legacy != nil {
		if filepath.Base(manifest.Legacy.File) != manifest.Legacy.File || manifest.Legacy.ValidBytes <= 0 || manifest.Legacy.EarliestSeq <= 0 || manifest.Legacy.LatestSeq < manifest.Legacy.EarliestSeq {
			return journalManifest{}, errors.New("legacy journal manifest metadata is invalid")
		}
		if manifest.Legacy.IndexFile == "" {
			manifest.Legacy.IndexFile = journalLegacyIndexName
		}
		if filepath.Base(manifest.Legacy.IndexFile) != manifest.Legacy.IndexFile {
			return journalManifest{}, errors.New("legacy journal index file is invalid")
		}
	}
	sort.Slice(manifest.Segments, func(i, k int) bool { return manifest.Segments[i].FirstSeq < manifest.Segments[k].FirstSeq })
	return manifest, nil
}

func writeJournalManifest(path string, manifest *journalManifest) error {
	if manifest == nil {
		return errors.New("journal manifest is required")
	}
	manifest.Version = journalFormatVersion
	manifest.Generation++
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode journal manifest: %w", err)
	}
	payload = append(payload, '\n')
	if err := writeFileAtomic(path, payload, 0o600); err != nil {
		return fmt.Errorf("persist journal manifest: %w", err)
	}
	// Reopening and syncing the renamed file flushes the S0FS WAL entry that
	// made the manifest visible. The directory sync below supplies the normal
	// POSIX rename durability guarantee; S0FS reports that operation unsupported.
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("reopen journal manifest: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil || closeErr != nil {
		return errors.Join(syncErr, closeErr)
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	directorySyncErr := directory.Sync()
	directoryCloseErr := directory.Close()
	if directorySyncErr != nil && !errors.Is(directorySyncErr, syscall.ENOSYS) && !errors.Is(directorySyncErr, syscall.EINVAL) {
		return errors.Join(directorySyncErr, directoryCloseErr)
	}
	if directoryCloseErr != nil {
		return directoryCloseErr
	}
	return nil
}

func writeLegacyJournalIndex(path string, validBytes int64, entries []journalEntry) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".legacy-index-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	header := make([]byte, legacyIndexHeaderSize)
	copy(header[:8], legacyIndexMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], journalFormatVersion)
	binary.LittleEndian.PutUint64(header[16:24], uint64(validBytes))
	binary.LittleEndian.PutUint64(header[24:32], uint64(len(entries)))
	if _, err := tmp.Write(header); err != nil {
		_ = tmp.Close()
		return err
	}
	buf := make([]byte, legacyIndexEntrySize)
	for _, entry := range entries {
		binary.LittleEndian.PutUint64(buf[0:8], uint64(entry.seq))
		binary.LittleEndian.PutUint64(buf[8:16], uint64(entry.offset))
		binary.LittleEndian.PutUint64(buf[16:24], uint64(entry.size))
		binary.LittleEndian.PutUint64(buf[24:32], uint64(entry.occurredAtNano))
		if _, err := tmp.Write(buf); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = file.Sync()
	return errors.Join(err, file.Close())
}

func readLegacyJournalIndex(path string, validBytes int64) ([]journalEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	header := make([]byte, legacyIndexHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, err
	}
	if string(header[:8]) != string(legacyIndexMagic[:]) || binary.LittleEndian.Uint32(header[8:12]) != journalFormatVersion {
		return nil, errors.New("legacy journal index header is invalid")
	}
	if got := int64(binary.LittleEndian.Uint64(header[16:24])); got != validBytes {
		return nil, fmt.Errorf("legacy journal index covers %d bytes, want %d", got, validBytes)
	}
	count := int64(binary.LittleEndian.Uint64(header[24:32]))
	if count < 0 || count > validBytes+1 {
		return nil, errors.New("legacy journal index entry count is invalid")
	}
	entries := make([]journalEntry, 0, count)
	buf := make([]byte, legacyIndexEntrySize)
	previousSeq := int64(0)
	previousEnd := int64(0)
	for i := int64(0); i < count; i++ {
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}
		entry := journalEntry{
			seq:            int64(binary.LittleEndian.Uint64(buf[0:8])),
			offset:         int64(binary.LittleEndian.Uint64(buf[8:16])),
			size:           int64(binary.LittleEndian.Uint64(buf[16:24])),
			occurredAtNano: int64(binary.LittleEndian.Uint64(buf[24:32])),
		}
		if entry.seq <= previousSeq || entry.offset != previousEnd || entry.size <= 0 || entry.offset+entry.size > validBytes {
			return nil, errors.New("legacy journal index entry is invalid")
		}
		entries = append(entries, entry)
		previousSeq = entry.seq
		previousEnd = entry.offset + entry.size
	}
	if previousEnd != validBytes {
		return nil, fmt.Errorf("legacy journal index ends at %d, want %d", previousEnd, validBytes)
	}
	return entries, nil
}

func segmentMetaLatest(segments []journalSegmentMeta) int64 {
	latest := int64(0)
	for _, segment := range segments {
		if segment.LastSeq > latest {
			latest = segment.LastSeq
		}
	}
	return latest
}

func maxInt64(values ...int64) int64 {
	maximum := int64(0)
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	return maximum
}
