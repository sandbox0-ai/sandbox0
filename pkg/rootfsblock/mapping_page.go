package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	mappingPageHeaderBytes = 32
	mappingEntryFixedBytes = 64
	MaxMappingPageEntries  = 65536
	MaxDataRangeBytes      = 8 << 20
)

var mappingPageMagic = [8]byte{'S', '0', 'B', 'M', 'P', 'G', '0', '1'}

type MappingEntryKind uint8

const (
	MappingEntryData  MappingEntryKind = 1
	MappingEntryChild MappingEntryKind = 2
)

// MappingPage is one immutable node in a complete block-map tree. Missing
// logical ranges read as zeroes. Internal pages point only at child pages;
// leaf pages point only at immutable data ranges.
type MappingPage struct {
	Level      uint8
	StartBlock uint64
	BlockCount uint64
	Entries    []MappingEntry
}

type MappingEntry struct {
	LogicalStart uint64
	BlockCount   uint32
	Kind         MappingEntryKind
	Object       ObjectRange
}

func EncodeMappingPage(page MappingPage) ([]byte, error) {
	if err := page.Validate(); err != nil {
		return nil, err
	}
	size := mappingPageHeaderBytes
	for _, entry := range page.Entries {
		size += mappingEntryFixedBytes + len(entry.Object.Key)
	}
	if size > MaxMappingRootBytes {
		return nil, fmt.Errorf("encoded mapping page exceeds %d bytes", MaxMappingRootBytes)
	}
	payload := make([]byte, size)
	copy(payload[:8], mappingPageMagic[:])
	binary.BigEndian.PutUint16(payload[8:10], MappingPageVersion)
	payload[10] = page.Level
	binary.BigEndian.PutUint32(payload[12:16], uint32(len(page.Entries)))
	binary.BigEndian.PutUint64(payload[16:24], page.StartBlock)
	binary.BigEndian.PutUint64(payload[24:32], page.BlockCount)
	offset := mappingPageHeaderBytes
	for _, entry := range page.Entries {
		binary.BigEndian.PutUint64(payload[offset:offset+8], entry.LogicalStart)
		binary.BigEndian.PutUint32(payload[offset+8:offset+12], entry.BlockCount)
		payload[offset+12] = byte(entry.Kind)
		binary.BigEndian.PutUint64(payload[offset+16:offset+24], uint64(entry.Object.Offset))
		binary.BigEndian.PutUint32(payload[offset+24:offset+28], uint32(entry.Object.Length))
		binary.BigEndian.PutUint16(payload[offset+28:offset+30], uint16(len(entry.Object.Key)))
		checksum, err := digestBytes(entry.Object.Checksum)
		if err != nil {
			return nil, err
		}
		copy(payload[offset+32:offset+64], checksum)
		offset += mappingEntryFixedBytes
		copy(payload[offset:offset+len(entry.Object.Key)], entry.Object.Key)
		offset += len(entry.Object.Key)
	}
	return payload, nil
}

func DecodeMappingPage(payload []byte) (MappingPage, error) {
	if len(payload) < mappingPageHeaderBytes || len(payload) > MaxMappingRootBytes {
		return MappingPage{}, fmt.Errorf("mapping page must contain %d..%d bytes", mappingPageHeaderBytes, MaxMappingRootBytes)
	}
	if !bytes.Equal(payload[:8], mappingPageMagic[:]) {
		return MappingPage{}, fmt.Errorf("mapping page magic is invalid")
	}
	if version := binary.BigEndian.Uint16(payload[8:10]); version != MappingPageVersion {
		return MappingPage{}, fmt.Errorf("unsupported mapping page version %d", version)
	}
	if payload[11] != 0 {
		return MappingPage{}, fmt.Errorf("mapping page reserved header bits are nonzero")
	}
	entryCount := binary.BigEndian.Uint32(payload[12:16])
	if entryCount > MaxMappingPageEntries {
		return MappingPage{}, fmt.Errorf("mapping page has too many entries")
	}
	page := MappingPage{
		Level: payload[10], StartBlock: binary.BigEndian.Uint64(payload[16:24]),
		BlockCount: binary.BigEndian.Uint64(payload[24:32]), Entries: make([]MappingEntry, 0, entryCount),
	}
	offset := mappingPageHeaderBytes
	for index := uint32(0); index < entryCount; index++ {
		if len(payload)-offset < mappingEntryFixedBytes {
			return MappingPage{}, fmt.Errorf("mapping page entry %d is truncated", index)
		}
		if payload[offset+13] != 0 || payload[offset+14] != 0 || payload[offset+15] != 0 ||
			payload[offset+30] != 0 || payload[offset+31] != 0 {
			return MappingPage{}, fmt.Errorf("mapping page entry %d reserved bits are nonzero", index)
		}
		objectOffset := binary.BigEndian.Uint64(payload[offset+16 : offset+24])
		if objectOffset > math.MaxInt64 {
			return MappingPage{}, fmt.Errorf("mapping page entry %d object offset overflows", index)
		}
		keyLength := int(binary.BigEndian.Uint16(payload[offset+28 : offset+30]))
		if keyLength == 0 || keyLength > MaxObjectKeyBytes || len(payload)-offset-mappingEntryFixedBytes < keyLength {
			return MappingPage{}, fmt.Errorf("mapping page entry %d object key is invalid", index)
		}
		checksum := digest.NewDigestFromEncoded(digest.SHA256,
			hex.EncodeToString(payload[offset+32:offset+64])).String()
		entry := MappingEntry{
			LogicalStart: binary.BigEndian.Uint64(payload[offset : offset+8]),
			BlockCount:   binary.BigEndian.Uint32(payload[offset+8 : offset+12]),
			Kind:         MappingEntryKind(payload[offset+12]),
			Object: ObjectRange{
				Offset: int64(objectOffset), Length: int64(binary.BigEndian.Uint32(payload[offset+24 : offset+28])),
				Checksum: checksum,
			},
		}
		offset += mappingEntryFixedBytes
		entry.Object.Key = string(payload[offset : offset+keyLength])
		offset += keyLength
		page.Entries = append(page.Entries, entry)
	}
	if offset != len(payload) {
		return MappingPage{}, fmt.Errorf("mapping page contains trailing data")
	}
	if err := page.Validate(); err != nil {
		return MappingPage{}, err
	}
	return page, nil
}

func (p MappingPage) Validate() error {
	if p.BlockCount == 0 || p.StartBlock > math.MaxUint64-p.BlockCount {
		return fmt.Errorf("mapping page logical range is invalid")
	}
	if len(p.Entries) > MaxMappingPageEntries {
		return fmt.Errorf("mapping page has too many entries")
	}
	pageEnd := p.StartBlock + p.BlockCount
	previousEnd := p.StartBlock
	for index, entry := range p.Entries {
		if entry.BlockCount == 0 || entry.LogicalStart < p.StartBlock || entry.LogicalStart > math.MaxUint64-uint64(entry.BlockCount) {
			return fmt.Errorf("mapping entry %d logical range is invalid", index)
		}
		entryEnd := entry.LogicalStart + uint64(entry.BlockCount)
		if entry.LogicalStart < previousEnd || entryEnd > pageEnd {
			return fmt.Errorf("mapping entry %d overlaps or exceeds its page", index)
		}
		switch entry.Kind {
		case MappingEntryData:
			if p.Level != 0 {
				return fmt.Errorf("mapping entry %d is data in an internal page", index)
			}
			if entry.Object.Length != int64(entry.BlockCount)*LogicalBlockSize {
				return fmt.Errorf("mapping entry %d data length does not match its blocks", index)
			}
			if err := entry.Object.Validate(MaxDataRangeBytes); err != nil {
				return fmt.Errorf("mapping entry %d: %w", index, err)
			}
		case MappingEntryChild:
			if p.Level == 0 {
				return fmt.Errorf("mapping entry %d is a child in a leaf page", index)
			}
			if err := entry.Object.Validate(MaxMappingRootBytes); err != nil {
				return fmt.Errorf("mapping entry %d: %w", index, err)
			}
		default:
			return fmt.Errorf("mapping entry %d has unsupported kind %d", index, entry.Kind)
		}
		previousEnd = entryEnd
	}
	return nil
}

func (p MappingPage) entryFor(block uint64) (MappingEntry, bool) {
	index := sort.Search(len(p.Entries), func(index int) bool {
		return p.Entries[index].LogicalStart+uint64(p.Entries[index].BlockCount) > block
	})
	if index >= len(p.Entries) || block < p.Entries[index].LogicalStart {
		return MappingEntry{}, false
	}
	return p.Entries[index], true
}

func digestBytes(value string) ([]byte, error) {
	parsed, err := digest.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return nil, fmt.Errorf("checksum must be a canonical sha256 digest")
	}
	decoded, err := hex.DecodeString(parsed.Encoded())
	if err != nil || len(decoded) != digest.SHA256.Size() {
		return nil, fmt.Errorf("decode checksum: %w", err)
	}
	return decoded, nil
}

var _ io.Reader = (*bytes.Reader)(nil)
