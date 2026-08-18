package rootfsblock

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/opencontainers/go-digest"
)

const (
	compositeTailVersion     = 1
	compositeTailHeaderBytes = 32
	compositeTailRecordBytes = 48 + LogicalBlockSize
)

var compositeTailMagic = [8]byte{'S', '0', 'B', 'T', 'A', 'I', 'L', '1'}

// CompositeTailTooLargeError identifies the one capacity condition that may
// fall back to publishing a materialized immutable generation. Other encoding
// errors remain correctness failures and must never silently take that path.
type CompositeTailTooLargeError struct {
	Required int
	Limit    int
}

func (e *CompositeTailTooLargeError) Error() string {
	return fmt.Sprintf("composite tail requires %d bytes, limit is %d", e.Required, e.Limit)
}

// EncodeCompositeTail serializes the ordered writes covered by one final
// node-local FLUSH. Repeated blocks are retained so the regional descriptor
// contains an auditable replay order rather than only an unordered final map.
func EncodeCompositeTail(records []BlockUpdate, totalBlocks uint64) (CompositeTail, error) {
	if len(records) == 0 {
		return CompositeTail{}, fmt.Errorf("composite tail requires at least one record")
	}
	size := compositeTailHeaderBytes + len(records)*compositeTailRecordBytes
	if size > MaxCompositeTailBytes {
		return CompositeTail{}, &CompositeTailTooLargeError{Required: size, Limit: MaxCompositeTailBytes}
	}
	payload := make([]byte, size)
	copy(payload[:8], compositeTailMagic[:])
	binary.BigEndian.PutUint16(payload[8:10], compositeTailVersion)
	binary.BigEndian.PutUint32(payload[12:16], uint32(len(records)))
	binary.BigEndian.PutUint64(payload[16:24], uint64(len(records)))
	binary.BigEndian.PutUint32(payload[24:28], LogicalBlockSize)
	offset := compositeTailHeaderBytes
	for index, record := range records {
		if record.Block >= totalBlocks || len(record.Data) != LogicalBlockSize {
			return CompositeTail{}, fmt.Errorf("composite tail record %d is invalid", index)
		}
		sequence := uint64(index + 1)
		binary.BigEndian.PutUint64(payload[offset:offset+8], sequence)
		binary.BigEndian.PutUint64(payload[offset+8:offset+16], record.Block)
		copy(payload[offset+48:offset+compositeTailRecordBytes], record.Data)
		checksum := tailRecordChecksum(payload[offset:offset+16], record.Data)
		copy(payload[offset+16:offset+48], checksum[:])
		offset += compositeTailRecordBytes
	}
	return CompositeTail{
		Encoding: CompositeTailEncoding, Checksum: digest.FromBytes(payload).String(), Payload: payload,
	}, nil
}

func DecodeCompositeTail(tail CompositeTail, totalBlocks uint64) ([]BlockUpdate, uint64, error) {
	if tail.Encoding != CompositeTailEncoding {
		return nil, 0, fmt.Errorf("unsupported tail encoding %q", tail.Encoding)
	}
	if len(tail.Payload) == 0 || len(tail.Payload) > MaxCompositeTailBytes {
		return nil, 0, fmt.Errorf("tail payload must contain 1..%d bytes", MaxCompositeTailBytes)
	}
	if digest.FromBytes(tail.Payload).String() != tail.Checksum {
		return nil, 0, fmt.Errorf("tail checksum does not match payload")
	}
	return decodeCompositeTailPayload(tail.Payload, totalBlocks)
}

func decodeCompositeTailPayload(payload []byte, totalBlocks uint64) ([]BlockUpdate, uint64, error) {
	if len(payload) < compositeTailHeaderBytes || len(payload) > MaxCompositeTailBytes ||
		!bytes.Equal(payload[:8], compositeTailMagic[:]) || binary.BigEndian.Uint16(payload[8:10]) != compositeTailVersion {
		return nil, 0, fmt.Errorf("composite tail header is invalid")
	}
	if payload[10] != 0 || payload[11] != 0 || binary.BigEndian.Uint32(payload[28:32]) != 0 ||
		binary.BigEndian.Uint32(payload[24:28]) != LogicalBlockSize {
		return nil, 0, fmt.Errorf("composite tail reserved fields or block size are invalid")
	}
	count := int(binary.BigEndian.Uint32(payload[12:16]))
	sealedSequence := binary.BigEndian.Uint64(payload[16:24])
	if count <= 0 || sealedSequence != uint64(count) || compositeTailHeaderBytes+count*compositeTailRecordBytes != len(payload) {
		return nil, 0, fmt.Errorf("composite tail record count or sealed sequence is invalid")
	}
	records := make([]BlockUpdate, 0, count)
	offset := compositeTailHeaderBytes
	for index := 0; index < count; index++ {
		sequence := binary.BigEndian.Uint64(payload[offset : offset+8])
		block := binary.BigEndian.Uint64(payload[offset+8 : offset+16])
		data := payload[offset+48 : offset+compositeTailRecordBytes]
		checksum := tailRecordChecksum(payload[offset:offset+16], data)
		if sequence != uint64(index+1) || block >= totalBlocks || !bytes.Equal(payload[offset+16:offset+48], checksum[:]) {
			return nil, 0, fmt.Errorf("composite tail record %d is invalid", index)
		}
		records = append(records, BlockUpdate{Sequence: sequence, Block: block, Data: append([]byte(nil), data...)})
		offset += compositeTailRecordBytes
	}
	return records, sealedSequence, nil
}

func tailRecordChecksum(metadata, payload []byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write(metadata)
	_, _ = hash.Write(payload)
	var checksum [sha256.Size]byte
	copy(checksum[:], hash.Sum(nil))
	return checksum
}

// BuildCompositeGeneration appends a flushed local record sequence to the
// existing durable tail without issuing any object-store request.
func BuildCompositeGeneration(base Descriptor, records []BlockUpdate) (Descriptor, []byte, error) {
	if err := base.Validate(); err != nil {
		return Descriptor{}, nil, err
	}
	totalBlocks := uint64(base.LogicalSizeBytes / LogicalBlockSize)
	combined := make([]BlockUpdate, 0, len(records))
	if base.CompositeTail != nil {
		existing, _, err := DecodeCompositeTail(*base.CompositeTail, totalBlocks)
		if err != nil {
			return Descriptor{}, nil, err
		}
		combined = append(combined, existing...)
	}
	combined = append(combined, records...)
	if len(combined) == 0 {
		payload, err := EncodeDescriptor(base)
		return base, payload, err
	}
	tail, err := EncodeCompositeTail(combined, totalBlocks)
	if err != nil {
		return Descriptor{}, nil, err
	}
	next := base
	next.CompositeTail = &tail
	payload, err := EncodeDescriptor(next)
	if err != nil {
		return Descriptor{}, nil, err
	}
	return next, payload, nil
}
