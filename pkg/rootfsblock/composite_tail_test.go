package rootfsblock

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompositeTailRoundTripAndReaderReplay(t *testing.T) {
	objects := newMemoryObjects()
	basePayload := bytes.Repeat([]byte{0x11}, 3*LogicalBlockSize)
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, BuildOptions{})
	require.NoError(t, err)
	records := []BlockUpdate{
		{Sequence: 11, Block: 1, Data: bytes.Repeat([]byte{0x22}, LogicalBlockSize)},
		{Sequence: 12, Block: 1, Data: bytes.Repeat([]byte{0x33}, LogicalBlockSize)},
		{Sequence: 13, Block: 2, Data: make([]byte, LogicalBlockSize)},
	}
	descriptor, payload, err := BuildCompositeGeneration(base.Descriptor, records)
	require.NoError(t, err)
	require.LessOrEqual(t, len(payload), MaxDescriptorBytes)
	decoded, sealed, err := DecodeCompositeTail(*descriptor.CompositeTail, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(3), sealed)
	require.Equal(t, []uint64{1, 2, 3}, []uint64{decoded[0].Sequence, decoded[1].Sequence, decoded[2].Sequence})

	reader, err := NewReader(objects, descriptor, 1<<20)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, basePayload[:LogicalBlockSize], actual[:LogicalBlockSize])
	require.Equal(t, bytes.Repeat([]byte{0x33}, LogicalBlockSize), actual[LogicalBlockSize:2*LogicalBlockSize])
	require.Equal(t, make([]byte, LogicalBlockSize), actual[2*LogicalBlockSize:])
}

func TestCompositeTailRejectsCorruptionAndCapacityOverflow(t *testing.T) {
	record := BlockUpdate{Block: 0, Data: make([]byte, LogicalBlockSize)}
	tail, err := EncodeCompositeTail([]BlockUpdate{record}, 1)
	require.NoError(t, err)
	tail.Payload[compositeTailHeaderBytes+7] ^= 0xff
	_, _, err = DecodeCompositeTail(tail, 1)
	require.ErrorContains(t, err, "checksum")

	records := make([]BlockUpdate, 12)
	for index := range records {
		records[index] = BlockUpdate{Block: uint64(index), Data: make([]byte, LogicalBlockSize)}
	}
	_, err = EncodeCompositeTail(records, 12)
	require.ErrorContains(t, err, "limit")
	var tooLarge *CompositeTailTooLargeError
	require.True(t, errors.As(err, &tooLarge))
	require.Greater(t, tooLarge.Required, tooLarge.Limit)
}

func TestMaximumCompositeTailFitsDescriptorLimit(t *testing.T) {
	records := make([]BlockUpdate, 11)
	for index := range records {
		records[index] = BlockUpdate{Block: uint64(index), Data: bytes.Repeat([]byte{byte(index + 1)}, LogicalBlockSize)}
	}
	descriptor := validDescriptor()
	descriptor.LogicalSizeBytes = int64(len(records) * LogicalBlockSize)
	tail, err := EncodeCompositeTail(records, uint64(len(records)))
	require.NoError(t, err)
	descriptor.CompositeTail = &tail
	payload, err := EncodeDescriptor(descriptor)
	require.NoError(t, err)
	require.LessOrEqual(t, len(payload), MaxDescriptorBytes)
}

func TestIncrementalBuildMaterializesExistingCompositeTail(t *testing.T) {
	objects := newMemoryObjects()
	basePayload := bytes.Repeat([]byte{0x11}, 3*LogicalBlockSize)
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(basePayload), int64(len(basePayload)), objects, BuildOptions{})
	require.NoError(t, err)
	composite, _, err := BuildCompositeGeneration(base.Descriptor, []BlockUpdate{
		{Block: 0, Data: bytes.Repeat([]byte{0x22}, LogicalBlockSize)},
	})
	require.NoError(t, err)
	next, err := BuildIncrementalGeneration(t.Context(), objects, composite, []BlockUpdate{
		{Block: 2, Data: bytes.Repeat([]byte{0x33}, LogicalBlockSize)},
	}, objects, BuildOptions{})
	require.NoError(t, err)
	require.Nil(t, next.Descriptor.CompositeTail)
	reader, err := NewReader(objects, next.Descriptor, 1<<20)
	require.NoError(t, err)
	actual := make([]byte, len(basePayload))
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0x22), actual[0])
	require.Equal(t, byte(0x11), actual[LogicalBlockSize])
	require.Equal(t, byte(0x33), actual[2*LogicalBlockSize])
}
