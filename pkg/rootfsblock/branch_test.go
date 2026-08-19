package rootfsblock

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestBranchPartialWriteFlushAndRestart(t *testing.T) {
	base := bytes.Repeat([]byte{0x11}, 3*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	_, err = branch.WriteAt([]byte("sandbox0"), LogicalBlockSize+17)
	require.NoError(t, err)
	require.NoError(t, branch.WriteZeroes(2*LogicalBlockSize, LogicalBlockSize))
	require.NoError(t, branch.Flush())
	require.Equal(t, []uint64{1, 2}, branch.DirtyBlocks())
	require.NoError(t, branch.Close())

	reopened, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	defer reopened.Close()
	payload := make([]byte, len(base))
	_, err = reopened.ReadAt(payload, 0)
	require.NoError(t, err)
	require.Equal(t, base[:LogicalBlockSize], payload[:LogicalBlockSize])
	require.Equal(t, []byte("sandbox0"), payload[LogicalBlockSize+17:LogicalBlockSize+25])
	require.Equal(t, make([]byte, LogicalBlockSize), payload[2*LogicalBlockSize:])
}

func TestBranchWriteZeroesPreservesAdjacentBytesAcrossMappingBlocks(t *testing.T) {
	base := bytes.Repeat([]byte{0x5a}, 3*LogicalBlockSize)
	branch, err := OpenBranch(
		filepath.Join(t.TempDir(), "branch.log"),
		testBranchIdentity(int64(len(base))),
		bytes.NewReader(base),
	)
	require.NoError(t, err)
	defer branch.Close()

	offset := int64(LogicalBlockSize - NBDDeviceSectorSize)
	length := int64(LogicalBlockSize + 2*NBDDeviceSectorSize)
	require.NoError(t, branch.WriteZeroes(offset, length))

	actual := make([]byte, len(base))
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, base[:offset], actual[:offset])
	require.Equal(t, make([]byte, length), actual[offset:offset+length])
	require.Equal(t, base[offset+length:], actual[offset+length:])
	require.Equal(t, []uint64{0, 1, 2}, branch.DirtyBlocks())
}

func TestBranchWriteZeroesValidatesRange(t *testing.T) {
	base := bytes.Repeat([]byte{0x5a}, LogicalBlockSize)
	branch, err := OpenBranch(
		filepath.Join(t.TempDir(), "branch.log"),
		testBranchIdentity(int64(len(base))),
		bytes.NewReader(base),
	)
	require.NoError(t, err)
	defer branch.Close()

	require.NoError(t, branch.WriteZeroes(LogicalBlockSize, 0))
	require.ErrorContains(t, branch.WriteZeroes(-1, 1), "within")
	require.ErrorContains(t, branch.WriteZeroes(LogicalBlockSize, 1), "within")
}

func TestBranchRecoversIncompleteTailButRejectsCorruption(t *testing.T) {
	base := make([]byte, 2*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{1}, LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = file.Write(bytes.Repeat([]byte{0xaa}, 31))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	recovered, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	require.NoError(t, recovered.Close())

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	contents[len(contents)-1] ^= 0xff
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	_, err = OpenBranch(path, identity, bytes.NewReader(base))
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestBranchRecoveryIgnoresIncompleteMappingRecord(t *testing.T) {
	base := bytes.Repeat([]byte{0x11}, 2*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	committed := bytes.Repeat([]byte{0x22}, LogicalBlockSize)
	_, err = branch.WriteAt(committed, 0)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{0x33}, LogicalBlockSize)
	header := make([]byte, 64)
	copy(header[:8], branchRecordMagic[:])
	binary.BigEndian.PutUint64(header[8:16], 2)
	binary.BigEndian.PutUint64(header[16:24], 0)
	binary.BigEndian.PutUint32(header[24:28], LogicalBlockSize)
	checksum := branchRecordChecksum(header[8:32], payload)
	copy(header[32:64], checksum[:])
	_, err = file.Write(append(header, payload[:NBDDeviceSectorSize]...))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	recovered, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	defer recovered.Close()
	actual := make([]byte, LogicalBlockSize)
	_, err = recovered.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, committed, actual)
	require.Equal(t, []uint64{0}, recovered.DirtyBlocks())
}

func TestBranchRejectsDifferentWriterBinding(t *testing.T) {
	base := make([]byte, LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	require.NoError(t, branch.Close())
	identity.WriterEpoch++
	_, err = OpenBranch(path, identity, bytes.NewReader(base))
	require.ErrorContains(t, err, "different immutable writer binding")
}

func TestBranchRejectsRecordMetadataCorruption(t *testing.T) {
	base := make([]byte, 2*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{1}, LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())

	file, err := os.Open(path)
	require.NoError(t, err)
	_, headerEnd, err := readBranchHeader(file)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	contents[headerEnd+23] = 1 // Change block 0 to the still-in-range block 1.
	require.NoError(t, os.WriteFile(path, contents, 0o600))
	_, err = OpenBranch(path, identity, bytes.NewReader(base))
	require.ErrorContains(t, err, "checksum mismatch")
}

func TestBranchConcurrentBlockWrites(t *testing.T) {
	const blocks = 32
	base := make([]byte, blocks*LogicalBlockSize)
	branch, err := OpenBranch(filepath.Join(t.TempDir(), "branch.log"), testBranchIdentity(int64(len(base))), bytes.NewReader(base))
	require.NoError(t, err)
	defer branch.Close()
	var wait sync.WaitGroup
	for block := range blocks {
		block := block
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, writeErr := branch.WriteAt(bytes.Repeat([]byte{byte(block + 1)}, LogicalBlockSize), int64(block*LogicalBlockSize))
			require.NoError(t, writeErr)
		}()
	}
	wait.Wait()
	require.NoError(t, branch.Flush())
	for block := range blocks {
		payload := make([]byte, LogicalBlockSize)
		_, err := branch.ReadAt(payload, int64(block*LogicalBlockSize))
		require.NoError(t, err)
		require.Equal(t, byte(block+1), payload[0])
	}
}

func TestBranchDirtyTailCapacityRejectsWholeRequestAndSurvivesRestart(t *testing.T) {
	base := bytes.Repeat([]byte{0x11}, 4*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	options := BranchOptions{MaxDirtyTailBytes: 2 * LogicalBlockSize}
	branch, err := OpenBranchWithOptions(path, identity, bytes.NewReader(base), options)
	require.NoError(t, err)

	first := bytes.Repeat([]byte{0x22}, LogicalBlockSize)
	_, err = branch.WriteAt(first, 0)
	require.NoError(t, err)
	before, err := os.Stat(path)
	require.NoError(t, err)

	// This request spans two mapping blocks, but only one record remains. The
	// capacity error must not leave the first half appended.
	n, err := branch.WriteAt(bytes.Repeat([]byte{0x33}, 2*LogicalBlockSize), LogicalBlockSize)
	require.Zero(t, n)
	var exhausted *DirtyTailCapacityError
	require.ErrorAs(t, err, &exhausted)
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.Equal(t, int64(LogicalBlockSize), exhausted.UsedBytes)
	require.Equal(t, int64(2*LogicalBlockSize), exhausted.RequestedBytes)
	require.Equal(t, int64(2*LogicalBlockSize), exhausted.LimitBytes)
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size())

	usage := branch.DirtyTailUsage()
	require.Equal(t, DirtyTailUsage{
		DirtyBytes: LogicalBlockSize, JournalBytes: branchRecordBytes,
		MaxBytes: 2 * LogicalBlockSize,
	}, usage)
	require.NoError(t, branch.Flush(), "capacity must not prevent flushing completed writes")
	require.NoError(t, branch.Close())

	reopened, err := OpenBranchWithOptions(path, identity, bytes.NewReader(base), options)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, usage, reopened.DirtyTailUsage())
	actual := make([]byte, 3*LogicalBlockSize)
	_, err = reopened.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, first, actual[:LogicalBlockSize])
	require.Equal(t, base[LogicalBlockSize:3*LogicalBlockSize], actual[LogicalBlockSize:])
}

func TestBranchDirtyTailCapacityAppliesToRepeatedAndZeroWrites(t *testing.T) {
	base := bytes.Repeat([]byte{0x55}, 3*LogicalBlockSize)
	branch, err := OpenBranchWithOptions(
		filepath.Join(t.TempDir(), "branch.log"), testBranchIdentity(int64(len(base))),
		bytes.NewReader(base), BranchOptions{MaxDirtyTailBytes: 2 * LogicalBlockSize},
	)
	require.NoError(t, err)
	defer branch.Close()

	_, err = branch.WriteAt(bytes.Repeat([]byte{0x66}, LogicalBlockSize), 0)
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{0x77}, LogicalBlockSize), 0)
	require.NoError(t, err, "repeated overwrites consume durable journal capacity")
	before := branch.DirtyTailUsage()
	require.ErrorIs(t, branch.WriteZeroes(LogicalBlockSize-1, 2), syscall.ENOSPC)
	require.Equal(t, before, branch.DirtyTailUsage(), "zero request must be admitted atomically")
	actual := make([]byte, LogicalBlockSize+1)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, byte(0x77), actual[LogicalBlockSize-1])
	require.Equal(t, byte(0x55), actual[LogicalBlockSize])
}

func TestBranchDirtyTailOptionsAllowRetirementAfterLimitReduction(t *testing.T) {
	base := make([]byte, 2*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{1}, 2*LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())

	reopened, err := OpenBranchWithOptions(path, identity, bytes.NewReader(base), BranchOptions{MaxDirtyTailBytes: LogicalBlockSize})
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.WriteAt([]byte{2}, 0)
	require.ErrorIs(t, err, syscall.ENOSPC)
	updates, err := reopened.DurableUpdates()
	require.NoError(t, err, "a lowered limit must not prevent terminal publication")
	require.Len(t, updates, 2)

	_, err = OpenBranchWithOptions(path+"-invalid", identity, bytes.NewReader(base), BranchOptions{MaxDirtyTailBytes: -1})
	require.ErrorContains(t, err, "non-negative")
	var exhausted *DirtyTailCapacityError
	require.False(t, errors.As(err, &exhausted))
}

func TestBranchCheckpointRemainsImmutableWhileWriterContinues(t *testing.T) {
	base := bytes.Repeat([]byte{0x11}, 3*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	branch, err := OpenBranch(path, testBranchIdentity(int64(len(base))), bytes.NewReader(base))
	require.NoError(t, err)

	first := bytes.Repeat([]byte{0x21}, LogicalBlockSize)
	second := bytes.Repeat([]byte{0x22}, LogicalBlockSize)
	_, err = branch.WriteAt(first, 0)
	require.NoError(t, err)
	_, err = branch.WriteAt(second, LogicalBlockSize)
	require.NoError(t, err)

	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err, "checkpoint must flush the captured boundary")
	require.Equal(t, uint64(2), checkpoint.Sequence())
	require.Equal(t, 2, checkpoint.RecordCount())
	require.Len(t, branch.records.chunks, 2, "checkpoint must rotate a partially filled append-index chunk")

	third := bytes.Repeat([]byte{0x33}, LogicalBlockSize)
	_, err = branch.WriteAt(third, 0)
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{0x34}, LogicalBlockSize), 2*LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, branch.Close())
	require.NoError(t, os.Remove(path), "the checkpoint must own the exact inode rather than reopen its path")

	blocks, err := checkpoint.Blocks()
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1}, blocks)
	actual := make([]byte, LogicalBlockSize)
	n, err := checkpoint.ReadBlock(0, actual)
	require.NoError(t, err)
	require.Equal(t, LogicalBlockSize, n)
	require.Equal(t, first, actual, "a later overwrite must not change the checkpoint")
	records, err := checkpoint.DurableRecords()
	require.NoError(t, err)
	require.Len(t, records, 2)
	require.Equal(t, second, records[1].Data)
	require.NoError(t, checkpoint.Close())
	_, err = checkpoint.Blocks()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestBranchCheckpointRejectsOversizedCompositeBeforeReadingPayload(t *testing.T) {
	base := make([]byte, 12*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch.log")
	branch, err := OpenBranch(path, testBranchIdentity(int64(len(base))), bytes.NewReader(base))
	require.NoError(t, err)
	for block := range 12 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{byte(block + 1)}, LogicalBlockSize), int64(block*LogicalBlockSize))
		require.NoError(t, err)
	}
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	require.NoError(t, branch.Close())
	require.NoError(t, os.Truncate(path, 0), "capacity must be rejected before checkpoint payload reads")

	_, err = checkpoint.DurableRecords()
	var tooLarge *CompositeTailTooLargeError
	require.ErrorAs(t, err, &tooLarge)
	require.Greater(t, tooLarge.Required, tooLarge.Limit)
	require.NoError(t, checkpoint.Close())
}

func testBranchIdentity(logicalSize int64) BranchIdentity {
	return BranchIdentity{
		Version: BranchFormatVersion, RootFSID: "rootfs-a", GenerationID: "generation-a", WriterEpoch: 1,
		LogicalSizeBytes: logicalSize, BaseRootDigest: digest.FromString("base-root").String(),
	}
}
