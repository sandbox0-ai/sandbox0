package rootfsblock

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestBranchRotationPreservesSuffixAndSequenceAcrossRestart(t *testing.T) {
	objects := newMemoryObjects()
	initial := bytes.Repeat([]byte{0x11}, 4*LogicalBlockSize)
	base, err := BuildMaterializedGeneration(t.Context(), bytes.NewReader(initial), int64(len(initial)), objects, BuildOptions{})
	require.NoError(t, err)
	reader, err := NewReader(objects, base.Descriptor, 0)
	require.NoError(t, err)
	identity := testBranchIdentity(int64(len(initial)))
	identity.BaseRootDigest = base.Descriptor.MappingRoot.RootDigest
	budget, err := NewDirtyTailBudget(32 * LogicalBlockSize)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "first.log")
	branch, err := OpenBranchWithOptions(path, identity, reader, BranchOptions{MaxDirtyTailBytes: 16 * LogicalBlockSize, NodeDirtyBudget: budget})
	require.NoError(t, err)
	defer branch.Close()
	for i := range 5 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{byte(i + 1)}, LogicalBlockSize), 0)
		require.NoError(t, err)
	}
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	committed, err := BuildIncrementalGenerationFromBlockReader(t.Context(), objects, base.Descriptor, checkpoint, objects, BuildOptions{})
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{0xcc}, LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, branch.WriteZeroes(LogicalBlockSize, LogicalBlockSize))
	committedReader, err := NewReader(objects, committed.Descriptor, 0)
	require.NoError(t, err)
	destination := filepath.Join(filepath.Dir(path), "second.log")
	called := false
	callback := func(next BranchIdentity) error {
		called = true
		require.Equal(t, uint64(5), next.SequenceBase)
		// Both journals must be charged while a durable installation is made.
		require.Equal(t, int64(9*LogicalBlockSize), budget.Usage().UsedBytes)
		_, statErr := os.Stat(path)
		require.NoError(t, statErr)
		return nil
	}
	require.ErrorContains(t, branch.RotateCommitted(cut, committedReader, committed.Descriptor.MappingRoot.RootDigest, destination, callback), "outstanding checkpoint")
	require.False(t, called)
	require.NoError(t, checkpoint.Close())
	require.NoError(t, branch.RotateCommitted(cut, committedReader, committed.Descriptor.MappingRoot.RootDigest, destination, callback))
	require.True(t, called)
	require.Equal(t, int64(2*LogicalBlockSize), branch.DirtyTailUsage().DirtyBytes)
	require.Equal(t, int64(2*LogicalBlockSize), budget.Usage().UsedBytes)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	sequence, err := branch.DurableSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(7), sequence)
	nextIdentity := branch.Identity()
	require.NoError(t, branch.Close())
	reopened, err := OpenBranchWithOptions(destination, nextIdentity, committedReader, BranchOptions{NodeDirtyBudget: budget})
	require.NoError(t, err)
	defer reopened.Close()
	actual := make([]byte, len(initial))
	_, err = reopened.ReadAt(actual, 0)
	require.NoError(t, err)
	expected := append([]byte(nil), initial...)
	copy(expected, bytes.Repeat([]byte{0xcc}, LogicalBlockSize))
	clear(expected[LogicalBlockSize : 2*LogicalBlockSize])
	require.Equal(t, expected, actual)
	_, err = reopened.WriteAt([]byte("next"), 2*LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, reopened.Flush())
	sequence, err = reopened.DurableSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(8), sequence)
	// A published snapshot retains the value at the earlier cut.
	_, err = committedReader.ReadAt(actual[:LogicalBlockSize], 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{5}, LogicalBlockSize), actual[:LogicalBlockSize])
}

func TestBranchRotationRetainsBothJournalsOnAmbiguousInstallation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "original.log")
	identity := testBranchIdentity(2 * LogicalBlockSize)
	initial := make([]byte, 2*LogicalBlockSize)
	branch, err := OpenBranch(path, identity, bytes.NewReader(initial))
	require.NoError(t, err)
	defer branch.Close()
	_, err = branch.WriteAt(bytes.Repeat([]byte{1}, LogicalBlockSize), 0)
	require.NoError(t, err)
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	require.NoError(t, checkpoint.Close())
	_, err = branch.WriteAt([]byte("suffix"), LogicalBlockSize)
	require.NoError(t, err)
	destination := filepath.Join(filepath.Dir(path), "rotated.log")
	newBase := append([]byte(nil), initial...)
	copy(newBase, bytes.Repeat([]byte{1}, LogicalBlockSize))
	var installed BranchIdentity
	injected := errors.New("local commit acknowledgement lost")
	err = branch.RotateCommitted(cut, bytes.NewReader(newBase), digest.FromString("new-base").String(), destination, func(next BranchIdentity) error {
		installed = next
		return injected
	})
	require.ErrorIs(t, err, injected)
	for _, journal := range []string{path, destination} {
		_, err = os.Stat(journal)
		require.NoError(t, err)
	}
	_, err = branch.WriteAt([]byte("must not append"), 0)
	require.ErrorIs(t, err, injected)
	// If durable state chose the successor, it contains the complete suffix.
	reopened, err := OpenBranch(destination, installed, bytes.NewReader(newBase))
	require.NoError(t, err)
	defer reopened.Close()
	actual := make([]byte, len(initial))
	_, err = reopened.ReadAt(actual, 0)
	require.NoError(t, err)
	copy(newBase[LogicalBlockSize:], []byte("suffix"))
	require.Equal(t, newBase, actual)
}

func TestBranchRotationDoesNotConsumeProtectedRetirementBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "original.log")
	budget, err := NewDirtyTailBudgetWithReserve(4*LogicalBlockSize, LogicalBlockSize)
	require.NoError(t, err)
	branch, err := OpenBranchWithOptions(path, testBranchIdentity(2*LogicalBlockSize), bytes.NewReader(make([]byte, 2*LogicalBlockSize)), BranchOptions{NodeDirtyBudget: budget})
	require.NoError(t, err)
	defer branch.Close()
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	require.NoError(t, checkpoint.Close())
	for range 2 {
		_, err = branch.WriteAt([]byte("suffix"), 0)
		require.NoError(t, err)
	}
	destination := filepath.Join(filepath.Dir(path), "rotated.log")
	err = branch.RotateCommitted(cut, bytes.NewReader(make([]byte, 2*LogicalBlockSize)), digest.FromString("new-base").String(), destination, func(BranchIdentity) error {
		t.Fatal("installation must not occur when the suffix cannot be budgeted")
		return nil
	})
	var capacity *DirtyTailCapacityError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, int64(2*LogicalBlockSize), budget.Usage().UsedBytes)
	_, err = branch.WriteAt([]byte("still usable"), 0)
	require.NoError(t, err)
	_, err = os.Stat(destination)
	require.ErrorIs(t, err, os.ErrNotExist)
}
