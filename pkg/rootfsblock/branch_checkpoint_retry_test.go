package rootfsblock

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckpointRetryExcludesLaterOverwritesAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "branch.wal")
	base := make([]byte, 3*LogicalBlockSize)
	identity := testBranchIdentity(int64(len(base)))
	identity.SequenceBase = 50
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	for _, value := range []byte{11, 22} {
		_, err = branch.WriteAt(bytes.Repeat([]byte{value}, LogicalBlockSize), 0)
		require.NoError(t, err)
	}
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	require.Equal(t, uint64(52), cut.Sequence)
	require.NoError(t, checkpoint.Close())
	_, err = branch.WriteAt(bytes.Repeat([]byte{33}, LogicalBlockSize), 0)
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{44}, LogicalBlockSize), LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())
	branch, err = OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	defer branch.Close()
	retry, err := branch.CheckpointAtCut(cut)
	require.NoError(t, err)
	defer retry.Close()
	blocks, err := retry.Blocks()
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, blocks)
	payload := make([]byte, LogicalBlockSize)
	_, err = retry.ReadBlock(0, payload)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{22}, LogicalBlockSize), payload)
	_, err = branch.WriteAt(bytes.Repeat([]byte{55}, LogicalBlockSize), 0)
	require.NoError(t, err)
	_, err = retry.ReadBlock(0, payload)
	require.NoError(t, err)
	require.Equal(t, byte(22), payload[0])
	for _, mutate := range []func(*BranchCut){
		func(c *BranchCut) { c.End++ }, func(c *BranchCut) { c.Sequence = 49 }, func(c *BranchCut) { c.Sequence = 55 },
		func(c *BranchCut) { c.Identity.SequenceBase++ }, func(c *BranchCut) { c.Identity.RootFSID = "other" },
	} {
		invalid := cut
		mutate(&invalid)
		_, err = branch.CheckpointAtCut(invalid)
		require.Error(t, err)
	}
}
