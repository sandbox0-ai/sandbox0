//go:build linux

package rootfsblock

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLiveBranchIndexPreservesOverwritesAndContinuesJournal(t *testing.T) {
	base := bytes.Repeat([]byte{7}, 4*LogicalBlockSize)
	path := filepath.Join(t.TempDir(), "branch")
	identity := testBranchIdentity(int64(len(base)))
	branch, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	for i := range 300 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{byte(i)}, LogicalBlockSize), int64(i%3*LogicalBlockSize))
		require.NoError(t, err)
	}
	_, err = branch.ExportLiveIndex()
	require.Error(t, err, "an unflushed cut cannot transfer")
	require.NoError(t, branch.Flush())
	index, err := branch.ExportLiveIndex()
	require.NoError(t, err)
	defer index.Close()
	_, err = index.WriteAt([]byte{0}, 0)
	require.Error(t, err, "the receiver cannot mutate the sealed cut")
	expected := make([]byte, len(base))
	_, err = branch.ReadAt(expected, 0)
	require.NoError(t, err)
	require.NoError(t, branch.Close())
	adopted, err := OpenBranchWithOptions(path, identity, bytes.NewReader(base), BranchOptions{LiveIndex: index})
	require.NoError(t, err)
	actual := make([]byte, len(base))
	_, err = adopted.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	sequence, err := adopted.DurableSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(300), sequence)
	_, err = adopted.WriteAt([]byte("successor"), 3*LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, adopted.Flush())
	require.NoError(t, adopted.Close())
	replayed, err := OpenBranch(path, identity, bytes.NewReader(base))
	require.NoError(t, err)
	defer replayed.Close()
	_, err = replayed.ReadAt(actual, 0)
	require.NoError(t, err)
	copy(expected[3*LogicalBlockSize:], "successor")
	require.Equal(t, expected, actual)
}

func TestLiveBranchIndexRejectsChangedCutAndUntrustedFiles(t *testing.T) {
	for _, scenario := range []string{"unsealed", "corrupt", "appended", "replacement inode", "other writer"} {
		t.Run(scenario, func(t *testing.T) {
			base := make([]byte, 2*LogicalBlockSize)
			path := filepath.Join(t.TempDir(), "branch")
			identity := testBranchIdentity(int64(len(base)))
			branch, err := OpenBranch(path, identity, bytes.NewReader(base))
			require.NoError(t, err)
			_, err = branch.WriteAt([]byte("before"), 0)
			require.NoError(t, err)
			require.NoError(t, branch.Flush())
			index, err := branch.ExportLiveIndex()
			require.NoError(t, err)
			defer index.Close()
			switch scenario {
			case "unsealed", "corrupt":
				data, err := io.ReadAll(io.NewSectionReader(index, 0, 1<<20))
				require.NoError(t, err)
				if scenario == "corrupt" {
					data[len(data)-1] ^= 1
				}
				copyIndex, err := newLiveIndexFile("")
				require.NoError(t, err)
				defer copyIndex.Close()
				_, err = copyIndex.Write(data)
				require.NoError(t, err)
				if scenario == "corrupt" {
					require.NoError(t, sealLiveIndex(copyIndex))
				}
				index = copyIndex
			case "appended":
				_, err = branch.WriteAt([]byte("after"), LogicalBlockSize)
				require.NoError(t, err)
				require.NoError(t, branch.Flush())
			case "other writer":
				identity.WriterEpoch++
			}
			require.NoError(t, branch.Close())
			if scenario == "replacement inode" {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.Rename(path, path+".old"))
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			_, err = OpenBranchWithOptions(path, identity, bytes.NewReader(base), BranchOptions{LiveIndex: index})
			require.Error(t, err)
		})
	}
}
