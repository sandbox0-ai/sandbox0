package rootfsblock

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestBranchRotationRecoveryKeepsUnselectedJournalCharged(t *testing.T) {
	for _, chooseNew := range []bool{false, true} {
		t.Run(map[bool]string{false: "old durable pointer", true: "new durable pointer"}[chooseNew], func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "old.wal")
			budget, err := NewDirtyTailBudget(8 * LogicalBlockSize)
			require.NoError(t, err)
			base := bytes.NewReader(make([]byte, 2*LogicalBlockSize))
			branch, err := OpenBranchWithOptions(path, testBranchIdentity(2*LogicalBlockSize), base, BranchOptions{NodeDirtyBudget: budget})
			require.NoError(t, err)
			defer branch.Close()
			_, err = branch.WriteAt([]byte("cut"), 0)
			require.NoError(t, err)
			checkpoint, err := branch.Checkpoint()
			require.NoError(t, err)
			cut := checkpoint.Cut()
			require.NoError(t, checkpoint.Close())
			_, err = branch.WriteAt([]byte("suffix"), LogicalBlockSize)
			require.NoError(t, err)
			var next BranchIdentity
			destination := filepath.Join(filepath.Dir(path), "new.wal")
			injected := errors.New("uncertain durable callback")
			err = branch.RotateCommitted(cut, base, digest.FromString("new").String(), destination, func(identity BranchIdentity) error { next = identity; return injected })
			require.ErrorIs(t, err, injected)
			require.Equal(t, int64(3*LogicalBlockSize), budget.Usage().UsedBytes)
			if chooseNew {
				require.NoError(t, branch.RecoverRotation(destination, next, base))
				require.Equal(t, int64(LogicalBlockSize), budget.Usage().UsedBytes)
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.NoError(t, branch.RecoverRotation(path, cut.Identity, base))
				require.Equal(t, int64(3*LogicalBlockSize), budget.Usage().UsedBytes)
				// Selection alone never authorizes deleting the possibly committed
				// candidate. The journal owner separately proves it obsolete.
				require.NoError(t, budget.ValidateOwnerDetached(destination))
				require.NoError(t, os.Remove(destination))
				require.NoError(t, budget.ReleaseOwner(destination))
				require.Equal(t, int64(2*LogicalBlockSize), budget.Usage().UsedBytes)
			}
			_, err = branch.WriteAt([]byte("resumed"), 0)
			require.NoError(t, err)
			require.NoError(t, branch.Flush())
			sequence, err := branch.DurableSequence()
			require.NoError(t, err)
			require.Equal(t, uint64(3), sequence)
		})
	}
}

func TestBranchRotationRecoveryRejectsDamagedSuffixAndCloseReleasesWaiter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.wal")
	base := bytes.NewReader(make([]byte, 2*LogicalBlockSize))
	branch, err := OpenBranch(path, testBranchIdentity(2*LogicalBlockSize), base)
	require.NoError(t, err)
	defer branch.Close()
	_, err = branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	require.NoError(t, checkpoint.Close())
	_, err = branch.WriteAt([]byte("suffix"), LogicalBlockSize)
	require.NoError(t, err)
	var next BranchIdentity
	destination := filepath.Join(filepath.Dir(path), "new.wal")
	require.Error(t, branch.RotateCommitted(cut, base, digest.FromString("new").String(), destination, func(identity BranchIdentity) error { next = identity; return errors.New("uncertain") }))
	// Recovery must not apply scanRecords' usual partial-tail repair to a
	// candidate that the installation journal may have selected.
	info, err := os.Stat(destination)
	require.NoError(t, err)
	truncatedSize := info.Size() - 1
	require.NoError(t, os.Truncate(destination, truncatedSize))
	require.ErrorContains(t, branch.RecoverRotation(destination, next, base), "incomplete")
	info, err = os.Stat(destination)
	require.NoError(t, err)
	require.Equal(t, truncatedSize, info.Size(), "ambiguous selected WAL must not be repaired in place")
	require.Equal(t, cut.Identity, branch.Identity())
	require.NotNil(t, branch.RotationFailure())
	wrong := next
	wrong.WriterEpoch++
	require.ErrorContains(t, branch.RecoverRotation(destination, wrong, base), "immutable branch owner")
	done := make(chan error, 1)
	go func() { _, writeErr := branch.WriteAt([]byte("blocked"), 0); done <- writeErr }()
	require.NoError(t, branch.Close())
	select {
	case writeErr := <-done:
		require.ErrorIs(t, writeErr, os.ErrClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("close did not release pending I/O")
	}
	for _, retained := range []string{path, destination} {
		_, err = os.Stat(retained)
		require.NoError(t, err)
	}
}

func TestBranchRotationReclaimsPreloadedTailAfterLimitReduction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.wal")
	base := bytes.NewReader(make([]byte, 2*LogicalBlockSize))
	identity := testBranchIdentity(2 * LogicalBlockSize)
	branch, err := OpenBranch(path, identity, base)
	require.NoError(t, err)
	for range 3 {
		_, err = branch.WriteAt([]byte("dirty"), 0)
		require.NoError(t, err)
	}
	require.NoError(t, branch.Flush())
	require.NoError(t, branch.Close())
	budget, err := NewDirtyTailBudgetWithReserve(2*LogicalBlockSize, LogicalBlockSize)
	require.NoError(t, err)
	require.NoError(t, budget.Preload(path, 3*LogicalBlockSize))
	branch, err = OpenBranchWithOptions(path, identity, base, BranchOptions{NodeDirtyBudget: budget})
	require.NoError(t, err)
	defer branch.Close()
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	cut := checkpoint.Cut()
	require.NoError(t, checkpoint.Close())
	require.NoError(t, branch.RotateCommitted(cut, base, digest.FromString("committed").String(), filepath.Join(filepath.Dir(path), "new.wal"), func(BranchIdentity) error { return nil }))
	require.Zero(t, budget.Usage().UsedBytes)
	_, err = branch.WriteAt([]byte("normal"), 0)
	require.NoError(t, err)
	_, err = branch.WriteAt([]byte("over limit"), 0)
	var capacity *DirtyTailCapacityError
	require.ErrorAs(t, err, &capacity)
	require.Equal(t, int64(LogicalBlockSize), budget.Usage().UsedBytes, "rotation must not allow new writes to borrow retirement reserve")
}
