//go:build linux

package migrationstaging

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This probe owns a disposable loop-mounted filesystem and never selects an
// existing block device. It demonstrates kernel enforcement, including open
// unlinked files, rather than accepting a mocked free-space counter as proof.
func TestPrivilegedXFSStagingQuota(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_STAGING_QUOTA") != "1" {
		t.Skip("requires an isolated Linux host with root, loop mounts and xfsprogs")
	}
	require.Zero(t, os.Geteuid())
	run := func(name string, args ...string) {
		t.Helper()
		output, err := exec.CommandContext(t.Context(), name, args...).CombinedOutput()
		require.NoError(t, err, "%s: %s", name, output)
	}
	directory := t.TempDir()
	image, mount := filepath.Join(directory, "quota.img"), filepath.Join(directory, "mnt")
	file, err := os.Create(image)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(512<<20))
	require.NoError(t, file.Close())
	require.NoError(t, os.Mkdir(mount, 0700))
	run("mkfs.xfs", "-f", image)
	run("mount", "-o", "loop,prjquota", image, mount)
	t.Cleanup(func() { require.NoError(t, unix.Unmount(mount, 0)) })
	root := filepath.Join(mount, "staging")
	require.NoError(t, os.Mkdir(root, 0700))
	require.False(t, strings.ContainsAny(root, " \t\n'\""), "test path must fit xfs_quota's command grammar")
	limits := Limits{ProjectID: 1395654657, Bytes: 8 << 20, Inodes: 64}
	quota := func(command string) { run("xfs_quota", "-x", "-c", command, mount) }
	quota(fmt.Sprintf("project -s -p %s %d", root, limits.ProjectID))
	quota(fmt.Sprintf("limit -p bhard=8m ihard=64 %d", limits.ProjectID))
	guard, err := Open(root, limits)
	require.NoError(t, err)
	require.NoError(t, guard.Admit(root))
	// A legacy child can predate the parent's quota assignment. Checking only
	// the parent would approve writes that bypass the promised pool limit.
	legacy := filepath.Join(root, "legacy.img")
	require.NoError(t, os.WriteFile(legacy, []byte("legacy"), 0600))
	run("xfs_io", "-c", "chproj 0", legacy)
	_, err = Open(root, limits)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	run("xfs_io", "-c", fmt.Sprintf("chproj %d", limits.ProjectID), legacy)
	require.NoError(t, os.Remove(legacy))
	link := filepath.Join(root, "outside-link")
	require.NoError(t, os.Symlink(mount, link))
	_, err = Open(root, limits)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NoError(t, os.Remove(link))
	relocated := root + "-retained"
	require.NoError(t, os.Rename(root, relocated))
	require.NoError(t, os.Mkdir(root, 0700))
	quota(fmt.Sprintf("project -s -p %s %d", root, limits.ProjectID))
	require.ErrorIs(t, guard.Verify(root), errdefs.ErrFailedPrecondition, "the same project ID cannot bless a replacement directory")
	require.NoError(t, os.Remove(root))
	require.NoError(t, os.Rename(relocated, root))
	require.NoError(t, guard.Verify(root))
	first, second := filepath.Join(root, "source"), filepath.Join(root, "destination")
	require.NoError(t, os.Mkdir(first, 0700))
	require.NoError(t, os.Mkdir(second, 0700))
	a, err := os.Create(filepath.Join(first, "pages.img"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close() })
	b, err := os.Create(filepath.Join(second, "pages.img"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	require.NoError(t, unix.Fallocate(int(a.Fd()), 0, 0, 6<<20))
	require.ErrorIs(t, guard.AdmitBudget(root, 3<<20, 1), errdefs.ErrResourceExhausted, "reject the known image before starting a write")
	require.NoError(t, guard.AdmitBudget(root, 1<<20, 1))
	// XFS returns ENOSPC for project quotas (xfs_trans_dqresv), unlike
	// user/group quotas. Prove the enclosing filesystem still has capacity.
	require.ErrorIs(t, unix.Fallocate(int(b.Fd()), 0, 0, 3<<20), unix.ENOSPC, "both image directories share one hard byte budget")
	var free unix.Statfs_t
	require.NoError(t, unix.Statfs(mount, &free))
	require.Greater(t, free.Bavail*uint64(free.Bsize), uint64(32<<20))
	charged, err := inspect(root, limits.ProjectID)
	require.NoError(t, err)
	require.LessOrEqual(t, charged.blocks, charged.hardBlocks)
	out, err := os.Create(filepath.Join(mount, "outside-project-blocks"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close() })
	require.NoError(t, unix.Fallocate(int(out.Fd()), 0, 0, 3<<20))
	require.NoError(t, os.Remove(a.Name()))
	require.ErrorIs(t, unix.Fallocate(int(b.Fd()), 0, 0, 3<<20), unix.ENOSPC, "unlink cannot release an open image's charged blocks")
	require.NoError(t, a.Close())
	// XFS may defer inode inactivation after the last close. A recovery
	// retry must wait for kernel accounting, never reset it optimistically.
	require.Eventually(t, func() bool {
		return unix.Fallocate(int(b.Fd()), 0, 0, 3<<20) == nil
	}, 10*time.Second, 50*time.Millisecond)
	reopened, err := Open(root, limits)
	require.NoError(t, err, "a new primary verifies the same kernel budget without resetting it")
	require.NoError(t, reopened.Verify(root))
	require.ErrorIs(t, reopened.AdmitBudget(root, 4096, limits.Inodes), errdefs.ErrResourceExhausted, "directories and existing files consume inode capacity too")
	exhausted := false
	for i := 0; i < 80; i++ {
		f, err := os.Create(filepath.Join(first, fmt.Sprintf("inode-%d", i)))
		if err != nil {
			require.ErrorIs(t, err, unix.ENOSPC)
			exhausted = true
			break
		}
		require.NoError(t, f.Close())
	}
	require.True(t, exhausted, "empty files cannot bypass the inode budget")
	require.ErrorIs(t, reopened.Admit(root), errdefs.ErrResourceExhausted)
	require.NoError(t, reopened.Verify(root))
	require.NoError(t, os.WriteFile(filepath.Join(mount, "outside-project"), []byte("journal remains writable"), 0600))
	quota(fmt.Sprintf("limit -p bhard=9m %d", limits.ProjectID))
	require.ErrorIs(t, guard.Verify(root), errdefs.ErrFailedPrecondition)
	quota(fmt.Sprintf("limit -p bhard=8m %d", limits.ProjectID))
	quota("disable -p")
	require.ErrorIs(t, guard.Verify(root), errdefs.ErrFailedPrecondition, "accounting without enforcement is insufficient")
}
