//go:build linux

package nomadruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

// Run on a separately provisioned physical fixture machine. PostgreSQL and S3
// are the source host's services through private loopback SSH forwards, never
// copied local databases/buckets. Source crash fencing must finish first.
func TestPrivilegedRemoteAutogrowReplacementMachineRestoresPublishedCut(t *testing.T) {
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REPLACEMENT_RESTORE") != "1" {
		t.Skip("requires separate physical replacement fixture and remote published cut")
	}
	require.Zero(t, os.Geteuid())
	require.Equal(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_UNIQUE"))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	data, err := os.ReadFile(os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_RECEIPT"))
	require.NoError(t, err)
	var receipt autogrowOwnerLossReceipt
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.True(t, strings.HasPrefix(receipt.Bucket, "s0-autogrow-"), "only the dedicated fixture bucket may be cleaned")
	require.NotEmpty(t, receipt.MachineBoot)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	require.NotEqual(t, receipt.MachineBoot, strings.TrimSpace(string(boot)), "replacement must be another machine/boot")
	_, err = os.Stat(receipt.Root)
	require.ErrorIs(t, err, os.ErrNotExist, "source session/WAL directory must not be copied to replacement")
	nodeUID := os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REPLACEMENT_NODE_UID")
	require.NotEmpty(t, nodeUID)
	require.NotEqual(t, receipt.Stage.Identity.NodeUID, nodeUID)
	root, err := os.MkdirTemp("", "sandbox0-replacement-autogrow-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		mounts, err := mountinfo.GetMounts(nil)
		require.NoError(t, err)
		for _, mount := range mounts {
			require.False(t, mount.Mountpoint == root || strings.HasPrefix(mount.Mountpoint, root+"/"), "retain mounted replacement fixture")
		}
		require.NoError(t, os.RemoveAll(root))
	})
	require.NoError(t, os.Mkdir(filepath.Join(root, "evidence"), 0700))
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: os.Getenv("INTEGRATION_DATABASE_URL"), Schema: "scheduler", MaxConns: 10})
	require.NoError(t, err)
	defer pool.Close()
	var database string
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database))
	require.True(t, strings.HasPrefix(database, "s0_rootfs_autogrow_owner_loss_"))
	store := sandboxstore.NewPGSandboxStore(pool)
	grant, err := store.GetRootFSWriterGrant(ctx, receipt.Stage.Identity.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateRetired, grant.State)
	filesystem, err := store.GetRootFSFilesystem(ctx, receipt.Stage.Identity.RootFSID)
	require.NoError(t, err)
	require.Equal(t, receipt.CommittedGeneration, filesystem.HeadGenerationID)
	objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: receipt.Bucket, Region: "us-east-1",
		Endpoint: os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"), AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY")})
	require.NoError(t, err)
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		for {
			items, more, _, err := objects.List("", "", "", "", 1000)
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, objects.Delete(item.Key))
			}
			if !more {
				break
			}
		}
	})
	client, _ := autogrowRemoteAuthorityForNode(t, root, store, nodeUID)
	autogrowVerifyColdRestore(t, ctx, root, conditional, store, client, receipt.Stage)
	t.Logf("replacement_machine_boot=%s source_machine_boot=%s replacement_node_uid=%s remote_cut_sequence=%d source_wal_absent=true fresh_cache_device=true remote_bytes_verified=%d unpublished_suffix_absent=true", strings.TrimSpace(string(boot)), receipt.MachineBoot, nodeUID, receipt.CommittedSequence, 600<<20)
}
