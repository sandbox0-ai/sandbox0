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

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCheckpointImageCancellationClosesDownloadAndReusesPhysicalCleanup(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		for _, phase := range []string{"undelivered", "downloading", "partial", "prepared", "intent-restart"} {
			t.Run(string(kind)+"/"+phase, func(t *testing.T) {
				d, image, runtime := checkpointDestinationFixture(t, kind)
				request := protocol.CheckpointImageCancelRequest{Image: image}
				var downloadDone chan error
				switch phase {
				case "downloading":
					runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
					downloadDone = make(chan error, 1)
					go func() { _, err := d.PrepareMigrationImage(t.Context(), image); downloadDone <- err }()
					select {
					case <-runtime.started:
					case <-time.After(time.Second):
						t.Fatal("download did not start")
					}
				case "partial":
					runtime.fail = true
					_, err := d.PrepareMigrationImage(t.Context(), image)
					require.ErrorContains(t, err, "interrupted image transfer")
				case "prepared", "intent-restart":
					_, err := d.PrepareMigrationImage(t.Context(), image)
					require.NoError(t, err)
				}
				if phase == "intent-restart" {
					require.NoError(t, d.journal.recordCheckpointImageCancellation(request, nil))
					path := d.journal.db.Path()
					require.NoError(t, d.journal.Close())
					var err error
					d.journal, err = newRuntimeSlotJournal(path, time.Hour)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
				}
				proof, err := d.CancelCheckpointImage(t.Context(), request)
				require.NoError(t, err)
				require.NoError(t, proof.ValidateFor(request))
				if downloadDone != nil {
					require.ErrorIs(t, <-downloadDone, context.Canceled)
				}
				record, err := d.journal.Get(image.Target.SlotID)
				require.NoError(t, err)
				require.Equal(t, runtimeSlotCheckpointCancelJournalVersion, record.Version)
				require.NoDirExists(t, record.MigrationDestination.ImageDirectory)
				require.False(t, record.hasMigrationImageCustody())
				require.False(t, record.retainsMigrationCountAdmission())
				require.Nil(t, record.Cleanup, "image absence does not release the carrier")
				require.Nil(t, record.Proof)
				_, err = d.PrepareMigrationImage(t.Context(), image)
				require.Error(t, err, "late preparation cannot reopen canceled authority")
				for _, version := range []int{runtimeSlotMigrationJournalVersion, runtimeSlotCheckpointRestoreJournalVersion} {
					old := record
					old.Version = version
					payload, err := json.Marshal(old)
					require.NoError(t, err)
					_, err = decodeRuntimeSlotJournalRecord(payload)
					require.Error(t, err, "older envelopes cannot erase a tombstone")
				}
				cleanup := testRuntimeSlotJournalCleanup(record.Registration)
				cleanup.NodeUID, cleanup.Resources = d.nodeUID, image.Resources
				digest, err := image.Resources.Digest()
				require.NoError(t, err)
				cleanup.ResourceLeaseDigest = strings.TrimPrefix(digest, "sha256:")
				d.mounter, d.runtimeSlotNetwork = &fakeMounter{}, newFakeCtldNetwork(t)
				cgroups := &fakeRuntimeResourceCgroup{}
				d.resourceCgroups = cgroups
				d.config.RootFSConsumerMountRoot = filepath.Dir(record.Registration.StableMount)
				d.config.RootFSConsumerNetNSRoot = filepath.Dir(record.Registration.NetNSPath)
				require.NoError(t, os.Remove(record.Registration.NetNSPath))
				_, err = d.CleanupRuntimeSlot(t.Context(), cleanup)
				require.ErrorContains(t, err, "resource cgroup remains present")
				cgroups.removeOK = true
				physical, err := d.CleanupRuntimeSlot(t.Context(), cleanup)
				require.NoError(t, err)
				require.NoError(t, physical.Validate())
				again, err := d.CancelCheckpointImage(t.Context(), request)
				require.NoError(t, err)
				require.Equal(t, proof, again)
				_, err = runtime.store.Download(t.Context(), image.Receipt.Binding, image.Receipt.Reference, filepath.Join(t.TempDir(), "retained"))
				require.NoError(t, err, "destination cancellation cannot delete retained regional bytes")
			})
		}
	}
}

func TestCheckpointImageCancellationCannotReplaceRestoreIntent(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		d, image, runtime := checkpointDestinationFixture(t, runtimecontrol.CheckpointResume)
		d, observation, _, _ := migrationRestoreForImageFixture(t, d, image, runtime)
		request := protocol.CheckpointImageCancelRequest{Image: image}
		if cancelFirst {
			require.NoError(t, d.journal.recordCheckpointImageCancellation(request, nil))
			require.ErrorIs(t, d.RecordMigrationRestore(t.Context(), observation), errdefs.ErrFailedPrecondition)
			require.ErrorIs(t, d.journal.recordMigrationRestore(observation), errdefs.ErrFailedPrecondition)
		} else {
			require.NoError(t, d.RecordMigrationRestore(t.Context(), observation))
			_, err := d.CancelCheckpointImage(t.Context(), request)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			record, err := d.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			require.Nil(t, record.MigrationDestination.Cancellation)
			require.DirExists(t, record.MigrationDestination.ImageDirectory)
		}
	}
}

func TestCheckpointImageCancellationChannelBindsExactAuthority(t *testing.T) {
	d, image, _ := checkpointDestinationFixture(t, runtimecontrol.CheckpointFork)
	request := protocol.CheckpointImageCancelRequest{Image: image}
	command, err := protocol.NewNodeChannelCheckpointImageCancelCommand(request)
	require.NoError(t, err)
	require.NoError(t, command.Validate())
	changed := command
	changed.Target.NodeBootID = "wrong-boot"
	require.Error(t, changed.Validate())
	changed = command
	changed.MigrationImagePrepare = &image
	require.Error(t, changed.Validate(), "command cannot grant preparation and cancellation together")
	executor := &nodeRuntimeChannelExecutor{clusterID: d.clusterID, nodeID: d.nodeID, nodeUID: "wrong-node", cleaner: d}
	_, err = executor.CancelCheckpointImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
	executor.nodeUID = d.nodeUID
	proof, err := executor.CancelCheckpointImage(t.Context(), request)
	require.NoError(t, err)
	response := protocol.NodeChannelResult{Version: protocol.NodeChannelVersion, Kind: command.Kind, RequestID: command.RequestID, CheckpointImageCancel: proof}
	require.NoError(t, response.ValidateFor(command))
	proof.ImageAbsent = false
	require.Error(t, response.ValidateFor(command))
	proof.ImageAbsent = true
	proof.RequestDigest = strings.Repeat("a", 64)
	require.Error(t, response.ValidateFor(command))
	legacy := request
	legacy.Image.Checkpoint = nil
	_, err = protocol.NewNodeChannelCheckpointImageCancelCommand(legacy)
	require.Error(t, err, "ordinary migration keeps its existing failure protocol")
}
