package nomadruntime

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationJournalRequest(t *testing.T, registration RuntimeSlotRegistration) protocol.MigrationCapture {
	t.Helper()
	request := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		AllocationID: registration.AllocationID, NodeID: registration.NodeID, NodeUID: "node-uid", NodeBootID: registration.NodeBootID,
		ControlEndpoint: "unix:///var/lib/sandbox0/control/source.sock"}, OperationID: "migration-1", LifecycleEpoch: 2, SandboxID: "sandbox-1", SourceGeneration: 1,
		AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: strings.Repeat("cd", 32), ResourceLeaseDigest: strings.Repeat("ef", 32), ProcdInstanceID: "procd-1"}
	digest, err := request.Digest()
	require.NoError(t, err)
	return protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}
}

func TestMigrationJournalPersistsCustodyAcrossRestartAndFencesCleanup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	registration := testRuntimeSlotJournalRegistration(t, "source-slot")
	require.NoError(t, journal.Register(registration))
	capture := migrationJournalRequest(t, registration)
	require.NoError(t, journal.RecordMigrationCapture(capture))
	require.NoError(t, journal.Close())
	journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	record, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotMigrationJournalVersion, record.Version, "older ctld must reject custody it cannot interpret")
	require.Equal(t, capture, record.Migration.Capture)
	require.Equal(t, filepath.Join(filepath.Dir(path), "migration-images", capture.RequestDigest), record.Migration.ImageDirectory)
	_, err = journal.BeginCleanup(testRuntimeSlotJournalCleanup(registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, journal.RecordMigrationCapture(capture))
	require.NoError(t, journal.RecordMigrationCapture(capture))
	capture.State = protocol.MigrationCaptureUncertain
	require.ErrorIs(t, journal.RecordMigrationCapture(capture), errdefs.ErrFailedPrecondition)
	capture.Request.LifecycleEpoch++
	capture.RequestDigest, err = capture.Request.Digest()
	require.NoError(t, err)
	require.ErrorIs(t, journal.RecordMigrationCapture(capture), errdefs.ErrAlreadyExists)
	deleted, err := journal.Prune(time.Now().Add(24 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted, "source custody has no time-based discard")
}

func TestMigrationJournalRejectsOutcomeWithoutIntentAndTerminalRevival(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	registration := testRuntimeSlotJournalRegistration(t, "source-slot")
	require.NoError(t, journal.Register(registration))
	capture := migrationJournalRequest(t, registration)
	capture.State = protocol.MigrationCaptureComplete
	require.ErrorIs(t, journal.RecordMigrationCapture(capture), errdefs.ErrFailedPrecondition)
	capture.State = protocol.MigrationCaptureIntent
	_, err = journal.BeginCleanup(testRuntimeSlotJournalCleanup(registration))
	require.NoError(t, err)
	require.ErrorIs(t, journal.RecordMigrationCapture(capture), errdefs.ErrFailedPrecondition)
}

func TestMigrationCustodyPrivateRPCBindsNodeAndExactRequest(t *testing.T) {
	root := t.TempDir()
	journal, err := newRuntimeSlotJournal(filepath.Join(root, "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	registration := testRuntimeSlotJournalRegistration(t, "source-slot")
	require.NoError(t, journal.Register(registration))
	capture := migrationJournalRequest(t, registration)
	daemon := &nodeRuntime{migrationStaging: testMigrationStagingGuard{}, journal: journal, clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: capture.Request.Target.NodeUID}
	socket := filepath.Join(root, "rpc.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, daemon)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(socket)
	require.NoError(t, err)
	require.NoError(t, client.RecordMigrationCapture(t.Context(), capture))
	recovered, err := client.GetMigrationCapture(t.Context(), registration.SlotID)
	require.NoError(t, err)
	require.Equal(t, capture, recovered.Capture)
	capture.State = protocol.MigrationCaptureComplete
	require.NoError(t, client.RecordMigrationCapture(t.Context(), capture))
	executor := &nodeRuntimeChannelExecutor{clusterID: registration.ClusterID, nodeID: registration.NodeID,
		nodeUID: capture.Request.Target.NodeUID, cleaner: daemon}
	observed, err := executor.CaptureMigration(t.Context(), capture.Request)
	require.NoError(t, err, "ctld retains completed evidence without a running driver socket")
	require.Equal(t, capture, *observed)
	changed := capture
	changed.Request.Target.NodeUID = "another-node"
	changed.RequestDigest, err = changed.Request.Digest()
	require.NoError(t, err)
	require.ErrorIs(t, client.RecordMigrationCapture(context.Background(), changed), errdefs.ErrPermissionDenied)
	changed = capture
	changed.Request.Target.NodeBootID = "rebooted"
	changed.RequestDigest, err = changed.Request.Digest()
	require.NoError(t, err)
	require.ErrorIs(t, client.RecordMigrationCapture(t.Context(), changed), errdefs.ErrFailedPrecondition)
}

func TestMigrationJournalBoundsUnresolvedCapturesAcrossRetries(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	for index := range maxMigrationImageCustodies + 1 {
		registration := testRuntimeSlotJournalRegistration(t, fmt.Sprintf("source-%d", index))
		require.NoError(t, journal.Register(registration))
		capture := migrationJournalRequest(t, registration)
		if index == maxMigrationImageCustodies {
			require.ErrorIs(t, journal.RecordMigrationCapture(capture), errdefs.ErrResourceExhausted)
			continue
		}
		require.NoError(t, journal.RecordMigrationCapture(capture))
		capture.State = protocol.MigrationCaptureUncertain
		require.NoError(t, journal.RecordMigrationCapture(capture))
		require.NoError(t, journal.RecordMigrationCapture(capture))
	}
}
