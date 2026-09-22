package nomadruntime

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	authority "github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type regionalAdoptionDeliveryStore struct {
	authority.Store
	mu                           sync.Mutex
	slot                         sandboxstore.RuntimeSlot
	want                         protocol.MigrationAdoptionReceipt
	committed                    *protocol.MigrationAdoptionReceipt
	calls, failBefore, failAfter int
	command                      *protocol.MigrationAdoptionRequest
	reads                        int
}

func (s *regionalAdoptionDeliveryStore) GetRuntimeSlot(_ context.Context, id string) (*sandboxstore.RuntimeSlot, error) {
	if id != s.slot.ID {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	return &s.slot, nil
}
func (s *regionalAdoptionDeliveryStore) GetNomadSandboxMigrationAdoptionForSlot(_ context.Context, slot string) (*protocol.MigrationAdoptionRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if slot != s.slot.ID {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	if s.command == nil {
		return nil, nil
	}
	copy := *s.command
	return &copy, nil
}

func (s *regionalAdoptionDeliveryStore) CommitNomadSandboxMigrationAdoption(_ context.Context, r protocol.MigrationAdoptionRequest, p protocol.MigrationAdoptionProof) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	receipt := protocol.MigrationAdoptionReceipt{Request: r, Proof: p}
	if receipt != s.want {
		return sandboxstore.ErrNomadSandboxMigrationConflict
	}
	if s.failBefore > 0 {
		s.failBefore--
		return errdefs.ErrUnavailable
	}
	s.committed = &receipt
	if s.failAfter > 0 {
		s.failAfter--
		return errdefs.ErrUnavailable
	}
	return nil
}

func installAdoptionReporter(t *testing.T, d *nodeRuntime, r protocol.MigrationAdoptionRequest) *regionalAdoptionDeliveryStore {
	t.Helper()
	target := r.Target
	digest, err := r.Digest()
	require.NoError(t, err)
	s := &regionalAdoptionDeliveryStore{want: protocol.MigrationAdoptionReceipt{Request: r, Proof: protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}}, slot: sandboxstore.RuntimeSlot{ID: target.SlotID, ClusterID: target.ClusterID, NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID, AllocationID: target.AllocationID}}
	handler, err := authority.NewHandler(authority.HandlerConfig{Store: s, Verifier: regionalFinalizationVerifier{nodeauth.Identity{ClusterID: target.ClusterID, NodeID: target.NodeID, NodeUID: target.NodeUID}}, HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca, token := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	require.NoError(t, os.WriteFile(token, []byte("test-node-token"), 0600))
	d.registrationAuthority, err = newRegistrationAbortAuthority(Config{RootFSAuthorityURL: server.URL, RootFSAuthorityCAFile: ca, RootFSAuthorityTokenFile: token})
	require.NoError(t, err)
	return s
}

func TestMigrationAdoptionDeliveryRetainsReceiptAcrossLostAcknowledgementAndRestart(t *testing.T) {
	d, request, _, runner := migrationAdoptionNodeFixture(t)
	proof, err := d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	s := installAdoptionReporter(t, d, request)
	s.failBefore = 1
	s.failAfter = 1
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	cleanup := testRuntimeSlotJournalCleanup(record.Registration)
	_, err = d.journal.BeginCleanup(cleanup)
	require.NoError(t, err)
	require.NoError(t, d.journal.CompleteCleanup(cleanup, testRuntimeSlotJournalProof(t, cleanup)))
	for range 2 {
		d.migrationAdoptionAfter = ""
		completed, err := d.reconcileMigrationAdoptions(t.Context())
		require.Error(t, err)
		require.Zero(t, completed)
		deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
		require.NoError(t, err)
		require.Zero(t, deleted)
		record, err = d.journal.Get(request.Target.SlotID)
		require.NoError(t, err)
		require.Nil(t, record.MigrationDestination.Adoption.RegionalAcknowledgement)
	}
	// Only the node journal and the region's idempotent receipt survive the
	// reconnect. No driver response or in-memory retry state is required.
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	d.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
	d.migrationAdoptionAfter = ""
	completed, err := d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.NoError(t, record.MigrationDestination.Adoption.RegionalAcknowledgement.ValidateFor(protocol.MigrationAdoptionReceipt{Request: request, Proof: *proof}))
	s.mu.Lock()
	require.Equal(t, 3, s.calls)
	require.Equal(t, &s.want, s.committed)
	s.mu.Unlock()
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
}

func TestMigrationAdoptionDeliveryPendingCommandPreservesCustodyAndExistingIntentCanFinish(t *testing.T) {
	d, request, _, runner := migrationAdoptionNodeFixture(t)
	s := installAdoptionReporter(t, d, request)
	completed, err := d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Zero(t, completed)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationDestination.Adoption)
	require.NoError(t, d.journal.recordMigrationAdoption(request, nil))
	runner.setState("stopped")
	d.migrationAdoptionAfter = ""
	completed, err = d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationDestination.Adopted())
	require.NotNil(t, record.MigrationDestination.Adoption.RegionalAcknowledgement)
	require.NoDirExists(t, record.MigrationDestination.ImageDirectory)
	s.mu.Lock()
	require.Equal(t, 1, s.calls)
	s.mu.Unlock()
	completed, err = d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Zero(t, completed)
}

func TestMigrationAdoptionAcknowledgementRequiresExactDurableReceiptAndNewEnvelope(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	proof, err := d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	receipt := protocol.MigrationAdoptionReceipt{Request: request, Proof: *proof}
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	record.Version = runtimeSlotLegacyAdoptionJournalVersion
	payload, err := json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.NoError(t, err, "legacy unacknowledged history remains readable")
	ack := protocol.MigrationAdoptionAcknowledgement{RequestDigest: proof.RequestDigest}
	wrong := receipt
	wrong.Request.OperationID = "other-operation"
	wrong.Proof.RequestDigest, _ = wrong.Request.Digest()
	require.Error(t, d.journal.acknowledgeMigrationAdoption(wrong, protocol.MigrationAdoptionAcknowledgement{RequestDigest: wrong.Proof.RequestDigest}))
	require.NoError(t, d.journal.acknowledgeMigrationAdoption(receipt, ack))
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotAdoptionJournalVersion, record.Version)
	record.Version = runtimeSlotLegacyAdoptionJournalVersion
	payload, err = json.Marshal(record)
	require.NoError(t, err)
	_, err = decodeRuntimeSlotJournalRecord(payload)
	require.Error(t, err, "old envelope cannot hide regional acknowledgement")
}

func TestMigrationAdoptionPendingReceiptConsumesBoundedCustodyUntilAcknowledged(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	proof, err := d.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	first := testRuntimeSlotJournalRegistration(t, "new-source-a")
	second := testRuntimeSlotJournalRegistration(t, "new-source-b")
	require.NoError(t, d.journal.Register(first))
	require.NoError(t, d.journal.Register(second))
	require.NoError(t, d.journal.RecordMigrationCapture(migrationJournalRequest(t, first)))
	require.ErrorIs(t, d.journal.RecordMigrationCapture(migrationJournalRequest(t, second)), errdefs.ErrResourceExhausted)
	receipt := protocol.MigrationAdoptionReceipt{Request: request, Proof: *proof}
	require.NoError(t, d.journal.acknowledgeMigrationAdoption(receipt, protocol.MigrationAdoptionAcknowledgement{RequestDigest: proof.RequestDigest}))
	require.NoError(t, d.journal.RecordMigrationCapture(migrationJournalRequest(t, second)))
}

func TestMigrationAdoptionCandidateScanMakesBoundedProgressPastUnrelatedSlots(t *testing.T) {
	d, request, _, _ := migrationAdoptionNodeFixture(t)
	require.NoError(t, d.journal.recordMigrationAdoption(request, nil))
	for i := range 129 {
		require.NoError(t, d.journal.Register(testRuntimeSlotJournalRegistration(t, fmt.Sprintf("a-%03d", i))))
	}
	rows, cursor, err := d.journal.migrationAdoptionCandidates("")
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, "a-127", cursor)
	rows, cursor, err = d.journal.migrationAdoptionCandidates(cursor)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, request, rows[0].MigrationDestination.Adoption.Request)
	require.Empty(t, cursor)
}

func TestMigrationAdoptionDeliveryRecoversCommandLostBeforeLocalIntent(t *testing.T) {
	d, request, runtime, runner := migrationAdoptionNodeFixture(t)
	s := installAdoptionReporter(t, d, request)
	s.command = &request
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationDestination.Adoption)
	completed, err := d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, completed)
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationDestination.Adopted())
	require.NotNil(t, record.MigrationDestination.Adoption.RegionalAcknowledgement)
	require.NoDirExists(t, record.MigrationDestination.ImageDirectory)
	require.Equal(t, request, record.MigrationDestination.Adoption.Request)
	require.Zero(t, runtime.crashCalls)
	require.Zero(t, runtime.retireCalls)
	require.Zero(t, runtime.externalReclaims)
	require.NotContains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	d.migrationAdoptionAfter = ""
	completed, err = d.reconcileMigrationAdoptions(t.Context())
	require.NoError(t, err)
	require.Zero(t, completed)
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, 1, s.reads)
	require.Equal(t, 1, s.calls)
	require.Equal(t, &s.want, s.committed)
}

func TestMigrationAdoptionDeliveryDoesNotPromoteUncertainOrForeignExecution(t *testing.T) {
	for _, failure := range []string{"pending", "stopped", "writer", "uncertain", "restore", "operation", "socket", "boot"} {
		t.Run(failure, func(t *testing.T) {
			d, request, runtime, runner := migrationAdoptionNodeFixture(t)
			s := installAdoptionReporter(t, d, request)
			command := request
			s.command = &command
			switch failure {
			case "pending":
				s.command = nil
			case "stopped":
				runner.setState("stopped")
			case "writer":
				runtime.recoverySessions[0].Live = false
			case "uncertain":
				custody, err := d.GetMigrationDestination(t.Context(), request.Target.SlotID)
				require.NoError(t, err)
				uncertain := *custody.Restore
				uncertain.State = protocol.MigrationRestoreUncertain
				require.NoError(t, d.RecordMigrationRestore(t.Context(), uncertain))
			case "restore":
				command.RestoreDigest = strings.Repeat("f", 64)
			case "operation":
				command.OperationID += "-changed"
			case "socket":
				command.Target.ControlEndpoint = "unix:///foreign.sock"
			case "boot":
				command.Target.NodeBootID += "-changed"
			}
			completed, err := d.reconcileMigrationAdoptions(t.Context())
			if failure == "pending" || failure == "uncertain" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Zero(t, completed)
			record, err := d.journal.Get(request.Target.SlotID)
			require.NoError(t, err)
			require.Nil(t, record.MigrationDestination.Adoption)
			require.DirExists(t, record.MigrationDestination.ImageDirectory)
			s.mu.Lock()
			defer s.mu.Unlock()
			require.Zero(t, s.calls)
			if failure == "uncertain" {
				require.Zero(t, s.reads)
			}
		})
	}
}
