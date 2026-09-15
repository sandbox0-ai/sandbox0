package nomadruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type fakeRegistrationAbortAuthority struct {
	call func(protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error)
}

func (a fakeRegistrationAbortAuthority) AbortRegistration(_ context.Context, r protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
	return a.call(r)
}

func TestRegistrationAbortRequiresRegionalFenceBeforePhysicalCleanup(t *testing.T) {
	for _, mode := range []string{"unavailable", "wrong-fence", "registered"} {
		t.Run(mode, func(t *testing.T) {
			journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
			require.NoError(t, err)
			defer journal.Close()
			registration := testRuntimeSlotJournalRegistration(t, "abort-slot")
			require.NoError(t, journal.Register(registration))
			record, err := journal.Get(registration.SlotID)
			require.NoError(t, err)
			request := registrationAbortRequestForTest(registration)
			daemon := &nodeRuntime{journal: journal}
			daemon.registrationAuthority = fakeRegistrationAbortAuthority{call: func(r protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
				switch mode {
				case "unavailable":
					return protocol.RegistrationAbortResponse{}, errdefs.ErrUnavailable
				case "registered":
					return protocol.RegistrationAbortResponse{Registered: true}, nil
				}
				cleanup, err := r.CleanupRequest()
				require.NoError(t, err)
				cleanup.NodeBootID = "wrong-boot"
				return protocol.RegistrationAbortResponse{Cleanup: &cleanup}, nil
			}}
			err = daemon.reconcileRegistration(t.Context(), record, request)
			if mode == "registered" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			stored, err := journal.Get(registration.SlotID)
			require.NoError(t, err)
			require.Nil(t, stored.Cleanup)
			require.Nil(t, stored.Proof)
			require.Equal(t, mode == "registered", stored.RegionalRegistrationObserved)
		})
	}
}

func TestRegistrationAbortRetainsProofUntilRegionalAcknowledgement(t *testing.T) {
	journalPath := filepath.Join(t.TempDir(), "slots.db")
	journal, err := newRuntimeSlotJournal(journalPath, time.Hour)
	require.NoError(t, err)
	defer func() { require.NoError(t, journal.Close()) }()
	registration := testRuntimeSlotJournalRegistration(t, "abort-slot")
	// A removed carrier leaves exact journaled identities; cleanup must still
	// perform the existing runsc/network absence protocol before its proof.
	require.NoError(t, os.Remove(registration.NetNSPath))
	require.NoError(t, os.Remove(registration.StableMount))
	require.NoError(t, journal.Register(registration))
	record, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	network := newFakeCtldNetwork(t)
	daemon := &nodeRuntime{runtime: &fakeRootFSRuntime{}, journal: journal, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network, clusterID: registration.ClusterID, nodeID: registration.NodeID, nodeUID: "node-uid-1", config: Config{RootFSConsumerMountRoot: filepath.Dir(registration.StableMount), RootFSConsumerNetNSRoot: filepath.Dir(registration.NetNSPath)}}
	request := registrationAbortRequestForTest(registration)
	lost := true
	var observedProof *protocol.NodeCleanupControlProof
	daemon.registrationAuthority = fakeRegistrationAbortAuthority{call: func(r protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
		cleanup, err := r.CleanupRequest()
		require.NoError(t, err)
		if r.Proof != nil {
			observedProof = r.Proof
			if lost {
				return protocol.RegistrationAbortResponse{}, errors.New("lost regional acknowledgement")
			}
		}
		return protocol.RegistrationAbortResponse{Cleanup: &cleanup, Completed: r.Proof != nil}, nil
	}}
	require.ErrorContains(t, daemon.reconcileRegistration(t.Context(), record, request), "lost regional acknowledgement")
	stored, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.NotNil(t, stored.Proof)
	require.False(t, stored.RegistrationAbortAcknowledged)
	require.Equal(t, 1, network.cleanupCount())
	require.Equal(t, observedProof, stored.Proof)
	require.NoError(t, journal.Close())
	journal, err = newRuntimeSlotJournal(journalPath, time.Hour)
	require.NoError(t, err)
	daemon.journal = journal
	stored, err = journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.Equal(t, observedProof, stored.Proof, "HA restart must replay the same durable proof")
	pruned, err := journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, pruned, "unacknowledged physical proof must remain durable")
	require.ErrorIs(t, journal.Register(registration), errdefs.ErrFailedPrecondition)
	lost = false
	daemon.runner = nil
	daemon.mounter = nil
	daemon.runtimeSlotNetwork = nil
	require.NoError(t, daemon.reconcileRegistration(t.Context(), stored, request))
	require.Equal(t, 1, network.cleanupCount(), "retry must replay the exact durable proof")
	stored, err = journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.True(t, stored.RegistrationAbortAcknowledged)
	pruned, err = journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
}

func registrationAbortRequestForTest(r RuntimeSlotRegistration) protocol.RegistrationAbortRequest {
	return protocol.RegistrationAbortRequest{SlotID: r.SlotID, ClusterID: r.ClusterID, AllocationID: r.AllocationID, NodeID: r.NodeID, NodeUID: "node-uid-1", NodeBootID: r.NodeBootID, NetNSIdentity: r.NetNSIdentity}
}

func TestRegistrationAbortRefusesUnexpectedLocalWriter(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	defer journal.Close()
	registration := testRuntimeSlotJournalRegistration(t, "legacy-writer-slot")
	require.NoError(t, journal.Register(registration))
	request, err := registrationAbortRequestForTest(registration).CleanupRequest()
	require.NoError(t, err)
	runner := newFakeRunsc()
	network := newFakeCtldNetwork(t)
	runtime := &fakeRootFSRuntime{recoverySessions: []rootfssession.RecoverySession{{
		Stage: rootfshandoff.StageRequest{Identity: rootfshandoff.Identity{SlotNonce: registration.SlotID, AllocationID: registration.AllocationID, WriterGrantID: "unexpected-writer"}},
	}}}
	daemon := &nodeRuntime{runtime: runtime, journal: journal, runner: runner, mounter: &fakeMounter{}, runtimeSlotNetwork: network, clusterID: registration.ClusterID, nodeID: registration.NodeID}
	_, err = daemon.CleanupRuntimeSlot(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Empty(t, runner.callsSnapshot())
	require.Zero(t, network.cleanupCount())
	stored, err := journal.Get(registration.SlotID)
	require.NoError(t, err)
	require.Nil(t, stored.Proof)
}
