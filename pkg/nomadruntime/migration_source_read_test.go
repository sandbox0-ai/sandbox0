package nomadruntime

import (
	"context"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	authority "github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type regionalFinalizationStore struct {
	authority.Store
	slot    sandboxstore.RuntimeSlot
	receipt *protocol.MigrationSourceFinalizationReceipt
	err     error
	reads   atomic.Int64
}

func (s *regionalFinalizationStore) GetRuntimeSlot(_ context.Context, slot string) (*sandboxstore.RuntimeSlot, error) {
	if slot != s.slot.ID {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	return &s.slot, nil
}
func (s *regionalFinalizationStore) GetNomadSandboxMigrationSourceFinalizationForSlot(_ context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	s.reads.Add(1)
	if slot != s.slot.ID {
		return nil, sandboxstore.ErrRuntimeSlotNotFound
	}
	return s.receipt, s.err
}

type regionalFinalizationVerifier struct{ identity nodeauth.Identity }

func (v regionalFinalizationVerifier) Verify(_ context.Context, token string) (nodeauth.Identity, error) {
	if token != "test-node-token" {
		return nodeauth.Identity{}, errdefs.ErrPermissionDenied
	}
	return v.identity, nil
}

func installRegionalFinalizationReader(t *testing.T, d *nodeRuntime, receipt *protocol.MigrationSourceFinalizationReceipt) *regionalFinalizationStore {
	t.Helper()
	c := receipt.Request.Cleanup
	store := &regionalFinalizationStore{receipt: receipt, slot: sandboxstore.RuntimeSlot{ID: c.SlotID, ClusterID: c.ClusterID, NodeID: c.NodeID, NodeUID: c.NodeUID, NodeBootID: c.NodeBootID, AllocationID: c.AllocationID, WriterGrantID: c.WriterGrantID, State: sandboxstore.RuntimeSlotStateTerminal}}
	handler, err := authority.NewHandler(authority.HandlerConfig{Store: store, Verifier: regionalFinalizationVerifier{nodeauth.Identity{ClusterID: c.ClusterID, NodeID: c.NodeID, NodeUID: c.NodeUID}}, HeartbeatTTL: time.Minute})
	require.NoError(t, err)
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	dir := t.TempDir()
	ca, token := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "token")
	require.NoError(t, os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	require.NoError(t, os.WriteFile(token, []byte("test-node-token\n"), 0600))
	d.registrationAuthority, err = newRegistrationAbortAuthority(Config{RootFSAuthorityURL: server.URL, RootFSAuthorityCAFile: ca, RootFSAuthorityTokenFile: token})
	require.NoError(t, err)
	return store
}

func TestMigrationSourceReceiptUsesRegionalProofAfterJournalPruning(t *testing.T) {
	d, gc := migrationSourceGCFixture(t)
	receipt, err := d.GetMigrationSourceFinalization(t.Context(), gc.Target.SlotID)
	require.NoError(t, err)
	store := installRegionalFinalizationReader(t, d, receipt)
	_, err = d.AcknowledgeMigrationSourceGC(t.Context(), gc)
	require.NoError(t, err)
	deleted, err := d.journal.Prune(time.Now().Add(48 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	// Exercise both the driver's root-only RPC and the real regional TLS
	// client/handler. Only the PostgreSQL boundary is replaced by a test store.
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "ctld.sock"))
	require.NoError(t, err)
	server := &http.Server{Handler: nodeRuntimeRPCHandler(nil, nil, nil, d)}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	client, err := NewClient(listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	for range 2 {
		actual, err := client.GetMigrationSourceFinalization(t.Context(), gc.Target.SlotID)
		require.NoError(t, err)
		require.Equal(t, receipt, actual)
		_, err = d.journal.Get(gc.Target.SlotID)
		require.ErrorIs(t, err, errdefs.ErrNotFound, "readback cannot recreate local custody")
	}
	require.Equal(t, int64(2), store.reads.Load())
}

func TestMigrationSourceRegionalReadCannotOverrideLocalCustody(t *testing.T) {
	for _, local := range []string{"complete", "pending", "invalidated", "corrupt"} {
		t.Run(local, func(t *testing.T) {
			d, gc := migrationSourceGCFixture(t)
			receipt, err := d.GetMigrationSourceFinalization(t.Context(), gc.Target.SlotID)
			require.NoError(t, err)
			store := installRegionalFinalizationReader(t, d, receipt)
			if local == "invalidated" {
				require.NoError(t, d.journal.invalidateMigrationExecution(gc.Target.SlotID))
			}
			if local == "corrupt" {
				require.NoError(t, d.journal.db.Update(func(tx *bolt.Tx) error {
					bucket, err := runtimeSlotJournalBucketFrom(tx)
					if err != nil {
						return err
					}
					return bucket.Put([]byte(gc.Target.SlotID), []byte("{broken"))
				}))
			}
			if local == "pending" {
				// Use a second source whose complete receipt has not yet been
				// journaled locally; regional history must not bypass this gate.
				pending, request, _, _ := migrationSourceFinalizeNodeFixture(t)
				pending.registrationAuthority = d.registrationAuthority
				actual, err := pending.GetMigrationSourceFinalization(t.Context(), request.Cleanup.SlotID)
				require.NoError(t, err)
				require.Nil(t, actual)
			} else {
				_, err = d.GetMigrationSourceFinalization(t.Context(), gc.Target.SlotID)
				if local == "invalidated" {
					require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
				} else if local == "corrupt" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			require.Zero(t, store.reads.Load())
		})
	}
}

func TestMigrationSourceRegionalReadFailsClosedOnUntrustedOrMissingEvidence(t *testing.T) {
	for _, failure := range []string{"cluster", "node", "uid", "slot", "invalid", "unavailable", "missing"} {
		t.Run(failure, func(t *testing.T) {
			d, gc := migrationSourceGCFixture(t)
			receipt, err := d.GetMigrationSourceFinalization(t.Context(), gc.Target.SlotID)
			require.NoError(t, err)
			store := installRegionalFinalizationReader(t, d, receipt)
			_, err = d.AcknowledgeMigrationSourceGC(t.Context(), gc)
			require.NoError(t, err)
			_, err = d.journal.Prune(time.Now().Add(48 * time.Hour))
			require.NoError(t, err)
			slot := gc.Target.SlotID
			switch failure {
			case "cluster":
				d.clusterID = "foreign"
			case "node":
				d.nodeID = "foreign"
			case "uid":
				d.nodeUID = "foreign"
			case "slot":
				slot = "other-slot"
			case "invalid":
				store.receipt = &protocol.MigrationSourceFinalizationReceipt{}
			case "unavailable":
				store.err = errdefs.ErrUnavailable
			case "missing":
				store.receipt = nil
			}
			actual, err := d.GetMigrationSourceFinalization(t.Context(), slot)
			if failure == "missing" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Nil(t, actual)
			_, err = d.journal.Get(gc.Target.SlotID)
			require.ErrorIs(t, err, errdefs.ErrNotFound)
		})
	}
}
