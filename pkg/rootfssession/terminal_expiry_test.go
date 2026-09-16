package session

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// Legacy descriptor bytes remain audit evidence, never executable input. No
// legacy encoder or reader is needed to retain the exact historical binding.
func historicalExternalProofFixture(t *testing.T, name string) (*Manager, rootfshandoff.StageRequest) {
	t.Helper()
	manager, _, request := reclaimedExternalProofFixture(t, name)
	var descriptor map[string]any
	require.NoError(t, json.Unmarshal(request.Generation.Descriptor, &descriptor))
	descriptor["version"] = 1
	payload, err := json.Marshal(descriptor)
	require.NoError(t, err)
	request.Generation.Descriptor = payload
	request.Generation.FormatGeneration = 1
	binding, err := request.BindingDigest()
	require.NoError(t, err)
	mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
		stage := cloneDurableStage(request)
		stored.Stage = &stage
		stored.BaseDescriptor = append([]byte(nil), payload...)
		stored.BindingDigest = hex.EncodeToString(binding[:])
		stored.CrashFence.Result.BindingDigest = stored.BindingDigest
	})
	return manager, request
}

func TestManagerRetainsHistoricalExternalProofAcrossRestartUntilExpiry(t *testing.T) {
	manager, request := historicalExternalProofFixture(t, "historical-expiry")
	require.Error(t, request.ValidateDurableBinding())
	require.NoError(t, request.ValidateTerminalBinding())
	stored, err := manager.load(request.Parent)
	require.NoError(t, err)
	deadline, eligible, err := externalProofQuietUntil(stored)
	require.NoError(t, err)
	require.True(t, eligible)
	config := Config{
		StatePath: manager.db.Path(), BranchRoot: manager.branchRoot, MountRoot: manager.mountRoot,
		Source: manager.source, Publisher: manager.publisher, Runtime: manager.runtime,
		MaxDirtyTailBytes: manager.maxDirty, MaxNodeDirtyTailBytes: manager.nodeDirty.Usage().MaxBytes,
		DirtyTailRetirementReserveBytes: manager.retirementReserve,
	}
	require.NoError(t, manager.Close())
	restarted, err := New(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restarted.Close()) })
	after, err := restarted.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, stored, after, "restart must not rewrite historical evidence")
	recovery, err := restarted.RecoverySessions()
	require.NoError(t, err)
	require.Empty(t, recovery, "unexpired history stays outside active recovery")
	forgotten, err := restarted.ForgetExpiredExternalTerminal(request, deadline.Add(-time.Nanosecond))
	require.NoError(t, err)
	require.False(t, forgotten)
	forgotten, err = restarted.ForgetExpiredExternalTerminal(request, deadline)
	require.NoError(t, err)
	require.True(t, forgotten)
	_, err = restarted.load(request.Parent)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestManagerHistoricalDescriptorRequiresExactReclaimedExternalProof(t *testing.T) {
	for _, field := range []string{"live", "active", "unreclaimed", "local-crash", "missing-proof", "planned", "binding", "descriptor", "physical-proof", "reservation"} {
		t.Run(field, func(t *testing.T) {
			manager, request := historicalExternalProofFixture(t, "historical-"+field)
			stored, err := manager.load(request.Parent)
			require.NoError(t, err)
			switch field {
			case "active":
				stored.State = stateReady
			case "unreclaimed":
				stored.BranchRemoved = false
			case "local-crash":
				stored.CrashFence.External = false
			case "missing-proof":
				stored.CrashFence.Result = nil
			case "planned":
				stored.RetireOperationID = "uncommitted"
			case "binding":
				stored.BindingDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			case "descriptor":
				stored.Stage.Generation.Descriptor = append(stored.Stage.Generation.Descriptor, ' ')
			case "physical-proof":
				stored.CrashFence.Result.WriterEpoch++
			case "reservation":
				stored.DeviceReservationReleased = false
			}
			_, err = recoverySessionFromRecord(request.Parent, stored, field == "live")
			require.Error(t, err)
		})
	}
}

func TestManagerForgetsOnlyExpiredReclaimedExternalProof(t *testing.T) {
	manager, runtime, request := reclaimedExternalProofFixture(t, "external-expiry")
	stored, err := manager.load(request.Parent)
	require.NoError(t, err)
	deadline, eligible, err := externalProofQuietUntil(stored)
	require.NoError(t, err)
	require.True(t, eligible)
	forgotten, err := manager.ForgetExpiredExternalTerminal(request, deadline.Add(-time.Nanosecond))
	require.NoError(t, err)
	require.False(t, forgotten, "the replay window must remain intact")
	_, err = manager.load(request.Parent)
	require.NoError(t, err)

	device, err := runtime.ReserveDevice("replacement-allocation")
	require.NoError(t, err)
	forgotten, err = manager.ForgetExpiredExternalTerminal(request, deadline)
	require.NoError(t, err)
	require.True(t, forgotten)
	_, err = manager.load(request.Parent)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	_, _, err = manager.findIdentity(request.Identity.RootFSID, request.Identity.WriterEpoch)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.Equal(t, "replacement-allocation", runtime.reservationOwner(device), "history expiry must not touch a reused device")
	manager.recoveryMu.RLock()
	_, active := manager.recoveryParents[request.Parent]
	_, quiet := manager.quietExternal[request.Parent]
	manager.recoveryMu.RUnlock()
	require.False(t, active)
	require.False(t, quiet)
	forgotten, err = manager.ForgetExpiredExternalTerminal(request, deadline)
	require.NoError(t, err)
	require.False(t, forgotten, "a missing local record is not new terminal authority")
}

func TestManagerExternalProofExpiryRejectsDifferentDurableBinding(t *testing.T) {
	for _, field := range []string{"allocation", "boot", "runtime-generation", "writer-grant", "issuance-nonce", "writer-epoch"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "binding-"+field)
			other := request
			switch field {
			case "allocation":
				other.Identity.AllocationID += "-other"
			case "boot":
				other.Identity.BootID += "-other"
			case "runtime-generation":
				other.Identity.RuntimeGeneration += "-other"
			case "writer-grant":
				other.Identity.WriterGrantID += "-other"
			case "issuance-nonce":
				other.Identity.WriterGrantTokenDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
			case "writer-epoch":
				other.Identity.WriterEpoch++
			}
			forgotten, err := manager.ForgetExpiredExternalTerminal(other, time.Now().Add(4*ExternalTerminalProofRetention))
			require.Error(t, err)
			require.False(t, forgotten)
			_, err = manager.load(request.Parent)
			require.NoError(t, err)
		})
	}
}

func TestManagerExternalProofExpiryKeepsUnreclaimedOrUnverifiedRecords(t *testing.T) {
	for _, field := range []string{"not-reclaimed", "local-crash", "missing-result", "planned-retirement"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "ineligible-"+field)
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				switch field {
				case "not-reclaimed":
					stored.BranchRemoved = false
				case "local-crash":
					stored.CrashFence.External = false
				case "missing-result":
					stored.CrashFence.Result = nil
				case "planned-retirement":
					stored.RetireOperationID = "not-published"
				}
			})
			forgotten, err := manager.ForgetExpiredExternalTerminal(request, time.Now().Add(4*ExternalTerminalProofRetention))
			require.NoError(t, err)
			require.False(t, forgotten)
			_, err = manager.load(request.Parent)
			require.NoError(t, err)
		})
	}
}

func TestManagerExternalProofExpiryFailsClosedOnMismatchedPhysicalEvidence(t *testing.T) {
	for _, field := range []string{"parent", "rootfs", "epoch", "operation", "binding", "device", "branch", "mount-attached", "reservation-owned"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "proof-"+field)
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				proof := stored.CrashFence.Result
				switch field {
				case "parent":
					proof.Parent += "-other"
				case "rootfs":
					proof.RootFSID += "-other"
				case "epoch":
					proof.WriterEpoch++
				case "operation":
					proof.OperationID += "-other"
				case "binding":
					proof.BindingDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
				case "device":
					proof.DevicePath = "/dev/fake-other"
				case "branch":
					proof.BranchPath += "-other"
				case "mount-attached":
					proof.MergedMountAbsent = false
				case "reservation-owned":
					stored.DeviceReservationReleased = false
				}
			})
			forgotten, err := manager.ForgetExpiredExternalTerminal(request, time.Now().Add(4*ExternalTerminalProofRetention))
			require.Error(t, err)
			require.False(t, forgotten)
			_, err = manager.load(request.Parent)
			require.NoError(t, err)
		})
	}
}

func TestManagerExternalProofExpiryKeepsLiveOwnersAndUnexpectedArtifacts(t *testing.T) {
	for _, field := range []string{"live-owner", "branch-file", "branch-symlink", "mount-directory", "identity-index"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "absence-"+field)
			paths := sessionPaths(manager.branchRoot, manager.mountRoot, request.Parent)
			switch field {
			case "live-owner":
				manager.mu.Lock()
				manager.live[request.Parent] = &liveSession{}
				manager.mu.Unlock()
				t.Cleanup(func() {
					manager.mu.Lock()
					delete(manager.live, request.Parent)
					manager.mu.Unlock()
				})
			case "branch-file":
				require.NoError(t, os.WriteFile(paths.branch, []byte("must survive"), 0o600))
			case "branch-symlink":
				require.NoError(t, os.Symlink(t.TempDir(), paths.branch))
			case "mount-directory":
				require.NoError(t, os.MkdirAll(filepath.Dir(paths.xfs), 0o700))
			case "identity-index":
				require.NoError(t, manager.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(sessionIdentityBucket).Put(writerIdentityKey(request.Identity.RootFSID, request.Identity.WriterEpoch), []byte("another-parent"))
				}))
			}
			forgotten, err := manager.ForgetExpiredExternalTerminal(request, time.Now().Add(4*ExternalTerminalProofRetention))
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			require.False(t, forgotten)
			_, err = manager.load(request.Parent)
			require.NoError(t, err)
			if field == "branch-file" || field == "branch-symlink" {
				_, err = os.Lstat(paths.branch)
				require.NoError(t, err, "expiry only removes compact records, never physical artifacts")
			}
		})
	}
}

func TestManagerExternalProofExpiryRetainsReplayAfterLatestDurableBoundary(t *testing.T) {
	for _, field := range []string{"request", "observation", "reclamation"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "late-"+field)
			now := time.Now().UTC()
			past := now.Add(-4 * ExternalTerminalProofRetention).Format(time.RFC3339Nano)
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				stored.CrashFence.RequestedAt = past
				stored.CrashFence.Result.ObservedAt = past
				stored.UpdatedAt = past
				switch field {
				case "request":
					stored.CrashFence.RequestedAt = now.Format(time.RFC3339Nano)
				case "observation":
					stored.CrashFence.Result.ObservedAt = now.Format(time.RFC3339Nano)
				case "reclamation":
					stored.UpdatedAt = now.Format(time.RFC3339Nano)
				}
			})
			require.NoError(t, manager.rebuildRecoveryIndex())
			recovery, err := manager.RecoverySessions()
			require.NoError(t, err)
			require.Empty(t, recovery, "recovery indexing must honor the latest durable boundary")
			forgotten, err := manager.ForgetExpiredExternalTerminal(request, now.Add(ExternalTerminalProofRetention-time.Nanosecond))
			require.NoError(t, err)
			require.False(t, forgotten)
			forgotten, err = manager.ForgetExpiredExternalTerminal(request, now.Add(ExternalTerminalProofRetention))
			require.NoError(t, err)
			require.True(t, forgotten)
		})
	}
}

func TestManagerExternalProofExpiryRejectsInvalidBoundaryTimestamps(t *testing.T) {
	for _, field := range []string{"request", "observation", "reclamation"} {
		t.Run(field, func(t *testing.T) {
			manager, _, request := reclaimedExternalProofFixture(t, "time-"+field)
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				switch field {
				case "request":
					stored.CrashFence.RequestedAt = "invalid"
				case "observation":
					stored.CrashFence.Result.ObservedAt = "invalid"
				case "reclamation":
					stored.UpdatedAt = "invalid"
				}
			})
			forgotten, err := manager.ForgetExpiredExternalTerminal(request, time.Now().Add(4*ExternalTerminalProofRetention))
			require.Error(t, err)
			require.False(t, forgotten)
			_, err = manager.load(request.Parent)
			require.NoError(t, err)
		})
	}
}

func TestManagerTerminalForgetRepeatsEligibilityInsideWriteTransaction(t *testing.T) {
	manager, _, request := reclaimedExternalProofFixture(t, "transaction-recheck")
	calls := 0
	forgotten, err := manager.forgetTerminal(request.Parent, request.Identity, func(current record) (bool, error) {
		calls++
		if calls == 1 {
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				stored.CrashFence.OperationID = "changed-operation"
				stored.CrashFence.Result.OperationID = "changed-operation"
			})
			return true, nil
		}
		return current.CrashFence.OperationID != "changed-operation", nil
	})
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.False(t, forgotten)
	require.Equal(t, 2, calls)
	_, err = manager.load(request.Parent)
	require.NoError(t, err)
	parent, _, err := manager.findIdentity(request.Identity.RootFSID, request.Identity.WriterEpoch)
	require.NoError(t, err)
	require.Equal(t, request.Parent, parent)
}

func TestManagerTerminalForgetRejectsReplacedProofDespiteEligibility(t *testing.T) {
	manager, _, request := reclaimedExternalProofFixture(t, "transaction-proof-replacement")
	calls := 0
	forgotten, err := manager.forgetTerminal(request.Parent, request.Identity, func(record) (bool, error) {
		calls++
		if calls == 1 {
			mutateExpiryRecord(t, manager, request.Parent, func(stored *record) {
				stored.CrashFence.OperationID = "replacement-operation"
				stored.CrashFence.Result.OperationID = "replacement-operation"
			})
		}
		return true, nil
	})
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.False(t, forgotten)
	require.Equal(t, 2, calls)
	stored, err := manager.load(request.Parent)
	require.NoError(t, err)
	require.Equal(t, "replacement-operation", stored.CrashFence.OperationID)
}

func TestManagerTerminalForgetDoesNotClaimTransactionTimeDisappearance(t *testing.T) {
	manager, _, request := reclaimedExternalProofFixture(t, "transaction-disappearance")
	forgotten, err := manager.forgetTerminal(request.Parent, request.Identity, func(record) (bool, error) {
		require.NoError(t, manager.db.Update(func(tx *bolt.Tx) error {
			if err := tx.Bucket(sessionIdentityBucket).Delete(writerIdentityKey(request.Identity.RootFSID, request.Identity.WriterEpoch)); err != nil {
				return err
			}
			return tx.Bucket(sessionBucket).Delete([]byte(request.Parent))
		}))
		return true, nil
	})
	require.NoError(t, err)
	require.False(t, forgotten, "only the transaction that actually deletes a checked record can report local expiry")
}

func reclaimedExternalProofFixture(t *testing.T, name string) (*Manager, *fakeHostRuntime, rootfshandoff.StageRequest) {
	t.Helper()
	manager, runtime, request := newTestManager(t, name)
	_, err := manager.Ensure(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, manager.ReleaseParent(t.Context(), request.Parent, request.Identity))
	_, err = manager.CrashFenceExternal(request.WithoutWriterGrantToken(), "regional-terminal-"+name)
	require.NoError(t, err)
	// In production this operation is called only after the regional writer
	// authority has confirmed terminal retirement for this exact binding.
	require.NoError(t, manager.ReclaimTerminalArtifacts(request.Parent, request.Identity))
	return manager, runtime, request.WithoutWriterGrantToken()
}

func mutateExpiryRecord(t *testing.T, manager *Manager, parent string, mutate func(*record)) {
	t.Helper()
	// Deliberately bypass save's validation to test corrupt persisted evidence.
	require.NoError(t, manager.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sessionBucket)
		var stored record
		if err := json.Unmarshal(bucket.Get([]byte(parent)), &stored); err != nil {
			return err
		}
		mutate(&stored)
		return putRecord(bucket, stored)
	}))
}
