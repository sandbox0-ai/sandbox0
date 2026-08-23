// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nomadruntime

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestRegionalTerminalProofOutlivesExternalCrashProofWindow(t *testing.T) {
	require.Greater(t, sandboxstore.RootFSWriterTerminalProofRetention, rootfssession.ExternalTerminalProofRetention)
}

func TestRuntimeSlotJournalPersistsExactCleanupProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	registration := testRuntimeSlotJournalRegistration(t, "slot-1")
	require.NoError(t, journal.Register(registration))
	require.NoError(t, journal.Register(registration))

	changedRegistration := registration
	changedRegistration.NodeBootID = "another-boot"
	require.ErrorIs(t, journal.Register(changedRegistration), errdefs.ErrAlreadyExists)

	request := testRuntimeSlotJournalCleanup(registration)
	started, err := journal.BeginCleanup(request)
	require.NoError(t, err)
	require.Equal(t, request, *started.Cleanup)
	proof := testRuntimeSlotJournalProof(t, request)
	require.NoError(t, journal.CompleteCleanup(request, proof))
	require.NoError(t, journal.CompleteCleanup(request, proof))
	require.NoError(t, journal.Close())

	journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	recovered, err := journal.BeginCleanup(request)
	require.NoError(t, err)
	require.Equal(t, proof, *recovered.Proof)

	changedRequest := request
	changedRequest.NodeBootID = "another-boot"
	require.ErrorIs(t, func() error {
		_, err := journal.BeginCleanup(changedRequest)
		return err
	}(), errdefs.ErrFailedPrecondition)
	changedProof := proof
	changedProof.NetworkPolicyAbsent = false
	require.Error(t, journal.CompleteCleanup(request, changedProof))
}

func TestRuntimeSlotJournalReplaysLegacyCleanupProof(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	registration := testRuntimeSlotJournalRegistration(t, "slot-legacy")
	require.NoError(t, journal.Register(registration))
	request := testRuntimeSlotJournalCleanup(registration)
	_, err = journal.BeginCleanup(request)
	require.NoError(t, err)
	proof := protocol.NodeCleanupControlProof{
		Version: 2, OperationID: request.OperationID, SlotID: request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	proof.ProofDigest, err = proof.Digest()
	require.NoError(t, err)
	require.NoError(t, journal.CompleteCleanup(request, proof))
	require.NoError(t, journal.Close())

	journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	recovered, err := journal.BeginCleanup(request)
	require.NoError(t, err)
	require.Equal(t, proof, *recovered.Proof)
}

func TestRuntimeSlotJournalPrunesOnlyExpiredCompletedProofs(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	completed := testRuntimeSlotJournalRegistration(t, "completed")
	active := testRuntimeSlotJournalRegistration(t, "active")
	for _, registration := range []RuntimeSlotRegistration{completed, active} {
		require.NoError(t, journal.Register(registration))
	}
	request := testRuntimeSlotJournalCleanup(completed)
	_, err = journal.BeginCleanup(request)
	require.NoError(t, err)
	require.NoError(t, journal.CompleteCleanup(request, testRuntimeSlotJournalProof(t, request)))

	deleted, err := journal.Prune(time.Now().Add(2 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, err = journal.Get(completed.SlotID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	stored, err := journal.Get(active.SlotID)
	require.NoError(t, err)
	require.Equal(t, active, stored.Registration)
}

func TestRuntimeSlotJournalRejectsSymlinkStateFile(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target.db")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	link := filepath.Join(root, "runtime-slots.db")
	require.NoError(t, os.Symlink(target, link))
	_, err := newRuntimeSlotJournal(link, time.Hour)
	require.Error(t, err)
}

func TestRuntimeSlotJournalFailsClosedWhenBucketIsMissing(t *testing.T) {
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "runtime-slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(runtimeSlotJournalBucket)
	}))

	require.ErrorIs(t, journal.Ping(), errdefs.ErrUnavailable)
	_, err = journal.Get("missing")
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
}

func TestRuntimeSlotJournalReusesBoltPagesAcrossTenThousandProofs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime-slots.db")
	journal, err := newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	completedAt := time.Now().Add(-2 * time.Hour).UTC().Format(time.RFC3339Nano)
	fill := func(cycle int) {
		require.NoError(t, journal.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(runtimeSlotJournalBucket)
			for index := range 10_000 {
				slotID := fmt.Sprintf("slot-%d-%05d", cycle, index)
				containerID := protocol.NomadRunscContainerID(slotID)
				registration := RuntimeSlotRegistration{
					Version: RuntimeSlotJournalVersion, SlotID: slotID, ClusterID: "cluster-1",
					AllocationID: "allocation-" + slotID, NodeID: "node-1", NodeBootID: "boot-1",
					NetNSPath: "/var/run/netns/" + slotID, NetNSIdentity: "netns-" + slotID,
					NetworkChain: networkChainName(containerID), RunscContainerID: containerID,
					StableMount: "/opt/nomad/" + slotID + "/rootfs", StableMountID: "mount-" + slotID,
					MountNamespaceID: "mnt:[1]",
				}
				request := testRuntimeSlotJournalCleanup(registration)
				proof := testRuntimeSlotJournalProof(t, request)
				record := runtimeSlotJournalRecord{
					Version: RuntimeSlotJournalVersion, Registration: registration,
					Cleanup: &request, Proof: &proof, CreatedAt: completedAt,
					UpdatedAt: completedAt, CompletedAt: completedAt,
				}
				if err := putRuntimeSlotJournalRecord(bucket, record); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	fill(1)
	deleted, err := journal.Prune(time.Now())
	require.NoError(t, err)
	require.Equal(t, 10_000, deleted)
	require.NoError(t, journal.db.Sync())
	warm, err := os.Stat(path)
	require.NoError(t, err)

	fill(2)
	deleted, err = journal.Prune(time.Now())
	require.NoError(t, err)
	require.Equal(t, 10_000, deleted)
	require.NoError(t, journal.db.Sync())
	final, err := os.Stat(path)
	require.NoError(t, err)
	require.LessOrEqual(t, final.Size(), warm.Size()+int64(os.Getpagesize()), "journal churn must reuse free Bolt pages")
}

func testRuntimeSlotJournalRegistration(t *testing.T, slotID string) RuntimeSlotRegistration {
	t.Helper()
	root := t.TempDir()
	netnsPath := filepath.Join(root, "network.ns")
	require.NoError(t, os.WriteFile(netnsPath, []byte("netns"), 0o600))
	stableMount := filepath.Join(root, "rootfs")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	stableMountID, err := stableMountIdentity(stableMount)
	require.NoError(t, err)
	mountNamespaceID, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	containerID := protocol.NomadRunscContainerID(slotID)
	return RuntimeSlotRegistration{
		Version: RuntimeSlotJournalVersion, SlotID: slotID, ClusterID: "cluster-1",
		AllocationID: "allocation-" + slotID, NodeID: "node-1", NodeBootID: "boot-1",
		NetNSPath: netnsPath, NetNSIdentity: "netns-v1:1:2", NetworkChain: networkChainName(containerID),
		RunscContainerID: containerID, StableMount: stableMount, StableMountID: stableMountID,
		MountNamespaceID: mountNamespaceID,
	}
}

func testRuntimeSlotJournalCleanup(registration RuntimeSlotRegistration) protocol.NodeCleanupControlRequest {
	return protocol.NodeCleanupControlRequest{
		OperationID: "cleanup-" + registration.SlotID, SlotID: registration.SlotID,
		ClusterID: registration.ClusterID, AllocationID: registration.AllocationID,
		NodeID: registration.NodeID, NodeUID: "node-uid-1", NodeBootID: registration.NodeBootID,
		NetNSIdentity: registration.NetNSIdentity, RunscContainerID: registration.RunscContainerID,
	}
}

func testRuntimeSlotJournalProof(t *testing.T, request protocol.NodeCleanupControlRequest) protocol.NodeCleanupControlProof {
	t.Helper()
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, WriterRetireKind: request.WriterRetireKind,
		SlotID:    request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterAuthorityDigest: request.WriterAuthorityDigest,
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	if request.WriterGrantID != "" {
		proof.RootFSOperationID = request.WriterOperationID
		proof.RootFSProofDigest = strings.Repeat("cd", sha256.Size)
	}
	var err error
	proof.ProofDigest, err = proof.Digest()
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	return proof
}
