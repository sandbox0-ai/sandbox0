//go:build linux

package driver

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// This optional fixture uses the complete production node authority, including
// allocation observation, source checkpoint, terminal purge and restore claim.
// Its certificate identity and OCI provenance remain disposable fixture inputs.
func liveNomadMemoryAuthority(t *testing.T, store *sandboxstore.PGSandboxStore, nodeID string) {
	t.Helper()
	root := "/opt/s0-live-update-test/f"
	pki := filepath.Join(root, "pki")
	component, err := nodeauthority.New(nodeauthority.Config{Store: store, Address: "127.0.0.1:19445",
		CertFile: filepath.Join(pki, "peer.pem"), KeyFile: filepath.Join(pki, "peer-key.pem"), ClientCAFile: filepath.Join(pki, "ca.pem"),
		WriterLeaseTTL: 10 * time.Minute, RuntimeSlotHeartbeatTTL: time.Minute, RegionID: "fixture-region",
		Identities: []nodeauth.CertificateIdentity{{CommonName: "s0-live-fixture", ClusterID: "fixture-cluster", NodeID: nodeID, NodeUID: "fixture-node-1", AgentUID: "fixture-ctld"}},
		Terminal:   runtimeslotterminal.Config{Enabled: true, NomadEndpointsFile: filepath.Join(root, "nomad-endpoints.json"), Interval: 100 * time.Millisecond, PassTimeout: 10 * time.Second}})
	require.NoError(t, err)
	key, err := internalauth.LoadEd25519PrivateKeyFromFile(filepath.Join(pki, "procd-key.pem"))
	require.NoError(t, err)
	writerKey := make([]byte, 32)
	_, err = rand.Read(writerKey)
	require.NoError(t, err)
	planner, err := component.NewClaimPlanner(nodeauthority.ClaimPlannerConfig{
		Prober:         procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: 10 * time.Second}),
		TokenGenerator: runtimeauth.NewInternalTokenGenerator(internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: key})),
		WriterTokenKey: writerKey, ClaimTTL: time.Minute})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	defer workers.Wait()
	defer cancel()
	errorsCh := make(chan error, 8)
	report := func(r nomadmigration.Report) {
		if r.Error != nil {
			t.Logf("checkpoint coordinator: %v", r.Error)
		}
	}
	start := func(run func() error) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := run(); err != nil && !errors.Is(err, context.Canceled) {
				errorsCh <- err
			}
		}()
	}
	start(func() error { return component.RunServer(ctx) })
	select {
	case <-component.Ready():
	case err := <-errorsCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("full node authority did not bind")
	}
	start(func() error { return component.RunTerminal(ctx, nil) })
	start(func() error { return component.RunMigrationCPUPreflights(ctx, report) })
	start(func() error { return component.RunMigrationStaging(ctx, report) })
	start(func() error { return component.RunMigrationStagingRelease(ctx, report) })
	start(func() error { return component.RunCheckpointPauses(ctx, report) })
	ready := filepath.Join(root, "region-ready")
	require.NoError(t, os.WriteFile(ready, []byte("full_node_authority_memory_fixture\n"), 0600))
	defer os.Remove(ready)
	performed := false
	for {
		select {
		case err := <-errorsCh:
			require.NoError(t, err)
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
			if _, err := os.Stat(filepath.Join(root, "stop-region")); err == nil {
				return
			}
			if _, err := os.Stat(filepath.Join(root, "request-memory")); err == nil && !performed {
				performed = true
				liveNomadPauseResumeMemory(t, ctx, store, planner, root)
			}
		}
	}
}

func liveNomadPauseResumeMemory(t *testing.T, parent context.Context, store *sandboxstore.PGSandboxStore, planner *runtimeslotclaim.Planner, root string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	defer cancel()
	data, err := os.ReadFile(filepath.Join(root, "regional-stage.json"))
	require.NoError(t, err)
	var stage rootfshandoff.StageRequest
	require.NoError(t, json.Unmarshal(data, &stage))
	inputs, err := store.GetRuntimeSlotClaimInputs(ctx, stage.Identity.SlotNonce)
	require.NoError(t, err)
	require.NotNil(t, inputs)
	sourceSlot, err := store.GetRuntimeSlot(ctx, stage.Identity.SlotNonce)
	require.NoError(t, err)
	_, err = store.RegisterRuntimeNodeCapacity(ctx, &sandboxstore.RegisterRuntimeNodeCapacityRequest{
		ClusterID: sourceSlot.ClusterID, NodeID: sourceSlot.NodeID, NodeUID: sourceSlot.NodeUID, NodeBootID: sourceSlot.NodeBootID,
		CPUMillicores: 1000, MemoryBytes: 1 << 30, CPUSetCPUs: "0-3", CPUSetMems: "0", TTL: 5 * time.Minute})
	require.NoError(t, err)
	pause, err := store.RequestNomadSandboxMemoryPause(ctx, stage.Identity.RootFSID)
	require.NoError(t, err)
	require.False(t, pause.AlreadyPaused)
	require.Eventually(t, func() bool {
		record, err := store.GetSandbox(ctx, stage.Identity.RootFSID)
		return err == nil && record.DesiredState == sandboxstore.SandboxDesiredStatePaused && record.RuntimeID == ""
	}, 3*time.Minute, 100*time.Millisecond, "real memory pause must publish retained image and retire its exact source")
	candidate, err := store.RequestNomadSandboxResume(ctx, &sandboxstore.RequestNomadSandboxResumeRequest{SandboxID: stage.Identity.RootFSID, ExpectedTeamID: inputs.Runtime.TeamID, Memory: true})
	require.NoError(t, err)
	require.NotNil(t, candidate.Checkpoint)
	require.Equal(t, sourceSlot.CompatibilityDigest, candidate.CheckpointCompatibilityDigest)
	request := runtimeslotclaim.Request{OperationID: candidate.OperationID, SandboxID: candidate.SandboxID, TeamID: candidate.Record.TeamID, UserID: candidate.Record.UserID,
		CompatibilityDigest: candidate.CheckpointCompatibilityDigest, ClusterID: sourceSlot.ClusterID, NetworkPolicy: inputs.NetworkPolicy,
		Runtime:   candidate.Checkpoint.Assignment.Target,
		Resources: runtimeslot.RuntimeResourceRequest{Version: runtimeslot.RuntimeResourceRequestVersion, CPUMillicores: 250, MemoryBytes: 256 << 20, PIDsLimit: runtimeslot.DefaultRuntimePIDsLimit}}
	var restored *runtimeslotclaim.Result
	require.Eventually(t, func() bool {
		restored, err = planner.ClaimCheckpoint(ctx, request, *candidate.Checkpoint)
		return err == nil
	}, time.Minute, 100*time.Millisecond, "real checkpoint restore claim: %v", err)
	require.NotNil(t, restored.MigrationRestore)
	resumed, err := store.CompleteNomadSandboxResume(ctx, &sandboxstore.CompleteNomadSandboxResumeRequest{
		SandboxID: candidate.SandboxID, OperationID: candidate.OperationID, SlotID: restored.Slot.ID, AllocationID: restored.Slot.AllocationID,
		AllocationNamespace: restored.Slot.AllocationNamespace, ResourceLeaseID: restored.Slot.ResourceLease.LeaseID, ResourceLeaseDigest: restored.Slot.ResourceLeaseDigest})
	require.NoError(t, err)
	require.Equal(t, sandboxstore.SandboxDesiredStateActive, resumed.DesiredState)
	require.NotEqual(t, sourceSlot.AllocationID, restored.Slot.AllocationID)
	data, err = os.ReadFile(filepath.Join(root, "managed-context.json"))
	require.NoError(t, err)
	var managed struct {
		ID string `json:"context_id"`
	}
	require.NoError(t, json.Unmarshal(data, &managed))
	require.NotEmpty(t, managed.ID)
	context := liveNomadManagedContext(t, restored.ProcdAddress, http.MethodGet, procdapi.ContextPath(managed.ID), http.StatusOK, nil)
	require.Equal(t, managed.ID, context.ID)
	require.True(t, context.Running, "original procd-managed context must remain running after memory restore")
	require.Nil(t, context.ExitCode)
	metadata, err := json.Marshal(restored.Stage.WithoutWriterGrantToken())
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "regional-stage.json"), metadata, 0600))
	metadata, err = json.Marshal(map[string]string{"slot_id": restored.Slot.ID, "allocation_id": restored.Slot.AllocationID, "procd_instance_id": restored.ProcdInstanceID})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "memory-result.json"), metadata, 0600))
	t.Log("real expanded RootFS memory pause, retained S3 checkpoint, exact source retirement and fresh-allocation memory restore completed")
}
