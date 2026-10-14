//go:build linux

package http

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/providerinstancealiyun"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxclaimreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	templatepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// This authority must remain on a different physical machine from the source.
// The external private harness owns cloud deletion and checks both physical
// identities; this test neither creates resources nor accepts synthetic absence.
// Its receipt covers provider retirement and automatic replacement admission.
// Full remote-data hashes and absence of unpublished bytes are separate worker
// receipts, required together before the overall provider-loss gate can pass.
func TestLiveNomadProviderLossAuthorityFixture(t *testing.T) {
	if os.Getenv("S0_NOMAD_LIVE_PROVIDER_LOSS") != "1" {
		t.Skip("requires the disposable three-machine provider-loss harness")
	}
	require.Zero(t, os.Geteuid())
	root := "/opt/s0-live-update-test/f"
	for _, path := range []string{"/opt/s0-live-update-test/isolated-fixture", "/opt/s0-rootfs-autogrow-test/isolated-fixture"} {
		_, err := os.Stat(path)
		require.NoError(t, err)
	}
	var cfg struct {
		Version       int                            `json:"version"`
		Address       string                         `json:"address"`
		AccountID     string                         `json:"account_id"`
		RegionID      string                         `json:"region_id"`
		InstanceID    string                         `json:"source_instance_id"`
		SourceNodeID  string                         `json:"source_node_id"`
		SourceBootID  string                         `json:"source_boot_id"`
		TargetNodeID  string                         `json:"target_node_id"`
		ProcdDigest   string                         `json:"procd_digest"`
		ProcdProtocol string                         `json:"procd_protocol"`
		PKIDirectory  string                         `json:"pki_directory"`
		Identities    []nodeauth.CertificateIdentity `json:"identities"`
	}
	configPath := filepath.Join(root, "provider-loss.json")
	info, err := os.Lstat(configPath)
	require.NoError(t, err)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	livePublicRootFSReadJSON(t, configPath, &cfg)
	require.Equal(t, 1, cfg.Version)
	require.Len(t, cfg.Identities, 2)
	require.NotEmpty(t, cfg.SourceBootID)
	require.NotEqual(t, cfg.SourceNodeID, cfg.TargetNodeID)
	address, _, err := net.SplitHostPort(cfg.Address)
	require.NoError(t, err)
	ip := net.ParseIP(address)
	require.NotNil(t, ip)
	require.True(t, ip.IsLoopback() || ip.IsPrivate(), "authority binds only the isolated transport")
	sourceUID := "ecs/" + cfg.RegionID + "/" + cfg.InstanceID
	require.Equal(t, cfg.SourceNodeID, cfg.Identities[0].NodeID)
	require.Equal(t, sourceUID, cfg.Identities[0].NodeUID)
	require.Equal(t, cfg.TargetNodeID, cfg.Identities[1].NodeID)
	require.NotEqual(t, sourceUID, cfg.Identities[1].NodeUID)
	require.NotEqual(t, cfg.Identities[0].CommonName, cfg.Identities[1].CommonName)
	require.Contains(t, []string{"sandbox0.procd.v1", "sandbox0.procd.v3"}, cfg.ProcdProtocol)
	require.True(t, strings.HasPrefix(cfg.ProcdDigest, "sha256:"))
	_, err = os.Stat(filepath.Join(root, "stop-region"))
	require.True(t, os.IsNotExist(err), "remove stale stop marker before starting")

	pool, err := dbpool.New(t.Context(), dbpool.Options{DatabaseURL: os.Getenv("INTEGRATION_DATABASE_URL"), Schema: "scheduler", MaxConns: 20})
	require.NoError(t, err)
	defer pool.Close()
	var database string
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT current_database()`).Scan(&database))
	require.True(t, strings.HasPrefix(database, "s0_rootfs_autogrow_nomad_"))
	_, err = pool.Exec(t.Context(), `CREATE SCHEMA IF NOT EXISTS scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column() RETURNS TRIGGER AS $$
		BEGIN NEW.updated_at = NOW(); RETURN NEW; END; $$ LANGUAGE plpgsql;`)
	require.NoError(t, err)
	logger := livePublicRootFSMigrationLogger{}
	require.NoError(t, quota.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, egressauthstore.RunMigrations(t.Context(), pool, logger))
	require.NoError(t, sandboxstore.RunSandboxStoreMigrations(t.Context(), pool, logger))
	store := sandboxstore.NewPGSandboxStore(pool)
	observer, err := providerinstancealiyun.NewAliyunObserver(cfg.RegionID, cfg.AccountID)
	require.NoError(t, err)
	scope := providerinstance.Scope{Provider: "aliyun", AccountID: cfg.AccountID, RegionID: cfg.RegionID, InstanceID: cfg.InstanceID}
	present, err := observer.Observe(t.Context(), scope)
	require.NoError(t, err)
	require.True(t, present.Present, "physical source must exist before loss injection")
	writeReceipt := func(name string, value any) {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, name), data, 0600))
	}
	writeReceipt("provider-source-present.json", present)
	require.True(t, strings.HasPrefix(filepath.Clean(cfg.PKIDirectory), "/opt/s0-rootfs-autogrow-test/provider-loss-transport/pki/"))
	pki := cfg.PKIDirectory
	component, err := nodeauthority.New(nodeauthority.Config{Store: store, Address: cfg.Address,
		CertFile: filepath.Join(pki, "peer.pem"), KeyFile: filepath.Join(pki, "peer-key.pem"), ClientCAFile: filepath.Join(pki, "ca.pem"),
		WriterLeaseTTL: 10 * time.Minute, RuntimeSlotHeartbeatTTL: time.Minute, RegionID: "fixture-region", Identities: cfg.Identities,
		Terminal: runtimeslotterminal.Config{Enabled: true, NomadEndpointsFile: filepath.Join(root, "nomad-endpoints.json"), Interval: time.Second, PassTimeout: 10 * time.Second,
			ProviderObserver: observer, ProviderAccountID: cfg.AccountID, ProviderRegion: cfg.RegionID}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	defer workers.Wait()
	defer cancel()
	errorsCh := make(chan error, 4)
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
		t.Fatal("provider-loss authority did not bind")
	}
	start(func() error { return component.RunTerminal(ctx, nil) })
	claims, err := sandboxclaimreconciler.New(sandboxclaimreconciler.Config{Store: store})
	require.NoError(t, err)
	start(func() error { claims.Run(ctx, nil); return ctx.Err() })
	writeReceipt("region-ready", map[string]any{"provider_loss_authority": true})
	defer os.Remove(filepath.Join(root, "region-ready"))
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(root, "request-provider-recovery"))
		return err == nil
	}, 15*time.Minute, 100*time.Millisecond, "external harness must finish the actual source claim and growth")
	var stage rootfshandoff.StageRequest
	livePublicRootFSReadJSON(t, filepath.Join(root, "regional-stage.json"), &stage)
	require.NoError(t, stage.ValidateDurableBinding())
	require.Equal(t, sourceUID, stage.Identity.NodeUID)
	require.Equal(t, cfg.SourceBootID, stage.Identity.BootID)
	source, err := store.GetRuntimeSlot(ctx, stage.Identity.SlotNonce)
	require.NoError(t, err)
	require.NotNil(t, source)
	require.Equal(t, cfg.SourceNodeID, source.NodeID)
	require.Equal(t, sandboxstore.RuntimeSlotStateActive, source.State)
	key, err := internalauth.LoadEd25519PrivateKeyFromFile(filepath.Join(root, "pki/procd-key.pem"))
	require.NoError(t, err)
	writerKey := make([]byte, 32)
	_, err = rand.Read(writerKey)
	require.NoError(t, err)
	planner, err := component.NewClaimPlanner(nodeauthority.ClaimPlannerConfig{
		Prober:         procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: 10 * time.Second}),
		TokenGenerator: runtimeauth.NewInternalTokenGenerator(internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: key})), WriterTokenKey: writerKey, ClaimTTL: time.Minute})
	require.NoError(t, err)
	classes, err := nomadclaim.LoadRuntimeClassCatalog(filepath.Join(root, "public-runtime-classes.json"))
	require.NoError(t, err)
	runtime, err := nomadclaim.New(nomadclaim.Config{Store: store, Templates: templatepg.NewStore(pool), RuntimeClasses: classes, Planner: planner,
		Allocation: component.NomadAllocationController(), PlannedRetire: component, RunningFork: component, PausedRebase: component,
		QuotaLimits: quota.NewRepository(pool), NetworkPolicies: networkpolicy.NewNetworkPolicyService(zap.NewNop()), ClaimTTL: time.Minute,
		RootFSFormatGeneration: stage.Generation.FormatGeneration, RootFSProcdProtocol: cfg.ProcdProtocol, RootFSProcdDigest: cfg.ProcdDigest})
	require.NoError(t, err)
	pauses := service.NewSandboxPauseController(store, runtime, zap.NewNop())
	runtime.SetPauseEnqueuer(pauses)
	start(func() error { return pauses.Run(ctx, 1) })
	writeReceipt("provider-recovery-ready.json", map[string]any{"sandbox_id": source.SandboxID, "source_slot_id": source.ID, "source_allocation_id": source.AllocationID})

	var target *sandboxstore.RuntimeSlot
	require.Eventually(t, func() bool {
		candidate, err := store.GetRuntimeSlotBySandboxID(ctx, source.SandboxID)
		if err != nil || candidate == nil || candidate.NodeID != cfg.TargetNodeID || candidate.State != sandboxstore.RuntimeSlotStateActive {
			return false
		}
		target = candidate
		return true
	}, 10*time.Minute, 100*time.Millisecond, "production crash reconstruction must claim the same Sandbox on the independent replacement")
	record, err := store.GetSandbox(ctx, source.SandboxID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.SandboxDesiredStateActive, record.DesiredState)
	require.Equal(t, target.AllocationID, record.RuntimeID)
	require.NotEqual(t, source.AllocationID, target.AllocationID)
	require.NotEqual(t, source.NodeBootID, target.NodeBootID)
	old, err := store.GetRuntimeSlot(ctx, source.ID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RuntimeSlotStateTerminal, old.State)
	grant, err := store.GetRootFSWriterGrant(ctx, source.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateRetired, grant.State)
	require.Equal(t, sandboxstore.RootFSWriterRetireKindCrashAbandon, grant.RetireKind)
	var lastRemoteGeneration, resourceState string
	var lastRemoteSequence int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT committed_generation_id,committed_sequence
		FROM manager.rootfs_writer_live_progress WHERE grant_id=$1`, grant.ID).Scan(&lastRemoteGeneration, &lastRemoteSequence))
	require.Positive(t, lastRemoteSequence, "growth must have actual regional live publication")
	require.Equal(t, lastRemoteGeneration, target.SourceGenerationID, "replacement must launch from the last remotely committed cut")
	require.NoError(t, pool.QueryRow(ctx, `SELECT lease_state FROM manager.runtime_resource_leases WHERE lease_id=$1`, source.ResourceLease.LeaseID).Scan(&resourceState))
	require.Equal(t, "released", resourceState)
	var proof sandboxstore.RuntimeProviderAbsenceProof
	proof.Version = 1
	proof.Target = sandboxstore.RuntimeProviderTarget{ClusterID: source.ClusterID, NodeID: source.NodeID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID}
	var scopeJSON, firstJSON, lastJSON, proofDigest []byte
	var revoked bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT scope,first_observation,last_observation,first_observed_at,last_observed_at,proof_digest,revoked
		FROM manager.runtime_provider_absence WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4`,
		source.ClusterID, source.NodeID, source.NodeUID, source.NodeBootID).Scan(&scopeJSON, &firstJSON, &lastJSON, &proof.FirstObservedAt, &proof.LastObservedAt, &proofDigest, &revoked))
	require.NoError(t, json.Unmarshal(scopeJSON, &proof.Scope))
	require.NoError(t, json.Unmarshal(firstJSON, &proof.First))
	require.NoError(t, json.Unmarshal(lastJSON, &proof.Last))
	require.False(t, revoked)
	digest, err := proof.Digest()
	require.NoError(t, err)
	require.Equal(t, digest, proofDigest)
	require.Equal(t, scope, proof.Scope)
	var retirements int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM manager.runtime_provider_absence_retirements WHERE grant_id=$1 AND proof_digest=$2`, source.WriterGrantID, proofDigest).Scan(&retirements))
	require.Equal(t, 1, retirements)
	newGrant, err := store.GetRootFSWriterGrant(ctx, target.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateConsumed, newGrant.State)
	require.Greater(t, newGrant.WriterEpoch, grant.WriterEpoch)
	writeReceipt("provider-admission-result.json", map[string]any{"provider_retirement_and_automatic_admission": true,
		"sandbox_id": source.SandboxID, "source": source, "target": target, "proof": proof,
		"proof_digest": hex.EncodeToString(digest), "source_writer_epoch": grant.WriterEpoch, "target_writer_epoch": newGrant.WriterEpoch,
		"last_remote_generation_id": lastRemoteGeneration, "last_remote_sequence": lastRemoteSequence})
	// Keep the authority alive until the harness captures replacement data and
	// requests ordinary product deletion. Admission alone is not the final gate.
	cleaned := false
	for {
		select {
		case err := <-errorsCh:
			require.NoError(t, err)
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			if _, err := os.Stat(filepath.Join(root, "request-provider-cleanup")); err == nil && !cleaned {
				require.NoError(t, runtime.TerminateSandbox(ctx, source.SandboxID))
				require.Eventually(t, func() bool {
					deleted, err := store.GetSandbox(ctx, source.SandboxID)
					return err == nil && deleted != nil && deleted.DesiredState == sandboxstore.SandboxDesiredStateDeleted
				}, 3*time.Minute, 100*time.Millisecond, "ordinary product cleanup must settle the replacement runtime")
				writeReceipt("provider-cleanup-result.json", map[string]any{"sandbox_id": source.SandboxID, "deleted": true})
				cleaned = true
			}
			if _, err := os.Stat(filepath.Join(root, "stop-region")); err == nil {
				require.True(t, cleaned, "capture replacement content and settle ordinary product cleanup before stopping")
				return
			}
		}
	}
}
