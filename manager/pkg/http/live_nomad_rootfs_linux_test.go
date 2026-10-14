//go:build linux

package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxclaimreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	templatepg "github.com/sandbox0-ai/sandbox0/pkg/template/store/pg"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type livePublicRootFSMigrationLogger struct{}

func (livePublicRootFSMigrationLogger) Printf(string, ...any) {}
func (livePublicRootFSMigrationLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// The isolated Nomad harness starts this authority instead of its basic
// regional fixture. Every public mutation uses the production authenticated
// router, product services, PostgreSQL and the actual node channel.
func TestLiveNomadPublicRootFSAuthorityFixture(t *testing.T) {
	if os.Getenv("S0_NOMAD_LIVE_PUBLIC_API") != "1" {
		t.Skip("requires the disposable public RootFS/Nomad fixture")
	}
	require.Equal(t, "1", os.Getenv("S0_NOMAD_LIVE_REAL_ROOTFS"))
	require.Zero(t, os.Geteuid())
	root := "/opt/s0-live-update-test/f"
	for _, marker := range []string{"/opt/s0-live-update-test/isolated-fixture", "/opt/s0-rootfs-autogrow-test/isolated-fixture"} {
		_, err := os.Stat(marker)
		require.NoError(t, err)
	}
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
	nodeID := os.Getenv("S0_NOMAD_LIVE_NODE_ID")
	require.NotEmpty(t, nodeID)
	pki := filepath.Join(root, "pki")
	component, err := nodeauthority.New(nodeauthority.Config{
		Store: store, Address: "127.0.0.1:19445", CertFile: filepath.Join(pki, "peer.pem"),
		KeyFile: filepath.Join(pki, "peer-key.pem"), ClientCAFile: filepath.Join(pki, "ca.pem"),
		WriterLeaseTTL: 10 * time.Minute, RuntimeSlotHeartbeatTTL: time.Minute, RegionID: "fixture-region",
		Identities: []nodeauth.CertificateIdentity{{CommonName: "s0-live-fixture", ClusterID: "fixture-cluster", NodeID: nodeID, NodeUID: "fixture-node-1", AgentUID: "fixture-ctld"}},
		Terminal:   runtimeslotterminal.Config{Enabled: true, NomadEndpointsFile: filepath.Join(root, "nomad-endpoints.json"), Interval: 100 * time.Millisecond, PassTimeout: 10 * time.Second},
	})
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
		t.Fatal("public fixture node authority did not bind")
	}
	start(func() error { return component.RunTerminal(ctx, nil) })
	claims, err := sandboxclaimreconciler.New(sandboxclaimreconciler.Config{Store: store})
	require.NoError(t, err)
	start(func() error {
		claims.Run(ctx, func(result sandboxclaimreconciler.Result, err error) {
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Logf("sandbox claim cleanup pass failed: %+v: %v", result, err)
			}
		})
		return ctx.Err()
	})
	ready := filepath.Join(root, "region-ready")
	require.NoError(t, os.WriteFile(ready, []byte("public_rootfs_real_node_authority\n"), 0600))
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
			if _, err := os.Stat(filepath.Join(root, "request-public-api")); err == nil && !performed {
				performed = true
				livePublicRootFSLifecycle(t, ctx, store, quota.NewRepository(pool), templatepg.NewStore(pool), component, root, start)
			}
		}
	}
}

func livePublicRootFSLifecycle(t *testing.T, ctx context.Context, store *sandboxstore.PGSandboxStore, quotas *quota.Repository,
	templates *templatepg.Store, component *nodeauthority.Component, root string, start func(func() error)) {
	t.Helper()
	var stage rootfshandoff.StageRequest
	livePublicRootFSReadJSON(t, filepath.Join(root, "regional-stage.json"), &stage)
	require.NoError(t, stage.ValidateDurableBinding())
	key, err := internalauth.LoadEd25519PrivateKeyFromFile(filepath.Join(root, "pki/procd-key.pem"))
	require.NoError(t, err)
	writerKey := make([]byte, 32)
	_, err = rand.Read(writerKey)
	require.NoError(t, err)
	planner, err := component.NewClaimPlanner(nodeauthority.ClaimPlannerConfig{
		Prober:         procdapi.NewProcdClient(procdapi.ProcdClientConfig{Timeout: 10 * time.Second}),
		TokenGenerator: runtimeauth.NewInternalTokenGenerator(internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "manager", PrivateKey: key})),
		WriterTokenKey: writerKey, ClaimTTL: time.Minute,
	})
	require.NoError(t, err)
	classes, err := nomadclaim.LoadRuntimeClassCatalog(filepath.Join(root, "public-runtime-classes.json"))
	require.NoError(t, err)
	procdDigest, err := exec.Command(filepath.Join(root, "bin/ctld"), "--procd-digest").Output()
	require.NoError(t, err)
	runtime, err := nomadclaim.New(nomadclaim.Config{Store: store, Templates: templates, RuntimeClasses: classes, Planner: planner,
		Allocation: component.NomadAllocationController(), PlannedRetire: component, RunningFork: component, PausedRebase: component,
		QuotaLimits: quotas, NetworkPolicies: networkpolicy.NewNetworkPolicyService(zap.NewNop()), ClaimTTL: time.Minute,
		RootFSFormatGeneration: stage.Generation.FormatGeneration, RootFSProcdProtocol: "sandbox0.procd.v3", RootFSProcdDigest: strings.TrimSpace(string(procdDigest)),
	})
	require.NoError(t, err)
	pauses := service.NewSandboxPauseController(store, runtime, zap.NewNop())
	runtime.SetPauseEnqueuer(pauses)
	start(func() error { return pauses.Run(ctx, 1) })
	rootFS, err := service.NewNomadSandboxRootFSService(store, runtime, time.Now)
	require.NoError(t, err)
	reader, err := service.NewNomadSandboxReader(store)
	require.NoError(t, err)
	obs, err := observability.New(observability.Config{ServiceName: "isolated-public-rootfs", Logger: zap.NewNop(), DisableTracing: true, DisableMetrics: true, DisableLogging: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, obs.Shutdown(context.Background())) }()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey})
	server := NewServerWithDependencies(ServerDependencies{SandboxReader: reader, SandboxRootFS: rootFS, SandboxClaimer: runtime,
		AuthValidator: internalauth.NewValidator(internalauth.ValidatorConfig{Target: "manager", PublicKey: publicKey, AllowedCallers: []string{"cluster-gateway"}}),
		Logger:        zap.NewNop(), ObservabilityProvider: obs})
	endpoint := httptest.NewServer(server.router)
	defer endpoint.Close()
	var warm struct {
		RootMount string `json:"root_mount"`
	}
	livePublicRootFSReadJSON(t, "/opt/s0-live-update-test/warm-state.json", &warm)
	require.True(t, strings.HasPrefix(warm.RootMount, "/opt/nomad/"))
	before := livePublicRootFSGuest(t, warm.RootMount)
	require.True(t, before["growth_verified"].(bool))
	expectedHash := livePublicRootFSFileHash(t, filepath.Join(warm.RootMount, "growing.bin"))
	source := stage.Identity.RootFSID
	snapshot := livePublicRootFSRequest[service.SandboxRootFSSnapshot](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/snapshots", "public-snapshot", map[string]string{"name": "grown-rootfs"}, http.StatusCreated)
	fork := livePublicRootFSRequest[service.ForkSandboxResponse](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/fork", "public-running-fork", nil, http.StatusCreated)
	require.NotNil(t, fork.Sandbox)
	child := fork.Sandbox.ID
	retry := livePublicRootFSRequest[service.ForkSandboxResponse](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/fork", "public-running-fork", nil, http.StatusCreated)
	require.Equal(t, child, retry.Sandbox.ID)
	snap, err := store.GetRootFSSnapshot(ctx, snapshot.ID, "live-nomad-team")
	require.NoError(t, err)
	objects := livePublicRootFSObjects(t)
	livePublicRootFSColdHash(t, store, objects, snap.HeadGenerationID, expectedHash, root)
	childFS, err := store.GetRootFSFilesystem(ctx, child)
	require.NoError(t, err)
	livePublicRootFSColdHash(t, store, objects, childFS.HeadGenerationID, expectedHash, root)
	after := livePublicRootFSGuest(t, warm.RootMount)
	for _, name := range []string{"pid", "token", "heap"} {
		require.Equal(t, before[name], after[name], "running snapshot/fork must preserve original process state")
	}
	require.Greater(t, after["counter"].(float64), before["counter"].(float64))
	list := livePublicRootFSRequest[service.ListSandboxRootFSSnapshotsResponse](t, endpoint.URL, generator, http.MethodGet,
		"/api/v1/sandboxes/"+source+"/snapshots", "public-list", nil, http.StatusOK)
	require.Equal(t, 1, list.Count)
	// This explicit filesystem-only lifecycle case stops only the disposable
	// guest. Production occupied-worker rollouts still require memory:true.
	livePublicRootFSRequest[service.PauseSandboxResponse](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/pause", "public-pause", map[string]bool{"memory": false}, http.StatusAccepted)
	require.Eventually(t, func() bool {
		r, err := store.GetSandbox(ctx, source)
		return err == nil && r.DesiredState == sandboxstore.SandboxDesiredStatePaused && r.RuntimeID == ""
	}, 3*time.Minute, 100*time.Millisecond)
	pausedSnapshot := livePublicRootFSRequest[service.SandboxRootFSSnapshot](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/snapshots", "public-paused-snapshot", nil, http.StatusCreated)
	pausedFork := livePublicRootFSRequest[service.ForkSandboxResponse](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+source+"/fork", "public-paused-fork", nil, http.StatusCreated)
	copyID := pausedFork.Sandbox.ID
	livePublicRootFSRequest[service.RestoreSandboxRootFSResponse](t, endpoint.URL, generator, http.MethodPost,
		"/api/v1/sandboxes/"+copyID+"/rootfs/restore", "public-restore", map[string]string{"snapshot_id": snapshot.ID}, http.StatusOK)
	target, err := os.ReadFile(filepath.Join(root, "public-rebase-artifact"))
	require.NoError(t, err)
	require.NotEqual(t, stage.Generation.BaseArtifactDigest, string(target))
	rebase := livePublicRootFSRequest[service.RebaseSandboxRootFSResponse](t, endpoint.URL, generator, http.MethodPut,
		"/api/v1/sandboxes/"+copyID+"/rootfs/rebase", "public-rebase", map[string]string{"target_base_artifact_digest": string(target)}, http.StatusOK)
	require.Equal(t, string(target), rebase.BaseArtifactDigest)
	for range 5 {
		_, err := store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(ctx, objects, "", 100, sandboxstore.DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	copyFS, err := store.GetRootFSFilesystem(ctx, copyID)
	require.NoError(t, err)
	livePublicRootFSColdHash(t, store, objects, copyFS.HeadGenerationID, expectedHash, root)
	for _, id := range []string{snapshot.ID, pausedSnapshot.ID} {
		livePublicRootFSRequest[map[string]bool](t, endpoint.URL, generator, http.MethodDelete,
			"/api/v1/sandbox-rootfs-snapshots/"+id, "public-delete-snapshot-"+id, nil, http.StatusOK)
	}
	for _, id := range []string{source, child, copyID} {
		livePublicRootFSRequest[map[string]any](t, endpoint.URL, generator, http.MethodDelete,
			"/api/v1/sandboxes/"+id, "public-delete-"+id, nil, http.StatusAccepted)
	}
	require.Eventually(t, func() bool {
		for _, id := range []string{source, child, copyID} {
			r, err := store.GetSandbox(ctx, id)
			if err != nil || r == nil || r.DeletedAt.IsZero() {
				return false
			}
		}
		return true
	}, time.Minute, 100*time.Millisecond)
	for range 5 {
		_, err := store.InventoryRootFSGeneration(ctx, objects, 100)
		require.NoError(t, err)
	}
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(ctx, objects, "", 100, sandboxstore.DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	result, err := json.Marshal(map[string]any{"public_api_lifecycle": true, "running_snapshot_fork_process_preserved": true,
		"snapshots": 2, "forks": 2, "restores": 1, "distinct_base_rebases": 1, "deleted_sandboxes": 3,
		"cold_full_file_checks": 3, "payload_sha256": expectedHash})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "public-api-result.json"), result, 0600))
	t.Log("public authenticated RootFS lifecycle with actual Nomad/node ownership and real S3 completed")
}

func livePublicRootFSRequest[T any](t *testing.T, base string, generator *internalauth.Generator, method, path, operation string, value any, status int) *T {
	t.Helper()
	t.Logf("public RootFS request: %s %s operation=%s", method, path, operation)
	started := time.Now().UTC()
	token, err := generator.Generate("manager", "live-nomad-team", "live-nomad-user", internalauth.GenerateOptions{
		Audit: &internalauth.AuditContext{Actor: internalauth.AuditActor{Kind: "user", ID: "live-nomad-user"}, OperationID: operation, IngressStartedAt: &started},
	})
	require.NoError(t, err)
	var body io.Reader
	if value != nil {
		data, err := json.Marshal(value)
		require.NoError(t, err)
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(t.Context(), method, base+path, body)
	require.NoError(t, err)
	request.Header.Set("X-Internal-Token", token)
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Minute}).Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	require.NoError(t, err)
	require.LessOrEqual(t, len(data), 1<<20)
	require.Equal(t, status, response.StatusCode, "%s %s: %s", method, path, data)
	result, apiError, err := spec.DecodeResponse[T](bytes.NewReader(data))
	require.NoError(t, err)
	require.Nil(t, apiError)
	require.NotNil(t, result)
	t.Logf("public RootFS response: %s %s status=%d", method, path, response.StatusCode)
	return result
}

func livePublicRootFSReadJSON(t *testing.T, path string, target any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, target))
}

func livePublicRootFSGuest(t *testing.T, mount string) map[string]any {
	t.Helper()
	var value map[string]any
	livePublicRootFSReadJSON(t, filepath.Join(mount, "guest-state.json"), &value)
	return value
}

func livePublicRootFSFileHash(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	require.NoError(t, err)
	return hex.EncodeToString(hash.Sum(nil))
}

func livePublicRootFSObjects(t *testing.T) objectstore.ContextConditionalStore {
	t.Helper()
	var config struct {
		Objects struct {
			Type      string `json:"type"`
			Bucket    string `json:"bucket"`
			Region    string `json:"region"`
			Endpoint  string `json:"endpoint"`
			AccessKey string `json:"access_key"`
			SecretKey string `json:"secret_key"`
		} `json:"rootfs_object_storage"`
	}
	livePublicRootFSReadJSON(t, "/etc/sandbox0/ctld.yaml", &config)
	require.Equal(t, "s3", config.Objects.Type)
	require.Equal(t, "http://127.0.0.1:19000", config.Objects.Endpoint)
	require.True(t, strings.HasPrefix(config.Objects.Bucket, "s0-autogrow-nomad-"))
	objects, err := objectstore.Create(objectstore.Config{Type: config.Objects.Type, Bucket: config.Objects.Bucket,
		Region: config.Objects.Region, Endpoint: config.Objects.Endpoint, AccessKey: config.Objects.AccessKey, SecretKey: config.Objects.SecretKey})
	require.NoError(t, err)
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	return conditional
}

// A fresh local reader has no source WAL/cache. Its disposable branch permits
// XFS log replay but never publishes writes or obtains a product writer grant.
func livePublicRootFSColdHash(t *testing.T, store *sandboxstore.PGSandboxStore, objects objectstore.ContextConditionalStore, head, expected, root string) {
	t.Helper()
	t.Logf("fresh NBD/XFS full-file hash: generation=%s", head)
	devicePath := "/dev/nbd63"
	state, err := os.ReadFile("/sys/block/nbd63/size")
	require.NoError(t, err)
	require.Equal(t, "0", strings.TrimSpace(string(state)))
	generation, err := store.GetRootFSGeneration(t.Context(), head)
	require.NoError(t, err)
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(objects, descriptor, 8<<20)
	require.NoError(t, err)
	directory, err := os.MkdirTemp(filepath.Join(root, "branches"), "public-cold-")
	require.NoError(t, err)
	mount, err := os.MkdirTemp(filepath.Join(root, "mounts"), "public-cold-")
	require.NoError(t, err)
	branch, err := rootfsblock.OpenBranchWithOptions(filepath.Join(directory, "read.wal"), rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: generation.FilesystemID, GenerationID: generation.ID,
		WriterEpoch: 1, LogicalSizeBytes: descriptor.LogicalSizeBytes, BaseRootDigest: descriptor.MappingRoot.RootDigest,
	}, reader, rootfsblock.BranchOptions{MaxDirtyTailBytes: 32 << 20})
	require.NoError(t, err)
	device, err := rootfsblock.StartKernelNBD(t.Context(), t.Context(), branch, rootfsblock.KernelNBDOptions{DevicePath: devicePath, RequestTimeout: 30 * time.Second})
	require.NoError(t, err)
	mounted := false
	defer func() {
		if mounted {
			if output, err := exec.Command("umount", mount).CombinedOutput(); err != nil {
				t.Errorf("retain cold reader after unmount failure: %v %s", err, output)
				return
			}
		}
		if err := device.Close(); err != nil {
			t.Errorf("retain cold reader after NBD close failure: %v", err)
			return
		}
		if err := branch.Close(); err != nil {
			t.Errorf("retain cold reader after WAL close failure: %v", err)
			return
		}
		require.NoError(t, os.RemoveAll(directory))
		require.NoError(t, os.RemoveAll(mount))
	}()
	output, err := exec.Command("mount", "-t", "xfs", "-o", "ro,nouuid", devicePath, mount).CombinedOutput()
	require.NoError(t, err, "%s", output)
	mounted = true
	require.Equal(t, expected, livePublicRootFSFileHash(t, filepath.Join(mount, "upper/growing.bin")))
	t.Logf("fresh NBD/XFS full-file hash passed: generation=%s sha256=%s", head, expected)
}
