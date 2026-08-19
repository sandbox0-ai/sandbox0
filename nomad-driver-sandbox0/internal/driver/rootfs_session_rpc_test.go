package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

func TestRootFSSessionRPCDelegatesLifecycleOverPrivateUnixSocket(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "sessiond.sock")
	runtime := &fakeRootFSRuntime{source: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serveRootFSSessionRuntime(ctx, socket, runtime, nil, nil) }()
	require.Eventually(t, func() bool {
		info, err := os.Stat(socket)
		return err == nil && info.Mode().Perm() == 0o600
	}, time.Second, 10*time.Millisecond)

	clientRuntime, err := newRootFSSessionClient(socket)
	require.NoError(t, err)
	require.NoError(t, clientRuntime.Ping(t.Context()))
	stage := rootfshandoff.StageRequest{Parent: "parent", Identity: rootfshandoff.Identity{
		RootFSID: "rootfs", WriterEpoch: 1,
	}}
	mount, err := clientRuntime.Ensure(t.Context(), stage, nil)
	require.NoError(t, err)
	require.Equal(t, runtime.source, mount.Source)
	lease, err := clientRuntime.RegisterConsumer(t.Context(), stage, RootFSConsumerRequest{
		ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]",
	})
	require.NoError(t, err)
	require.Equal(t, "fake-consumer", lease.LeaseID)
	_, err = clientRuntime.RenewConsumer(t.Context(), stage, lease)
	require.NoError(t, err)
	retired, err := clientRuntime.Retire(t.Context(), stage, "retire")
	require.NoError(t, err)
	require.Equal(t, "retire", retired.OperationID)
	crashed, err := clientRuntime.CrashFence(t.Context(), stage, "crash", crashTaskObservation{})
	require.NoError(t, err)
	require.Equal(t, "crash", crashed.OperationID)

	cancel()
	require.NoError(t, <-done)
}

func TestRootFSSessionDaemonClientModeNeedsNoStorageCredentialsInPlugin(t *testing.T) {
	config := &PluginConfig{
		RootFSEnabled: true, RootFSSessiondSocket: "/run/sandbox0/rootfs-sessiond.sock",
		RootFSMountRoot: "/run/sandbox0/rootfs", RootFSConsumerMountRoot: "/opt/nomad",
	}
	require.NoError(t, validateRootFSConfig(config))
	config.RootFSConsumerMountRoot = ""
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_consumer_mount_root")
}

func TestPluginFingerprintRequiresHealthyRootFSSessionDaemon(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "sessiond.sock")
	runtime := &fakeRootFSRuntime{source: t.TempDir()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serveRootFSSessionRuntime(ctx, socket, runtime, nil, nil) }()
	require.Eventually(t, func() bool {
		_, err := os.Stat(socket)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	plugin.config.RootFSEnabled = true
	plugin.config.RootFSSessiondSocket = socket
	fingerprint := plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateHealthy, fingerprint.Health)

	cancel()
	require.NoError(t, <-done)
	fingerprint = plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateUndetected, fingerprint.Health)
}

func TestRootFSSessionRPCPreservesErrorClass(t *testing.T) {
	runtime := &fakeRootFSRuntime{
		source: t.TempDir(), ensureErr: errors.Join(errors.New("binding mismatch"), errdefs.ErrFailedPrecondition),
	}
	server := rootFSSessionRPCHandler(runtime, nil, nil)
	request := rootFSSessionRPCRequest{Stage: rootfshandoff.StageRequest{Parent: "parent"}}
	payload, err := json.Marshal(request)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/v1/sessions/ensure", bytes.NewReader(payload)))
	require.Equal(t, http.StatusPreconditionFailed, recorder.Code)
	var response rootFSSessionRPCResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.ErrorIs(t, remoteRootFSError(response.Error, response.ErrorClass), errdefs.ErrFailedPrecondition)
}

func TestRootFSSessionReconcileSelectionUsesDurableConsumerLease(t *testing.T) {
	now := time.Now()
	base := rootfssession.RecoverySession{
		Kind: rootfssession.RecoveryCrashAbandon, CreatedAt: now.Add(-time.Minute), Live: true,
	}
	require.False(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.CreatedAt = now.Add(-rootFSSessionAttachGrace)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))

	base.Consumer = &rootfssession.ConsumerRegistration{
		LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: "/tmp/task/rootfs",
		HostMountNamespace: "mnt:[1]", LeaseExpiresAt: now.Add(time.Minute).Format(time.RFC3339Nano),
	}
	require.False(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Live = false
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Live = true
	base.Consumer.LeaseExpiresAt = now.Add(-time.Second).Format(time.RFC3339Nano)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
	base.Kind = rootfssession.RecoveryPlannedRetire
	base.Consumer.LeaseExpiresAt = now.Add(time.Minute).Format(time.RFC3339Nano)
	require.True(t, rootFSSessionNeedsReconciliation(base, now, false))
}

func TestRootFSSessionDaemonFencesRegisteredRunscAndStableMount(t *testing.T) {
	consumerRoot := t.TempDir()
	stableMount := filepath.Join(consumerRoot, "alloc", "rootfs")
	require.NoError(t, os.MkdirAll(stableMount, 0o755))
	runner := newFakeRunsc()
	runner.stateErr = errdefs.ErrNotFound
	mounter := &fakeMounter{}
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	daemon := &rootFSSessionDaemon{
		runner: runner, mounter: mounter, config: PluginConfig{RootFSConsumerMountRoot: consumerRoot},
	}
	observation, err := daemon.fenceHostRuntime(t.Context(), rootfssession.RecoverySession{
		Stage: rootfshandoff.StageRequest{Identity: rootfshandoff.Identity{ClaimID: "claim"}},
		Consumer: &rootfssession.ConsumerRegistration{
			LeaseID: "lease", ActiveKey: "task", ContainerID: "container", StableMount: stableMount,
			HostMountNamespace: hostMountNamespace, LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		},
	})
	require.NoError(t, err)
	require.Equal(t, "task", observation.ActiveKey)
	require.Equal(t, "container", observation.ContainerID)
	require.True(t, observation.ContainerAbsent)
	require.Equal(t, []string{"kill:KILL", "delete:force", "state"}, runner.callsSnapshot())
	require.Equal(t, []string{stableMount}, mounter.unmounts)
}

func TestRootFSSessionDaemonConvergesDurablePlannedAndCrashIntents(t *testing.T) {
	runtime := &fakeRootFSRuntime{}
	daemon := &rootFSSessionDaemon{runtime: runtime}
	stage := rootfshandoff.StageRequest{
		Parent: "parent", Identity: rootfshandoff.Identity{
			ClaimID: "claim", WriterGrantID: "grant", WriterEpoch: 7,
		},
	}
	require.NoError(t, daemon.reconcile(t.Context(), rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryPlannedRetire, RetireOperationID: "retire-operation",
	}))
	ensureCalls, retireCalls, parent, operation := runtime.snapshot()
	require.Zero(t, ensureCalls)
	require.Equal(t, 1, retireCalls)
	require.Equal(t, stage.Parent, parent)
	require.Equal(t, "retire-operation", operation)

	require.NoError(t, daemon.reconcile(t.Context(), rootfssession.RecoverySession{
		Stage: stage, Kind: rootfssession.RecoveryCrashAbandon,
	}))
	runtime.mu.Lock()
	require.Equal(t, 1, runtime.crashCalls)
	require.Equal(t, crashOperationID(stage), runtime.lastOperation)
	runtime.mu.Unlock()
}

func TestClassifyRunscNotFound(t *testing.T) {
	err := classifyRunscError("state", errors.New("exit status 1"), "container does not exist")
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestNomadAllocationSourceUsesAbsenceAsThePurgeFence(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "nomad.token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600))
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "/v1/node/node-a/allocations", request.URL.Path)
		require.Equal(t, "secret-token", request.Header.Get("X-Nomad-Token"))
		_, _ = writer.Write([]byte(`[
            {"ID":"running","DesiredStatus":"run","ClientStatus":"running"},
            {"ID":"pending","DesiredStatus":"run","ClientStatus":"pending"},
            {"ID":"stopping","DesiredStatus":"stop","ClientStatus":"running"},
            {"ID":"complete","DesiredStatus":"run","ClientStatus":"complete"}
        ]`))
	}))
	defer server.Close()
	source, err := newNomadAllocationSource(NomadAllocationConfig{
		Address: server.URL, NodeID: "node-a", TokenFile: tokenFile,
	})
	require.NoError(t, err)
	active, err := source.ActiveAllocations(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[string]bool{
		"running": true, "pending": true, "stopping": true, "complete": true,
	}, active)
	require.False(t, active["purged"])
}
