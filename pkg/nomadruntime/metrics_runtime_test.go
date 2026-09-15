package nomadruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

type metricTestBackend struct {
	*fakeRootFSRuntime
	pointReads int
}

func (r *metricTestBackend) RecoverySession(parent string) (rootfssession.RecoverySession, error) {
	r.pointReads++
	for _, session := range r.recoverySessions {
		if session.Stage.Parent == parent {
			return session, nil
		}
	}
	return rootfssession.RecoverySession{}, errdefs.ErrNotFound
}

type metricTestRunsc struct {
	*fakeRunsc
	beforeSample func()
}

func (r *metricTestRunsc) Stats(_ context.Context, id string) (RunscStats, error) {
	r.record("stats:" + id)
	if r.beforeSample != nil {
		r.beforeSample()
	}
	return RunscStats{Type: "stats", ID: id}, nil
}

func metricNodeFixture(t *testing.T) (*nodeRuntime, *metricTestBackend, *metricTestRunsc, specs.Spec) {
	t.Helper()
	root := t.TempDir()
	request := testNomadNodeClaimControlRequest(t)
	stage := request.Stage.WithoutWriterGrantToken()
	registration := testRuntimeSlotJournalRegistration(t, stage.Identity.SlotNonce)
	registration.ClusterID, registration.NodeID = request.Resources.ClusterID, request.Resources.NodeID
	registration.NodeBootID, registration.AllocationID = stage.Identity.BootID, stage.Identity.AllocationID
	registration.StableMount = filepath.Join(root, "task", "bundle", "rootfs")
	require.NoError(t, os.MkdirAll(filepath.Dir(registration.StableMount), 0o750))
	journal, err := newRuntimeSlotJournal(filepath.Join(root, "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Register(registration))
	bootFile := filepath.Join(root, "boot")
	require.NoError(t, os.WriteFile(bootFile, []byte(stage.Identity.BootID), 0o600))
	session := rootfssession.RecoverySession{Stage: stage, State: "ready", Live: true,
		Consumer: &rootfssession.ConsumerRegistration{LeaseID: "lease", ActiveKey: stage.Identity.SlotNonce,
			ContainerID: registration.RunscContainerID, StableMount: registration.StableMount,
			HostMountNamespace: registration.MountNamespaceID, LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}}
	backend := &metricTestBackend{fakeRootFSRuntime: &fakeRootFSRuntime{recoverySessions: []rootfssession.RecoverySession{session}}}
	runner := &metricTestRunsc{fakeRunsc: newFakeRunsc()}
	daemon := &nodeRuntime{runtime: backend, runner: runner, journal: journal, clusterID: registration.ClusterID,
		nodeID: registration.NodeID, nodeUID: stage.Identity.NodeUID,
		config: Config{RootFSConsumerMountRoot: root, RuntimeSlotNodeBootIDFile: bootFile}}
	assignment, err := json.Marshal(request.Runtime)
	require.NoError(t, err)
	period, quota, memory := uint64(100000), int64(150000), int64(768<<20)
	spec := specs.Spec{Root: &specs.Root{Path: "rootfs"},
		Process:     &specs.Process{Env: []string{runtimecontrol.EnvStaticAssignment + "=" + string(assignment)}},
		Linux:       &specs.Linux{Resources: &specs.LinuxResources{CPU: &specs.LinuxCPU{Period: &period, Quota: &quota}, Memory: &specs.LinuxMemory{Limit: &memory}}},
		Annotations: map[string]string{"com.sandbox0.alloc-id": stage.Identity.AllocationID, "com.sandbox0.task-id": stage.Identity.SlotNonce}}
	writeMetricBundle(t, session, spec)
	return daemon, backend, runner, spec
}

func writeMetricBundle(t *testing.T, session rootfssession.RecoverySession, spec specs.Spec) {
	t.Helper()
	data, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(session.Consumer.StableMount), "config.json"), data, 0o600))
}

func TestProductionNodeMetricProviderUsesExactBindingAndPointReads(t *testing.T) {
	d, backend, runner, _ := metricNodeFixture(t)
	targets, err := d.ListRuntimeMetricTargets(t.Context())
	require.NoError(t, err)
	require.Len(t, targets, 1)
	target := targets[0]
	require.Equal(t, "team-1", target.TeamID)
	require.Equal(t, "sandbox-1", target.SandboxID)
	require.Equal(t, int64(1500), target.CPUMillicpu)
	require.Equal(t, int64(768), target.MemoryMiB)
	encoded, err := json.Marshal(target)
	require.NoError(t, err)
	for _, secret := range []string{"writer-token", "rootfs", "netns", "lease_id", "EnvVars"} {
		require.NotContains(t, string(encoded), secret)
	}
	sample, err := d.RuntimeMetricStats(t.Context(), target)
	require.NoError(t, err)
	require.NoError(t, sample.Validate(target))
	require.Equal(t, 2, backend.pointReads, "sampling must not scan every session for each sandbox")
	require.Equal(t, []string{"stats:" + target.RunscContainerID}, runner.callsSnapshot())
	foreign := target
	foreign.SandboxID = "another-team-sandbox"
	_, err = d.RuntimeMetricStats(t.Context(), foreign)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.Len(t, runner.callsSnapshot(), 1)
	// The actual production daemon, rather than a fake metric provider, must
	// implement both RPC endpoints on its existing root-owned control socket.
	socket := filepath.Join(t.TempDir(), "runtime.sock")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serveNodeRuntime(ctx, socket, backend, nil, nil, d) }()
	client, err := NewClient(socket)
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, err := os.Stat(socket); return err == nil }, time.Second, time.Millisecond)
	listed, err := client.ListRuntimeMetricTargets(t.Context())
	require.NoError(t, err)
	require.Equal(t, targets, listed)
	_, err = client.RuntimeMetricStats(t.Context(), listed[0])
	require.NoError(t, err)
	cancel()
	require.NoError(t, <-done)
}

func TestRuntimeMetricsExcludeRetiringExpiredAndForeignBootSessions(t *testing.T) {
	for _, state := range []string{"not-live", "expired", "planned", "crashed", "pressure", "old-boot", "cleaning"} {
		t.Run(state, func(t *testing.T) {
			d, backend, _, _ := metricNodeFixture(t)
			session := &backend.recoverySessions[0]
			switch state {
			case "not-live":
				session.Live = false
			case "expired":
				session.Consumer.LeaseExpiresAt = time.Now().Add(-time.Second).Format(time.RFC3339Nano)
			case "planned":
				session.RetireOperationID = "pause"
			case "crashed":
				session.ExternalCrash = true
			case "pressure":
				session.PressureOperationID = "pressure"
			case "old-boot":
				require.NoError(t, os.WriteFile(d.config.RuntimeSlotNodeBootIDFile, []byte("another-boot"), 0o600))
			case "cleaning":
				record, err := d.journal.Get(session.Stage.Identity.SlotNonce)
				require.NoError(t, err)
				_, err = d.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
				require.NoError(t, err)
			}
			targets, err := d.ListRuntimeMetricTargets(t.Context())
			require.NoError(t, err)
			require.Empty(t, targets)
		})
	}
}

func TestRuntimeMetricSampleRejectsCleanupDuringRunscObservation(t *testing.T) {
	d, backend, runner, _ := metricNodeFixture(t)
	targets, err := d.ListRuntimeMetricTargets(t.Context())
	require.NoError(t, err)
	runner.beforeSample = func() { backend.recoverySessions[0].RetireOperationID = "pause-during-stats" }
	_, err = d.RuntimeMetricStats(t.Context(), targets[0])
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestRuntimeMetricsAcceptNomadTaskDirectoryButAuthenticateItsBundle(t *testing.T) {
	d, backend, _, _ := metricNodeFixture(t)
	taskDir := filepath.Dir(filepath.Dir(backend.recoverySessions[0].Consumer.StableMount))
	require.NoError(t, os.Chmod(taskDir, 0o777))
	if os.Geteuid() == 0 {
		require.NoError(t, os.Chown(taskDir, 65534, 65534))
	}
	targets, err := d.ListRuntimeMetricTargets(t.Context())
	require.NoError(t, err)
	require.Len(t, targets, 1)
}

func TestRuntimeMetricBundleRejectsChangedAssignmentAndUnsafePaths(t *testing.T) {
	for _, change := range []string{"assignment", "duplicate", "allocation", "memory", "unbounded", "file-link", "parent-link", "writable-parent", "writable-file", "oversized-file"} {
		t.Run(change, func(t *testing.T) {
			d, backend, _, spec := metricNodeFixture(t)
			session := backend.recoverySessions[0]
			bundle := filepath.Dir(session.Consumer.StableMount)
			path := filepath.Join(bundle, "config.json")
			switch change {
			case "assignment":
				spec.Process.Env[0] = strings.ReplaceAll(spec.Process.Env[0], "team-1", "another-team")
			case "duplicate":
				spec.Process.Env = append(spec.Process.Env, spec.Process.Env[0])
			case "allocation":
				spec.Annotations["com.sandbox0.alloc-id"] = "another-allocation"
			case "memory":
				spec.Linux.Resources.Memory.Limit = nil
			case "unbounded":
				*spec.Linux.Resources.CPU.Period = 0
			}
			writeMetricBundle(t, session, spec)
			switch change {
			case "file-link":
				require.NoError(t, os.Rename(path, path+".original"))
				require.NoError(t, os.Symlink(path+".original", path))
			case "parent-link":
				require.NoError(t, os.Rename(bundle, bundle+".original"))
				require.NoError(t, os.Symlink(bundle+".original", bundle))
			case "writable-parent":
				require.NoError(t, os.Chmod(bundle, 0o777))
			case "writable-file":
				require.NoError(t, os.Chmod(path, 0o666))
			case "oversized-file":
				require.NoError(t, os.WriteFile(path, []byte(strings.Repeat("x", runtimeMetricBundleMaxBytes+1)), 0o600))
			}
			_, err := d.ListRuntimeMetricTargets(t.Context())
			require.Error(t, err)
			require.NotContains(t, err.Error(), "writer-token")
		})
	}
}
