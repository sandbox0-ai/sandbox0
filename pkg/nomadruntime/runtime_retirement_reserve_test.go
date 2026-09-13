package nomadruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Use real session journals, branches, retirement accounting and reclamation.
// Only kernel side effects and the regional authority are simulated here.
func TestNodeRuntimeExternalReclamationReleasesSharedRetirementReserve(t *testing.T) {
	for _, tc := range []struct {
		name         string
		err          error
		missingProof bool
	}{
		{name: "pending_terminal_authority", err: errdefs.ErrFailedPrecondition},
		{name: "denied_terminal_authority", err: errdefs.ErrPermissionDenied},
		{name: "missing_physical_proof", missingProof: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRuntimeTerminalExpiryFixture(t)
			require.NoError(t, fixture.manager.Close())
			host := &retirementReserveHost{runtimeTerminalExpiryHost: fixture.host, devices: make(map[string]*retirementReserveDevice)}
			fixture.config.Runtime = host
			var err error
			fixture.manager, err = rootfssession.New(fixture.config)
			require.NoError(t, err)
			first := fixture.stage
			second := first
			second.Parent = digest.FromString("second-retirement-parent").String()
			second.Identity.RootFSID = "second-rootfs"
			second.Identity.SlotNonce = "second-slot"
			second.Identity.WriterGrantID = "second-grant"
			generation := *first.Generation
			generation.FilesystemID = second.Identity.RootFSID
			second.Generation = &generation
			deadline := time.Now().Add(time.Minute)
			for _, stage := range []rootfshandoff.StageRequest{first, second} {
				_, err := fixture.manager.Ensure(t.Context(), stage)
				require.NoError(t, err)
				require.NoError(t, fixture.manager.RegisterConsumer(stage.Parent, stage.Identity, rootfssession.ConsumerRegistration{
					LeaseID: stage.Parent, ActiveKey: "task", ContainerID: stage.Identity.SlotNonce,
					StableMount: filepath.Join(t.TempDir(), "rootfs"), HostMountNamespace: "mnt:[1]",
					LeaseExpiresAt: deadline.Format(time.RFC3339Nano),
				}))
			}
			require.Positive(t, fixture.manager.NodeDirtyTailUsage().UsedBytes)
			require.Equal(t, 2, fixture.manager.NodeDirtyTailUsage().Owners)
			require.NoError(t, fixture.manager.Release(t.Context(), first.Identity))
			if tc.missingProof {
				host.inspectErr = errdefs.ErrUnavailable
			}
			proof, err := fixture.manager.CrashFenceExternal(first, "external-retirement")
			if tc.missingProof {
				require.ErrorIs(t, err, errdefs.ErrUnavailable)
			} else {
				require.NoError(t, err)
				require.NoError(t, proof.Validate())
			}
			var busy *rootfsblock.DirtyTailRetirementBusyError
			require.ErrorAs(t, fixture.manager.Release(t.Context(), second.Identity), &busy)
			require.Equal(t, first.Parent, busy.ActiveGroup)
			require.Equal(t, second.Parent, busy.RequestedGroup)

			before := retirementBranchFiles(t, fixture.config.BranchRoot)
			require.Len(t, before, 2)
			usage := fixture.manager.NodeDirtyTailUsage()
			authority := &retirementReserveAuthority{err: tc.err}
			runtime := &rootfsRuntime{sessions: fixture.manager, authority: authority}
			daemon := &nodeRuntime{runtime: runtime, logger: newLogger(zap.NewNop())}
			daemon.scanAt(t.Context(), "", time.Now())
			waitRecoveryWorkers(t, daemon)
			require.Len(t, authority.requests, 1, "reclamation must be attempted before consumer expiry")
			require.Equal(t, []rootfshandoff.StageRequest{first}, authority.requests,
				"periodic recovery must check the external writer before its consumer lease expires")
			require.Equal(t, before, retirementBranchFiles(t, fixture.config.BranchRoot), "unacknowledged bytes must remain intact")
			require.Equal(t, usage, fixture.manager.NodeDirtyTailUsage())
			require.ErrorAs(t, fixture.manager.Release(t.Context(), second.Identity), &busy)

			// Region acknowledgement, not elapsed consumer TTL or forced fencing,
			// permits branch deletion and hands the shared reserve to the next writer.
			authority.err = nil
			if tc.missingProof {
				host.inspectErr = nil
				proof, err = fixture.manager.CrashFenceExternal(first, "external-retirement")
				require.NoError(t, err)
				require.NoError(t, proof.Validate())
			}
			daemon.mu.Lock()
			next := daemon.recoveryRetries[first.Parent].next
			daemon.mu.Unlock()
			require.True(t, next.Before(deadline))
			daemon.scanAt(t.Context(), "", next)
			waitRecoveryWorkers(t, daemon)
			require.Equal(t, []rootfshandoff.StageRequest{first, first}, authority.requests)
			require.Len(t, retirementBranchFiles(t, fixture.config.BranchRoot), 1)
			require.Equal(t, 1, fixture.manager.NodeDirtyTailUsage().Owners)
			require.NoError(t, fixture.manager.Release(t.Context(), second.Identity))
			require.True(t, time.Now().Before(deadline), "reserve transfer must not wait for consumer expiry")
		})
	}
}

func retirementBranchFiles(t *testing.T, root string) map[string]string {
	t.Helper()
	files := make(map[string]string)
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		payload, err := os.ReadFile(path)
		files[path] = string(payload)
		return err
	}))
	return files
}

type retirementReserveAuthority struct {
	rootFSWriterAuthority
	err      error
	requests []rootfshandoff.StageRequest
}

func (a *retirementReserveAuthority) VerifyTerminalWriterGrant(_ context.Context, stage rootfshandoff.StageRequest) error {
	a.requests = append(a.requests, stage)
	return a.err
}

type retirementReserveHost struct {
	*runtimeTerminalExpiryHost
	devices    map[string]*retirementReserveDevice
	inspectErr error
}

func (h *retirementReserveHost) ReserveDevice(allocationID string) (string, error) {
	return "/dev/test-" + allocationID, nil
}

func (h *retirementReserveHost) AttachDevice(_ context.Context, _ context.Context, path, _ string, backend rootfsblock.WritableBlockDevice) (rootfssession.Device, error) {
	_, err := backend.WriteAt([]byte{0x71}, 0)
	if err != nil {
		return nil, err
	}
	device := &retirementReserveDevice{path: path}
	h.devices[path] = device
	return device, nil
}

func (h *retirementReserveHost) ReleaseDeviceReservation(path, _ string) {
	require.True(h.t, h.devices[path].closed)
}

func (h *retirementReserveHost) MountXFS(_, _ string) error     { return nil }
func (h *retirementReserveHost) MountOverlay(_, _ string) error { return nil }

func (h *retirementReserveHost) InspectCrashFence(path, _, _ string) (rootfssession.CrashFenceHostObservation, error) {
	if h.inspectErr != nil {
		return rootfssession.CrashFenceHostObservation{}, h.inspectErr
	}
	require.True(h.t, h.devices[path].closed)
	return rootfssession.CrashFenceHostObservation{NBDPoolAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true}, nil
}

type retirementReserveDevice struct {
	path   string
	closed bool
}

func (d *retirementReserveDevice) Path() string { return d.path }
func (d *retirementReserveDevice) Close() error {
	d.closed = true
	return nil
}
