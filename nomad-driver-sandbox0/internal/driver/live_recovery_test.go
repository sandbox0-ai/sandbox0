//go:build linux

package driver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestLiveRegionalRecoveryKeepsOriginalCarrierRegistration(t *testing.T) {
	for _, scenario := range []string{"active", "starting", "foreign claim", "warm", "quiescing"} {
		t.Run(scenario, func(t *testing.T) {
			authority := &fakeRuntimeSlotAuthority{state: protocol.StateActive, heartbeatTTL: time.Minute}
			switch scenario {
			case "starting":
				authority.state = protocol.StateStarting
			case "foreign claim":
				authority.claimID = "foreign"
			case "warm":
				authority.state = protocol.StateFastpathReady
			case "quiescing":
				authority.state = protocol.StateQuiescing
			}
			lifecycle := &runtimeSlotLifecycle{authority: authority, slotID: "slot"}
			_, err := lifecycle.recoverLive(t.Context(), &claimMetadata{OperationID: "operation-1", ClaimID: "claim-1"})
			if scenario == "active" || scenario == "starting" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Empty(t, authority.registrations)
			require.Empty(t, authority.readiness)
			require.Len(t, authority.heartbeats, 1)
		})
	}
}

type liveTestRunsc struct {
	*fakeRunsc
	identity RunscState
}

func (r liveTestRunsc) State(context.Context, string) (RunscState, error) { return r.identity, nil }

type liveTestRootFS struct {
	*fakeRootFSRuntime
	adoptErr error
	adopted  chan RootFSConsumerRequest
	renew    func(context.Context, RootFSConsumerLease) (RootFSConsumerLease, error)
}

func (r *liveTestRootFS) AdoptLiveConsumer(_ context.Context, _ rootfshandoff.StageRequest, request RootFSConsumerRequest) (RootFSConsumerLease, error) {
	if r.adopted != nil {
		r.adopted <- request
	}
	return RootFSConsumerLease{LeaseID: "successor", ExpiresAt: time.Now().Add(time.Minute)}, r.adoptErr
}
func (r *liveTestRootFS) RenewConsumer(ctx context.Context, _ rootfshandoff.StageRequest, lease RootFSConsumerLease) (RootFSConsumerLease, error) {
	return r.renew(ctx, lease)
}

func TestLiveRecoveryNeverRecreatesOrStopsRunningGuest(t *testing.T) {
	for _, scenario := range []string{"success", "owner unavailable", "identity changed", "adoption rejected"} {
		t.Run(scenario, func(t *testing.T) {
			base := newFakeRunsc()
			runtime := &liveTestRootFS{fakeRootFSRuntime: &fakeRootFSRuntime{}, adopted: make(chan RootFSConsumerRequest, 1)}
			runtime.runtimeInfo, _ = runtime.RuntimeInfo(t.Context())
			runtime.runtimeInfo.LiveUpdateProtocol = 1
			bundle := t.TempDir()
			runner := liveTestRunsc{base, RunscState{ID: "guest", Status: "running", Bundle: bundle, PID: 123}}
			if scenario == "owner unavailable" {
				runtime.pingErr = errdefs.ErrUnavailable
			}
			if scenario == "identity changed" {
				runner.identity.ID = "foreign"
			}
			if scenario == "adoption rejected" {
				runtime.adoptErr = errdefs.ErrFailedPrecondition
			}
			handle := newTaskHandle(taskHandleOptions{taskConfig: &drivers.TaskConfig{ID: "task"}, rootfs: runtime, runner: runner, containerID: "guest", bundleDir: bundle, rootMount: "/task/rootfs"})
			handle.stage = &rootfshandoff.StageRequest{}
			attempted, err := handle.recoverLiveRootFS()
			require.True(t, attempted, "the live protocol must not fall through to destructive crash recovery")
			if scenario == "success" {
				require.NoError(t, err)
				request := <-runtime.adopted
				require.Equal(t, 1, request.RenewalProtocol)
				require.NotEmpty(t, request.OwnerProcess)
				require.Equal(t, phaseActive, handle.phase)
				require.NotNil(t, handle.recoveredConsumerLease)
				handle.stopExitWatch()
			} else {
				require.Error(t, err)
			}
			for _, call := range base.callsSnapshot() {
				require.Equal(t, "wait", call)
			}
		})
	}
}

func TestLiveConsumerRenewalRetriesOnlyWithinProvenLease(t *testing.T) {
	for _, scenario := range []string{"transient", "revoked", "expired", "changed lease", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			calls := make(chan struct{}, 10)
			base := newFakeRunsc()
			runtime := &liveTestRootFS{fakeRootFSRuntime: &fakeRootFSRuntime{}}
			handle := newTaskHandle(taskHandleOptions{taskConfig: &drivers.TaskConfig{ID: "task"}, rootfs: runtime, runner: base, containerID: "guest", bundleDir: t.TempDir(), mounter: &fakeMounter{}})
			handle.phase = phaseActive
			runtime.renew = func(ctx context.Context, lease RootFSConsumerLease) (RootFSConsumerLease, error) {
				calls <- struct{}{}
				switch scenario {
				case "revoked":
					return RootFSConsumerLease{}, errdefs.ErrPermissionDenied
				case "changed lease":
					lease.LeaseID = "foreign"
					return lease, nil
				case "canceled":
					<-ctx.Done()
					return RootFSConsumerLease{}, ctx.Err()
				default:
					return RootFSConsumerLease{}, fmt.Errorf("temporary node service outage: %w", errdefs.ErrUnavailable)
				}
			}
			duration := 3 * time.Second
			if scenario == "expired" {
				duration = 1200 * time.Millisecond
			}
			handle.startConsumerRenewal(rootfshandoff.StageRequest{}, RootFSConsumerLease{LeaseID: "current", ExpiresAt: time.Now().Add(duration)})
			defer handle.stopConsumerRenewal()
			select {
			case <-calls:
			case <-time.After(2 * time.Second):
				t.Fatal("renewal was not attempted")
			}
			if scenario == "transient" || scenario == "canceled" {
				handle.stopConsumerRenewal()
				time.Sleep(50 * time.Millisecond)
				require.True(t, handle.IsRunning())
				require.Empty(t, base.callsSnapshot())
			} else {
				select {
				case <-handle.done:
				case <-time.After(2 * time.Second):
					t.Fatal("lost lease did not fence task")
				}
			}
		})
	}
}
