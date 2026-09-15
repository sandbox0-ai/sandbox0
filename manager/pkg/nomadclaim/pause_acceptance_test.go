package nomadclaim

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/stretchr/testify/require"
)

func TestPauseAcceptsDurableIntentWhileNodeChannelRecovers(t *testing.T) {
	for name, cause := range map[string]error{
		"disconnected": errdefs.ErrUnavailable,
		"deadline":     context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClaimServiceFixture(t)
			fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{
				SandboxID: "sandbox-1", OperationID: "retire-1", Source: sandboxstore.SandboxLifecycleSourceManual,
				ClaimOperationID: "claim-operation-1", ClaimID: "claim-1", SlotID: "slot-1",
				ClusterID: "cluster-1", AllocationID: "allocation-1", AllocationNamespace: "nomad", NodeID: "node-1",
				NodeUID: "node-uid-1", NodeBootID: "boot-1", WriterGrantID: "grant-1", WriterEpoch: 7,
				BindingVersion: sandboxstore.RootFSWriterBindingVersion, BindingDigest: bytes.Repeat([]byte{0x41}, 32),
			}
			enqueuer := &recordingPauseEnqueuer{}
			fixture.service.SetPauseEnqueuer(enqueuer)
			fixture.plannedRetire.err = fmt.Errorf("exact node channel unavailable: %w", cause)

			response, err := fixture.service.PauseSandboxAndWait(t.Context(), "sandbox-1")
			require.NoError(t, err)
			require.False(t, response.Paused)
			require.Equal(t, managerapi.SandboxStatusStarting, response.Status)
			require.Equal(t, []string{"sandbox-1"}, enqueuer.sandboxIDs)
			require.Equal(t, []string{"sandbox-1:manual"}, fixture.store.pauseSources)
			require.Empty(t, fixture.store.quiesceCalls)
			require.Empty(t, fixture.allocation.requests)
			require.Equal(t, []string{"plan"}, *fixture.pauseOrder)

			// Background retry must retain the exact retirement and writer binding.
			fixture.plannedRetire.err = nil
			require.ErrorIs(t, fixture.service.CompletePausingSandboxRuntime(t.Context(), "sandbox-1"), errNomadSandboxPausePending)
			require.Len(t, fixture.plannedRetire.requests, 2)
			require.Equal(t, fixture.plannedRetire.requests[0], fixture.plannedRetire.requests[1])
			require.Equal(t, fixture.plannedRetire.targets[0], fixture.plannedRetire.targets[1])
			require.Equal(t, []string{"plan", "plan", "quiesce", "stop"}, *fixture.pauseOrder)
			require.Equal(t, []string{"sandbox-1:manual"}, fixture.store.pauseSources)
		})
	}
}

func TestPauseDoesNotAcceptUncommittedIntent(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseErr = errdefs.ErrUnavailable
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)

	response, err := fixture.service.PauseSandboxAndWait(t.Context(), "sandbox-1")
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	require.Nil(t, response)
	require.Empty(t, enqueuer.sandboxIDs)
	require.Empty(t, fixture.plannedRetire.requests)
	require.Empty(t, fixture.allocation.requests)
}

func TestPausePreservesNonTransientCompletionError(t *testing.T) {
	fixture := newClaimServiceFixture(t)
	fixture.store.pauseCandidate = &sandboxstore.NomadSandboxPauseCandidate{SandboxID: "sandbox-1"}
	fixture.plannedRetire.err = errdefs.ErrFailedPrecondition
	enqueuer := &recordingPauseEnqueuer{}
	fixture.service.SetPauseEnqueuer(enqueuer)

	response, err := fixture.service.PauseSandboxAndWait(t.Context(), "sandbox-1")
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Nil(t, response)
	require.Equal(t, []string{"sandbox-1"}, enqueuer.sandboxIDs)
	require.Empty(t, fixture.store.quiesceCalls)
	require.Empty(t, fixture.allocation.requests)
}
