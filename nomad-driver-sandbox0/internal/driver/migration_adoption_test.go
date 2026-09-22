package driver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func (c *restoreCustodian) AdoptMigrationDestination(_ context.Context, request protocol.MigrationAdoptionRequest) (*protocol.MigrationAdoptionProof, error) {
	c.mu.Lock()
	if c.custody.Restore == nil || request.ValidateFor(*c.custody.Restore) != nil {
		c.mu.Unlock()
		return nil, errdefs.ErrFailedPrecondition
	}
	digest, _ := request.Digest()
	if c.custody.Adoption != nil && c.custody.Adoption.RequestDigest != digest {
		c.mu.Unlock()
		return nil, errdefs.ErrAlreadyExists
	}
	proof := protocol.MigrationAdoptionProof{RequestDigest: digest, ImageAbsent: true}
	c.custody.Adoption = &nomadruntime.MigrationAdoptionCustody{Request: request, RequestDigest: digest, Proof: &proof}
	hook, lost := c.adoptionHook, c.loseAdoptionResponse
	c.adoptionHook, c.loseAdoptionResponse = nil, false
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	if lost {
		return nil, errdefs.ErrUnavailable
	}
	return &proof, nil
}

func TestMigrationAdoptionCommandReadyReturnsDurableReceiptAndAllowsStop(t *testing.T) {
	h, claim, runner, _, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	proof := commandReadyProof(fixture, *claim.Stage)
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	client, err := protocol.NewNodeClient(protocol.NodeClientConfig{AllowedSocketRoot: fixture.config.ControlDir})
	require.NoError(t, err)
	result, err := client.CommandReady(t.Context(), "unix://"+h.socketPath, protocol.CommandReadyControlRequest{Proof: proof})
	require.NoError(t, err)
	require.NotNil(t, result.MigrationAdoption)
	require.NoError(t, result.MigrationAdoption.Proof.ValidateFor(result.MigrationAdoption.Request))
	retry, err := client.CommandReady(t.Context(), "unix://"+h.socketPath, protocol.CommandReadyControlRequest{Proof: proof})
	require.NoError(t, err)
	require.Equal(t, result.MigrationAdoption, retry.MigrationAdoption)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.NoError(t, h.Signal("USR1"))
	require.NoError(t, h.Stop(0, "TERM"))
	require.Contains(t, runner.callsSnapshot(), "kill:TERM")
}

func TestMigrationAdoptionLostResponseRetryDoesNotRestoreAgain(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	proof := commandReadyProof(fixture, *claim.Stage)
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	c.mu.Lock()
	c.loseAdoptionResponse = true
	c.mu.Unlock()
	require.ErrorIs(t, h.CommandReady(CommandReadyRequest{Proof: proof}), errdefs.ErrUnavailable)
	h.closeMu.Lock()
	refreshErr := h.refreshMigrationAdoption()
	h.closeMu.Unlock()
	require.NoError(t, refreshErr)
	require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: proof}))
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	h.mu.Lock()
	unresolved := h.hasMigrationRestoreLocked()
	h.mu.Unlock()
	require.False(t, unresolved)
}

func TestMigrationAdoptionLeaseLossDuringRPCStillFencesExecution(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "receipt", true: "lost receipt"}[lost], func(t *testing.T) {
			h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
			require.NoError(t, h.Claim(claim))
			proof := commandReadyProof(fixture, *claim.Stage)
			proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
			c.mu.Lock()
			c.loseAdoptionResponse = lost
			c.adoptionHook = func() { h.handleWriterLeaseLoss(errors.New("injected lease loss after durable adoption")) }
			c.mu.Unlock()
			require.Error(t, h.CommandReady(CommandReadyRequest{Proof: proof}))
			require.Eventually(t, func() bool {
				h.mu.Lock()
				defer h.mu.Unlock()
				return !h.hasMigrationRestoreLocked() && (h.phase == phasePoisoned || h.closed)
			}, 3*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return contains(runner.callsSnapshot(), "kill:KILL") || contains(runner.callsSnapshot(), "delete:force")
			}, 3*time.Second, 10*time.Millisecond)
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
		})
	}
}

func TestMigrationAdoptionRecoveryFencesEvenWithStaleWarmHandle(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	proof := commandReadyProof(fixture, *claim.Stage)
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: proof}))
	h.stopExitWatch()
	h.stopConsumerRenewal()
	state := h.PersistedState()
	state.Claim = nil
	state.RootMounted = false
	h.mu.Lock()
	h.claim = nil
	h.stage = nil
	h.rootMounted = false
	h.mu.Unlock()
	handled, err := h.recoverMigrationRestore(state)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, phasePoisoned, h.PersistedState().Phase)
	require.Contains(t, runner.callsSnapshot(), "delete:force")
	custody, err := c.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
	require.NoError(t, err)
	require.True(t, custody.Adopted())
	require.Equal(t, protocol.MigrationRestoreComplete, custody.Restore.State)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
}

type adoptedSourceCustodian struct {
	*restoreCustodian
	source *migrationCustodian
}

func (c *adoptedSourceCustodian) GetMigrationCapture(ctx context.Context, slot string) (*nomadruntime.MigrationCaptureCustody, error) {
	return c.source.GetMigrationCapture(ctx, slot)
}
func (c *adoptedSourceCustodian) RecordMigrationCapture(ctx context.Context, capture protocol.MigrationCapture) error {
	return c.source.RecordMigrationCapture(ctx, capture)
}

func TestMigrationAdoptionRecoveryPreservesSubsequentSourceCustody(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	proof := commandReadyProof(fixture, *claim.Stage)
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: proof}))
	h.stopExitWatch()
	h.stopConsumerRenewal()
	state := h.PersistedState()
	metadata := *state.Claim
	request := protocol.MigrationCaptureRequest{Target: claim.MigrationRestore.Image.Target, OperationID: "next-migration", LifecycleEpoch: 4,
		SandboxID: metadata.SandboxID, SourceGeneration: claim.Runtime.RuntimeGeneration, AssignmentRevision: metadata.RuntimeRevision,
		BindingDigest: metadata.RootFSBindingDigest, ResourceLeaseDigest: metadata.ResourceLeaseDigest, ProcdInstanceID: metadata.ProcdInstanceID}
	digest, err := request.Digest()
	require.NoError(t, err)
	source := &migrationCustodian{root: t.TempDir()}
	require.NoError(t, source.RecordMigrationCapture(t.Context(), protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}))
	h.rootfs = &adoptedSourceCustodian{restoreCustodian: c, source: source}
	state.Claim = nil
	state.RootMounted = false
	h.mu.Lock()
	h.claim = nil
	h.stage = nil
	h.rootMounted = false
	h.mu.Unlock()
	handled, err := h.recoverMigrationRestore(state)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, phaseMigrating, h.PersistedState().Phase)
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.Equal(t, protocol.MigrationCaptureUncertain, h.PersistedState().Migration.State)
}

// installBackgroundAdoption simulates ctld finishing the region's committed
// command while the original command-ready response never reaches the driver.
func installBackgroundAdoption(t *testing.T, claim ClaimRequest, c *restoreCustodian, fixture *runtimeSlotPluginFixture) protocol.MigrationAdoptionRequest {
	t.Helper()
	proof := commandReadyProof(fixture, *claim.Stage)
	proof.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	readyDigest, err := proof.Digest()
	require.NoError(t, err)
	fixture.authority.mu.Lock()
	request := *fixture.authority.adoption
	fixture.authority.mu.Unlock()
	request.CommandReadyDigest = readyDigest
	_, err = c.AdoptMigrationDestination(t.Context(), request)
	require.NoError(t, err)
	return request
}

func TestMigrationAdoptionControlsRecoverBackgroundReceiptWithoutStartingAgain(t *testing.T) {
	for _, action := range []string{"signal", "stop", "close", "capture"} {
		t.Run(action, func(t *testing.T) {
			h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
			require.NoError(t, h.Claim(claim))
			request := installBackgroundAdoption(t, claim, c, fixture)
			state := h.PersistedState()
			require.Empty(t, state.Claim.ProcdInstanceID)
			require.Empty(t, state.Claim.CommandReadyDigest)
			require.Nil(t, state.Claim.MigrationAdoption)
			switch action {
			case "signal":
				require.NoError(t, h.Signal("USR1"))
			case "stop":
				require.NoError(t, h.Stop(0, "TERM"))
			case "close":
				require.NoError(t, h.Close(true))
			case "capture":
				source := &migrationCustodian{root: t.TempDir()}
				h.rootfs = &adoptedSourceCustodian{restoreCustodian: c, source: source}
				metadata := state.Claim
				captureRequest := protocol.MigrationCaptureRequest{Target: request.Target, OperationID: "next-migration", LifecycleEpoch: 4,
					SandboxID: metadata.SandboxID, SourceGeneration: claim.Runtime.RuntimeGeneration, AssignmentRevision: metadata.RuntimeRevision,
					BindingDigest: metadata.RootFSBindingDigest, ResourceLeaseDigest: metadata.ResourceLeaseDigest, ProcdInstanceID: request.ProcdInstanceID}
				_, err := h.CaptureMigration(t.Context(), captureRequest)
				require.NoError(t, err)
				require.NoError(t, h.PersistedState().Claim.MigrationCPULaunch.ValidateRestore(*claim.MigrationRestore))
				require.Eventually(t, func() bool {
					h.mu.Lock()
					defer h.mu.Unlock()
					return !h.migrationInFlight
				}, 3*time.Second, 10*time.Millisecond)
				require.Contains(t, runner.callsSnapshot(), "checkpoint")
			}
			state = h.PersistedState()
			require.Equal(t, request.ProcdInstanceID, state.Claim.ProcdInstanceID)
			require.Equal(t, request.CommandReadyDigest, state.Claim.CommandReadyDigest)
			require.Equal(t, request, state.Claim.MigrationAdoption.Request)
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
		})
	}
}

func TestMigrationAdoptionRefreshCannotUnlockIncompleteOrChangedCustody(t *testing.T) {
	for _, failure := range []string{"missing", "intent", "proof", "restore", "process", "readiness", "local-process", "local-readiness"} {
		t.Run(failure, func(t *testing.T) {
			h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
			require.NoError(t, h.Claim(claim))
			request := installBackgroundAdoption(t, claim, c, fixture)
			c.mu.Lock()
			switch failure {
			case "missing":
				c.custody.Adoption = nil
			case "intent":
				c.custody.Adoption.Proof = nil
			case "proof":
				c.custody.Adoption.Proof.ImageAbsent = false
			case "restore":
				c.custody.Restore.RequestDigest = strings.Repeat("f", 64)
			case "process":
				c.custody.Adoption.Request.ProcdInstanceID += "-changed"
			case "readiness":
				c.custody.Adoption.Request.CommandReadyDigest = strings.Repeat("f", 64)
			}
			c.mu.Unlock()
			h.mu.Lock()
			if failure == "local-process" {
				h.claim.ProcdInstanceID = request.ProcdInstanceID + "-changed"
			}
			if failure == "local-readiness" {
				h.claim.CommandReadyDigest = strings.Repeat("f", 64)
			}
			h.mu.Unlock()
			require.Error(t, h.Signal("USR1"))
			require.Nil(t, h.PersistedState().Claim.MigrationAdoption)
			require.NotContains(t, runner.callsSnapshot(), "kill:USR1")
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
		})
	}
}

func TestMigrationAdoptionRefreshPreservesConcurrentLeaseFence(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	installBackgroundAdoption(t, claim, c, fixture)
	c.mu.Lock()
	c.adoptionHook = func() { h.handleWriterLeaseLoss(errors.New("lease lost during adoption refresh")) }
	c.mu.Unlock()
	require.Error(t, h.Signal("USR1"))
	require.NotContains(t, runner.callsSnapshot(), "kill:USR1")
	require.Eventually(t, func() bool {
		state := h.PersistedState()
		return state.Claim.MigrationAdoption != nil && (state.Phase == phasePoisoned || state.Phase == phaseExited)
	}, 3*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return contains(runner.callsSnapshot(), "kill:KILL") || contains(runner.callsSnapshot(), "delete:force")
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
}

func TestMigrationAdoptionPendingReceiptCannotCreateNextCaptureIntent(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	require.NoError(t, h.Claim(claim))
	request := installBackgroundAdoption(t, claim, c, fixture)
	c.mu.Lock()
	c.custody.Adoption = nil
	c.mu.Unlock()
	// Readiness may have reached the driver before ctld received adoption.
	// That readiness alone must not authorize another checkpoint operation.
	h.mu.Lock()
	h.claim.ProcdInstanceID = request.ProcdInstanceID
	h.claim.CommandReadyDigest = request.CommandReadyDigest
	metadata := *h.claim
	h.mu.Unlock()
	source := &migrationCustodian{root: t.TempDir()}
	h.rootfs = &adoptedSourceCustodian{restoreCustodian: c, source: source}
	capture := protocol.MigrationCaptureRequest{Target: request.Target, OperationID: "next-migration", LifecycleEpoch: 4,
		SandboxID: metadata.SandboxID, SourceGeneration: claim.Runtime.RuntimeGeneration, AssignmentRevision: metadata.RuntimeRevision,
		BindingDigest: metadata.RootFSBindingDigest, ResourceLeaseDigest: metadata.ResourceLeaseDigest, ProcdInstanceID: request.ProcdInstanceID}
	_, err := h.CaptureMigration(t.Context(), capture)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.Nil(t, h.PersistedState().Migration)
	require.Equal(t, phaseActive, h.PersistedState().Phase)
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
}

func TestMigrationBackgroundAdoptionPreservesExitAfterLostReceiptRead(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	runner.mu.Lock()
	runner.waitResult.ExitStatus = 17
	runner.mu.Unlock()
	require.NoError(t, h.Claim(claim))
	t.Cleanup(h.stopExitWatch)
	request := installBackgroundAdoption(t, claim, c, fixture)
	c.mu.Lock()
	c.adoptionReadError = errors.New("ctld temporarily unavailable")
	beforeReads := c.adoptionReads
	c.mu.Unlock()
	require.NoError(t, runner.Kill(t.Context(), h.containerID, "TERM"))
	require.Eventually(t, func() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.adoptionReads > beforeReads }, time.Second, 5*time.Millisecond)
	select {
	case <-h.done:
		t.Fatal("missing adoption custody released the pending exit")
	default:
	}
	c.mu.Lock()
	c.adoptionReadError = nil
	c.mu.Unlock()
	select {
	case result := <-h.WaitChannel(t.Context()):
		require.Equal(t, 17, result.ExitCode)
		require.NoError(t, result.Err)
	case <-time.After(3 * time.Second):
		t.Fatal("background adoption lost the observed exit")
	}
	state := h.PersistedState()
	require.Equal(t, phaseExited, state.Phase)
	require.Equal(t, request, state.Claim.MigrationAdoption.Request)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "wait"))
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.NotContains(t, runner.callsSnapshot(), "start")
}

func TestMigrationExitWaiterDoesNotDeadlockStopOrBypassPendingCustody(t *testing.T) {
	for _, adopted := range []bool{false, true} {
		name := "pending"
		if adopted {
			name = "adopted"
		}
		t.Run(name, func(t *testing.T) {
			h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
			require.NoError(t, h.Claim(claim))
			t.Cleanup(h.stopExitWatch)
			if adopted {
				installBackgroundAdoption(t, claim, c, fixture)
				finished := make(chan error, 1)
				go func() { finished <- h.Stop(5*time.Second, "TERM") }()
				select {
				case err := <-finished:
					require.NoError(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("Stop waited on an exit watcher blocked by closeMu")
				}
				require.NotContains(t, runner.callsSnapshot(), "delete:force", "observed exit should finish Stop before its timeout")
			} else {
				require.NoError(t, runner.Kill(t.Context(), h.containerID, "TERM"))
				require.Never(t, func() bool {
					select {
					case <-h.done:
						return true
					default:
						return false
					}
				}, 100*time.Millisecond, 5*time.Millisecond)
				require.Nil(t, h.PersistedState().Claim.MigrationAdoption)
				h.stopExitWatch()
				require.Equal(t, phaseActive, h.PersistedState().Phase, "pending custody remains region-owned")
			}
		})
	}
}
