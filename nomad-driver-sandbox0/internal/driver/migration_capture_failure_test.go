package driver

import (
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func migrationFailedCaptureHandleFixture(t *testing.T) (*taskHandle, *finalizationCustodian, *migrationRunsc) {
	t.Helper()
	h, c, runner := migrationFinalizationHandleFixture(t)
	capture := c.custody.Capture
	capture.State, capture.RootFS = protocol.MigrationCaptureUncertain, nil
	cleanup := c.receipt.Request.Cleanup
	cleanup.OperationID = protocol.MigrationCaptureFailureOperationID(capture.Request.OperationID)
	cleanup.WriterOperationID = cleanup.OperationID
	cleanup.WriterRetireKind = protocol.WriterRetireKindCrashAbandon
	cleanup.WriterAuthorityDigest = capture.RequestDigest
	request := protocol.MigrationCaptureFailureRequest{Capture: capture, Cleanup: cleanup}
	cd, err := request.Digest()
	require.NoError(t, err)
	p := c.receipt.Proof.Cleanup
	p.OperationID, p.WriterOperationID, p.RootFSOperationID = cleanup.OperationID, cleanup.WriterOperationID, cleanup.WriterOperationID
	p.WriterRetireKind, p.WriterAuthorityDigest = cleanup.WriterRetireKind, cleanup.WriterAuthorityDigest
	p.RootFSProofDigest = strings.Repeat("c", 64)
	p.ProofDigest, err = p.Digest()
	require.NoError(t, err)
	final := protocol.MigrationCaptureFailureFinalizeRequest{Request: request, Proof: protocol.MigrationCaptureFailureProof{RequestDigest: cd, Cleanup: p}}
	fd, err := final.Digest()
	require.NoError(t, err)
	c.custody = &nomadruntime.MigrationCaptureCustody{Capture: capture, ExecutionInvalidated: true,
		Failure: &nomadruntime.MigrationCaptureFailureCustody{Request: request, RequestDigest: cd, Stage: *h.stage,
			Finalization: &nomadruntime.MigrationCaptureFailureFinalization{Request: final, RequestDigest: fd,
				Proof: &protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: fd, RootFSArtifactsAbsent: true, ImageAbsent: true}}}}
	c.receipt = nil
	return h, c, runner
}

func TestFailedCaptureFinalizationExitsWithoutReplayingOrRetiringAgain(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(map[bool]string{false: "claimed", true: "original-warm-handle"}[warm], func(t *testing.T) {
			h, _, runner := migrationFailedCaptureHandleFixture(t)
			if warm {
				h.claim, h.stage, h.migration = nil, nil, nil
				h.rootMounted, h.phase = false, phaseWarm
			}
			before := runner.callsSnapshot()
			done, err := h.observeMigrationFinalization()
			require.NoError(t, err)
			require.True(t, done)
			require.True(t, h.migrationFinalized)
			require.Equal(t, phaseExited, h.phase)
			require.Equal(t, 1, h.exitResult.ExitCode)
			require.Equal(t, protocol.MigrationCaptureUncertain, h.migration.State)
			require.NoError(t, h.Close(true))
			require.Equal(t, before, runner.callsSnapshot(), "finalized source must never execute another teardown or restore")
		})
	}
}

func TestFailedCaptureFinalizationRejectsChangedOrIncompleteReceipt(t *testing.T) {
	for _, name := range []string{"pending", "physical", "image", "binding", "writer", "allocation", "digest", "in-flight", "invalidation", "history"} {
		t.Run(name, func(t *testing.T) {
			h, c, runner := migrationFailedCaptureHandleFixture(t)
			f := c.custody.Failure.Finalization
			switch name {
			case "pending":
				f.Proof = nil
			case "physical":
				f.Request.Proof.Cleanup.ResourceCgroupAbsent = false
			case "image":
				f.Proof.ImageAbsent = false
			case "binding":
				c.custody.Failure.Stage.Identity.WriterEpoch++
			case "writer":
				h.stage.Identity.WriterGrantID = "other-writer"
			case "allocation":
				h.taskConfig.AllocID = "other-allocation"
			case "digest":
				c.custody.Failure.RequestDigest = strings.Repeat("a", 64)
			case "in-flight":
				h.migrationInFlight = true
			case "invalidation":
				c.custody.ExecutionInvalidated = false
			case "history":
				h.migration.Request.SourceGeneration++
			}
			before := runner.callsSnapshot()
			done, err := h.observeMigrationFinalization()
			require.Error(t, err)
			require.True(t, errdefs.IsFailedPrecondition(err) || errdefs.IsUnavailable(err))
			require.False(t, done)
			require.False(t, h.migrationFinalized)
			require.Equal(t, before, runner.callsSnapshot())
		})
	}
}
