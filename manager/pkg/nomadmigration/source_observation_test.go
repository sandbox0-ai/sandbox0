package nomadmigration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type captureObservationNode struct {
	call func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
}

func (n captureObservationNode) RecoverMigrationCapture(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	return n.call(ctx, request)
}

func TestSourceCaptureObservationAwaitsExactJournalOutcome(t *testing.T) {
	request := protocol.MigrationCaptureRequest{
		Target:      protocol.NodeChannelTarget{SlotID: "slot", ClusterID: "cluster", AllocationID: "allocation", NodeID: "node", NodeUID: "uid", NodeBootID: "boot", ControlEndpoint: "unix:///private/source.sock"},
		OperationID: "operation", LifecycleEpoch: 2, SandboxID: "sandbox", SourceGeneration: 1,
		AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64), ResourceLeaseDigest: strings.Repeat("c", 64), ProcdInstanceID: "procd",
	}
	digest, err := request.Digest()
	require.NoError(t, err)
	capture := &protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}
	calls := 0
	node := captureObservationNode{call: func(_ context.Context, got protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
		require.Equal(t, request, got)
		calls++
		if calls == 1 {
			return nil, errdefs.ErrNotFound
		}
		if calls == 2 {
			return nil, errdefs.ErrUnavailable
		}
		if calls == 4 {
			capture.State = protocol.MigrationCaptureComplete
		}
		return capture, nil
	}}
	result, err := observeSourceCapture(t.Context(), node, request)
	require.NoError(t, err)
	require.Equal(t, 4, calls)
	require.Equal(t, protocol.MigrationCaptureComplete, result.State)

	for _, state := range []string{protocol.MigrationCaptureUncertain, protocol.MigrationCaptureIntent} {
		capture.State = state
		if state == protocol.MigrationCaptureIntent {
			capture.Request.ProcdInstanceID = "replacement"
		}
		calls = 0
		node.call = func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
			calls++
			return capture, nil
		}
		_, err := observeSourceCapture(t.Context(), node, request)
		require.NoError(t, err)
		require.Equal(t, 1, calls, "uncertain or changed evidence must reach validation without waiting")
	}
}

func TestSourceCaptureObservationBoundsMissingNodeAndCancellation(t *testing.T) {
	calls := 0
	node := captureObservationNode{call: func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
		calls++
		return nil, errdefs.ErrNotFound
	}}
	_, err := observeSourceCapture(t.Context(), node, protocol.MigrationCaptureRequest{})
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.LessOrEqual(t, calls, 42, "missing-node retries must remain bounded")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	_, err = observeSourceCapture(ctx, node, protocol.MigrationCaptureRequest{})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	calls = 0
	node.call = func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
		calls++
		return nil, errdefs.ErrPermissionDenied
	}
	_, err = observeSourceCapture(t.Context(), node, protocol.MigrationCaptureRequest{})
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
	require.Equal(t, 1, calls, "other failures retain normal reconciliation backoff")
}
