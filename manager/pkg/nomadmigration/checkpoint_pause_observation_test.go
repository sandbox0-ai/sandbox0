package nomadmigration

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type checkpointCaptureObservationNode struct {
	CheckpointPauseNode
	recover func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
	capture func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error)
}

func (n checkpointCaptureObservationNode) RecoverMigrationCapture(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	return n.recover(ctx, request)
}

func (n checkpointCaptureObservationNode) CaptureMigration(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	return n.capture(ctx, request)
}

func TestCheckpointPauseReentersDriverAfterLostDispatchResponse(t *testing.T) {
	source := protocol.MigrationCaptureRequest{
		Target: protocol.NodeChannelTarget{
			SlotID: "slot", ClusterID: "cluster", AllocationID: "allocation", NodeID: "node",
			NodeUID: "uid", NodeBootID: "boot", ControlEndpoint: "unix:///private/source.sock",
		},
		OperationID: "operation", LifecycleEpoch: 2, SandboxID: "sandbox", SourceGeneration: 1,
		AssignmentRevision: strings.Repeat("a", 64), BindingDigest: strings.Repeat("b", 64),
		ResourceLeaseDigest: strings.Repeat("c", 64), ProcdInstanceID: "procd",
	}
	digest, err := source.Digest()
	require.NoError(t, err)
	intent := &protocol.MigrationCapture{Request: source, RequestDigest: digest, State: protocol.MigrationCaptureIntent}
	uncertain := &protocol.MigrationCapture{Request: source, RequestDigest: digest, State: protocol.MigrationCaptureUncertain}
	completed := &protocol.MigrationCapture{Request: source, RequestDigest: digest, State: protocol.MigrationCaptureComplete}

	for _, test := range []struct {
		name         string
		recovered    *protocol.MigrationCapture
		recoverError error
		wantDispatch bool
		wantState    string
	}{
		{"lost dispatch response", intent, nil, true, protocol.MigrationCaptureUncertain},
		{"missing intent", nil, errdefs.ErrNotFound, true, protocol.MigrationCaptureUncertain},
		{"completed capture", completed, nil, false, protocol.MigrationCaptureComplete},
		{"unavailable node", nil, errdefs.ErrUnavailable, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			var authorized, dispatched int
			node := checkpointCaptureObservationNode{
				recover: func(_ context.Context, got protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
					require.Equal(t, source, got)
					return test.recovered, test.recoverError
				},
				capture: func(_ context.Context, got protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
					require.Equal(t, source, got)
					dispatched++
					return uncertain, nil
				},
			}
			got, err := observeCheckpointCapture(t.Context(), source, func(_ context.Context, request protocol.MigrationCaptureRequest) error {
				require.Equal(t, source, request)
				authorized++
				return nil
			}, node)
			if test.recoverError != nil && !test.wantDispatch {
				require.ErrorIs(t, err, test.recoverError)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantState, got.State)
			}
			if test.wantDispatch {
				require.Equal(t, 1, authorized)
				require.Equal(t, 1, dispatched)
			} else {
				require.Zero(t, authorized)
				require.Zero(t, dispatched)
			}
		})
	}

	authorizationError := errors.New("source authority changed")
	dispatched := false
	node := checkpointCaptureObservationNode{
		recover: func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
			return intent, nil
		},
		capture: func(context.Context, protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
			dispatched = true
			return uncertain, nil
		},
	}
	_, err = observeCheckpointCapture(t.Context(), source, func(context.Context, protocol.MigrationCaptureRequest) error {
		return authorizationError
	}, node)
	require.ErrorIs(t, err, authorizationError)
	require.False(t, dispatched, "a stale lifecycle must not reach the driver")
}
