// Package runtimeslotnode adapts authenticated region-to-node dispatch to the
// runtime slot terminal reconciler.
package runtimeslotnode

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Target is trusted routing input. Transport must authenticate the remote node
// incarnation instead of trusting identities echoed in the request body.
type Target struct {
	ClusterID  string
	NodeID     string
	NodeUID    string
	NodeBootID string
}

// Transport delivers cleanup through mTLS, an outbound node stream, or an
// equivalent authenticated channel. A task-driver Unix socket is not a valid
// region transport.
type Transport interface {
	CleanupRuntimeSlot(context.Context, Target, protocol.NodeCleanupControlRequest) (protocol.NodeCleanupControlProof, error)
}

// Controller implements the reconciler's node-cleanup boundary.
type Controller struct {
	transport Transport
}

var _ runtimeslotreconciler.NodeCleaner = (*Controller)(nil)

// New constructs a node cleanup controller.
func New(transport Transport) (*Controller, error) {
	if transport == nil {
		return nil, errors.New("runtime slot node transport is required")
	}
	return &Controller{transport: transport}, nil
}

// Cleanup dispatches one exact request and accepts only a canonical proof for
// the same trusted node target and physical slot incarnation.
func (c *Controller) Cleanup(
	ctx context.Context,
	request runtimeslotreconciler.NodeCleanupRequest,
) (runtimeslotreconciler.NodeCleanupProof, error) {
	controlRequest := protocol.NodeCleanupControlRequest{
		OperationID: request.OperationID, WriterOperationID: request.WriterOperationID,
		SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID,
	}
	if len(request.WriterFenceDigest) != 0 {
		controlRequest.WriterFenceDigest = hex.EncodeToString(request.WriterFenceDigest)
	}
	if err := controlRequest.Validate(); err != nil {
		return runtimeslotreconciler.NodeCleanupProof{}, fmt.Errorf("validate runtime slot node cleanup: %w", err)
	}
	target := Target{
		ClusterID: request.ClusterID, NodeID: request.NodeID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
	}
	proof, err := c.transport.CleanupRuntimeSlot(ctx, target, controlRequest)
	if err != nil {
		return runtimeslotreconciler.NodeCleanupProof{}, fmt.Errorf("dispatch runtime slot node cleanup: %w", err)
	}
	if err := proof.Validate(); err != nil {
		return runtimeslotreconciler.NodeCleanupProof{}, fmt.Errorf("validate runtime slot node proof: %w", err)
	}
	if proof.Request() != controlRequest {
		return runtimeslotreconciler.NodeCleanupProof{}, errors.New("runtime slot node proof belongs to another request")
	}
	digest, err := protocol.DecodeProof("proof_digest", proof.ProofDigest)
	if err != nil {
		return runtimeslotreconciler.NodeCleanupProof{}, err
	}
	return runtimeslotreconciler.NodeCleanupProof{
		OperationID: proof.OperationID, SlotID: proof.SlotID, AllocationID: proof.AllocationID,
		NodeUID: proof.NodeUID, NodeBootID: proof.NodeBootID, ProofDigest: digest,
	}, nil
}
