package nomadruntime

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func (c *Client) GetMigrationSourceFinalization(ctx context.Context, slotID string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return nil, err
	}
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, runtimeMigrationFinalizationGetPath, nodeRuntimeRPCRequest{MigrationSlotID: slotID}, &response); err != nil {
		return nil, err
	}
	receipt := response.MigrationFinalization
	if receipt != nil && (receipt.Validate() != nil || receipt.Request.Cleanup.SlotID != slotID) {
		return nil, fmt.Errorf("invalid migration finalization observation: %w", errdefs.ErrUnavailable)
	}
	return receipt, nil
}

func (c *Client) GetMigrationCapture(ctx context.Context, slotID string) (*MigrationCaptureCustody, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return nil, err
	}
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, runtimeMigrationGetPath, nodeRuntimeRPCRequest{MigrationSlotID: slotID}, &response); err != nil {
		return nil, err
	}
	if custody := response.Migration; custody != nil {
		if custody.Capture.Validate() != nil || custody.validateFinalization() != nil || custody.Capture.Request.Target.SlotID != slotID ||
			!filepath.IsAbs(custody.ImageDirectory) || filepath.Clean(custody.ImageDirectory) != custody.ImageDirectory ||
			filepath.Base(custody.ImageDirectory) != custody.Capture.RequestDigest {
			return nil, fmt.Errorf("invalid migration custody observation: %w", errdefs.ErrUnavailable)
		}
	}
	return response.Migration, nil
}

func (c *Client) SealMigrationRootFS(ctx context.Context, request protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error) {
	var zero rootfshandoff.MigrationRootFSCut
	digest, err := request.Digest()
	if err != nil {
		return zero, err
	}
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, runtimeMigrationSealPath, nodeRuntimeRPCRequest{MigrationRequest: &request}, &response); err != nil {
		return zero, err
	}
	cut := response.MigrationRootFS
	if cut == nil || cut.Validate() != nil || cut.Request.OperationID != request.OperationID ||
		cut.Request.CaptureRequestDigest != digest || cut.Request.SourceBindingDigest != request.BindingDigest ||
		cut.Request.GenerationID != "migration-"+digest {
		return zero, fmt.Errorf("invalid migration filesystem cut: %w", errdefs.ErrUnavailable)
	}
	return *cut, nil
}

func (c *Client) RecordMigrationCapture(ctx context.Context, capture protocol.MigrationCapture) error {
	if err := capture.Validate(); err != nil {
		return err
	}
	var response nodeRuntimeRPCResponse
	return c.call(ctx, runtimeMigrationRecordPath, nodeRuntimeRPCRequest{Migration: &capture}, &response)
}
