package runtimeslotauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationAdoptionStore interface {
	CommitNomadSandboxMigrationAdoption(context.Context, protocol.MigrationAdoptionRequest, protocol.MigrationAdoptionProof) error
}

func serveMigrationAdoptionReceipt(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var receipt protocol.MigrationAdoptionReceipt
	if !decodeBody(writer, request, &receipt) || !validateBody(writer, receipt.Validate()) {
		return
	}
	target := receipt.Request.Target
	if target.SlotID != slotID || target.ClusterID != identity.ClusterID || target.NodeID != identity.NodeID || target.NodeUID != identity.NodeUID {
		writeError(writer, http.StatusForbidden, protocol.ErrorPermissionDenied, "migration adoption belongs to another node")
		return
	}
	slot, err := config.Store.GetRuntimeSlot(request.Context(), slotID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !authorizeNode(writer, slot, identity) {
		return
	}
	if slot.ID != slotID || slot.AllocationID != target.AllocationID || slot.NodeBootID != target.NodeBootID {
		writeError(writer, http.StatusForbidden, protocol.ErrorPermissionDenied, "migration adoption changed historical allocation")
		return
	}
	store, ok := config.Store.(migrationAdoptionStore)
	if !ok {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration adoption authority is unavailable")
		return
	}
	if err := store.CommitNomadSandboxMigrationAdoption(request.Context(), receipt.Request, receipt.Proof); err != nil {
		writeStoreError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(protocol.MigrationAdoptionAcknowledgement{RequestDigest: receipt.Proof.RequestDigest})
}

// ReportMigrationAdoption reuses authenticated node transport to retry a
// historical receipt independently of the driver or command-ready connection.
func (c *Client) ReportMigrationAdoption(ctx context.Context, receipt protocol.MigrationAdoptionReceipt) (protocol.MigrationAdoptionAcknowledgement, error) {
	var empty protocol.MigrationAdoptionAcknowledgement
	if err := receipt.Validate(); err != nil {
		return empty, invalidRequest(err)
	}
	if c == nil || c.transport == nil {
		return empty, errdefs.ErrUnavailable
	}
	payload, err := json.Marshal(receipt)
	if err != nil || len(payload) > maxRequestBytes {
		return empty, errdefs.ErrInvalidArgument
	}
	request, err := c.transport.NewRequest(ctx, http.MethodPut, protocol.MigrationAdoptionReceiptPath(receipt.Request.Target.SlotID), bytes.NewReader(payload))
	if err != nil {
		return empty, err
	}
	response, err := c.transport.Do(request)
	if err != nil {
		return empty, fmt.Errorf("report migration adoption: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return empty, decodeResponseError(response)
	}
	payload, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(payload) > maxResponseBytes {
		return empty, errdefs.ErrUnavailable
	}
	var ack protocol.MigrationAdoptionAcknowledgement
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ack); err != nil {
		return empty, fmt.Errorf("decode migration adoption acknowledgement: %w", errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return empty, fmt.Errorf("migration adoption acknowledgement has trailing data: %w", errdefs.ErrUnavailable)
	}
	if err := ack.ValidateFor(receipt); err != nil {
		return empty, fmt.Errorf("migration adoption acknowledgement changed receipt: %w", errdefs.ErrUnavailable)
	}
	return ack, nil
}
