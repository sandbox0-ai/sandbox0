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

type migrationFinalizationStore interface {
	GetNomadSandboxMigrationSourceFinalizationForSlot(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error)
}

func serveMigrationSourceFinalization(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeMethodNotAllowed(writer, http.MethodGet)
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
	if slot.ID != slotID {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration source lookup changed slot identity")
		return
	}
	store, ok := config.Store.(migrationFinalizationStore)
	if !ok {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration source receipt authority is unavailable")
		return
	}
	receipt, err := store.GetNomadSandboxMigrationSourceFinalizationForSlot(request.Context(), slotID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	response := protocol.MigrationSourceFinalizationResponse{SlotID: slotID, Receipt: receipt}
	if response.ValidateFor(slotID) != nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration source receipt is invalid")
		return
	}
	if receipt != nil {
		c := receipt.Request.Cleanup
		if c.ClusterID != slot.ClusterID || c.NodeID != slot.NodeID || c.NodeUID != slot.NodeUID || c.NodeBootID != slot.NodeBootID || c.AllocationID != slot.AllocationID || c.WriterGrantID != slot.WriterGrantID {
			writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration source receipt changed node custody")
			return
		}
	}
	payload, err := json.Marshal(response)
	if err != nil || len(payload) > protocol.NodeChannelMaxBytes {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration source receipt exceeds response limit")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(payload)
}

// GetMigrationSourceFinalization reads a committed receipt using the existing
// node certificate and token. No slot registration or lease renewal occurs.
func (c *Client) GetMigrationSourceFinalization(ctx context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	if err := protocol.ValidateSlotID(slot); err != nil {
		return nil, invalidRequest(err)
	}
	if c == nil || c.transport == nil {
		return nil, errdefs.ErrUnavailable
	}
	request, err := c.transport.NewRequest(ctx, http.MethodGet, protocol.MigrationSourceFinalizationPath(slot), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.transport.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read regional migration source receipt: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return nil, decodeResponseError(response)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, protocol.NodeChannelMaxBytes+1))
	if err != nil || len(payload) > protocol.NodeChannelMaxBytes {
		return nil, fmt.Errorf("migration source receipt response exceeds limit: %w", errdefs.ErrUnavailable)
	}
	var result protocol.MigrationSourceFinalizationResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode migration source receipt: %w: %w", err, errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("migration source receipt contains trailing data: %w", errdefs.ErrUnavailable)
	}
	if err := result.ValidateFor(slot); err != nil {
		return nil, fmt.Errorf("validate migration source receipt: %w: %w", err, errdefs.ErrUnavailable)
	}
	return result.Receipt, nil
}
