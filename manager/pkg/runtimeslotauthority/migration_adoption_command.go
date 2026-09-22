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

type migrationAdoptionCommandStore interface {
	GetNomadSandboxMigrationAdoptionForSlot(context.Context, string) (*protocol.MigrationAdoptionRequest, error)
}

func serveMigrationAdoptionCommand(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
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
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration adoption lookup changed slot")
		return
	}
	store, ok := config.Store.(migrationAdoptionCommandStore)
	if !ok {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration adoption command authority is unavailable")
		return
	}
	command, err := store.GetNomadSandboxMigrationAdoptionForSlot(request.Context(), slotID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	response := protocol.MigrationAdoptionCommandResponse{SlotID: slotID, Command: command}
	if response.ValidateFor(slotID) != nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "invalid migration adoption command")
		return
	}
	if command != nil {
		t := command.Target
		if t.ClusterID != slot.ClusterID || t.NodeID != slot.NodeID || t.NodeUID != slot.NodeUID || t.NodeBootID != slot.NodeBootID || t.AllocationID != slot.AllocationID {
			writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration adoption command changed allocation")
			return
		}
	}
	payload, err := json.Marshal(response)
	if err != nil || len(payload) > maxResponseBytes {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "migration adoption command exceeds limit")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write(payload)
}

func (c *Client) GetMigrationAdoptionCommand(ctx context.Context, slot string) (*protocol.MigrationAdoptionRequest, error) {
	if err := protocol.ValidateSlotID(slot); err != nil {
		return nil, invalidRequest(err)
	}
	if c == nil || c.transport == nil {
		return nil, errdefs.ErrUnavailable
	}
	request, err := c.transport.NewRequest(ctx, http.MethodGet, protocol.MigrationAdoptionCommandPath(slot), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.transport.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read migration adoption command: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return nil, decodeResponseError(response)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(payload) > maxResponseBytes {
		return nil, errdefs.ErrUnavailable
	}
	var result protocol.MigrationAdoptionCommandResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errdefs.ErrUnavailable
	}
	if result.ValidateFor(slot) != nil {
		return nil, errdefs.ErrUnavailable
	}
	return result.Command, nil
}
