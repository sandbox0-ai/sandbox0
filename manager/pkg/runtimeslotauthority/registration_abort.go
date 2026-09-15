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

type registrationAbortStore interface {
	AbortRuntimeSlotRegistration(context.Context, protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error)
}

func serveRegistrationAbort(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var body protocol.RegistrationAbortRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	if body.SlotID != slotID || body.ClusterID != identity.ClusterID || body.NodeID != identity.NodeID || body.NodeUID != identity.NodeUID {
		writeError(writer, http.StatusForbidden, protocol.ErrorPermissionDenied, "registration abort belongs to another node incarnation")
		return
	}
	store, ok := config.Store.(registrationAbortStore)
	if !ok {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "registration abort authority is unavailable")
		return
	}
	response, err := store.AbortRuntimeSlotRegistration(request.Context(), body)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if err := response.ValidateFor(body); err != nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "registration abort authority returned an invalid acknowledgement")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

// AbortRegistration never interprets a missing endpoint or an uncertain reply
// as permission to clean up. The returned fence must match every identity byte.
func (c *Client) AbortRegistration(ctx context.Context, request protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
	if err := request.Validate(); err != nil {
		return protocol.RegistrationAbortResponse{}, invalidRequest(err)
	}
	if c == nil || c.transport == nil {
		return protocol.RegistrationAbortResponse{}, errdefs.ErrUnavailable
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return protocol.RegistrationAbortResponse{}, invalidRequest(err)
	}
	httpRequest, err := c.transport.NewRequest(ctx, http.MethodPut, protocol.RegistrationAbortPath(request.SlotID), bytes.NewReader(payload))
	if err != nil {
		return protocol.RegistrationAbortResponse{}, err
	}
	response, err := c.transport.Do(httpRequest)
	if err != nil {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("call registration abort authority: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return protocol.RegistrationAbortResponse{}, decodeResponseError(response)
	}
	payload, err = io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(payload) > maxResponseBytes {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("registration abort response exceeds limit: %w", errdefs.ErrUnavailable)
	}
	var acknowledgement protocol.RegistrationAbortResponse
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&acknowledgement); err != nil {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("invalid registration abort response: %w", errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("registration abort response contains trailing data: %w", errdefs.ErrUnavailable)
	}
	if err := acknowledgement.ValidateFor(request); err != nil {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("invalid registration abort acknowledgement: %w: %w", err, errdefs.ErrUnavailable)
	}
	return acknowledgement, nil
}
