package slotnetwork

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Handler serves the root-only local endpoint owned by the elected ctld HA
// primary.
type Handler struct {
	registry *Registry
}

// NewHandler constructs a strict runtime-slot network control handler.
func NewHandler(registry *Registry) (*Handler, error) {
	if registry == nil {
		return nil, fmt.Errorf("runtime slot network registry is required")
	}
	return &Handler{registry: registry}, nil
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h == nil || h.registry == nil {
		writeError(writer, fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable))
		return
	}
	switch request.URL.Path {
	case protocol.RuntimeSlotNetworkHealthPath:
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writeError(writer, fmt.Errorf("method must be GET: %w", errdefs.ErrInvalidArgument))
			return
		}
		if err := h.registry.Ping(); err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, struct{}{})
	case protocol.RuntimeSlotNetworkRegisterPath:
		if request.Method != http.MethodPut {
			writer.Header().Set("Allow", http.MethodPut)
			writeError(writer, fmt.Errorf("method must be PUT: %w", errdefs.ErrInvalidArgument))
			return
		}
		var input protocol.RuntimeSlotNetworkRegistrationRequest
		if err := decodeRequest(request, &input); err != nil {
			writeError(writer, err)
			return
		}
		if err := h.registry.Register(request.Context(), input); err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, protocol.RuntimeSlotNetworkRegistrationResponse{NetworkPolicyApplied: true})
	case protocol.RuntimeSlotNetworkPreparePath:
		if request.Method != http.MethodPut {
			writer.Header().Set("Allow", http.MethodPut)
			writeError(writer, fmt.Errorf("method must be PUT: %w", errdefs.ErrInvalidArgument))
			return
		}
		var input protocol.RuntimeSlotNetworkPrepareRequest
		if err := decodeRequest(request, &input); err != nil {
			writeError(writer, err)
			return
		}
		token, err := h.registry.Prepare(request.Context(), input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, token)
	case protocol.RuntimeSlotNetworkCleanupPath:
		if request.Method != http.MethodPut {
			writer.Header().Set("Allow", http.MethodPut)
			writeError(writer, fmt.Errorf("method must be PUT: %w", errdefs.ErrInvalidArgument))
			return
		}
		var input protocol.NodeCleanupControlRequest
		if err := decodeRequest(request, &input); err != nil {
			writeError(writer, err)
			return
		}
		if err := h.registry.Cleanup(request.Context(), input); err != nil {
			writeError(writer, err)
			return
		}
		writeJSON(writer, protocol.RuntimeSlotNetworkCleanupResponse{NetworkPolicyAbsent: true})
	default:
		http.NotFound(writer, request)
	}
}

func decodeRequest(request *http.Request, destination any) error {
	if request.Body == nil || request.ContentLength > maxRecordBytes {
		return fmt.Errorf("runtime slot network request is empty or too large: %w", errdefs.ErrInvalidArgument)
	}
	contentType := request.Header.Get("Content-Type")
	if contentType != "application/json" {
		return fmt.Errorf("content type must be application/json: %w", errdefs.ErrInvalidArgument)
	}
	payload, err := io.ReadAll(io.LimitReader(request.Body, maxRecordBytes+1))
	if err != nil || len(payload) == 0 || len(payload) > maxRecordBytes {
		return fmt.Errorf("read bounded runtime slot network request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode runtime slot network request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("runtime slot network request contains trailing data: %w", errdefs.ErrInvalidArgument)
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, value any) {
	payload, err := json.Marshal(value)
	if err != nil {
		writeError(writer, fmt.Errorf("encode runtime slot network response: %w", err))
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(payload)
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	switch {
	case errdefs.IsInvalidArgument(err):
		status = http.StatusBadRequest
	case errdefs.IsPermissionDenied(err):
		status = http.StatusForbidden
	case errdefs.IsNotFound(err):
		status = http.StatusNotFound
	case errdefs.IsAlreadyExists(err):
		status = http.StatusConflict
	case errdefs.IsFailedPrecondition(err):
		status = http.StatusPreconditionFailed
	case errdefs.IsResourceExhausted(err):
		status = http.StatusTooManyRequests
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 4096 {
		message = message[:4096]
	}
	payload, _ := json.Marshal(struct {
		Error string `json:"error"`
	}{Error: message})
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(payload)
}

var _ http.Handler = (*Handler)(nil)
