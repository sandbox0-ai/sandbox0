// Package runtimeslotauthority serves the authenticated node-to-region warm
// runtime slot protocol.
package runtimeslotauthority

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const maxRequestBytes = 64 << 10

type Store interface {
	RegisterRuntimeSlot(context.Context, *sandboxstore.RegisterRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error)
	GetRuntimeSlot(context.Context, string) (*sandboxstore.RuntimeSlot, error)
	ReportRuntimeSlotReady(context.Context, *sandboxstore.ReportRuntimeSlotReadyRequest) (*sandboxstore.RuntimeSlot, error)
	HeartbeatRuntimeSlot(context.Context, *sandboxstore.HeartbeatRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error)
	StartRuntimeSlot(context.Context, *sandboxstore.StartRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error)
	MarkRuntimeSlotCommandReady(context.Context, *sandboxstore.MarkRuntimeSlotCommandReadyRequest) (*sandboxstore.RuntimeSlot, error)
}

type HandlerConfig struct {
	Verifier     nodeauth.Verifier
	Store        Store
	HeartbeatTTL time.Duration
}

type routeAction string

const (
	actionSlot         routeAction = ""
	actionReady        routeAction = "ready"
	actionHeartbeat    routeAction = "heartbeat"
	actionStarting     routeAction = "starting"
	actionCommandReady routeAction = "command-ready"
)

func NewHandler(config HandlerConfig) (http.Handler, error) {
	if config.Verifier == nil || config.Store == nil {
		return nil, fmt.Errorf("runtime slot verifier and store are required")
	}
	if config.HeartbeatTTL < time.Second || config.HeartbeatTTL > 5*time.Minute {
		return nil, fmt.Errorf("runtime slot heartbeat TTL must be between 1s and 5m")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		slotID, action, err := parseRoute(request.URL.EscapedPath())
		if err != nil || request.URL.RawQuery != "" {
			writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, "invalid runtime slot path")
			return
		}
		identity, ok := authenticate(config.Verifier, writer, request)
		if !ok {
			return
		}
		switch action {
		case actionSlot:
			if request.Method == http.MethodGet {
				serveObserve(config, identity, slotID, writer, request)
				return
			}
			if request.Method == http.MethodPut {
				serveRegister(config, identity, slotID, writer, request)
				return
			}
			writeMethodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
		case actionReady:
			serveReady(config, identity, slotID, writer, request)
		case actionHeartbeat:
			serveHeartbeat(config, identity, slotID, writer, request)
		case actionStarting:
			serveStarting(config, identity, slotID, writer, request)
		case actionCommandReady:
			serveCommandReady(config, identity, slotID, writer, request)
		default:
			writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, "invalid runtime slot action")
		}
	}), nil
}

func serveRegister(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	var body protocol.RegistrationRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	slot, err := config.Store.RegisterRuntimeSlot(request.Context(), &sandboxstore.RegisterRuntimeSlotRequest{
		SlotID: slotID, ClusterID: body.ClusterID, AllocationID: body.AllocationID,
		AllocationNamespace: body.AllocationNamespace, NodeID: body.NodeID,
		NodeUID: identity.NodeUID, NodeBootID: body.NodeBootID,
		NetNSIdentity: body.NetNSIdentity, ControlEndpoint: body.ControlEndpoint,
		CompatibilityDigest: body.RuntimeCompatibility, HeartbeatTTL: config.HeartbeatTTL,
	})
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !authorizeNode(writer, slot, identity) {
		return
	}
	writeObservation(writer, slot)
}

func serveObserve(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	slot, err := config.Store.GetRuntimeSlot(request.Context(), slotID)
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	if !authorizeNode(writer, slot, identity) {
		return
	}
	writeObservation(writer, slot)
}

func serveReady(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var body protocol.ReadinessRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	if !authorizeAction(config, identity, slotID, body.AllocationID, body.NodeBootID, writer, request) {
		return
	}
	runtimeProof, _ := protocol.DecodeProof("runtime_ready_digest", body.RuntimeReadyDigest)
	networkProof, _ := protocol.DecodeProof("network_ready_digest", body.NetworkReadyDigest)
	storageProof, _ := protocol.DecodeProof("storage_ready_digest", body.StorageReadyDigest)
	slot, err := config.Store.ReportRuntimeSlotReady(request.Context(), &sandboxstore.ReportRuntimeSlotReadyRequest{
		SlotID: slotID, AllocationID: body.AllocationID, NodeUID: identity.NodeUID,
		NodeBootID: body.NodeBootID, RuntimeReadyDigest: runtimeProof,
		NetworkReadyDigest: networkProof, StorageReadyDigest: storageProof,
		HeartbeatTTL: config.HeartbeatTTL,
	})
	writeStoreResult(writer, slot, err)
}

func serveHeartbeat(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var body protocol.HeartbeatRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	if !authorizeAction(config, identity, slotID, body.AllocationID, body.NodeBootID, writer, request) {
		return
	}
	slot, err := config.Store.HeartbeatRuntimeSlot(request.Context(), &sandboxstore.HeartbeatRuntimeSlotRequest{
		SlotID: slotID, AllocationID: body.AllocationID, NodeUID: identity.NodeUID,
		NodeBootID: body.NodeBootID, TTL: config.HeartbeatTTL,
	})
	writeStoreResult(writer, slot, err)
}

func serveStarting(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var body protocol.StartingRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	if !authorizeAction(config, identity, slotID, body.AllocationID, body.NodeBootID, writer, request) {
		return
	}
	rootfsProof, _ := protocol.DecodeProof("rootfs_binding_digest", body.RootFSBindingDigest)
	networkProof, _ := protocol.DecodeProof("claim_network_digest", body.ClaimNetworkDigest)
	slot, err := config.Store.StartRuntimeSlot(request.Context(), &sandboxstore.StartRuntimeSlotRequest{
		SlotID: slotID, AllocationID: body.AllocationID, NodeUID: identity.NodeUID,
		NodeBootID: body.NodeBootID, OperationID: body.OperationID, ClaimID: body.ClaimID,
		LaunchAttempt: body.LaunchAttempt, RunscContainerID: body.RunscContainerID,
		RootFSBindingDigest: rootfsProof, ClaimNetworkDigest: networkProof,
	})
	writeStoreResult(writer, slot, err)
}

func serveCommandReady(config HandlerConfig, identity nodeauth.Identity, slotID string, writer http.ResponseWriter, request *http.Request) {
	if !requirePut(writer, request) {
		return
	}
	var body protocol.CommandReadyRequest
	if !decodeBody(writer, request, &body) || !validateBody(writer, body.Validate()) {
		return
	}
	if !authorizeAction(config, identity, slotID, body.AllocationID, body.NodeBootID, writer, request) {
		return
	}
	proof, _ := protocol.DecodeProof("command_ready_digest", body.CommandReadyDigest)
	slot, err := config.Store.MarkRuntimeSlotCommandReady(request.Context(), &sandboxstore.MarkRuntimeSlotCommandReadyRequest{
		SlotID: slotID, AllocationID: body.AllocationID, NodeUID: identity.NodeUID,
		NodeBootID: body.NodeBootID, OperationID: body.OperationID, ClaimID: body.ClaimID,
		ProcdInstanceID: body.ProcdInstanceID, CommandReadyDigest: proof,
	})
	writeStoreResult(writer, slot, err)
}

func authenticate(verifier nodeauth.Verifier, writer http.ResponseWriter, request *http.Request) (nodeauth.Identity, bool) {
	fields := strings.Fields(request.Header.Get("Authorization"))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || fields[1] == "" {
		writeError(writer, http.StatusUnauthorized, protocol.ErrorUnauthenticated, "a bearer token is required")
		return nodeauth.Identity{}, false
	}
	identity, err := verifier.Verify(request.Context(), fields[1])
	if err != nil {
		status, code := classifyVerifierError(err)
		writeError(writer, status, code, err.Error())
		return nodeauth.Identity{}, false
	}
	identity.NodeUID = strings.TrimSpace(identity.NodeUID)
	identity.PodUID = strings.TrimSpace(identity.PodUID)
	if identity.NodeUID == "" || len(identity.NodeUID) > 512 {
		writeError(writer, http.StatusForbidden, protocol.ErrorPermissionDenied, "authenticated node identity is invalid")
		return nodeauth.Identity{}, false
	}
	return identity, true
}

func authorizeAction(
	config HandlerConfig,
	identity nodeauth.Identity,
	slotID, allocationID, nodeBootID string,
	writer http.ResponseWriter,
	request *http.Request,
) bool {
	slot, err := config.Store.GetRuntimeSlot(request.Context(), slotID)
	if err != nil {
		writeStoreError(writer, err)
		return false
	}
	if !authorizeNode(writer, slot, identity) {
		return false
	}
	if slot.AllocationID != allocationID || slot.NodeBootID != nodeBootID {
		writeError(writer, http.StatusConflict, protocol.ErrorConflict, "request does not match the slot incarnation")
		return false
	}
	return true
}

func authorizeNode(writer http.ResponseWriter, slot *sandboxstore.RuntimeSlot, identity nodeauth.Identity) bool {
	if slot == nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "runtime slot store returned no record")
		return false
	}
	if slot.NodeUID != identity.NodeUID {
		writeError(writer, http.StatusForbidden, protocol.ErrorPermissionDenied, "runtime slot belongs to another node")
		return false
	}
	return true
}

func requirePut(writer http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodPut {
		return true
	}
	writeMethodNotAllowed(writer, http.MethodPut)
	return false
}

func decodeBody(writer http.ResponseWriter, request *http.Request, target any) bool {
	mediaType := strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0])
	if !strings.EqualFold(mediaType, "application/json") {
		writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, "Content-Type must be application/json")
		return false
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, "decode request: "+err.Error())
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, "request must contain exactly one JSON value")
		return false
	}
	return true
}

func validateBody(writer http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}
	writeError(writer, http.StatusBadRequest, protocol.ErrorInvalidArgument, err.Error())
	return false
}

func writeStoreResult(writer http.ResponseWriter, slot *sandboxstore.RuntimeSlot, err error) {
	if err != nil {
		writeStoreError(writer, err)
		return
	}
	writeObservation(writer, slot)
}

func writeStoreError(writer http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, protocol.ErrorUnavailable
	switch {
	case errors.Is(err, sandboxstore.ErrRuntimeSlotNotFound):
		status, code = http.StatusNotFound, protocol.ErrorNotFound
	case errors.Is(err, sandboxstore.ErrRuntimeSlotConflict):
		status, code = http.StatusConflict, protocol.ErrorConflict
	case errors.Is(err, sandboxstore.ErrRuntimeSlotInvalid):
		status, code = http.StatusPreconditionFailed, protocol.ErrorFailedPrecondition
	case errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable):
		status, code = http.StatusServiceUnavailable, protocol.ErrorUnavailable
	case errdefs.IsInvalidArgument(err):
		status, code = http.StatusBadRequest, protocol.ErrorInvalidArgument
	case errdefs.IsPermissionDenied(err):
		status, code = http.StatusForbidden, protocol.ErrorPermissionDenied
	case errors.Is(err, context.DeadlineExceeded), errdefs.IsUnavailable(err):
		status, code = http.StatusServiceUnavailable, protocol.ErrorUnavailable
	}
	writeError(writer, status, code, err.Error())
}

func classifyVerifierError(err error) (int, string) {
	if errdefs.IsPermissionDenied(err) {
		return http.StatusForbidden, protocol.ErrorPermissionDenied
	}
	if errdefs.IsUnavailable(err) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusServiceUnavailable, protocol.ErrorUnavailable
	}
	return http.StatusUnauthorized, protocol.ErrorUnauthenticated
}

func writeObservation(writer http.ResponseWriter, slot *sandboxstore.RuntimeSlot) {
	if slot == nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "runtime slot store returned no record")
		return
	}
	observation := protocol.Observation{
		SlotID: slot.ID, State: protocol.State(slot.State), Revision: slot.Revision,
		ServerTime: slot.AuthorityObservedAt, HeartbeatExpiresAt: slot.HeartbeatExpiresAt,
		ClaimOperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
	}
	if !slot.ClaimLeaseExpiresAt.IsZero() {
		claimExpiry := slot.ClaimLeaseExpiresAt
		observation.ClaimLeaseExpiresAt = &claimExpiry
	}
	if err := observation.Validate(); err != nil {
		writeError(writer, http.StatusServiceUnavailable, protocol.ErrorUnavailable, "runtime slot store returned an invalid observation")
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(observation)
}

func writeMethodNotAllowed(writer http.ResponseWriter, allow string) {
	writer.Header().Set("Allow", allow)
	writeError(writer, http.StatusMethodNotAllowed, protocol.ErrorInvalidArgument, "method is not supported")
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	response := protocol.ErrorResponse{Code: code, Message: strings.TrimSpace(message)}
	if response.Message == "" {
		response.Message = http.StatusText(status)
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(response)
}

func parseRoute(path string) (string, routeAction, error) {
	if !strings.HasPrefix(path, protocol.PathPrefix) {
		return "", "", fmt.Errorf("invalid runtime slot path")
	}
	relative := strings.TrimPrefix(path, protocol.PathPrefix)
	segments := strings.Split(relative, "/")
	if len(segments) < 1 || len(segments) > 2 || segments[0] == "" {
		return "", "", fmt.Errorf("invalid runtime slot path")
	}
	slotID, err := url.PathUnescape(segments[0])
	if err != nil || strings.TrimSpace(slotID) == "" || len(slotID) > 512 || url.PathEscape(slotID) != segments[0] {
		return "", "", fmt.Errorf("invalid runtime slot ID")
	}
	if len(segments) == 1 {
		return slotID, actionSlot, nil
	}
	action := routeAction(segments[1])
	switch action {
	case actionReady, actionHeartbeat, actionStarting, actionCommandReady:
		return slotID, action, nil
	default:
		return "", "", fmt.Errorf("invalid runtime slot action")
	}
}
