// Package runtimeslot defines the versioned node-to-region protocol for
// generic warm runtime allocations.
package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"
)

const (
	PathPrefix                  = "/internal/v1/runtime-slots/"
	ProcdCommandReadyProbePath  = "/api/v1/runtime/command-ready-probe"
	NodeCommandReadyControlPath = "/command-ready"
	CommandReadyProofVersion    = 1

	readyPathSuffix        = "/ready"
	heartbeatPathSuffix    = "/heartbeat"
	startingPathSuffix     = "/starting"
	commandReadyPathSuffix = "/command-ready"
)

// CommandReadyProof is the canonical evidence produced after a manager has
// completed an authenticated, runtime-gated command against one procd process.
// It is submitted over the root-only node control socket; the driver hashes it
// before advancing the regional slot.
type CommandReadyProof struct {
	Version            int    `json:"version"`
	SlotID             string `json:"slot_id"`
	OperationID        string `json:"operation_id"`
	ClaimID            string `json:"claim_id"`
	LaunchAttempt      string `json:"launch_attempt"`
	RunscContainerID   string `json:"runsc_container_id"`
	ProcdInstanceID    string `json:"procd_instance_id"`
	RequestMethod      string `json:"request_method"`
	RequestPath        string `json:"request_path"`
	ResponseStatus     int    `json:"response_status"`
	ResponseBodyDigest string `json:"response_body_digest"`
}

// CommandReadyControlRequest is sent by manager over the root-only driver
// control socket after the procd probe succeeds.
type CommandReadyControlRequest struct {
	Proof CommandReadyProof `json:"proof"`
}

func (p CommandReadyProof) Validate() error {
	if p.Version != CommandReadyProofVersion {
		return fmt.Errorf("unsupported command-ready proof version %d", p.Version)
	}
	for name, value := range map[string]string{
		"slot_id": p.SlotID, "operation_id": p.OperationID, "claim_id": p.ClaimID,
		"launch_attempt": p.LaunchAttempt, "runsc_container_id": p.RunscContainerID,
		"procd_instance_id": p.ProcdInstanceID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if p.RequestMethod != "PUT" || p.RequestPath != ProcdCommandReadyProbePath || p.ResponseStatus != 200 {
		return fmt.Errorf("command-ready proof does not describe the canonical procd probe")
	}
	if _, err := DecodeProof("response_body_digest", p.ResponseBodyDigest); err != nil {
		return err
	}
	return nil
}

// Digest returns the canonical regional command-ready digest.
func (p CommandReadyProof) Digest() (string, error) {
	if err := p.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode command-ready proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// State is the region-authoritative lifecycle of one physical warm slot.
type State string

const (
	StateRegistered    State = "registered"
	StateFastpathReady State = "fastpath_ready"
	StateClaiming      State = "claiming"
	StateStarting      State = "starting"
	StateActive        State = "active"
	StateQuiescing     State = "quiescing"
	StateOrphaned      State = "orphaned"
	StateTerminal      State = "terminal"
)

func (s State) Valid() bool {
	switch s {
	case StateRegistered, StateFastpathReady, StateClaiming, StateStarting,
		StateActive, StateQuiescing, StateOrphaned, StateTerminal:
		return true
	default:
		return false
	}
}

// RegistrationRequest contains physical placement identity supplied by the
// trusted node agent. NodeUID is deliberately derived from authentication.
type RegistrationRequest struct {
	ClusterID            string `json:"cluster_id"`
	AllocationID         string `json:"allocation_id"`
	AllocationNamespace  string `json:"allocation_namespace"`
	NodeID               string `json:"node_id"`
	NodeBootID           string `json:"node_boot_id"`
	NetNSIdentity        string `json:"netns_identity"`
	ControlEndpoint      string `json:"control_endpoint"`
	RuntimeCompatibility string `json:"runtime_compatibility"`
}

func (r RegistrationRequest) Validate() error {
	for name, value := range map[string]string{
		"cluster_id": r.ClusterID, "allocation_id": r.AllocationID,
		"allocation_namespace": r.AllocationNamespace, "node_id": r.NodeID,
		"node_boot_id": r.NodeBootID, "netns_identity": r.NetNSIdentity,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if err := validateControlEndpoint(r.ControlEndpoint); err != nil {
		return err
	}
	return validateCompatibility(r.RuntimeCompatibility)
}

// ReadinessRequest proves that all claim-independent fast-path resources are
// ready for the exact allocation and node-boot incarnation.
type ReadinessRequest struct {
	AllocationID       string `json:"allocation_id"`
	NodeBootID         string `json:"node_boot_id"`
	RuntimeReadyDigest string `json:"runtime_ready_digest"`
	NetworkReadyDigest string `json:"network_ready_digest"`
	StorageReadyDigest string `json:"storage_ready_digest"`
}

func (r ReadinessRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"runtime_ready_digest": r.RuntimeReadyDigest,
		"network_ready_digest": r.NetworkReadyDigest,
		"storage_ready_digest": r.StorageReadyDigest,
	} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	return nil
}

// HeartbeatRequest extends liveness without changing readiness or claim state.
type HeartbeatRequest struct {
	AllocationID string `json:"allocation_id"`
	NodeBootID   string `json:"node_boot_id"`
}

func (r HeartbeatRequest) Validate() error {
	return validateCaller(r.AllocationID, r.NodeBootID)
}

// StartingRequest records the exact runsc launch and post-claim RootFS/network
// bindings. The regional writer grant must already be consumed.
type StartingRequest struct {
	AllocationID        string `json:"allocation_id"`
	NodeBootID          string `json:"node_boot_id"`
	OperationID         string `json:"operation_id"`
	ClaimID             string `json:"claim_id"`
	LaunchAttempt       string `json:"launch_attempt"`
	RunscContainerID    string `json:"runsc_container_id"`
	RootFSBindingDigest string `json:"rootfs_binding_digest"`
	ClaimNetworkDigest  string `json:"claim_network_digest"`
}

func (r StartingRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID,
		"launch_attempt": r.LaunchAttempt, "runsc_container_id": r.RunscContainerID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if _, err := DecodeProof("rootfs_binding_digest", r.RootFSBindingDigest); err != nil {
		return err
	}
	_, err := DecodeProof("claim_network_digest", r.ClaimNetworkDigest)
	return err
}

// CommandReadyRequest proves that the launched procd instance accepts commands.
type CommandReadyRequest struct {
	AllocationID       string `json:"allocation_id"`
	NodeBootID         string `json:"node_boot_id"`
	OperationID        string `json:"operation_id"`
	ClaimID            string `json:"claim_id"`
	ProcdInstanceID    string `json:"procd_instance_id"`
	CommandReadyDigest string `json:"command_ready_digest"`
}

func (r CommandReadyRequest) Validate() error {
	if err := validateCaller(r.AllocationID, r.NodeBootID); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"operation_id": r.OperationID, "claim_id": r.ClaimID,
		"procd_instance_id": r.ProcdInstanceID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	_, err := DecodeProof("command_ready_digest", r.CommandReadyDigest)
	return err
}

// Observation is the bounded node-visible projection of one durable slot.
type Observation struct {
	SlotID              string     `json:"slot_id"`
	State               State      `json:"state"`
	Revision            int64      `json:"revision"`
	ServerTime          time.Time  `json:"server_time"`
	HeartbeatExpiresAt  time.Time  `json:"heartbeat_expires_at"`
	ClaimOperationID    string     `json:"claim_operation_id,omitempty"`
	ClaimID             string     `json:"claim_id,omitempty"`
	ClaimLeaseExpiresAt *time.Time `json:"claim_lease_expires_at,omitempty"`
}

const (
	ErrorInvalidArgument    = "invalid_argument"
	ErrorUnauthenticated    = "unauthenticated"
	ErrorPermissionDenied   = "permission_denied"
	ErrorNotFound           = "not_found"
	ErrorConflict           = "conflict"
	ErrorFailedPrecondition = "failed_precondition"
	ErrorUnavailable        = "unavailable"
)

// ErrorResponse provides a stable machine-readable failure class.
type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e ErrorResponse) Validate() error {
	switch e.Code {
	case ErrorInvalidArgument, ErrorUnauthenticated, ErrorPermissionDenied,
		ErrorNotFound, ErrorConflict, ErrorFailedPrecondition, ErrorUnavailable:
	default:
		return fmt.Errorf("error code is invalid")
	}
	if strings.TrimSpace(e.Message) == "" {
		return fmt.Errorf("error message is required")
	}
	return nil
}

func (o Observation) Validate() error {
	if err := validateRequiredID("slot_id", o.SlotID); err != nil {
		return err
	}
	if !o.State.Valid() {
		return fmt.Errorf("state is invalid")
	}
	if o.Revision <= 0 {
		return fmt.Errorf("revision must be positive")
	}
	if o.ServerTime.IsZero() || o.HeartbeatExpiresAt.IsZero() {
		return fmt.Errorf("server_time and heartbeat_expires_at are required")
	}
	claimed := strings.TrimSpace(o.ClaimOperationID) != "" || strings.TrimSpace(o.ClaimID) != ""
	if claimed {
		if err := validateRequiredID("claim_operation_id", o.ClaimOperationID); err != nil {
			return err
		}
		if err := validateRequiredID("claim_id", o.ClaimID); err != nil {
			return err
		}
		if o.ClaimLeaseExpiresAt == nil || o.ClaimLeaseExpiresAt.IsZero() {
			return fmt.Errorf("claim_lease_expires_at is required for a claimed slot")
		}
	} else if o.ClaimLeaseExpiresAt != nil {
		return fmt.Errorf("claim_lease_expires_at requires a claim binding")
	}
	return nil
}

func SlotPath(slotID string) string {
	return PathPrefix + url.PathEscape(slotID)
}

func ValidateSlotID(slotID string) error {
	return validateRequiredID("slot_id", slotID)
}

func ReadyPath(slotID string) string        { return SlotPath(slotID) + readyPathSuffix }
func HeartbeatPath(slotID string) string    { return SlotPath(slotID) + heartbeatPathSuffix }
func StartingPath(slotID string) string     { return SlotPath(slotID) + startingPathSuffix }
func CommandReadyPath(slotID string) string { return SlotPath(slotID) + commandReadyPathSuffix }

// DecodeProof validates and decodes a canonical lowercase SHA-256 proof.
func DecodeProof(name, value string) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	decoded, err := hex.DecodeString(trimmed)
	if value != trimmed || err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != trimmed {
		return nil, fmt.Errorf("%s must be a canonical 32-byte lowercase hexadecimal digest", name)
	}
	return decoded, nil
}

func validateCaller(allocationID, nodeBootID string) error {
	if err := validateRequiredID("allocation_id", allocationID); err != nil {
		return err
	}
	return validateRequiredID("node_boot_id", nodeBootID)
}

func validateRequiredID(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || trimmed == "" || len(value) > 512 {
		return fmt.Errorf("%s is required and must not exceed 512 bytes", name)
	}
	return nil
}

func validateCompatibility(value string) error {
	trimmed := strings.TrimSpace(value)
	parsed, err := digest.Parse(trimmed)
	if value != trimmed || err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return fmt.Errorf("runtime_compatibility must be a canonical sha256 digest")
	}
	return nil
}

func validateControlEndpoint(value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || value == "" || len(value) > 2_048 {
		return fmt.Errorf("control_endpoint is required and must not exceed 2048 bytes")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("control_endpoint is invalid")
	}
	switch parsed.Scheme {
	case "http", "https":
		if parsed.Host == "" {
			return fmt.Errorf("control_endpoint HTTP origin requires a host")
		}
	case "unix":
		if parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") {
			return fmt.Errorf("control_endpoint unix origin requires an absolute path")
		}
	default:
		return fmt.Errorf("control_endpoint scheme must be http, https, or unix")
	}
	return nil
}
