// Package rootfswriterauthority defines the authenticated node-to-manager
// protocol used to consume, renew, and prove terminal state for a regional
// RootFS writer grant.
package rootfswriterauthority

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const ConsumePathPrefix = "/internal/v1/rootfs-writer-grants/"

const BatchRenewPath = "/internal/v1/rootfs-writer-grants:renew"

const (
	renewPathSuffix           = "/renew"
	pressurePathSuffix        = "/pressure"
	runningForkPathSuffix     = "/fork-running"
	terminalPathSuffix        = "/terminal"
	preconsumeAbortPathSuffix = "/terminal/preconsume-abort"
)

// ConsumeRequest deliberately excludes caller identity and lease policy.
// Manager derives both from TokenReview and server configuration.
type ConsumeRequest struct {
	WriterEpoch    int64  `json:"writer_epoch"`
	BindingVersion int    `json:"binding_version"`
	BindingDigest  string `json:"binding_digest"`
	WriterToken    string `json:"writer_grant_token"`
}

// LeaseObservation is the regional authority's clock-relative view of one
// consumed writer lease. Nodes schedule renewal from these timestamps instead
// of assuming that their local clock or configuration matches Manager.
type LeaseObservation struct {
	ServerTime     time.Time `json:"server_time"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
	RenewAfter     time.Time `json:"renew_after"`
}

func (o LeaseObservation) Validate() error {
	if o.ServerTime.IsZero() || o.LeaseExpiresAt.IsZero() || o.RenewAfter.IsZero() {
		return fmt.Errorf("server_time, lease_expires_at, and renew_after are required")
	}
	if !o.LeaseExpiresAt.After(o.ServerTime) {
		return fmt.Errorf("lease_expires_at must be after server_time")
	}
	if o.RenewAfter.Before(o.ServerTime) || !o.RenewAfter.Before(o.LeaseExpiresAt) {
		return fmt.Errorf("renew_after must be between server_time and lease_expires_at")
	}
	return nil
}

func (r ConsumeRequest) Validate() error {
	if r.WriterEpoch <= 0 {
		return fmt.Errorf("writer_epoch must be positive")
	}
	if r.BindingVersion <= 0 {
		return fmt.Errorf("binding_version must be positive")
	}
	if _, err := r.DecodedBindingDigest(); err != nil {
		return err
	}
	if strings.TrimSpace(r.WriterToken) == "" {
		return fmt.Errorf("writer_grant_token is required")
	}
	return nil
}

func (r ConsumeRequest) DecodedBindingDigest() ([]byte, error) {
	value, err := hex.DecodeString(strings.TrimSpace(r.BindingDigest))
	if err != nil || len(value) != 32 {
		return nil, fmt.Errorf("binding_digest must be a 32-byte lowercase hexadecimal digest")
	}
	if hex.EncodeToString(value) != strings.TrimSpace(r.BindingDigest) {
		return nil, fmt.Errorf("binding_digest must use canonical lowercase hexadecimal encoding")
	}
	return value, nil
}

func ConsumePath(grantID string) string {
	return ConsumePathPrefix + url.PathEscape(grantID)
}

// RenewRequest identifies the exact consumed writer binding. The authenticated
// node is the durable owner after Consume, so the one-time raw grant token is
// deliberately absent. Lease duration and renewal grace remain server policy.
type RenewRequest struct {
	WriterEpoch    int64  `json:"writer_epoch"`
	BindingVersion int    `json:"binding_version"`
	BindingDigest  string `json:"binding_digest"`
}

const MaxBatchRenewItems = 256

type BatchRenewItem struct {
	GrantID string `json:"grant_id"`
	RenewRequest
}

type BatchRenewRequest struct {
	Items []BatchRenewItem `json:"items"`
}

func (r BatchRenewRequest) Validate() error {
	if len(r.Items) == 0 || len(r.Items) > MaxBatchRenewItems {
		return fmt.Errorf("items must contain between 1 and %d writer grants", MaxBatchRenewItems)
	}
	seen := make(map[string]struct{}, len(r.Items))
	for index, item := range r.Items {
		if strings.TrimSpace(item.GrantID) == "" {
			return fmt.Errorf("items[%d].grant_id is required", index)
		}
		if _, exists := seen[item.GrantID]; exists {
			return fmt.Errorf("items[%d].grant_id is duplicated", index)
		}
		seen[item.GrantID] = struct{}{}
		if err := item.RenewRequest.Validate(); err != nil {
			return fmt.Errorf("items[%d]: %w", index, err)
		}
	}
	return nil
}

const (
	RenewErrorInvalidArgument    = "invalid_argument"
	RenewErrorPermissionDenied   = "permission_denied"
	RenewErrorNotFound           = "not_found"
	RenewErrorFailedPrecondition = "failed_precondition"
	RenewErrorDeadlineExceeded   = "deadline_exceeded"
	RenewErrorUnavailable        = "unavailable"
)

type BatchRenewResult struct {
	GrantID     string            `json:"grant_id"`
	Observation *LeaseObservation `json:"observation,omitempty"`
	ErrorCode   string            `json:"error_code,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type BatchRenewResponse struct {
	Results []BatchRenewResult `json:"results"`
}

func (r BatchRenewResponse) Validate(expected int) error {
	if len(r.Results) != expected {
		return fmt.Errorf("renew result count %d does not match request count %d", len(r.Results), expected)
	}
	seen := make(map[string]struct{}, len(r.Results))
	for index, result := range r.Results {
		if strings.TrimSpace(result.GrantID) == "" {
			return fmt.Errorf("results[%d].grant_id is required", index)
		}
		if _, exists := seen[result.GrantID]; exists {
			return fmt.Errorf("results[%d].grant_id is duplicated", index)
		}
		seen[result.GrantID] = struct{}{}
		if result.ErrorCode == "" {
			if result.Observation == nil {
				return fmt.Errorf("results[%d].observation is required", index)
			}
			if err := result.Observation.Validate(); err != nil {
				return fmt.Errorf("results[%d].observation: %w", index, err)
			}
			continue
		}
		if result.Observation != nil || strings.TrimSpace(result.Error) == "" || !validRenewErrorCode(result.ErrorCode) {
			return fmt.Errorf("results[%d] has an invalid renewal error", index)
		}
	}
	return nil
}

func validRenewErrorCode(value string) bool {
	switch value {
	case RenewErrorInvalidArgument, RenewErrorPermissionDenied, RenewErrorNotFound,
		RenewErrorFailedPrecondition, RenewErrorDeadlineExceeded, RenewErrorUnavailable:
		return true
	default:
		return false
	}
}

func (r RenewRequest) Validate() error {
	return TerminalRequest(r).Validate()
}

func (r RenewRequest) DecodedBindingDigest() ([]byte, error) {
	return TerminalRequest(r).DecodedBindingDigest()
}

func RenewPath(grantID string) string {
	return ConsumePath(grantID) + renewPathSuffix
}

const (
	DirtyTailPressureScopeSession = "session"
	DirtyTailPressureScopeNode    = "node"
)

// DirtyTailPressureRequest identifies one normal write blocked before it
// consumes protected retirement capacity. Manager derives the sandbox from
// the authenticated writer grant rather than trusting a node-supplied ID.
type DirtyTailPressureRequest struct {
	TerminalRequest
	Scope          string `json:"scope"`
	UsedBytes      int64  `json:"used_bytes"`
	RequestedBytes int64  `json:"requested_bytes"`
	LimitBytes     int64  `json:"limit_bytes"`
}

func (r DirtyTailPressureRequest) Validate() error {
	if err := r.TerminalRequest.Validate(); err != nil {
		return err
	}
	if r.Scope != DirtyTailPressureScopeSession && r.Scope != DirtyTailPressureScopeNode {
		return fmt.Errorf("scope must be session or node")
	}
	if r.UsedBytes < 0 || r.RequestedBytes <= 0 || r.LimitBytes <= 0 ||
		r.UsedBytes <= r.LimitBytes && r.RequestedBytes <= r.LimitBytes-r.UsedBytes {
		return fmt.Errorf("usage must describe a positive request that crosses the normal limit")
	}
	return nil
}

// DirtyTailPressureResponse returns the deterministic regional operation that
// the node must persist locally before unblocking retirement I/O.
type DirtyTailPressureResponse struct {
	OperationID string `json:"operation_id"`
}

func (r DirtyTailPressureResponse) Validate() error {
	if strings.TrimSpace(r.OperationID) == "" || r.OperationID != strings.TrimSpace(r.OperationID) {
		return fmt.Errorf("operation_id must be canonical and non-empty")
	}
	return nil
}

func DirtyTailPressurePath(grantID string) string {
	return ConsumePath(grantID) + pressurePathSuffix
}

// RunningForkPath publishes a consistent checkpoint without retiring the
// source writer grant.
func RunningForkPath(grantID string) string {
	return ConsumePath(grantID) + runningForkPathSuffix
}

// TerminalRequest identifies the exact immutable writer binding whose
// terminal state is being queried. State and writer credentials are
// deliberately absent: Manager reads state from regional storage and derives
// the caller node from TokenReview.
type TerminalRequest struct {
	WriterEpoch    int64  `json:"writer_epoch"`
	BindingVersion int    `json:"binding_version"`
	BindingDigest  string `json:"binding_digest"`
}

func (r TerminalRequest) Validate() error {
	if r.WriterEpoch <= 0 {
		return fmt.Errorf("writer_epoch must be positive")
	}
	if r.BindingVersion <= 0 {
		return fmt.Errorf("binding_version must be positive")
	}
	_, err := r.DecodedBindingDigest()
	return err
}

func (r TerminalRequest) DecodedBindingDigest() ([]byte, error) {
	return ConsumeRequest{BindingDigest: r.BindingDigest}.DecodedBindingDigest()
}

// TerminalPath returns the canonical authenticated terminal-proof endpoint.
func TerminalPath(grantID string) string {
	return ConsumePath(grantID) + terminalPathSuffix
}

// PreconsumeAbortPath cancels an exact grant that is still issued. It closes
// the crash window between durable node admission and Consume response.
func PreconsumeAbortPath(grantID string) string {
	return ConsumePath(grantID) + preconsumeAbortPathSuffix
}
