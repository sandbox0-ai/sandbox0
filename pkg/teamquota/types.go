// Package teamquota defines region-scoped limits that protect shared platform
// resources from exhaustion by a single team.
package teamquota

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	maxExactRedisInteger  int64 = 1<<53 - 1
	minRateIntervalMillis int64 = 1
	maxRateIntervalMillis int64 = int64(time.Hour / time.Millisecond)
)

// RateIntervalMillis converts a rate-policy interval to its canonical integer
// representation without losing sub-millisecond precision.
func RateIntervalMillis(interval time.Duration) (int64, error) {
	if interval < time.Millisecond || interval > time.Hour {
		return 0, fmt.Errorf("rate policy interval must be between 1ms and 1h")
	}
	if interval%time.Millisecond != 0 {
		return 0, fmt.Errorf("rate policy interval must use whole milliseconds")
	}
	return int64(interval / time.Millisecond), nil
}

// Kind identifies the enforcement model used by a quota policy.
type Kind string

const (
	KindCapacity Kind = "capacity"
	KindRate     Kind = "rate"
	// KindConcurrency is enforced through short-lived region-shared Redis
	// leases. It uses Policy.Limit but never participates in the durable
	// PostgreSQL capacity allocation ledger.
	KindConcurrency Kind = "concurrency"
)

// PolicySource identifies where an effective policy was defined.
type PolicySource string

const (
	PolicySourceDefault  PolicySource = "default"
	PolicySourceOverride PolicySource = "override"
)

// Key identifies a quota-controlled resource.
type Key string

const (
	KeySandboxIdentityCount         Key = "sandbox_identity_count"
	KeySandboxRuntimeCount          Key = "sandbox_runtime_count"
	KeySandboxCPUMillicores         Key = "sandbox_cpu_millicores"
	KeySandboxMemoryBytes           Key = "sandbox_memory_bytes"
	KeySandboxEphemeralStorageBytes Key = "sandbox_ephemeral_storage_bytes"
	KeyVolumeStorageBytes           Key = "volume_storage_bytes"
	KeySnapshotStorageBytes         Key = "snapshot_storage_bytes"
	KeyRootFSStorageBytes           Key = "rootfs_storage_bytes"
	KeyTemplateImageStorageBytes    Key = "template_image_storage_bytes"
	KeyStorageObjectCount           Key = "storage_object_count"
	KeyControlPlaneObjectCount      Key = "control_plane_object_count"
	KeyAPIRequests                  Key = "api_requests"
	KeySandboxServiceRequests       Key = "sandbox_service_requests"
	KeySandboxStarts                Key = "sandbox_starts"
	KeyNetworkIngressBytes          Key = "network_ingress_bytes"
	KeyNetworkEgressBytes           Key = "network_egress_bytes"
	KeyStorageOperations            Key = "storage_operations"
	KeyObservabilityIngestBytes     Key = "observability_ingest_bytes"
	KeyActiveConnectionCount        Key = "active_connection_count"
	KeyActiveRequestCount           Key = "active_request_count"
	KeyNetworkOperations            Key = "network_operations"
)

var keyKinds = map[Key]Kind{
	KeySandboxIdentityCount:         KindCapacity,
	KeySandboxRuntimeCount:          KindCapacity,
	KeySandboxCPUMillicores:         KindCapacity,
	KeySandboxMemoryBytes:           KindCapacity,
	KeySandboxEphemeralStorageBytes: KindCapacity,
	KeyVolumeStorageBytes:           KindCapacity,
	KeySnapshotStorageBytes:         KindCapacity,
	KeyRootFSStorageBytes:           KindCapacity,
	KeyTemplateImageStorageBytes:    KindCapacity,
	KeyStorageObjectCount:           KindCapacity,
	KeyControlPlaneObjectCount:      KindCapacity,
	KeyAPIRequests:                  KindRate,
	KeySandboxServiceRequests:       KindRate,
	KeySandboxStarts:                KindRate,
	KeyNetworkIngressBytes:          KindRate,
	KeyNetworkEgressBytes:           KindRate,
	KeyStorageOperations:            KindRate,
	KeyObservabilityIngestBytes:     KindRate,
	KeyActiveConnectionCount:        KindConcurrency,
	KeyActiveRequestCount:           KindConcurrency,
	KeyNetworkOperations:            KindRate,
}

var keyUnits = map[Key]string{
	KeySandboxIdentityCount:         "count",
	KeySandboxRuntimeCount:          "count",
	KeySandboxCPUMillicores:         "millicores",
	KeySandboxMemoryBytes:           "bytes",
	KeySandboxEphemeralStorageBytes: "bytes",
	KeyVolumeStorageBytes:           "bytes",
	KeySnapshotStorageBytes:         "bytes",
	KeyRootFSStorageBytes:           "bytes",
	KeyTemplateImageStorageBytes:    "bytes",
	KeyStorageObjectCount:           "count",
	KeyControlPlaneObjectCount:      "count",
	KeyAPIRequests:                  "requests",
	KeySandboxServiceRequests:       "requests",
	KeySandboxStarts:                "count",
	KeyNetworkIngressBytes:          "bytes",
	KeyNetworkEgressBytes:           "bytes",
	KeyStorageOperations:            "operations",
	KeyObservabilityIngestBytes:     "bytes",
	KeyActiveConnectionCount:        "count",
	KeyActiveRequestCount:           "count",
	KeyNetworkOperations:            "operations",
}

// KnownKey reports whether key is supported by this version of the service.
func KnownKey(key Key) bool {
	_, ok := keyKinds[key]
	return ok
}

// KindForKey returns the enforcement kind registered for key.
func KindForKey(key Key) (Kind, bool) {
	kind, ok := keyKinds[key]
	return kind, ok
}

// UnitForKey returns the unit used by a key's values.
func UnitForKey(key Key) string {
	return keyUnits[key]
}

// Keys returns all known keys in stable lexical order.
func Keys() []Key {
	keys := make([]Key, 0, len(keyKinds))
	for key := range keyKinds {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

// Policy is a capacity limit, concurrency limit, or token-bucket rate policy.
//
// Capacity and concurrency policies use Limit. Rate policies use Tokens,
// IntervalMillis, and Burst. Fields that do not belong to the selected kind
// must be zero.
type Policy struct {
	TeamID         string `json:"team_id,omitempty"`
	Key            Key    `json:"key"`
	Kind           Kind   `json:"kind"`
	Revision       int64  `json:"revision,omitempty"`
	Limit          int64  `json:"limit,omitempty"`
	Tokens         int64  `json:"tokens,omitempty"`
	IntervalMillis int64  `json:"interval_ms,omitempty"`
	Burst          int64  `json:"burst,omitempty"`
}

// Validate rejects unknown keys and malformed kind-specific policy fields.
func (p Policy) Validate() error {
	if !KnownKey(p.Key) {
		return fmt.Errorf("unknown team quota key %q", p.Key)
	}
	if p.Revision < 0 {
		return fmt.Errorf("policy revision must be non-negative")
	}
	expectedKind, _ := KindForKey(p.Key)
	if p.Kind != expectedKind {
		return fmt.Errorf("team quota key %q requires kind %q", p.Key, expectedKind)
	}
	switch p.Kind {
	case KindCapacity, KindConcurrency:
		if p.Limit < 0 {
			return fmt.Errorf("%s limit must be non-negative", p.Kind)
		}
		if p.Kind == KindConcurrency && p.Limit > maxExactRedisInteger {
			return fmt.Errorf("concurrency limit exceeds the exact Redis integer range")
		}
		if p.Tokens != 0 || p.IntervalMillis != 0 || p.Burst != 0 {
			return fmt.Errorf("%s policy must not set rate fields", p.Kind)
		}
	case KindRate:
		if p.Limit != 0 {
			return fmt.Errorf("rate policy must not set capacity limit")
		}
		if p.Tokens <= 0 {
			return fmt.Errorf("rate policy tokens must be positive")
		}
		if p.Tokens > maxExactRedisInteger {
			return fmt.Errorf("rate policy tokens exceed the exact Redis integer range")
		}
		if p.IntervalMillis < minRateIntervalMillis || p.IntervalMillis > maxRateIntervalMillis {
			return fmt.Errorf("rate policy interval_ms must be between %d and %d", minRateIntervalMillis, maxRateIntervalMillis)
		}
		if p.Burst < p.Tokens {
			return fmt.Errorf("rate policy burst must be greater than or equal to tokens")
		}
		if p.Burst > maxExactRedisInteger {
			return fmt.Errorf("rate policy burst exceeds the exact Redis integer range")
		}
	default:
		return fmt.Errorf("unknown team quota kind %q", p.Kind)
	}
	return nil
}

// Status describes an effective policy and its strongly consistent capacity
// allocation state. Rate usage is maintained by the enforcing runtime, so
// Remaining is nil for rate policies.
type Status struct {
	TeamID    string       `json:"team_id"`
	Key       Key          `json:"key"`
	Kind      Kind         `json:"kind"`
	Unit      string       `json:"unit"`
	Source    PolicySource `json:"source"`
	Policy    Policy       `json:"policy"`
	Committed int64        `json:"committed"`
	Reserved  int64        `json:"reserved"`
	Used      int64        `json:"used"`
	Remaining *int64       `json:"remaining"`
}

// Owner identifies the durable resource whose allocation is being changed.
type Owner struct {
	TeamID    string `json:"team_id"`
	Kind      string `json:"kind"`
	ID        string `json:"id"`
	ClusterID string `json:"cluster_id,omitempty"`
}

// Validate checks the identity required for idempotent allocation operations.
func (o Owner) Validate() error {
	if strings.TrimSpace(o.TeamID) == "" {
		return fmt.Errorf("owner team_id is required")
	}
	if strings.TrimSpace(o.Kind) == "" {
		return fmt.Errorf("owner kind is required")
	}
	if strings.TrimSpace(o.ID) == "" {
		return fmt.Errorf("owner id is required")
	}
	return nil
}

// Operation identifies one idempotent external resource mutation.
type Operation struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Generation int64  `json:"generation,omitempty"`
}

// Validate checks the operation identity used for compare-and-swap updates.
func (o Operation) Validate() error {
	if strings.TrimSpace(o.ID) == "" {
		return fmt.Errorf("operation id is required")
	}
	if strings.TrimSpace(o.Kind) == "" {
		return fmt.Errorf("operation kind is required")
	}
	if o.Generation < 0 {
		return fmt.Errorf("operation generation must be non-negative")
	}
	return nil
}

// OperationRef identifies an already reserved operation.
type OperationRef struct {
	Owner      Owner  `json:"owner"`
	ID         string `json:"id"`
	Generation int64  `json:"generation,omitempty"`
}

// Ref returns a compare-and-swap reference for an operation.
func Ref(owner Owner, operation Operation) OperationRef {
	return OperationRef{Owner: owner, ID: operation.ID, Generation: operation.Generation}
}

func (r OperationRef) validate() error {
	if err := r.Owner.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("operation id is required")
	}
	if r.Generation < 0 {
		return fmt.Errorf("operation generation must be non-negative")
	}
	return nil
}

// RuntimeRef identifies the exact Kubernetes runtime behind an allocation.
type RuntimeRef struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
	Generation int64  `json:"generation,omitempty"`
}

// Values is a quota-keyed resource bundle.
type Values map[Key]int64

// Clone returns an independent copy of v.
func (v Values) Clone() Values {
	if v == nil {
		return nil
	}
	cloned := make(Values, len(v))
	for key, value := range v {
		cloned[key] = value
	}
	return cloned
}

// Keys returns the bundle keys in stable lexical order.
func (v Values) Keys() []Key {
	keys := make([]Key, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func (v Values) validateCapacity(allowZero bool) error {
	if len(v) == 0 {
		return fmt.Errorf("quota values are required")
	}
	for key, value := range v {
		kind, ok := KindForKey(key)
		if !ok {
			return fmt.Errorf("unknown team quota key %q", key)
		}
		if kind != KindCapacity {
			return fmt.Errorf("team quota key %q is not a capacity key", key)
		}
		if value < 0 || (!allowZero && value == 0) {
			if allowZero {
				return fmt.Errorf("quota value for %q must be non-negative", key)
			}
			return fmt.Errorf("quota delta for %q must be positive", key)
		}
	}
	return nil
}

// ReserveRequest sets an owner's complete target allocation.
type ReserveRequest struct {
	Owner     Owner     `json:"owner"`
	Operation Operation `json:"operation"`
	Target    Values    `json:"target"`
}

func (r ReserveRequest) validate() error {
	if err := r.Owner.Validate(); err != nil {
		return err
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	return r.Target.validateCapacity(true)
}

// DeltaRequest atomically adds non-negative values to an owner's committed
// target. An all-zero delta still creates a durable owner operation fence.
type DeltaRequest struct {
	Owner     Owner     `json:"owner"`
	Operation Operation `json:"operation"`
	Delta     Values    `json:"delta"`
	// Observed is the caller's diagnostic physical baseline. PostgreSQL
	// admission never trusts it; the target is always current committed plus
	// Delta.
	Observed Values `json:"observed,omitempty"`
}

func (r DeltaRequest) validate() error {
	if err := r.Owner.Validate(); err != nil {
		return err
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	if err := r.Delta.validateCapacity(true); err != nil {
		return err
	}
	if len(r.Observed) > 0 {
		return r.Observed.validateCapacity(true)
	}
	return nil
}

// ReleaseRequest begins a conservative transition to a smaller target.
type ReleaseRequest struct {
	Owner     Owner      `json:"owner"`
	Operation Operation  `json:"operation"`
	Target    Values     `json:"target"`
	Runtime   RuntimeRef `json:"runtime,omitempty"`
}

func (r ReleaseRequest) validate() error {
	if err := r.Owner.Validate(); err != nil {
		return err
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	return r.Target.validateCapacity(true)
}

// TransferRequest atomically moves committed capacity from one owner to
// another. SourceDecrease is subtracted from Source, while DestinationTarget
// replaces Destination's complete committed target. TransitionReserve holds
// additional capacity only while the external ownership handoff is prepared.
type TransferRequest struct {
	Source            Owner      `json:"source"`
	Destination       Owner      `json:"destination"`
	Operation         Operation  `json:"operation"`
	SourceDecrease    Values     `json:"source_decrease"`
	DestinationTarget Values     `json:"destination_target"`
	TransitionReserve Values     `json:"transition_reserve,omitempty"`
	Runtime           RuntimeRef `json:"runtime"`
}

func (r TransferRequest) validate() error {
	if err := r.Source.Validate(); err != nil {
		return fmt.Errorf("source: %w", err)
	}
	if err := r.Destination.Validate(); err != nil {
		return fmt.Errorf("destination: %w", err)
	}
	if r.Source.TeamID != r.Destination.TeamID {
		return fmt.Errorf("source and destination must belong to the same team")
	}
	if r.Source.Kind == r.Destination.Kind && r.Source.ID == r.Destination.ID {
		return fmt.Errorf("source and destination owners must differ")
	}
	if r.Source.ClusterID != "" && r.Destination.ClusterID != "" &&
		r.Source.ClusterID != r.Destination.ClusterID {
		return fmt.Errorf("source and destination must belong to the same cluster")
	}
	if err := r.Operation.Validate(); err != nil {
		return err
	}
	if err := r.SourceDecrease.validateCapacity(false); err != nil {
		return fmt.Errorf("source_decrease: %w", err)
	}
	if err := r.DestinationTarget.validateCapacity(true); err != nil {
		return fmt.Errorf("destination_target: %w", err)
	}
	if len(r.TransitionReserve) > 0 {
		if err := r.TransitionReserve.validateCapacity(false); err != nil {
			return fmt.Errorf("transition_reserve: %w", err)
		}
	}
	hasRuntime := strings.TrimSpace(r.Runtime.Namespace) != "" ||
		strings.TrimSpace(r.Runtime.Name) != "" ||
		strings.TrimSpace(r.Runtime.UID) != "" ||
		r.Runtime.Generation != 0
	if hasRuntime &&
		(strings.TrimSpace(r.Runtime.Namespace) == "" ||
			strings.TrimSpace(r.Runtime.Name) == "" ||
			strings.TrimSpace(r.Runtime.UID) == "") {
		return fmt.Errorf("runtime namespace, name, and uid must either all be set or all be empty")
	}
	if r.Runtime.Generation < 0 {
		return fmt.Errorf("runtime generation must be non-negative")
	}
	return nil
}

// Reservation is the durable result of an accepted target operation.
type Reservation struct {
	AllocationID string    `json:"allocation_id"`
	Owner        Owner     `json:"owner"`
	Operation    Operation `json:"operation"`
	State        string    `json:"state"`
	Committed    Values    `json:"committed"`
	Target       Values    `json:"target"`
	Reserved     Values    `json:"reserved"`
}

// ExceededError reports the key that rejected an atomic allocation bundle.
type ExceededError struct {
	TeamID    string
	Key       Key
	Limit     int64
	Committed int64
	Reserved  int64
	Requested int64
}

func (e *ExceededError) Error() string {
	if e == nil {
		return "team quota exceeded"
	}
	return fmt.Sprintf(
		"team quota exceeded for %s: committed %d + reserved %d + requested %d exceeds limit %d",
		e.Key, e.Committed, e.Reserved, e.Requested, e.Limit,
	)
}

// IsExceeded reports whether err was caused by a capacity policy decision.
func IsExceeded(err error) bool {
	var exceeded *ExceededError
	return errors.As(err, &exceeded)
}

// ConcurrencyExceededError reports that every live lease slot for a team has
// already been claimed.
type ConcurrencyExceededError struct {
	TeamID string
	Key    Key
	Limit  int64
	Used   int64
}

func (e *ConcurrencyExceededError) Error() string {
	if e == nil {
		return "team concurrency quota exceeded"
	}
	return fmt.Sprintf(
		"team concurrency quota exceeded for %s: used %d reaches limit %d",
		e.Key,
		e.Used,
		e.Limit,
	)
}

// IsConcurrencyExceeded reports whether a live lease admission was denied.
func IsConcurrencyExceeded(err error) bool {
	var exceeded *ConcurrencyExceededError
	return errors.As(err, &exceeded)
}

// RateExceededError reports an immediate token-bucket denial.
type RateExceededError struct {
	TeamID     string
	Key        Key
	Remaining  int64
	RetryAfter time.Duration
}

func (e *RateExceededError) Error() string {
	if e == nil {
		return "team rate quota exceeded"
	}
	return fmt.Sprintf(
		"team rate quota exceeded for %s; retry after %s",
		e.Key,
		e.RetryAfter,
	)
}

// IsRateExceeded reports whether an immediate rate admission was denied.
func IsRateExceeded(err error) bool {
	var exceeded *RateExceededError
	return errors.As(err, &exceeded)
}

// UnavailableError indicates that quota admission could not make a safe
// decision. Resource-increasing callers must fail closed on this error.
type UnavailableError struct {
	Operation string
	Err       error
}

func (e *UnavailableError) Error() string {
	if e == nil {
		return "team quota unavailable"
	}
	if e.Err == nil {
		return fmt.Sprintf("team quota unavailable during %s", e.Operation)
	}
	return fmt.Sprintf("team quota unavailable during %s: %v", e.Operation, e.Err)
}

func (e *UnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// IsUnavailable reports whether a safe quota decision could not be made.
func IsUnavailable(err error) bool {
	var unavailable *UnavailableError
	return errors.As(err, &unavailable)
}

// OperationConflictError reports a non-idempotent concurrent owner mutation.
type OperationConflictError struct {
	Owner       Owner
	OperationID string
}

func (e *OperationConflictError) Error() string {
	if e == nil {
		return "team quota operation conflict"
	}
	return fmt.Sprintf("team quota owner %s/%s already has operation %s in progress", e.Owner.Kind, e.Owner.ID, e.OperationID)
}

// OperationAbortedError makes a repeated aborted operation fail idempotently.
type OperationAbortedError struct {
	OperationID string
}

func (e *OperationAbortedError) Error() string {
	if e == nil {
		return "team quota operation was aborted"
	}
	return fmt.Sprintf("team quota operation %s was already aborted", e.OperationID)
}
