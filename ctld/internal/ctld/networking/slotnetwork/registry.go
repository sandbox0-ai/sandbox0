package slotnetwork

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/watcher"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bbolt "go.etcd.io/bbolt"
	"k8s.io/apimachinery/pkg/types"
)

const (
	registryVersion          = 1
	defaultTerminalRetention = 48 * time.Hour
	defaultMaxRecords        = 100000
	maxRecordBytes           = 256 << 10

	recordStateWarm      = "warm"
	recordStateClaimed   = "claimed"
	recordStateTerminal  = "terminal"
	runtimeSlotNamespace = "sandbox0-nomad-runtime-slots"
	runtimeSlotOwnerKind = "NomadRuntimeSlot"
)

var (
	recordsBucket    = []byte("records-v1")
	operationsBucket = []byte("operations-v1")
	metadataBucket   = []byte("metadata-v1")
	generationKey    = []byte("generation")
)

// Config constrains durable state and namespace inspection for one node-local
// ctld HA pair.
type Config struct {
	StatePath         string
	NetNSRoot         string
	NodeName          string
	TerminalRetention time.Duration
	MaxRecords        int
}

// NamespaceInspector proves an exact namespace incarnation and returns its
// single routable IPv4 address while holding a handle to that namespace.
type NamespaceInspector interface {
	Inspect(path, expectedIdentity string) (string, error)
}

type registryRecord struct {
	Version        int                                            `json:"version"`
	State          string                                         `json:"state"`
	Registration   protocol.RuntimeSlotNetworkRegistrationRequest `json:"registration"`
	Prepare        *protocol.NodeNetworkPrepareControlRequest     `json:"prepare,omitempty"`
	PodSandboxID   string                                         `json:"pod_sandbox_id"`
	PodIP          string                                         `json:"pod_ip"`
	NetworkEpoch   int64                                          `json:"network_epoch"`
	CtldGeneration string                                         `json:"ctld_generation"`
	SandboxID      string                                         `json:"sandbox_id,omitempty"`
	TeamID         string                                         `json:"team_id,omitempty"`
	Cleanup        *protocol.NodeCleanupControlRequest            `json:"cleanup,omitempty"`
	CreatedAt      string                                         `json:"created_at"`
	UpdatedAt      string                                         `json:"updated_at"`
	TerminalAt     string                                         `json:"terminal_at,omitempty"`
}

type registryEntry struct {
	record   registryRecord
	revision uint64
}

// Stats is a lock-consistent view of bounded registry cardinality and apply
// progress for readiness and operational telemetry.
type Stats struct {
	Warm            int
	Claimed         int
	Terminal        int
	Revision        uint64
	AppliedRevision uint64
}

// Registry is ctld's durable desired-state authority for Nomad runtime-slot
// policies. Its acknowledgement only advances after the caller has reconciled
// policy compilation and node redirect state for the returned snapshot.
type Registry struct {
	db         *bbolt.DB
	inspector  NamespaceInspector
	config     Config
	generation string

	mu              sync.Mutex
	entries         map[string]registryEntry
	operationSlots  map[string]string
	revision        uint64
	appliedRevision uint64
	appliedChanged  chan struct{}
	notify          func()
	closed          bool
}

// NewRegistry opens the shared-host journal used by both ctld HA slots.
func NewRegistry(config Config, inspector NamespaceInspector) (*Registry, error) {
	statePath := filepath.Clean(strings.TrimSpace(config.StatePath))
	if !filepath.IsAbs(statePath) || statePath == string(filepath.Separator) || statePath != strings.TrimSpace(config.StatePath) {
		return nil, fmt.Errorf("runtime slot network state path must be canonical, absolute, and non-root: %w", errdefs.ErrInvalidArgument)
	}
	netnsRoot := filepath.Clean(strings.TrimSpace(config.NetNSRoot))
	if !filepath.IsAbs(netnsRoot) || netnsRoot == string(filepath.Separator) || netnsRoot != strings.TrimSpace(config.NetNSRoot) {
		return nil, fmt.Errorf("runtime slot network namespace root must be canonical, absolute, and non-root: %w", errdefs.ErrInvalidArgument)
	}
	resolvedRoot, err := filepath.EvalSymlinks(netnsRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime slot network namespace root: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if resolvedRoot != netnsRoot {
		return nil, fmt.Errorf("runtime slot network namespace root must not traverse symlinks: %w", errdefs.ErrInvalidArgument)
	}
	netnsRootInfo, err := os.Lstat(netnsRoot)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime slot network namespace root: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if !netnsRootInfo.IsDir() || netnsRootInfo.Mode().Perm()&0o022 != 0 ||
		!pathOwnedByRoot(netnsRootInfo) {
		return nil, fmt.Errorf("runtime slot network namespace root must be root-owned and not writable by group or other: %w", errdefs.ErrPermissionDenied)
	}
	config.StatePath = statePath
	config.NetNSRoot = netnsRoot
	config.NodeName = strings.TrimSpace(config.NodeName)
	if config.NodeName == "" || len(config.NodeName) > 512 {
		return nil, fmt.Errorf("runtime slot network node name is required and at most 512 bytes: %w", errdefs.ErrInvalidArgument)
	}
	if config.TerminalRetention <= 0 {
		config.TerminalRetention = defaultTerminalRetention
	}
	if config.MaxRecords <= 0 {
		config.MaxRecords = defaultMaxRecords
	}
	if err := os.MkdirAll(filepath.Dir(statePath), 0o750); err != nil {
		return nil, fmt.Errorf("create runtime slot network state directory: %w", err)
	}
	stateDirectory := filepath.Dir(statePath)
	resolvedStateDirectory, err := filepath.EvalSymlinks(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime slot network state directory: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if resolvedStateDirectory != stateDirectory {
		return nil, fmt.Errorf("runtime slot network state directory must not traverse symlinks: %w", errdefs.ErrPermissionDenied)
	}
	stateDirectoryInfo, err := os.Lstat(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime slot network state directory: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	if !stateDirectoryInfo.IsDir() || stateDirectoryInfo.Mode().Perm()&0o022 != 0 ||
		!pathOwnedByRoot(stateDirectoryInfo) {
		return nil, fmt.Errorf("runtime slot network state directory must be root-owned and not writable by group or other: %w", errdefs.ErrPermissionDenied)
	}
	if info, err := os.Lstat(statePath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !pathOwnedByRoot(info) {
			return nil, fmt.Errorf("runtime slot network state must be a root-owned mode-0600 regular file: %w", errdefs.ErrPermissionDenied)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect runtime slot network state: %w", err)
	}
	db, err := bbolt.Open(statePath, 0o600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open runtime slot network state: %w", err)
	}
	if err := os.Chmod(statePath, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("secure runtime slot network state: %w", err)
	}
	registry := &Registry{
		db: db, inspector: inspector, config: config,
		entries: make(map[string]registryEntry), operationSlots: make(map[string]string),
		revision: 1, appliedChanged: make(chan struct{}),
	}
	if registry.inspector == nil {
		registry.inspector = newNamespaceInspector(netnsRoot)
	}
	if err := registry.initialize(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return registry, nil
}

func (r *Registry) initialize() error {
	return r.db.Update(func(tx *bbolt.Tx) error {
		records, err := tx.CreateBucketIfNotExists(recordsBucket)
		if err != nil {
			return err
		}
		operations, err := tx.CreateBucketIfNotExists(operationsBucket)
		if err != nil {
			return err
		}
		metadata, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		generation := metadata.Get(generationKey)
		if len(generation) == 0 {
			generated, err := newGeneration()
			if err != nil {
				return err
			}
			if err := metadata.Put(generationKey, []byte(generated)); err != nil {
				return err
			}
			r.generation = generated
		} else {
			r.generation = string(generation)
		}
		if strings.TrimSpace(r.generation) != r.generation || r.generation == "" || len(r.generation) > 128 {
			return fmt.Errorf("runtime slot network generation is corrupt")
		}
		count := 0
		if err := records.ForEach(func(key, payload []byte) error {
			if payload == nil || len(key) == 0 || len(payload) > maxRecordBytes {
				return fmt.Errorf("runtime slot network record is corrupt")
			}
			record, err := decodeRecord(payload)
			if err != nil {
				return fmt.Errorf("decode runtime slot network record %q: %w", key, err)
			}
			if string(key) != record.Registration.SlotID {
				return fmt.Errorf("runtime slot network record key does not match slot")
			}
			if record.CtldGeneration != r.generation {
				return fmt.Errorf("runtime slot network record belongs to another journal generation")
			}
			if record.Prepare != nil {
				operationSlot := operations.Get([]byte(record.Prepare.OperationID))
				if string(operationSlot) != record.Registration.SlotID {
					return fmt.Errorf("runtime slot network operation index is corrupt")
				}
				r.operationSlots[record.Prepare.OperationID] = record.Registration.SlotID
			}
			r.entries[record.Registration.SlotID] = registryEntry{record: record, revision: r.revision}
			count++
			return nil
		}); err != nil {
			return err
		}
		if count > r.config.MaxRecords {
			return fmt.Errorf("runtime slot network state exceeds configured record limit")
		}
		return operations.ForEach(func(operation, slot []byte) error {
			entry, ok := r.entries[string(slot)]
			if !ok || entry.record.Prepare == nil || entry.record.Prepare.OperationID != string(operation) {
				return fmt.Errorf("runtime slot network operation index has an orphan")
			}
			return nil
		})
	})
}

// SetNotify installs a coalescing synchronization trigger. The caller should
// invoke Snapshot and Acknowledge from its normal node redirect loop.
func (r *Registry) SetNotify(notify func()) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.notify = notify
	closed := r.closed
	r.mu.Unlock()
	if notify != nil && !closed {
		notify()
	}
}

// Register durably enrolls one exact warm slot under default deny and waits
// until the normal ctld policy and redirect loop has applied it.
func (r *Registry) Register(
	ctx context.Context,
	request protocol.RuntimeSlotNetworkRegistrationRequest,
) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate runtime slot network registration: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if r == nil || r.db == nil {
		return fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	if revision, found, err := r.existingRegistration(request); found || err != nil {
		if err != nil {
			return err
		}
		return r.waitForRegistration(ctx, request, revision)
	}
	podIP, err := r.inspectRegistration(request)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	if entry, ok := r.entries[request.SlotID]; ok {
		revision, err := matchRegistration(entry, request)
		r.mu.Unlock()
		if err != nil {
			return err
		}
		return r.waitForRegistration(ctx, request, revision)
	}
	if len(r.entries) >= r.config.MaxRecords {
		r.mu.Unlock()
		return fmt.Errorf("runtime slot network registry is full: %w", errdefs.ErrResourceExhausted)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var record registryRecord
	err = r.db.Update(func(tx *bbolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		if records == nil {
			return fmt.Errorf("runtime slot network records bucket is absent")
		}
		if records.Get([]byte(request.SlotID)) != nil {
			return fmt.Errorf("runtime slot network record appeared concurrently: %w", errdefs.ErrAlreadyExists)
		}
		epoch, err := records.NextSequence()
		if err != nil || epoch == 0 || epoch > math.MaxInt64 {
			return fmt.Errorf("allocate runtime slot network epoch: %w", err)
		}
		record = registryRecord{
			Version: registryVersion, State: recordStateWarm, Registration: request,
			PodSandboxID: request.IncarnationID(), PodIP: podIP, NetworkEpoch: int64(epoch),
			CtldGeneration: r.generation, CreatedAt: now, UpdatedAt: now,
		}
		if err := validateRecord(record); err != nil {
			return err
		}
		return putRecord(records, record)
	})
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.revision++
	revision := r.revision
	r.entries[request.SlotID] = registryEntry{record: record, revision: revision}
	notify := r.notify
	r.mu.Unlock()
	if notify != nil {
		notify()
	}
	return r.waitForRegistration(ctx, request, revision)
}

func (r *Registry) existingRegistration(
	request protocol.RuntimeSlotNetworkRegistrationRequest,
) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, true, fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	entry, ok := r.entries[request.SlotID]
	if !ok {
		return 0, false, nil
	}
	revision, err := matchRegistration(entry, request)
	return revision, true, err
}

func matchRegistration(entry registryEntry, request protocol.RuntimeSlotNetworkRegistrationRequest) (uint64, error) {
	if entry.record.Registration != request {
		return 0, fmt.Errorf("runtime slot network registration changed: %w", errdefs.ErrAlreadyExists)
	}
	if entry.record.State == recordStateTerminal {
		return 0, fmt.Errorf("runtime slot network registration is terminal: %w", errdefs.ErrFailedPrecondition)
	}
	return entry.revision, nil
}

func (r *Registry) waitForRegistration(
	ctx context.Context,
	request protocol.RuntimeSlotNetworkRegistrationRequest,
	revision uint64,
) error {
	if err := r.waitApplied(ctx, revision); err != nil {
		return err
	}
	r.mu.Lock()
	entry, ok := r.entries[request.SlotID]
	if !ok {
		r.mu.Unlock()
		return fmt.Errorf("runtime slot network registration disappeared: %w", errdefs.ErrUnavailable)
	}
	_, err := matchRegistration(entry, request)
	r.mu.Unlock()
	return err
}

// Prepare durably transitions one registered warm slot to its exact claimed
// policy and waits for a successful redirect synchronization that contains it.
func (r *Registry) Prepare(
	ctx context.Context,
	request protocol.RuntimeSlotNetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	policySpec, err := validatePolicy(request)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if r == nil || r.db == nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	if token, revision, ready, err := r.prepareState(request); ready || err != nil {
		if err != nil {
			return rootfshandoff.NetworkPolicyToken{}, err
		}
		return r.waitForPrepare(ctx, request, token, revision)
	}
	podIP, err := r.inspector.Inspect(
		filepath.Join(r.config.NetNSRoot, request.NetNSRelativePath),
		request.Request.NetNSIdentity,
	)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("inspect runtime slot network namespace: %w", err)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	entry, ok := r.entries[request.Request.SlotID]
	if !ok {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot has no warm network registration: %w", errdefs.ErrFailedPrecondition)
	}
	if entry.record.State == recordStateClaimed {
		token, revision, err := matchPrepare(entry, request)
		r.mu.Unlock()
		if err != nil {
			return rootfshandoff.NetworkPolicyToken{}, err
		}
		return r.waitForPrepare(ctx, request, token, revision)
	}
	if err := matchWarmPrepare(entry.record, request, podIP); err != nil {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if slot := r.operationSlots[request.Request.OperationID]; slot != "" && slot != request.Request.SlotID {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot network operation is bound to slot %q: %w", slot, errdefs.ErrAlreadyExists)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	record := entry.record
	prepare := request.Request
	record.State = recordStateClaimed
	record.Prepare = &prepare
	record.SandboxID = policySpec.SandboxID
	record.TeamID = policySpec.TeamID
	record.UpdatedAt = now
	err = r.db.Update(func(tx *bbolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		operations := tx.Bucket(operationsBucket)
		if records == nil || operations == nil {
			return fmt.Errorf("runtime slot network buckets are absent")
		}
		if slot := operations.Get([]byte(request.Request.OperationID)); slot != nil && string(slot) != request.Request.SlotID {
			return fmt.Errorf("runtime slot network operation is bound to slot %q: %w", slot, errdefs.ErrAlreadyExists)
		}
		if err := validateRecord(record); err != nil {
			return err
		}
		if err := putRecord(records, record); err != nil {
			return err
		}
		return operations.Put([]byte(request.Request.OperationID), []byte(request.Request.SlotID))
	})
	if err != nil {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	r.revision++
	revision := r.revision
	r.entries[record.Registration.SlotID] = registryEntry{record: record, revision: revision}
	r.operationSlots[request.Request.OperationID] = request.Request.SlotID
	notify := r.notify
	token := policyToken(record)
	r.mu.Unlock()
	if notify != nil {
		notify()
	}
	return r.waitForPrepare(ctx, request, token, revision)
}

func (r *Registry) prepareState(
	request protocol.RuntimeSlotNetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return rootfshandoff.NetworkPolicyToken{}, 0, true, fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	if slot := r.operationSlots[request.Request.OperationID]; slot != "" && slot != request.Request.SlotID {
		return rootfshandoff.NetworkPolicyToken{}, 0, true, fmt.Errorf("runtime slot network operation is bound to slot %q: %w", slot, errdefs.ErrAlreadyExists)
	}
	entry, ok := r.entries[request.Request.SlotID]
	if !ok {
		return rootfshandoff.NetworkPolicyToken{}, 0, true, fmt.Errorf("runtime slot has no warm network registration: %w", errdefs.ErrFailedPrecondition)
	}
	switch entry.record.State {
	case recordStateClaimed:
		token, revision, err := matchPrepare(entry, request)
		return token, revision, true, err
	case recordStateWarm:
		if !entry.record.Registration.MatchesPrepare(request.Request) ||
			entry.record.Registration.NetNSRelativePath != request.NetNSRelativePath {
			return rootfshandoff.NetworkPolicyToken{}, 0, true, fmt.Errorf("runtime slot network claim belongs to another registration: %w", errdefs.ErrFailedPrecondition)
		}
		return rootfshandoff.NetworkPolicyToken{}, 0, false, nil
	default:
		return rootfshandoff.NetworkPolicyToken{}, 0, true, fmt.Errorf("runtime slot network policy is terminal: %w", errdefs.ErrFailedPrecondition)
	}
}

func matchWarmPrepare(record registryRecord, request protocol.RuntimeSlotNetworkPrepareRequest, podIP string) error {
	if record.State != recordStateWarm || !record.Registration.MatchesPrepare(request.Request) ||
		record.Registration.NetNSRelativePath != request.NetNSRelativePath {
		return fmt.Errorf("runtime slot network claim belongs to another registration: %w", errdefs.ErrFailedPrecondition)
	}
	if record.PodIP != podIP {
		return fmt.Errorf("runtime slot network source IP changed before claim: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func matchPrepare(entry registryEntry, request protocol.RuntimeSlotNetworkPrepareRequest) (rootfshandoff.NetworkPolicyToken, uint64, error) {
	if entry.record.State != recordStateClaimed || entry.record.Prepare == nil {
		return rootfshandoff.NetworkPolicyToken{}, 0, fmt.Errorf("runtime slot network policy is not claimed: %w", errdefs.ErrFailedPrecondition)
	}
	if *entry.record.Prepare != request.Request || entry.record.Registration.NetNSRelativePath != request.NetNSRelativePath {
		return rootfshandoff.NetworkPolicyToken{}, 0, fmt.Errorf("runtime slot network policy is bound to another request: %w", errdefs.ErrAlreadyExists)
	}
	return policyToken(entry.record), entry.revision, nil
}

func (r *Registry) waitForPrepare(
	ctx context.Context,
	request protocol.RuntimeSlotNetworkPrepareRequest,
	token rootfshandoff.NetworkPolicyToken,
	revision uint64,
) (rootfshandoff.NetworkPolicyToken, error) {
	if err := r.waitApplied(ctx, revision); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	r.mu.Lock()
	entry, ok := r.entries[request.Request.SlotID]
	if !ok {
		r.mu.Unlock()
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot network policy disappeared: %w", errdefs.ErrUnavailable)
	}
	matched, _, err := matchPrepare(entry, request)
	r.mu.Unlock()
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if matched != token {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("runtime slot network token changed: %w", errdefs.ErrUnavailable)
	}
	return token, nil
}

// Cleanup durably removes one exact warm or claimed record from the desired
// set and waits until the node redirect loop acknowledges that absence.
func (r *Registry) Cleanup(ctx context.Context, request protocol.NodeCleanupControlRequest) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("validate runtime slot network cleanup: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if r == nil || r.db == nil {
		return fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	entry, ok := r.entries[request.SlotID]
	if !ok {
		revision := r.revision
		r.mu.Unlock()
		if request.WriterGrantID != "" {
			return fmt.Errorf("claimed runtime slot network record is absent: %w", errdefs.ErrFailedPrecondition)
		}
		return r.waitApplied(ctx, revision)
	}
	if err := recordMatchesCleanup(entry.record, request); err != nil {
		r.mu.Unlock()
		return err
	}
	if entry.record.State == recordStateTerminal {
		revision := entry.revision
		r.mu.Unlock()
		return r.waitApplied(ctx, revision)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	clone := request
	record := entry.record
	record.State = recordStateTerminal
	record.Cleanup = &clone
	record.UpdatedAt = now
	record.TerminalAt = now
	err := r.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(recordsBucket)
		if bucket == nil {
			return fmt.Errorf("runtime slot network records bucket is absent")
		}
		if err := validateRecord(record); err != nil {
			return err
		}
		return putRecord(bucket, record)
	})
	if err != nil {
		r.mu.Unlock()
		return err
	}
	r.revision++
	revision := r.revision
	r.entries[record.Registration.SlotID] = registryEntry{record: record, revision: revision}
	notify := r.notify
	r.mu.Unlock()
	if notify != nil {
		notify()
	}
	return r.waitApplied(ctx, revision)
}

func recordMatchesCleanup(record registryRecord, request protocol.NodeCleanupControlRequest) error {
	registration := record.Registration
	if registration.SlotID != request.SlotID || registration.ClusterID != request.ClusterID ||
		registration.AllocationID != request.AllocationID || registration.NodeID != request.NodeID ||
		registration.NodeUID != request.NodeUID || registration.NodeBootID != request.NodeBootID ||
		registration.NetNSIdentity != request.NetNSIdentity {
		return fmt.Errorf("runtime slot network cleanup belongs to another incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	if record.Cleanup != nil && *record.Cleanup != request {
		return fmt.Errorf("runtime slot network policy is bound to another cleanup: %w", errdefs.ErrAlreadyExists)
	}
	return nil
}

// Snapshot returns the exact warm and claimed desired set and the revision that
// an external redirect reconciliation may acknowledge.
func (r *Registry) Snapshot() ([]*watcher.SandboxInfo, uint64, error) {
	if r == nil {
		return nil, 0, fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, 0, fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	keys := make([]string, 0, len(r.entries))
	for key, entry := range r.entries {
		if entry.record.State == recordStateWarm || entry.record.State == recordStateClaimed {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	sandboxes := make([]*watcher.SandboxInfo, 0, len(keys))
	for _, key := range keys {
		record := r.entries[key].record
		raw, digest, sandboxID, teamID := recordPolicy(record)
		sandboxes = append(sandboxes, &watcher.SandboxInfo{
			Namespace: runtimeSlotNamespace, Name: record.Registration.SlotID,
			UID: types.UID(record.PodSandboxID), ResourceVersion: fmt.Sprintf("%d", record.NetworkEpoch),
			PodIP: record.PodIP, NodeName: r.config.NodeName,
			SandboxID: sandboxID, TeamID: teamID, OwnerKind: runtimeSlotOwnerKind,
			NetworkPolicy: raw, NetworkPolicyHash: digest,
		})
	}
	return sandboxes, r.revision, nil
}

// Stats returns registry cardinality and apply lag without exposing records.
func (r *Registry) Stats() Stats {
	if r == nil {
		return Stats{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stats := Stats{Revision: r.revision, AppliedRevision: r.appliedRevision}
	for _, entry := range r.entries {
		switch entry.record.State {
		case recordStateWarm:
			stats.Warm++
		case recordStateClaimed:
			stats.Claimed++
		case recordStateTerminal:
			stats.Terminal++
		}
	}
	return stats
}

// Acknowledge advances only to a revision returned by Snapshot after all node
// policy and redirect application for that snapshot succeeded.
func (r *Registry) Acknowledge(revision uint64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if !r.closed && revision > r.appliedRevision && revision <= r.revision {
		r.appliedRevision = revision
		close(r.appliedChanged)
		r.appliedChanged = make(chan struct{})
	}
	r.mu.Unlock()
}

func (r *Registry) waitApplied(ctx context.Context, revision uint64) error {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
		}
		if r.appliedRevision >= revision {
			r.mu.Unlock()
			return nil
		}
		changed := r.appliedChanged
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Prune deletes terminal replay records only after the configured retention.
func (r *Registry) Prune(now time.Time) (int, error) {
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	cutoff := now.UTC().Add(-r.config.TerminalRetention)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	keys := make([]string, 0)
	for key, entry := range r.entries {
		if entry.record.State != recordStateTerminal || entry.record.TerminalAt == "" {
			continue
		}
		terminalAt, err := time.Parse(time.RFC3339Nano, entry.record.TerminalAt)
		if err != nil {
			return 0, fmt.Errorf("parse runtime slot network terminal time: %w", err)
		}
		if !terminalAt.After(cutoff) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return 0, nil
	}
	err := r.db.Update(func(tx *bbolt.Tx) error {
		records := tx.Bucket(recordsBucket)
		operations := tx.Bucket(operationsBucket)
		if records == nil || operations == nil {
			return fmt.Errorf("runtime slot network buckets are absent")
		}
		for _, key := range keys {
			entry := r.entries[key]
			if err := records.Delete([]byte(key)); err != nil {
				return err
			}
			if entry.record.Prepare != nil {
				if err := operations.Delete([]byte(entry.record.Prepare.OperationID)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		entry := r.entries[key]
		if entry.record.Prepare != nil {
			delete(r.operationSlots, entry.record.Prepare.OperationID)
		}
		delete(r.entries, key)
	}
	return len(keys), nil
}

// Ping checks the durable journal and expected buckets.
func (r *Registry) Ping() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("runtime slot network registry is unavailable: %w", errdefs.ErrUnavailable)
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return fmt.Errorf("runtime slot network registry is closed: %w", errdefs.ErrUnavailable)
	}
	return r.db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket(recordsBucket) == nil || tx.Bucket(operationsBucket) == nil || tx.Bucket(metadataBucket) == nil {
			return fmt.Errorf("runtime slot network buckets are absent: %w", errdefs.ErrUnavailable)
		}
		return nil
	})
}

// Close wakes every waiter before closing the durable journal.
func (r *Registry) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.appliedChanged)
	r.mu.Unlock()
	return r.db.Close()
}

func (r *Registry) inspectRegistration(request protocol.RuntimeSlotNetworkRegistrationRequest) (string, error) {
	podIP, err := r.inspector.Inspect(
		filepath.Join(r.config.NetNSRoot, request.NetNSRelativePath),
		request.NetNSIdentity,
	)
	if err != nil {
		return "", fmt.Errorf("inspect runtime slot network namespace: %w", err)
	}
	parsed := net.ParseIP(podIP)
	if parsed == nil || parsed.To4() == nil || !parsed.IsGlobalUnicast() || parsed.String() != podIP {
		return "", fmt.Errorf("runtime slot network inspector returned a non-canonical routable IPv4 address: %w", errdefs.ErrFailedPrecondition)
	}
	return podIP, nil
}

func validatePolicy(request protocol.RuntimeSlotNetworkPrepareRequest) (*v1alpha1.NetworkPolicySpec, error) {
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("validate runtime slot network prepare: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if request.Request.NetworkPolicy == "" {
		return nil, fmt.Errorf("claimed runtime slot network policy must carry v1 sandbox and team identity: %w", errdefs.ErrInvalidArgument)
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotationStrict(request.Request.NetworkPolicy)
	if err != nil {
		return nil, fmt.Errorf("decode runtime slot network policy: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if spec == nil || spec.Version != "v1" {
		return nil, fmt.Errorf("runtime slot network policy version must be v1: %w", errdefs.ErrInvalidArgument)
	}
	for name, value := range map[string]string{"sandbox_id": spec.SandboxID, "team_id": spec.TeamID} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return nil, fmt.Errorf("runtime slot network policy %s is required, canonical, and at most 512 bytes: %w", name, errdefs.ErrInvalidArgument)
		}
	}
	if spec.Mode != v1alpha1.NetworkModeAllowAll && spec.Mode != v1alpha1.NetworkModeBlockAll {
		return nil, fmt.Errorf("runtime slot network policy mode is unsupported: %w", errdefs.ErrInvalidArgument)
	}
	if _, err := policy.CompileNetworkPolicy(spec); err != nil {
		return nil, fmt.Errorf("compile runtime slot network policy: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return spec, nil
}

func validateRecord(record registryRecord) error {
	if record.Version != registryVersion ||
		(record.State != recordStateWarm && record.State != recordStateClaimed && record.State != recordStateTerminal) {
		return fmt.Errorf("runtime slot network record version or state is invalid")
	}
	if err := record.Registration.Validate(); err != nil {
		return err
	}
	parsed := net.ParseIP(record.PodIP)
	if parsed == nil || parsed.To4() == nil || !parsed.IsGlobalUnicast() || parsed.String() != record.PodIP ||
		record.NetworkEpoch <= 0 || record.PodSandboxID != record.Registration.IncarnationID() ||
		strings.TrimSpace(record.CtldGeneration) != record.CtldGeneration || record.CtldGeneration == "" ||
		len(record.CtldGeneration) > 128 {
		return fmt.Errorf("runtime slot network physical identity is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, record.CreatedAt); err != nil {
		return fmt.Errorf("runtime slot network created_at is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, record.UpdatedAt); err != nil {
		return fmt.Errorf("runtime slot network updated_at is invalid")
	}
	if record.Prepare == nil {
		if record.State == recordStateClaimed || record.SandboxID != "" || record.TeamID != "" {
			return fmt.Errorf("runtime slot network record lacks its claimed policy")
		}
	} else {
		local := protocol.RuntimeSlotNetworkPrepareRequest{
			Request: *record.Prepare, NetNSRelativePath: record.Registration.NetNSRelativePath,
		}
		spec, err := validatePolicy(local)
		if err != nil {
			return err
		}
		if !record.Registration.MatchesPrepare(*record.Prepare) ||
			record.SandboxID != spec.SandboxID || record.TeamID != spec.TeamID {
			return fmt.Errorf("runtime slot network claimed policy does not match its registration or logical identity")
		}
	}
	if record.State == recordStateWarm || record.State == recordStateClaimed {
		if record.Cleanup != nil || record.TerminalAt != "" {
			return fmt.Errorf("active runtime slot network record contains terminal state")
		}
		return nil
	}
	if record.Cleanup == nil || record.TerminalAt == "" {
		return fmt.Errorf("terminal runtime slot network record is incomplete")
	}
	if err := record.Cleanup.Validate(); err != nil {
		return err
	}
	if err := recordMatchesCleanup(record, *record.Cleanup); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, record.TerminalAt); err != nil {
		return fmt.Errorf("runtime slot network terminal_at is invalid")
	}
	return nil
}

func decodeRecord(payload []byte) (registryRecord, error) {
	var record registryRecord
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return registryRecord{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return registryRecord{}, fmt.Errorf("record contains trailing data")
	}
	if err := validateRecord(record); err != nil {
		return registryRecord{}, err
	}
	return record, nil
}

func putRecord(bucket *bbolt.Bucket, record registryRecord) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode runtime slot network record: %w", err)
	}
	if len(payload) > maxRecordBytes {
		return fmt.Errorf("runtime slot network record exceeds %d bytes", maxRecordBytes)
	}
	return bucket.Put([]byte(record.Registration.SlotID), payload)
}

func policyToken(record registryRecord) rootfshandoff.NetworkPolicyToken {
	if record.Prepare == nil {
		return rootfshandoff.NetworkPolicyToken{}
	}
	return rootfshandoff.NetworkPolicyToken{
		PodUID: record.Registration.AllocationID, PodSandboxID: record.PodSandboxID,
		ClaimID: record.Prepare.ClaimID, NetworkEpoch: record.NetworkEpoch,
		PolicyDigest: record.Prepare.PolicyDigest, PodIP: record.PodIP,
		CtldGeneration: record.CtldGeneration, NetNSIdentity: record.Registration.NetNSIdentity,
	}
}

func recordPolicy(record registryRecord) (raw, digest, sandboxID, teamID string) {
	if record.State == recordStateClaimed && record.Prepare != nil {
		return record.Prepare.NetworkPolicy, record.Prepare.PolicyDigest, record.SandboxID, record.TeamID
	}
	raw = protocol.RuntimeSlotWarmNetworkPolicy
	return raw, protocol.NetworkPolicyDigest(raw), protocol.RuntimeSlotWarmSandboxID, protocol.RuntimeSlotWarmTeamID
}

func newGeneration() (string, error) {
	payload := make([]byte, 16)
	if _, err := rand.Read(payload); err != nil {
		return "", fmt.Errorf("generate runtime slot network generation: %w", err)
	}
	return "ctld-network-v1:" + hex.EncodeToString(payload), nil
}
