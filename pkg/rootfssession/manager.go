package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	bolt "go.etcd.io/bbolt"
)

const (
	legacySessionSchemaVersion = 2
	sessionSchemaVersion       = 3
	stateReserved              = "reserved"
	stateDeviceReserved        = "device_reserved"
	stateDeviceReady           = "device_ready"
	stateXFSMounted            = "xfs_mounted"
	stateReady                 = "ready"
	stateRetireRequested       = "retire_requested"
	stateReleasing             = "releasing"
	stateTombstoned            = "tombstoned"
	stateFailed                = "failed"
)

var (
	sessionBucket         = []byte("rootfs-sessions-v1")
	sessionIdentityBucket = []byte("rootfs-session-identities-v1")
)

// Device is one live kernel block-device attachment. Close must not return
// until the kernel transport can no longer issue requests to the branch.
type Device interface {
	Path() string
	Close() error
}

// HostRuntime owns the Linux device and mount side effects. Every method must
// be idempotent only for the exact paths supplied by one durable session.
type HostRuntime interface {
	ReserveDevice(allocationID string) (string, error)
	AdoptDeviceReservation(devicePath, allocationID string) error
	ReleaseDeviceReservation(devicePath, allocationID string)
	AttachDevice(lifetime, readyContext context.Context, devicePath, allocationID string, backend rootfsblock.WritableBlockDevice) (Device, error)
	RecoverOrphanDevice(ctx context.Context, devicePath, allocationID string) error
	MountXFS(devicePath, target string) error
	MountOverlay(xfsRoot, mergedRoot string) error
	UnmountOverlay(mergedRoot string, requireSync bool) error
	UnmountXFS(xfsRoot string, requireSync bool) error
}

// CrashFenceHostObservation is a fail-closed host inspection of the exact
// paths and block device recorded by one durable physical session.
type CrashFenceHostObservation struct {
	NBDPID            int
	NBDHolders        []string
	NBDPoolAbsent     bool
	MergedMountAbsent bool
	XFSMountAbsent    bool
}

// CrashFenceHostInspector is deliberately separate from HostRuntime so test
// and non-Linux runtimes cannot accidentally claim fencing support. Production
// crash abandonment is unavailable unless the runtime implements it.
type CrashFenceHostInspector interface {
	InspectCrashFence(devicePath, xfsRoot, mergedRoot string) (CrashFenceHostObservation, error)
}

// CrashFenceUnattachedHostInspector handles legacy journal records written
// before exact device allocation was made durable ahead of attach. It must
// inspect the complete configured pool, not infer absence from an empty path.
type CrashFenceUnattachedHostInspector interface {
	InspectUnattachedCrashFence(xfsRoot, mergedRoot string) (CrashFenceHostObservation, error)
}

// CrashFencePreAttachmentHostInspector verifies the deterministic mount roots
// for a current-schema record whose device reservation was never persisted.
// The durable ordering guarantees that no NBD attach could have started.
type CrashFencePreAttachmentHostInspector interface {
	InspectPreAttachmentCrashFence(xfsRoot, mergedRoot string) (CrashFenceHostObservation, error)
}

// Config defines the host-only state roots used for writable RootFS sessions.
// StatePath and BranchRoot must survive process restarts; MountRoot is boot
// local and must be in the same mount namespace as containerd.
type Config struct {
	StatePath  string
	BranchRoot string
	MountRoot  string
	Source     rootfsblock.RangeSource
	Publisher  rootfsblock.ImmutableObjectPublisher
	Runtime    HostRuntime
}

// Mount is the storage-owned merged root exported to the Snapshotter's stable
// open_tree/move_mount capture. It is not directly tenant reachable.
type Mount struct {
	Source  string
	Type    string
	Options []string
}

type record struct {
	Version                   int               `json:"version"`
	Parent                    string            `json:"parent"`
	BindingDigest             string            `json:"binding_digest"`
	RootFSID                  string            `json:"rootfs_id"`
	WriterEpoch               int64             `json:"writer_epoch"`
	GenerationID              string            `json:"generation_id"`
	BaseDescriptor            []byte            `json:"base_descriptor,omitempty"`
	BranchPath                string            `json:"branch_path"`
	DevicePath                string            `json:"device_path,omitempty"`
	DeviceAllocationID        string            `json:"device_allocation_id,omitempty"`
	DeviceReservationReleased bool              `json:"device_reservation_released,omitempty"`
	XFSRoot                   string            `json:"xfs_root"`
	MergedRoot                string            `json:"merged_root"`
	State                     string            `json:"state"`
	RetireOperationID         string            `json:"retire_operation_id,omitempty"`
	SealedDescriptor          []byte            `json:"sealed_descriptor,omitempty"`
	SealedBlockHead           string            `json:"sealed_block_head,omitempty"`
	SealedDurability          string            `json:"sealed_durability,omitempty"`
	DetachProof               string            `json:"detach_proof,omitempty"`
	CrashFence                *crashFenceRecord `json:"crash_fence,omitempty"`
	Failure                   string            `json:"failure,omitempty"`
	CreatedAt                 string            `json:"created_at"`
	UpdatedAt                 string            `json:"updated_at"`
}

type crashFenceRecord struct {
	OperationID string                                      `json:"operation_id"`
	RequestedAt string                                      `json:"requested_at"`
	Result      *rootfshandoff.CrashFenceSessionObservation `json:"result,omitempty"`
}

type liveSession struct {
	branch *rootfsblock.Branch
	device Device
}

// RetireResult is the immutable local result produced only after the runtime
// mount, NBD endpoint, and branch writer have all been revoked.
type RetireResult struct {
	Parent           string
	RootFSID         string
	WriterEpoch      int64
	OperationID      string
	CurrentBlockHead string
	DurabilityState  string
	Descriptor       []byte
	DetachProof      []byte
}

// Manager is the single physical owner of a D generation on a node. The
// regional Stage request remains in the Snapshotter journal; this database
// records only device and mount side effects needed for crash-safe cleanup.
type Manager struct {
	db         *bolt.DB
	branchRoot string
	mountRoot  string
	source     rootfsblock.RangeSource
	publisher  rootfsblock.ImmutableObjectPublisher
	readCache  *rootfsblock.ReadCache
	runtime    HostRuntime
	mu         sync.Mutex
	live       map[string]*liveSession
	locks      sync.Map
	lifetime   context.Context
	cancel     context.CancelFunc
}

func New(config Config) (*Manager, error) {
	if config.Source == nil || config.Publisher == nil || config.Runtime == nil {
		return nil, fmt.Errorf("range source, immutable publisher, and host runtime are required")
	}
	statePath, err := privatePath(config.StatePath, false)
	if err != nil {
		return nil, fmt.Errorf("state path: %w", err)
	}
	branchRoot, err := privatePath(config.BranchRoot, true)
	if err != nil {
		return nil, fmt.Errorf("branch root: %w", err)
	}
	mountRoot, err := privatePath(config.MountRoot, true)
	if err != nil {
		return nil, fmt.Errorf("mount root: %w", err)
	}
	db, err := bolt.Open(statePath, 0o600, &bolt.Options{Timeout: time.Second, NoFreelistSync: false})
	if err != nil {
		return nil, fmt.Errorf("open RootFS session journal: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(sessionBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(sessionIdentityBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize RootFS session journal: %w", err)
	}
	readCache, err := rootfsblock.NewReadCache(rootfsblock.DefaultReadCacheBytes)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize RootFS read cache: %w", err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &Manager{
		db: db, branchRoot: branchRoot, mountRoot: mountRoot,
		source: config.Source, publisher: config.Publisher, readCache: readCache, runtime: config.Runtime, live: make(map[string]*liveSession),
		lifetime: lifetime, cancel: cancel,
	}, nil
}

// ReconcileReleases completes every physical cleanup whose durable intent was
// recorded before a process crash. It intentionally ignores Ready and
// RetireRequested sessions because only the Kubelet-owned Task teardown may
// advance those states to Releasing.
func (m *Manager) ReconcileReleases(ctx context.Context) error {
	if err := m.reconcileDeviceReservations(); err != nil {
		return fmt.Errorf("reconcile NBD device reservations: %w", err)
	}
	var parents []string
	if err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(sessionBucket).ForEach(func(key, payload []byte) error {
			if payload == nil {
				return nil
			}
			var current record
			if err := json.Unmarshal(payload, &current); err != nil {
				return fmt.Errorf("decode RootFS session %q: %w", key, err)
			}
			if current.Parent != string(key) {
				return fmt.Errorf("RootFS session key does not match record parent %q", current.Parent)
			}
			if current.State == stateReleasing {
				parents = append(parents, current.Parent)
			}
			return nil
		})
	}); err != nil {
		return fmt.Errorf("list releasing RootFS sessions: %w", err)
	}
	var result error
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		unlock := m.lock(parent)
		current, err := m.load(parent)
		if err == nil && current.State == stateReleasing {
			err = m.releaseLocked(ctx, current)
		}
		unlock()
		if err != nil {
			result = errors.Join(result, fmt.Errorf("resume RootFS release %q: %w", parent, err))
		}
	}
	return result
}

func (m *Manager) reconcileDeviceReservations() error {
	type reservation struct {
		parent       string
		devicePath   string
		allocationID string
	}
	var reservations []reservation
	err := m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sessionBucket)
		owners := make(map[string]string)
		var migrations []record
		if err := bucket.ForEach(func(key, payload []byte) error {
			if payload == nil {
				return nil
			}
			var current record
			if err := json.Unmarshal(payload, &current); err != nil {
				return fmt.Errorf("decode RootFS session %q: %w", key, err)
			}
			if current.Parent != string(key) {
				return fmt.Errorf("RootFS session key does not match record parent %q", current.Parent)
			}
			if !supportedSessionVersion(current.Version) {
				return fmt.Errorf("RootFS session %q has unsupported schema version %d", current.Parent, current.Version)
			}
			if strings.TrimSpace(current.DevicePath) == "" {
				return nil
			}
			// Version 2 released process-local reservations as soon as
			// Device.Close returned. Its failed and terminal records may name a
			// path that a later live session legitimately reused, and older crash
			// proofs predate the current allocation-aware validation contract.
			// They are historical evidence only: do not revalidate or adopt them.
			if current.Version == legacySessionSchemaVersion && !legacyReservationNeedsAdoption(current.State) {
				return nil
			}
			changed := false
			terminal, terminalErr := terminalDeviceProof(current)
			if terminalErr != nil {
				return fmt.Errorf("validate device terminal proof for parent %q: %w", current.Parent, terminalErr)
			}
			if terminal {
				if current.Version == sessionSchemaVersion && current.DeviceAllocationID != "" && !current.DeviceReservationReleased {
					current.DeviceReservationReleased = true
					changed = true
				}
			} else {
				if strings.TrimSpace(current.DeviceAllocationID) == "" {
					if current.Version != legacySessionSchemaVersion {
						return fmt.Errorf("current RootFS session %q lacks a device allocation identity", current.Parent)
					}
					current.DeviceAllocationID = legacyDeviceAllocationID(current.Parent, current.DevicePath)
					current.Version = sessionSchemaVersion
					changed = true
				}
				if current.DeviceReservationReleased {
					return fmt.Errorf("non-terminal RootFS session %q released device reservation", current.Parent)
				}
				if owner := owners[current.DevicePath]; owner != "" && owner != current.Parent {
					return fmt.Errorf("NBD device %s is reserved by both %q and %q", current.DevicePath, owner, current.Parent)
				}
				owners[current.DevicePath] = current.Parent
				reservations = append(reservations, reservation{
					parent: current.Parent, devicePath: current.DevicePath, allocationID: current.DeviceAllocationID,
				})
			}
			if changed {
				current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
				migrations = append(migrations, current)
			}
			return nil
		}); err != nil {
			return err
		}
		// bbolt's ForEach cursor must not mutate its bucket. Apply migrations
		// only after the complete ownership graph has been validated.
		for _, current := range migrations {
			if err := putRecord(bucket, current); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, reservation := range reservations {
		if err := m.runtime.AdoptDeviceReservation(reservation.devicePath, reservation.allocationID); err != nil {
			return fmt.Errorf("adopt device reservation for parent %q: %w", reservation.parent, err)
		}
	}
	return nil
}

// Ensure creates the exact branch/device/XFS/Overlay session once. It returns
// only after the merged root is mounted and all physical states are journaled.
func (m *Manager) Ensure(ctx context.Context, request rootfshandoff.StageRequest) (Mount, error) {
	durable := request.WithoutWriterGrantToken()
	if err := durable.ValidateDurableBinding(); err != nil || durable.Generation == nil {
		return Mount{}, fmt.Errorf("invalid durable generation binding: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(durable.Parent)
	defer unlock()
	binding, err := durable.BindingDigest()
	if err != nil {
		return Mount{}, err
	}
	bindingText := hex.EncodeToString(binding[:])
	current, err := m.load(durable.Parent)
	if err == nil {
		if !sameBinding(current, durable, bindingText) {
			return Mount{}, fmt.Errorf("RootFS session parent is bound to another generation: %w", errdefs.ErrAlreadyExists)
		}
		if current.State == stateReady {
			m.mu.Lock()
			_, live := m.live[durable.Parent]
			m.mu.Unlock()
			if !live {
				return Mount{}, fmt.Errorf("ready RootFS session lost its userspace device owner: %w", errdefs.ErrFailedPrecondition)
			}
			return mountFromRecord(current), nil
		}
		if current.State == stateFailed || current.State == stateReleasing || current.State == stateTombstoned {
			return Mount{}, fmt.Errorf("RootFS session is %s: %s: %w", current.State, current.Failure, errdefs.ErrFailedPrecondition)
		}
		return Mount{}, fmt.Errorf("incomplete RootFS session requires startup reconciliation: %w", errdefs.ErrUnavailable)
	}
	if !errdefs.IsNotFound(err) {
		return Mount{}, err
	}

	paths := sessionPaths(m.branchRoot, m.mountRoot, durable.Parent)
	allocationID, err := newDeviceAllocationID()
	if err != nil {
		return Mount{}, fmt.Errorf("generate NBD allocation identity: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	current = record{
		Version: sessionSchemaVersion, Parent: durable.Parent, BindingDigest: bindingText,
		RootFSID: durable.Identity.RootFSID, WriterEpoch: durable.Identity.WriterEpoch,
		GenerationID: durable.InitialGeneration, BaseDescriptor: append([]byte(nil), durable.Generation.Descriptor...), BranchPath: paths.branch,
		DeviceAllocationID: allocationID, XFSRoot: paths.xfs, MergedRoot: paths.merged, State: stateReserved,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := m.saveNew(current); err != nil {
		return Mount{}, err
	}

	descriptor, err := rootfsblock.DecodeDescriptor(durable.Generation.Descriptor)
	if err != nil {
		return Mount{}, m.fail(current, fmt.Errorf("decode immutable generation: %w", err))
	}
	reader, err := rootfsblock.NewReaderWithCache(m.source, descriptor, m.readCache)
	if err != nil {
		return Mount{}, m.fail(current, fmt.Errorf("open immutable generation: %w", err))
	}
	branch, err := rootfsblock.OpenBranch(current.BranchPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
		GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch,
		LogicalSizeBytes: int64(reader.Size()), BaseRootDigest: durable.Generation.CurrentBlockHead,
	}, reader)
	if err != nil {
		return Mount{}, m.fail(current, fmt.Errorf("open writable branch: %w", err))
	}
	devicePath, err := m.runtime.ReserveDevice(current.DeviceAllocationID)
	if err != nil {
		_ = branch.Close()
		return Mount{}, m.fail(current, fmt.Errorf("reserve block device: %w", err))
	}
	current.DevicePath = devicePath
	current.State = stateDeviceReserved
	if err := m.save(current); err != nil {
		m.runtime.ReleaseDeviceReservation(current.DevicePath, current.DeviceAllocationID)
		_ = branch.Close()
		return Mount{}, err
	}
	device, err := m.runtime.AttachDevice(m.lifetime, ctx, current.DevicePath, current.DeviceAllocationID, branch)
	if err != nil {
		_ = branch.Close()
		return Mount{}, m.fail(current, fmt.Errorf("attach block device: %w", err))
	}
	if device.Path() != current.DevicePath {
		failure := errors.Join(
			fmt.Errorf("attached NBD path %q does not match reservation %q", device.Path(), current.DevicePath),
			device.Close(), branch.Close(),
		)
		return Mount{}, m.fail(current, failure)
	}
	current.State = stateDeviceReady
	if err := m.save(current); err != nil {
		_ = device.Close()
		_ = branch.Close()
		return Mount{}, err
	}
	if err := m.runtime.MountXFS(current.DevicePath, current.XFSRoot); err != nil {
		failure := errors.Join(
			fmt.Errorf("mount XFS: %w", err),
			device.Close(),
			branch.Close(),
		)
		return Mount{}, m.fail(current, failure)
	}
	current.State = stateXFSMounted
	if err := m.save(current); err != nil {
		_ = m.runtime.UnmountXFS(current.XFSRoot, false)
		_ = device.Close()
		_ = branch.Close()
		return Mount{}, err
	}
	if err := m.runtime.MountOverlay(current.XFSRoot, current.MergedRoot); err != nil {
		_ = m.runtime.UnmountXFS(current.XFSRoot, false)
		_ = device.Close()
		_ = branch.Close()
		return Mount{}, m.fail(current, fmt.Errorf("mount OverlayFS: %w", err))
	}
	current.State = stateReady
	if err := m.save(current); err != nil {
		_ = m.runtime.UnmountOverlay(current.MergedRoot, false)
		_ = m.runtime.UnmountXFS(current.XFSRoot, false)
		_ = device.Close()
		_ = branch.Close()
		return Mount{}, err
	}
	m.mu.Lock()
	m.live[durable.Parent] = &liveSession{branch: branch, device: device}
	m.mu.Unlock()
	return mountFromRecord(current), nil
}

// BeginRetire makes sealing mandatory for the exact live writer before the
// caller asks Kubernetes to delete its Pod. The operation is durable and
// idempotent; a different operation can never take over the same session.
func (m *Manager) BeginRetire(parent string, identity rootfshandoff.Identity, operationID string) error {
	operationID = strings.TrimSpace(operationID)
	if strings.TrimSpace(parent) == "" || operationID == "" {
		return fmt.Errorf("parent and retire operation are required: %w", errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch {
		return fmt.Errorf("RootFS session belongs to another writer identity: %w", errdefs.ErrFailedPrecondition)
	}
	if current.RetireOperationID != "" && current.RetireOperationID != operationID {
		return fmt.Errorf("RootFS session is bound to retire operation %q: %w", current.RetireOperationID, errdefs.ErrAlreadyExists)
	}
	if current.RetireOperationID == operationID && containsSessionState(current.State, stateRetireRequested, stateReleasing, stateTombstoned) {
		return nil
	}
	if current.State != stateReady {
		return fmt.Errorf("RootFS session cannot retire from state %q: %w", current.State, errdefs.ErrFailedPrecondition)
	}
	current.RetireOperationID = operationID
	current.State = stateRetireRequested
	return m.save(current)
}

// RetireResult returns the sealed composite generation after physical writer
// teardown. It never treats a plain crash cleanup as a successful retire.
func (m *Manager) RetireResult(parent string, identity rootfshandoff.Identity, operationID string) (RetireResult, error) {
	current, err := m.load(parent)
	if err != nil {
		return RetireResult{}, err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch ||
		current.RetireOperationID != strings.TrimSpace(operationID) {
		return RetireResult{}, fmt.Errorf("retire result does not match the writer binding: %w", errdefs.ErrFailedPrecondition)
	}
	if current.State != stateTombstoned || len(current.SealedDescriptor) == 0 || current.DetachProof == "" {
		return RetireResult{}, fmt.Errorf("retire result is not sealed: %w", errdefs.ErrUnavailable)
	}
	proof, err := hex.DecodeString(current.DetachProof)
	if err != nil || len(proof) != sha256.Size {
		return RetireResult{}, fmt.Errorf("stored detach proof is invalid: %w", errdefs.ErrFailedPrecondition)
	}
	return RetireResult{
		Parent: current.Parent, RootFSID: current.RootFSID, WriterEpoch: current.WriterEpoch,
		OperationID: current.RetireOperationID, CurrentBlockHead: current.SealedBlockHead,
		DurabilityState: current.SealedDurability, Descriptor: append([]byte(nil), current.SealedDescriptor...),
		DetachProof: proof,
	}, nil
}

// CrashFence durably proves that a non-cooperatively stopped session has no
// remaining userspace owner, mount, or NBD endpoint. It never seals or
// publishes the branch. The same operation is idempotent; a competing
// operation can never replace an existing intent.
func (m *Manager) CrashFence(
	request rootfshandoff.StageRequest,
	operationID string,
) (rootfshandoff.CrashFenceSessionObservation, error) {
	parent := request.Parent
	identity := request.Identity
	operationID = strings.TrimSpace(operationID)
	if strings.TrimSpace(parent) == "" || operationID == "" {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("parent and crash fence operation are required: %w", errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return rootfshandoff.CrashFenceSessionObservation{}, err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session belongs to another writer identity: %w", errdefs.ErrFailedPrecondition)
	}
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	if err != nil || current.BindingDigest != hex.EncodeToString(binding[:]) {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session belongs to another writer binding: %w", errdefs.ErrFailedPrecondition)
	}
	if current.State != stateTombstoned || current.RetireOperationID != "" {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session is not an unplanned tombstone: %w", errdefs.ErrFailedPrecondition)
	}
	if strings.TrimSpace(current.BranchPath) == "" {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session lacks a recorded physical attachment: %w", errdefs.ErrFailedPrecondition)
	}
	if current.CrashFence != nil {
		if current.CrashFence.OperationID != operationID {
			return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session is bound to crash fence operation %q: %w", current.CrashFence.OperationID, errdefs.ErrAlreadyExists)
		}
		if current.CrashFence.Result != nil {
			result := *current.CrashFence.Result
			if err := result.Validate(); err != nil {
				return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("stored crash fence result is invalid: %w", errdefs.ErrFailedPrecondition)
			}
			if err := m.releaseDeviceReservation(&current); err != nil {
				return rootfshandoff.CrashFenceSessionObservation{}, err
			}
			return result, nil
		}
	} else {
		current.CrashFence = &crashFenceRecord{
			OperationID: operationID,
			RequestedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
		if err := m.save(current); err != nil {
			return rootfshandoff.CrashFenceSessionObservation{}, err
		}
	}

	m.mu.Lock()
	_, live := m.live[parent]
	m.mu.Unlock()
	if live {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS session still has a live userspace owner: %w", errdefs.ErrFailedPrecondition)
	}
	deviceBound := strings.TrimSpace(current.DevicePath) != ""
	var host CrashFenceHostObservation
	if deviceBound {
		inspector, ok := m.runtime.(CrashFenceHostInspector)
		if !ok {
			return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("host runtime cannot attest crash fencing: %w", errdefs.ErrUnavailable)
		}
		host, err = inspector.InspectCrashFence(current.DevicePath, current.XFSRoot, current.MergedRoot)
	} else if current.Version == sessionSchemaVersion && strings.TrimSpace(current.DeviceAllocationID) != "" {
		inspector, ok := m.runtime.(CrashFencePreAttachmentHostInspector)
		if !ok {
			return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("host runtime cannot attest a pre-attachment session: %w", errdefs.ErrUnavailable)
		}
		host, err = inspector.InspectPreAttachmentCrashFence(current.XFSRoot, current.MergedRoot)
	} else {
		inspector, ok := m.runtime.(CrashFenceUnattachedHostInspector)
		if !ok {
			return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("host runtime cannot attest an uncommitted device attachment: %w", errdefs.ErrUnavailable)
		}
		host, err = inspector.InspectUnattachedCrashFence(current.XFSRoot, current.MergedRoot)
	}
	if err != nil {
		return rootfshandoff.CrashFenceSessionObservation{}, err
	}
	result := rootfshandoff.CrashFenceSessionObservation{
		Parent: current.Parent, RootFSID: current.RootFSID, WriterEpoch: current.WriterEpoch,
		OperationID: operationID, BindingDigest: current.BindingDigest,
		SessionState: current.State, BranchPath: current.BranchPath,
		DeviceBound: deviceBound, DevicePath: current.DevicePath, NBDPoolAbsent: host.NBDPoolAbsent,
		NBDPID: host.NBDPID, NBDHolders: append([]string(nil), host.NBDHolders...),
		LiveSessionAbsent: true, MergedMountAbsent: host.MergedMountAbsent,
		XFSMountAbsent: host.XFSMountAbsent, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := result.Validate(); err != nil {
		return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("physical RootFS remains attached: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	current.CrashFence.Result = &result
	if current.DevicePath != "" && current.DeviceAllocationID != "" {
		current.DeviceReservationReleased = true
	}
	if err := m.save(current); err != nil {
		return rootfshandoff.CrashFenceSessionObservation{}, err
	}
	if current.DeviceReservationReleased {
		m.runtime.ReleaseDeviceReservation(current.DevicePath, current.DeviceAllocationID)
	}
	return result, nil
}

func (m *Manager) releaseDeviceReservation(current *record) error {
	if current.DevicePath == "" || current.DeviceAllocationID == "" || current.DeviceReservationReleased {
		return nil
	}
	current.DeviceReservationReleased = true
	if err := m.save(*current); err != nil {
		return err
	}
	m.runtime.ReleaseDeviceReservation(current.DevicePath, current.DeviceAllocationID)
	return nil
}

// Resolve returns a ready mount only while this process still owns the live
// NBD userspace endpoint. A journal record alone is never treated as live.
func (m *Manager) Resolve(parent string, request rootfshandoff.StageRequest) (Mount, error) {
	unlock := m.lock(parent)
	defer unlock()
	record, err := m.load(parent)
	if err != nil {
		return Mount{}, err
	}
	binding, err := request.WithoutWriterGrantToken().BindingDigest()
	if err != nil || !sameBinding(record, request, hex.EncodeToString(binding[:])) || record.State != stateReady {
		return Mount{}, fmt.Errorf("RootFS session does not match the staged binding: %w", errdefs.ErrFailedPrecondition)
	}
	m.mu.Lock()
	_, live := m.live[parent]
	m.mu.Unlock()
	if !live {
		return Mount{}, fmt.Errorf("RootFS session has no live userspace device owner: %w", errdefs.ErrUnavailable)
	}
	return mountFromRecord(record), nil
}

// Parent returns the one durable parent bound to a writer identity. It is used
// only to remove the derived ctld ready observation after physical teardown.
func (m *Manager) Parent(identity rootfshandoff.Identity) (string, error) {
	parent, _, err := m.findIdentity(identity.RootFSID, identity.WriterEpoch)
	return parent, err
}

// ReleaseParent revokes an exact parent and writer identity. If attachment has
// not started yet, it persists a tombstone fence so a stale supervisor request
// cannot create the physical session after the logical handoff was aborted.
func (m *Manager) ReleaseParent(ctx context.Context, parent string, identity rootfshandoff.Identity) error {
	if strings.TrimSpace(parent) == "" || strings.TrimSpace(identity.RootFSID) == "" || identity.WriterEpoch <= 0 {
		return fmt.Errorf("parent and writer identity are required: %w", errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		return m.saveNew(record{
			Version: sessionSchemaVersion, Parent: parent, RootFSID: identity.RootFSID,
			WriterEpoch: identity.WriterEpoch, State: stateTombstoned,
			CreatedAt: now, UpdatedAt: now,
		})
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch {
		return fmt.Errorf("RootFS session parent belongs to another writer identity: %w", errdefs.ErrFailedPrecondition)
	}
	if current.State == stateTombstoned {
		return nil
	}
	return m.releaseLocked(ctx, current)
}

// Release drains the merged mount, XFS and NBD endpoint in dependency order.
// It is safe to retry after any completed step within the same process.
func (m *Manager) Release(ctx context.Context, identity rootfshandoff.Identity) error {
	parent, current, err := m.findIdentity(identity.RootFSID, identity.WriterEpoch)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if current.State == stateTombstoned {
		return nil
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err = m.load(parent)
	if err != nil {
		return err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch {
		return fmt.Errorf("RootFS session identity changed during release: %w", errdefs.ErrFailedPrecondition)
	}
	if current.State == stateTombstoned {
		return nil
	}
	return m.releaseLocked(ctx, current)
}

func (m *Manager) releaseLocked(ctx context.Context, current record) error {
	parent := current.Parent
	plannedRetire := current.RetireOperationID != ""
	current.State = stateReleasing
	if err := m.save(current); err != nil {
		return err
	}
	var releaseErr error
	if err := m.runtime.UnmountOverlay(current.MergedRoot, plannedRetire); err != nil {
		releaseErr = errors.Join(releaseErr, fmt.Errorf("unmount OverlayFS: %w", err))
	}
	if releaseErr == nil {
		if err := m.runtime.UnmountXFS(current.XFSRoot, plannedRetire); err != nil {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("unmount XFS: %w", err))
		}
	}
	m.mu.Lock()
	live := m.live[parent]
	m.mu.Unlock()
	var sealedDescriptor []byte
	var sealedBlockHead, sealedDurability string
	if releaseErr == nil && plannedRetire {
		branch := (*rootfsblock.Branch)(nil)
		closeBranch := false
		if live != nil {
			branch = live.branch
		} else {
			branch, releaseErr = m.reopenBranch(current)
			closeBranch = branch != nil
		}
		if releaseErr == nil {
			if err := branch.Flush(); err != nil {
				releaseErr = errors.Join(releaseErr, fmt.Errorf("flush retiring branch: %w", err))
			}
		}
		var records []rootfsblock.BlockUpdate
		if releaseErr == nil {
			records, releaseErr = branch.DurableRecords()
			if releaseErr != nil {
				releaseErr = fmt.Errorf("read retiring branch: %w", releaseErr)
			}
		}
		if releaseErr == nil {
			base, decodeErr := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
			if decodeErr != nil {
				releaseErr = fmt.Errorf("decode retiring base generation: %w", decodeErr)
			} else {
				sealed, payload, buildErr := rootfsblock.BuildCompositeGeneration(base, records)
				var tooLarge *rootfsblock.CompositeTailTooLargeError
				if errors.As(buildErr, &tooLarge) {
					var updates []rootfsblock.BlockUpdate
					updates, buildErr = branch.DurableUpdates()
					if buildErr == nil {
						var materialized rootfsblock.BuildResult
						materialized, buildErr = rootfsblock.BuildIncrementalGeneration(
							ctx, m.source, base, updates, m.publisher, rootfsblock.BuildOptions{},
						)
						if buildErr == nil {
							sealed = materialized.Descriptor
							payload = materialized.Payload
						}
					}
				}
				if buildErr != nil {
					releaseErr = fmt.Errorf("seal durable generation: %w", buildErr)
				} else {
					sealedDescriptor = payload
					sealedBlockHead = sealed.MappingRoot.RootDigest
					sealedDurability = rootfsblock.DurabilityS3
					if sealed.CompositeTail != nil {
						sealedDurability = rootfsblock.DurabilityComposite
					}
				}
			}
		}
		if closeBranch {
			releaseErr = errors.Join(releaseErr, branch.Close())
		}
	}
	if releaseErr == nil && live != nil {
		if err := live.device.Close(); err != nil {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("close NBD device: %w", err))
		}
		if err := live.branch.Close(); err != nil {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("close branch: %w", err))
		}
	}
	if releaseErr == nil && live == nil && current.DevicePath != "" {
		if err := m.runtime.RecoverOrphanDevice(ctx, current.DevicePath, current.DeviceAllocationID); err != nil {
			releaseErr = fmt.Errorf("disconnect interrupted NBD release: %w", err)
		} else {
			inspector, ok := m.runtime.(CrashFenceHostInspector)
			if !ok {
				releaseErr = fmt.Errorf("host runtime cannot verify an interrupted NBD release: %w", errdefs.ErrUnavailable)
			} else if _, err := inspector.InspectCrashFence(current.DevicePath, current.XFSRoot, current.MergedRoot); err != nil {
				releaseErr = fmt.Errorf("verify interrupted NBD release: %w", err)
			}
		}
	}
	if releaseErr != nil {
		current.Failure = releaseErr.Error()
		_ = m.save(current)
		return releaseErr
	}
	m.mu.Lock()
	delete(m.live, parent)
	m.mu.Unlock()
	current.State = stateTombstoned
	current.Failure = ""
	if plannedRetire {
		current.SealedDescriptor = sealedDescriptor
		current.SealedBlockHead = sealedBlockHead
		current.SealedDurability = sealedDurability
		current.DetachProof = detachProof(current)
		if current.DevicePath != "" && current.DeviceAllocationID != "" {
			current.DeviceReservationReleased = true
		}
	}
	if err := m.save(current); err != nil {
		return err
	}
	if current.DeviceReservationReleased {
		m.runtime.ReleaseDeviceReservation(current.DevicePath, current.DeviceAllocationID)
	}
	return nil
}

func (m *Manager) reopenBranch(current record) (*rootfsblock.Branch, error) {
	descriptor, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
	if err != nil {
		return nil, fmt.Errorf("decode session base generation: %w", err)
	}
	reader, err := rootfsblock.NewReaderWithCache(m.source, descriptor, m.readCache)
	if err != nil {
		return nil, fmt.Errorf("open session base generation: %w", err)
	}
	branch, err := rootfsblock.OpenBranch(current.BranchPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
		GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch,
		LogicalSizeBytes: int64(reader.Size()), BaseRootDigest: descriptor.MappingRoot.RootDigest,
	}, reader)
	if err != nil {
		return nil, fmt.Errorf("reopen session branch: %w", err)
	}
	return branch, nil
}

func detachProof(current record) string {
	payload, _ := json.Marshal(struct {
		Version          int    `json:"version"`
		Parent           string `json:"parent"`
		RootFSID         string `json:"rootfs_id"`
		WriterEpoch      int64  `json:"writer_epoch"`
		RetireOperation  string `json:"retire_operation"`
		DescriptorDigest string `json:"descriptor_digest"`
	}{
		Version: 1, Parent: current.Parent, RootFSID: current.RootFSID,
		WriterEpoch: current.WriterEpoch, RetireOperation: current.RetireOperationID,
		DescriptorDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(current.SealedDescriptor)),
	})
	proof := sha256.Sum256(payload)
	return hex.EncodeToString(proof[:])
}

func containsSessionState(state string, allowed ...string) bool {
	for _, candidate := range allowed {
		if state == candidate {
			return true
		}
	}
	return false
}

func (m *Manager) Close() error {
	m.cancel()
	m.mu.Lock()
	live := make([]*liveSession, 0, len(m.live))
	for _, session := range m.live {
		live = append(live, session)
	}
	m.mu.Unlock()
	var result error
	for _, session := range live {
		result = errors.Join(result, session.device.Close(), session.branch.Close())
	}
	return errors.Join(result, m.db.Close())
}

type paths struct{ branch, xfs, merged string }

func sessionPaths(branchRoot, mountRoot, parent string) paths {
	sum := sha256.Sum256([]byte(parent))
	name := hex.EncodeToString(sum[:])
	return paths{
		branch: filepath.Join(branchRoot, name+".wal"),
		xfs:    filepath.Join(mountRoot, name, "xfs"), merged: filepath.Join(mountRoot, name, "merged"),
	}
}

func mountFromRecord(value record) Mount {
	return Mount{Source: value.MergedRoot, Type: "bind", Options: []string{"rbind", "rw", "nosuid", "nodev"}}
}

func sameBinding(value record, request rootfshandoff.StageRequest, digest string) bool {
	return supportedSessionVersion(value.Version) && value.Parent == request.Parent && value.BindingDigest == digest &&
		value.RootFSID == request.Identity.RootFSID && value.WriterEpoch == request.Identity.WriterEpoch &&
		value.GenerationID == request.InitialGeneration
}

func supportedSessionVersion(version int) bool {
	return version == legacySessionSchemaVersion || version == sessionSchemaVersion
}

func newDeviceAllocationID() (string, error) {
	payload := make([]byte, sha256.Size)
	if _, err := rand.Read(payload); err != nil {
		return "", err
	}
	return hex.EncodeToString(payload), nil
}

func legacyDeviceAllocationID(parent, devicePath string) string {
	digest := sha256.Sum256([]byte("sandbox0-legacy-nbd-allocation\x00" + parent + "\x00" + devicePath))
	return hex.EncodeToString(digest[:])
}

func terminalDeviceProof(value record) (bool, error) {
	if value.State != stateTombstoned {
		return false, nil
	}
	if value.RetireOperationID != "" && value.DetachProof != "" {
		proof, err := hex.DecodeString(value.DetachProof)
		if err != nil || len(proof) != sha256.Size || len(value.SealedDescriptor) == 0 || value.SealedBlockHead == "" {
			return false, fmt.Errorf("planned detach proof is invalid")
		}
		return true, nil
	}
	if value.CrashFence == nil || value.CrashFence.Result == nil {
		return false, nil
	}
	if err := value.CrashFence.Result.Validate(); err != nil {
		return false, fmt.Errorf("crash fence result is invalid: %w", err)
	}
	return true, nil
}

func legacyReservationNeedsAdoption(state string) bool {
	return containsSessionState(
		state, stateReserved, stateDeviceReady, stateXFSMounted, stateReady, stateRetireRequested, stateReleasing,
	)
}

func (m *Manager) fail(value record, failure error) error {
	value.State = stateFailed
	value.Failure = failure.Error()
	if err := m.save(value); err != nil {
		return errors.Join(failure, err)
	}
	return failure
}

func (m *Manager) load(parent string) (record, error) {
	var value record
	err := m.db.View(func(tx *bolt.Tx) error {
		payload := tx.Bucket(sessionBucket).Get([]byte(parent))
		if payload == nil {
			return errdefs.ErrNotFound
		}
		return json.Unmarshal(payload, &value)
	})
	return value, err
}

func (m *Manager) saveNew(value record) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sessionBucket)
		if bucket.Get([]byte(value.Parent)) != nil {
			return errdefs.ErrAlreadyExists
		}
		identityBucket := tx.Bucket(sessionIdentityBucket)
		identityKey := writerIdentityKey(value.RootFSID, value.WriterEpoch)
		if existing := identityBucket.Get(identityKey); existing != nil {
			return fmt.Errorf("writer identity is already bound to parent %s: %w", existing, errdefs.ErrAlreadyExists)
		}
		if err := putRecord(bucket, value); err != nil {
			return err
		}
		return identityBucket.Put(identityKey, []byte(value.Parent))
	})
}

func (m *Manager) save(value record) error {
	value.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return m.db.Update(func(tx *bolt.Tx) error { return putRecord(tx.Bucket(sessionBucket), value) })
}

func putRecord(bucket *bolt.Bucket, value record) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(value.Parent), payload)
}

func (m *Manager) findIdentity(rootfsID string, epoch int64) (string, record, error) {
	var parent string
	var found record
	err := m.db.View(func(tx *bolt.Tx) error {
		parent = string(tx.Bucket(sessionIdentityBucket).Get(writerIdentityKey(rootfsID, epoch)))
		if parent == "" {
			return errdefs.ErrNotFound
		}
		payload := tx.Bucket(sessionBucket).Get([]byte(parent))
		if payload == nil {
			return fmt.Errorf("writer identity index references a missing session: %w", errdefs.ErrFailedPrecondition)
		}
		return json.Unmarshal(payload, &found)
	})
	if err != nil {
		return "", record{}, err
	}
	if found.RootFSID != rootfsID || found.WriterEpoch != epoch {
		return "", record{}, fmt.Errorf("writer identity index does not match its session: %w", errdefs.ErrFailedPrecondition)
	}
	return parent, found, nil
}

func writerIdentityKey(rootfsID string, epoch int64) []byte {
	return []byte(fmt.Sprintf("%s\x00%020d", rootfsID, epoch))
}

func (m *Manager) lock(key string) func() {
	value, _ := m.locks.LoadOrStore(key, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func privatePath(path string, directory bool) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return "", fmt.Errorf("must be a non-root absolute path")
	}
	root := path
	if !directory {
		root = filepath.Dir(path)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return "", fmt.Errorf("path must not contain symlinks")
	}
	if !directory {
		if info, statErr := os.Lstat(path); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("state path must not be a symlink")
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", statErr
		}
	}
	return path, nil
}
