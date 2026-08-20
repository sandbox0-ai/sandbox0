package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	bolt "go.etcd.io/bbolt"
)

const (
	// DefaultMaxDirtyTailBytes bounds one active writer to 10 GiB of local,
	// unpublished block-record payload. Operators must also size the node's
	// branch volume for configured session concurrency.
	DefaultMaxDirtyTailBytes       = int64(10 << 30)
	legacySessionSchemaVersion     = 2
	allocationSessionSchemaVersion = 3
	durableBindingSchemaVersion    = 4
	sessionSchemaVersion           = 6
	stateReserved                  = "reserved"
	stateDeviceReserved            = "device_reserved"
	stateDeviceReady               = "device_ready"
	stateXFSMounted                = "xfs_mounted"
	stateReady                     = "ready"
	stateRetireRequested           = "retire_requested"
	stateReleasing                 = "releasing"
	stateTombstoned                = "tombstoned"
	stateFailed                    = "failed"
)

var (
	sessionBucket         = []byte("rootfs-sessions-v1")
	sessionIdentityBucket = []byte("rootfs-session-identities-v1")
	rebaseBucket          = []byte("rootfs-rebases-v1")
	rebaseAckBucket       = []byte("rootfs-rebase-acks-v1")
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
	FreezeXFS(target string) error
	ThawXFS(target string) error
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
	StatePath         string
	BranchRoot        string
	MountRoot         string
	MaxDirtyTailBytes int64
	Source            rootfsblock.RangeSource
	Publisher         rootfsblock.ImmutableObjectPublisher
	Runtime           HostRuntime
	RebaseEngine      RebaseEngine
}

// RebaseEngine performs the semantic three-way merge while Manager owns all
// NBD, XFS, OverlayFS, branch, and journal side effects.
type RebaseEngine interface {
	Apply(context.Context, rootfsrebase.WorkerRequest, string, string, string, []uint64) (*rootfsrebase.ApplyResult, error)
}

// Mount is the storage-owned merged root exported to the Snapshotter's stable
// open_tree/move_mount capture. It is not directly tenant reachable.
type Mount struct {
	Source  string
	Type    string
	Options []string
}

type record struct {
	Version                   int                                         `json:"version"`
	Parent                    string                                      `json:"parent"`
	BindingDigest             string                                      `json:"binding_digest"`
	RootFSID                  string                                      `json:"rootfs_id"`
	WriterEpoch               int64                                       `json:"writer_epoch"`
	GenerationID              string                                      `json:"generation_id"`
	BaseDescriptor            []byte                                      `json:"base_descriptor,omitempty"`
	BranchPath                string                                      `json:"branch_path"`
	DevicePath                string                                      `json:"device_path,omitempty"`
	DeviceAllocationID        string                                      `json:"device_allocation_id,omitempty"`
	DeviceReservationReleased bool                                        `json:"device_reservation_released,omitempty"`
	XFSRoot                   string                                      `json:"xfs_root"`
	MergedRoot                string                                      `json:"merged_root"`
	State                     string                                      `json:"state"`
	FreezeOperationID         string                                      `json:"freeze_operation_id,omitempty"`
	RunningForkRequest        *rootfshandoff.RunningForkCheckpointRequest `json:"running_fork_request,omitempty"`
	RunningForkResult         *rootfshandoff.RunningForkCheckpointResult  `json:"running_fork_result,omitempty"`
	RetireOperationID         string                                      `json:"retire_operation_id,omitempty"`
	SealedDescriptor          []byte                                      `json:"sealed_descriptor,omitempty"`
	SealedBlockHead           string                                      `json:"sealed_block_head,omitempty"`
	SealedDurability          string                                      `json:"sealed_durability,omitempty"`
	DetachProof               string                                      `json:"detach_proof,omitempty"`
	CrashFence                *crashFenceRecord                           `json:"crash_fence,omitempty"`
	BranchRemoved             bool                                        `json:"branch_removed,omitempty"`
	Failure                   string                                      `json:"failure,omitempty"`
	CreatedAt                 string                                      `json:"created_at"`
	UpdatedAt                 string                                      `json:"updated_at"`
	Stage                     *rootfshandoff.StageRequest                 `json:"stage,omitempty"`
	Consumer                  *ConsumerRegistration                       `json:"consumer,omitempty"`
}

type crashFenceRecord struct {
	OperationID string                                      `json:"operation_id"`
	RequestedAt string                                      `json:"requested_at"`
	External    bool                                        `json:"external,omitempty"`
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

// RecoveryKind describes the only safe terminal action for a durable
// session after its userspace owner process has disappeared.
type RecoveryKind string

const (
	RecoveryUnavailable   RecoveryKind = "unavailable"
	RecoveryCrashAbandon  RecoveryKind = "crash_abandon"
	RecoveryPlannedRetire RecoveryKind = "planned_retire"
)

// RecoverySession is the tokenless durable input required by a node daemon to
// finish writer retirement without Nomad allocation or plugin state. Live is
// process-local and is true only while this Manager owns the NBD endpoint.
type RecoverySession struct {
	Stage             rootfshandoff.StageRequest
	Kind              RecoveryKind
	State             string
	RetireOperationID string
	CrashOperationID  string
	CrashRequestedAt  time.Time
	ExternalCrash     bool
	BranchRemoved     bool
	Live              bool
	Consumer          *ConsumerRegistration
	CreatedAt         time.Time
}

// ConsumerRegistration is the durable host runtime identity that a node
// daemon must fence before crash-abandoning a writer. LeaseID changes whenever
// a restarted plugin re-adopts the same immutable runtime paths.
type ConsumerRegistration struct {
	LeaseID            string `json:"lease_id"`
	ActiveKey          string `json:"active_key"`
	ContainerID        string `json:"container_id"`
	StableMount        string `json:"stable_mount"`
	HostMountNamespace string `json:"host_mount_namespace"`
	NetNSPath          string `json:"netns_path,omitempty"`
	NetNSIdentity      string `json:"netns_identity,omitempty"`
	NetworkChain       string `json:"network_chain,omitempty"`
	LeaseExpiresAt     string `json:"lease_expires_at"`
}

// Manager is the single physical owner of a D generation on a node. Its
// journal stores the tokenless regional Stage binding together with device and
// mount side effects so a process-independent reconciler can finish cleanup.
type Manager struct {
	db              *bolt.DB
	branchRoot      string
	mountRoot       string
	source          rootfsblock.RangeSource
	publisher       rootfsblock.ImmutableObjectPublisher
	readCache       *rootfsblock.ReadCache
	runtime         HostRuntime
	maxDirty        int64
	mu              sync.Mutex
	live            map[string]*liveSession
	captures        map[string]bool
	locks           sync.Map
	lifetime        context.Context
	cancel          context.CancelFunc
	rebaseEngine    RebaseEngine
	rebaseAdmission chan struct{}
	rebaseWG        sync.WaitGroup
	closing         bool
}

func New(config Config) (*Manager, error) {
	if config.Source == nil || config.Publisher == nil || config.Runtime == nil {
		return nil, fmt.Errorf("range source, immutable publisher, and host runtime are required")
	}
	if config.MaxDirtyTailBytes < 0 {
		return nil, fmt.Errorf("maximum dirty tail bytes must be non-negative")
	}
	if config.MaxDirtyTailBytes == 0 {
		config.MaxDirtyTailBytes = DefaultMaxDirtyTailBytes
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
		if _, err := tx.CreateBucketIfNotExists(sessionIdentityBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(rebaseBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(rebaseAckBucket)
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
	rebaseEngine := config.RebaseEngine
	if rebaseEngine == nil {
		rebaseEngine = filesystemRebaseEngine{}
	}
	return &Manager{
		db: db, branchRoot: branchRoot, mountRoot: mountRoot,
		source: config.Source, publisher: config.Publisher, readCache: readCache,
		runtime: config.Runtime, maxDirty: config.MaxDirtyTailBytes,
		live: make(map[string]*liveSession), captures: make(map[string]bool),
		lifetime: lifetime, cancel: cancel,
		rebaseEngine: rebaseEngine, rebaseAdmission: make(chan struct{}, 1),
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

// ReconcileFreezes thaws every filesystem whose durable freeze intent
// survived a session-owner crash. Thaw is idempotent for an already thawed or
// absent mount, so clearing the intent is safe only after this call succeeds.
func (m *Manager) ReconcileFreezes(ctx context.Context) error {
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
			if current.Parent != string(key) || !supportedSessionVersion(current.Version) {
				return fmt.Errorf("invalid RootFS recovery record %q version %d", key, current.Version)
			}
			if current.FreezeOperationID != "" {
				parents = append(parents, current.Parent)
			}
			return nil
		})
	}); err != nil {
		return fmt.Errorf("list interrupted RootFS freezes: %w", err)
	}
	var result error
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		unlock := m.lock(parent)
		current, err := m.load(parent)
		if err == nil && current.FreezeOperationID != "" {
			if err = m.runtime.ThawXFS(current.XFSRoot); err == nil {
				current.FreezeOperationID = ""
				err = m.save(current)
			}
		}
		unlock()
		if err != nil {
			result = errors.Join(result, fmt.Errorf("recover RootFS freeze %q: %w", parent, err))
		}
	}
	return result
}

// ReconcileRunningForkCaptures drops operations that had not produced a
// durable checkpoint result before the previous owner exited. Immutable
// objects from a partial build are content addressed and may be reclaimed by
// regional object GC; no regional transaction could have started yet.
func (m *Manager) ReconcileRunningForkCaptures(ctx context.Context) error {
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
			if current.Parent != string(key) || !supportedSessionVersion(current.Version) {
				return fmt.Errorf("invalid RootFS recovery record %q version %d", key, current.Version)
			}
			if current.RunningForkRequest != nil && current.RunningForkResult == nil {
				parents = append(parents, current.Parent)
			}
			return nil
		})
	}); err != nil {
		return fmt.Errorf("list interrupted running RootFS forks: %w", err)
	}
	var result error
	for _, parent := range parents {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		unlock := m.lock(parent)
		current, err := m.load(parent)
		if err == nil && current.RunningForkRequest != nil && current.RunningForkResult == nil {
			m.mu.Lock()
			active := m.captures[parent]
			m.mu.Unlock()
			if active {
				// This owner is still building the durable checkpoint. Only a
				// subsequent pass after it exits may classify the intent as stale.
			} else if current.FreezeOperationID != "" {
				err = fmt.Errorf("running fork remains frozen: %w", errdefs.ErrFailedPrecondition)
			} else {
				current.RunningForkRequest = nil
				err = m.save(current)
			}
		}
		unlock()
		if err != nil {
			result = errors.Join(result, fmt.Errorf("recover running RootFS fork %q: %w", parent, err))
		}
	}
	return result
}

// AcknowledgeRunningFork removes the one bounded node retry checkpoint only
// after the regional transaction returned success. A response-loss retry
// therefore resubmits byte-identical proof instead of freezing a later source
// boundary under the same operation ID.
func (m *Manager) AcknowledgeRunningFork(
	stage rootfshandoff.StageRequest,
	operationID string,
	proofDigest string,
) error {
	if err := stage.ValidateDurableBinding(); err != nil {
		return err
	}
	if strings.TrimSpace(operationID) == "" || strings.TrimSpace(operationID) != operationID ||
		strings.TrimSpace(proofDigest) == "" || strings.TrimSpace(proofDigest) != proofDigest {
		return fmt.Errorf("running fork operation and proof digest must be canonical: %w", errdefs.ErrInvalidArgument)
	}
	parent, _, err := m.findIdentity(stage.Identity.RootFSID, stage.Identity.WriterEpoch)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return err
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if !sameBinding(current, stage, hex.EncodeToString(binding[:])) {
		return fmt.Errorf("running fork acknowledgement does not match the writer: %w", errdefs.ErrFailedPrecondition)
	}
	if current.RunningForkRequest == nil && current.RunningForkResult == nil {
		return nil
	}
	if current.RunningForkRequest == nil || current.RunningForkResult == nil ||
		current.RunningForkRequest.OperationID != operationID || current.RunningForkResult.ProofDigest != proofDigest {
		return fmt.Errorf("running fork acknowledgement changed operation or proof: %w", errdefs.ErrFailedPrecondition)
	}
	current.RunningForkRequest = nil
	current.RunningForkResult = nil
	return m.save(current)
}

// CaptureRunningFork briefly freezes XFS, captures an immutable branch
// boundary, thaws the source, and only then performs potentially slow object
// publication. The source branch remains writable throughout publication.
func (m *Manager) CaptureRunningFork(
	ctx context.Context,
	stage rootfshandoff.StageRequest,
	request rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if err := stage.ValidateDurableBinding(); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if err := request.Validate(); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if stage.Generation == nil || stage.Generation.LocatorVersion == math.MaxInt64 {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("source generation locator cannot advance")
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	bindingText := hex.EncodeToString(binding[:])
	parent, _, err := m.findIdentity(stage.Identity.RootFSID, stage.Identity.WriterEpoch)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}

	unlock := m.lock(parent)
	current, err := m.load(parent)
	if err != nil {
		unlock()
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if !sameBinding(current, stage, bindingText) || current.State != stateReady || current.Stage == nil {
		unlock()
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("RootFS session is not a live matching writer: %w", errdefs.ErrFailedPrecondition)
	}
	if current.RunningForkRequest != nil {
		if *current.RunningForkRequest != request {
			unlock()
			return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf(
				"RootFS session has another pending running fork %q: %w",
				current.RunningForkRequest.OperationID, errdefs.ErrAlreadyExists,
			)
		}
		if current.RunningForkResult == nil {
			unlock()
			return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf(
				"running fork %q is still being captured: %w", request.OperationID, errdefs.ErrUnavailable,
			)
		}
		result := *current.RunningForkResult
		unlock()
		if err := result.Validate(); err != nil {
			return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("validate cached running fork checkpoint: %w", err)
		}
		return result, nil
	}
	m.mu.Lock()
	live := m.live[parent]
	m.mu.Unlock()
	if live == nil {
		unlock()
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("RootFS session has no live branch owner: %w", errdefs.ErrUnavailable)
	}
	m.mu.Lock()
	m.captures[parent] = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.captures, parent)
		m.mu.Unlock()
	}()
	if current.FreezeOperationID != "" {
		if err := m.runtime.ThawXFS(current.XFSRoot); err != nil {
			unlock()
			return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("recover prior RootFS freeze: %w", err)
		}
		current.FreezeOperationID = ""
		if err := m.save(current); err != nil {
			unlock()
			return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("clear prior RootFS freeze intent: %w", err)
		}
	}
	current.RunningForkRequest = &request
	current.RunningForkResult = nil
	current.FreezeOperationID = request.OperationID
	current.Version = sessionSchemaVersion
	if err := m.save(current); err != nil {
		unlock()
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("persist RootFS freeze intent: %w", err)
	}
	if freezeErr := m.runtime.FreezeXFS(current.XFSRoot); freezeErr != nil {
		thawErr := m.runtime.ThawXFS(current.XFSRoot)
		if thawErr == nil {
			current.FreezeOperationID = ""
			current.RunningForkRequest = nil
			thawErr = m.save(current)
		}
		unlock()
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(
			fmt.Errorf("freeze running RootFS: %w", freezeErr),
			wrapIfError("recover failed RootFS freeze", thawErr),
		)
	}
	checkpoint, checkpointErr := live.branch.Checkpoint()
	thawErr := m.runtime.ThawXFS(current.XFSRoot)
	var clearErr error
	if thawErr == nil {
		current.FreezeOperationID = ""
		if checkpointErr != nil {
			current.RunningForkRequest = nil
		}
		clearErr = m.save(current)
	}
	unlock()
	if checkpointErr != nil || thawErr != nil || clearErr != nil {
		if checkpoint != nil {
			_ = checkpoint.Close()
		}
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(
			wrapIfError("checkpoint running RootFS", checkpointErr),
			wrapIfError("thaw running RootFS", thawErr),
			wrapIfError("clear RootFS freeze intent", clearErr),
		)
	}

	base, err := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
	if err != nil {
		_ = checkpoint.Close()
		clearErr := m.clearRunningForkCapture(parent, request)
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(
			fmt.Errorf("decode running fork base generation: %w", err), clearErr,
		)
	}
	sealed, payload, durability, err := buildBranchCheckpoint(ctx, checkpoint, base, m.source, m.publisher)
	checkpointSequence := checkpoint.Sequence()
	err = errors.Join(err, checkpoint.Close())
	if err != nil {
		clearErr := m.clearRunningForkCapture(parent, request)
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(
			fmt.Errorf("publish running fork checkpoint: %w", err), clearErr,
		)
	}
	generation := rootfshandoff.GenerationDescriptor{
		Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: request.TargetGenerationID,
		FilesystemID: request.TargetSandboxID, SourceOCIDigest: stage.Generation.SourceOCIDigest,
		BaseArtifactDigest: stage.Generation.BaseArtifactDigest, BaseBlockRoot: stage.Generation.BaseBlockRoot,
		CurrentBlockHead: sealed.MappingRoot.RootDigest, WriterEpoch: stage.Identity.WriterEpoch,
		FormatGeneration: stage.Generation.FormatGeneration, DurabilityState: durability,
		LocatorVersion: stage.Generation.LocatorVersion + 1, Descriptor: payload,
	}
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version: rootfshandoff.RunningForkCheckpointVersion, OperationID: request.OperationID,
		SourceSandboxID: request.SourceSandboxID, SourceFilesystemID: stage.Identity.RootFSID,
		TargetSandboxID: request.TargetSandboxID, SourceWriterGrantID: stage.Identity.WriterGrantID,
		SourceWriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: bindingText, ExpectedSourceGenerationID: stage.InitialGeneration,
		CheckpointGenerationID: request.TargetGenerationID, CheckpointSequence: checkpointSequence,
		CheckpointDescriptorDigest: digest.FromBytes(payload).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		clearErr := m.clearRunningForkCapture(parent, request)
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(err, clearErr)
	}
	result := rootfshandoff.RunningForkCheckpointResult{
		Generation: generation, Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	if err := result.Validate(); err != nil {
		clearErr := m.clearRunningForkCapture(parent, request)
		return rootfshandoff.RunningForkCheckpointResult{}, errors.Join(err, clearErr)
	}
	if err := m.storeRunningForkResult(parent, stage, bindingText, request, result); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	return result, nil
}

func (m *Manager) storeRunningForkResult(
	parent string,
	stage rootfshandoff.StageRequest,
	binding string,
	request rootfshandoff.RunningForkCheckpointRequest,
	result rootfshandoff.RunningForkCheckpointResult,
) error {
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return err
	}
	if current.RunningForkRequest == nil || *current.RunningForkRequest != request ||
		!sameBinding(current, stage, binding) || current.State != stateReady {
		if current.RunningForkRequest != nil && *current.RunningForkRequest == request && current.RunningForkResult == nil {
			current.RunningForkRequest = nil
			_ = m.save(current)
		}
		return fmt.Errorf("RootFS writer changed while its running fork published: %w", errdefs.ErrFailedPrecondition)
	}
	current.RunningForkResult = &result
	return m.save(current)
}

func (m *Manager) clearRunningForkCapture(parent string, request rootfshandoff.RunningForkCheckpointRequest) error {
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return err
	}
	if current.RunningForkRequest == nil {
		return nil
	}
	if *current.RunningForkRequest != request || current.RunningForkResult != nil {
		return fmt.Errorf("running fork capture ownership changed: %w", errdefs.ErrFailedPrecondition)
	}
	current.RunningForkRequest = nil
	return m.save(current)
}

func wrapIfError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func buildBranchCheckpoint(
	ctx context.Context,
	checkpoint *rootfsblock.BranchCheckpoint,
	base rootfsblock.Descriptor,
	source rootfsblock.RangeSource,
	publisher rootfsblock.ImmutableObjectPublisher,
) (rootfsblock.Descriptor, []byte, string, error) {
	records, err := checkpoint.DurableRecords()
	materialize := false
	var tooLarge *rootfsblock.CompositeTailTooLargeError
	if errors.As(err, &tooLarge) {
		err = nil
		materialize = true
	}
	if err != nil {
		return rootfsblock.Descriptor{}, nil, "", err
	}
	var sealed rootfsblock.Descriptor
	var payload []byte
	if !materialize {
		sealed, payload, err = rootfsblock.BuildCompositeGeneration(base, records)
		if errors.As(err, &tooLarge) {
			err = nil
			materialize = true
		}
	}
	if materialize {
		var built rootfsblock.BuildResult
		built, err = rootfsblock.BuildIncrementalGenerationFromBlockReader(
			ctx, source, base, checkpoint, publisher, rootfsblock.BuildOptions{},
		)
		if err == nil {
			sealed = built.Descriptor
			payload = built.Payload
		}
	}
	if err != nil {
		return rootfsblock.Descriptor{}, nil, "", err
	}
	durability := rootfsblock.DurabilityS3
	if sealed.CompositeTail != nil {
		durability = rootfsblock.DurabilityComposite
	}
	return sealed, payload, durability, nil
}

// RecoverySessions enumerates every durable session needed by an independent
// node reconciler. Older records remain visible as RecoveryUnavailable and
// must be fenced by their legacy owner; they are never guessed from partial
// identities.
func (m *Manager) RecoverySessions() ([]RecoverySession, error) {
	m.mu.Lock()
	live := make(map[string]bool, len(m.live))
	for parent := range m.live {
		live[parent] = true
	}
	m.mu.Unlock()

	result := make([]RecoverySession, 0)
	err := m.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(sessionBucket).ForEach(func(key, payload []byte) error {
			if payload == nil {
				return nil
			}
			var current record
			if err := json.Unmarshal(payload, &current); err != nil {
				return fmt.Errorf("decode RootFS session %q: %w", key, err)
			}
			if current.Parent != string(key) || !supportedSessionVersion(current.Version) {
				return fmt.Errorf("invalid RootFS recovery record %q version %d", key, current.Version)
			}
			if current.Version >= durableBindingSchemaVersion && current.Stage == nil {
				return fmt.Errorf("RootFS recovery record %q lacks its durable Stage binding", key)
			}
			recovery := RecoverySession{
				Kind: RecoveryUnavailable, State: current.State,
				RetireOperationID: current.RetireOperationID, BranchRemoved: current.BranchRemoved,
				Live: live[current.Parent],
			}
			createdAt, err := time.Parse(time.RFC3339Nano, current.CreatedAt)
			if err != nil {
				return fmt.Errorf("parse RootFS recovery creation time %q: %w", key, err)
			}
			recovery.CreatedAt = createdAt
			if current.CrashFence != nil {
				recovery.CrashOperationID = current.CrashFence.OperationID
				recovery.ExternalCrash = current.CrashFence.External
				requestedAt, err := time.Parse(time.RFC3339Nano, current.CrashFence.RequestedAt)
				if err != nil {
					return fmt.Errorf("parse RootFS crash request time %q: %w", key, err)
				}
				recovery.CrashRequestedAt = requestedAt
			}
			if current.Consumer != nil {
				consumer := *current.Consumer
				if _, err := consumer.Validate(); err != nil {
					return fmt.Errorf("validate RootFS consumer %q: %w", key, err)
				}
				recovery.Consumer = &consumer
			}
			if current.Stage != nil {
				stage := cloneDurableStage(*current.Stage)
				if stage.Identity.WriterGrantToken != "" {
					return fmt.Errorf("RootFS recovery record %q contains a raw writer token", key)
				}
				if err := stage.ValidateDurableBinding(); err != nil || stage.Generation == nil {
					return fmt.Errorf("validate RootFS recovery binding %q: %v", key, err)
				}
				binding, err := stage.BindingDigest()
				if err != nil || !sameBinding(current, stage, hex.EncodeToString(binding[:])) {
					return fmt.Errorf("RootFS recovery binding %q does not match its physical session", key)
				}
				recovery.Stage = stage
				if current.RetireOperationID != "" {
					recovery.Kind = RecoveryPlannedRetire
				} else {
					recovery.Kind = RecoveryCrashAbandon
				}
			}
			result = append(result, recovery)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Validate checks a durable consumer record and returns its wall-clock lease
// deadline. The daemon, not the task plugin, chooses and enforces this value.
func (c ConsumerRegistration) Validate() (time.Time, error) {
	for name, value := range map[string]string{
		"lease_id": c.LeaseID, "active_key": c.ActiveKey, "container_id": c.ContainerID,
		"stable_mount": c.StableMount, "host_mount_namespace": c.HostMountNamespace,
		"lease_expires_at": c.LeaseExpiresAt,
	} {
		if strings.TrimSpace(value) == "" {
			return time.Time{}, fmt.Errorf("%s is required", name)
		}
	}
	if !filepath.IsAbs(c.StableMount) || filepath.Clean(c.StableMount) == string(filepath.Separator) {
		return time.Time{}, fmt.Errorf("stable_mount must be a non-root absolute path")
	}
	networkFields := 0
	for _, value := range []string{c.NetNSPath, c.NetNSIdentity, c.NetworkChain} {
		if strings.TrimSpace(value) != "" {
			networkFields++
		}
	}
	if networkFields != 0 && networkFields != 3 {
		return time.Time{}, fmt.Errorf("network namespace path, identity, and chain must be configured together")
	}
	if networkFields == 3 && (!filepath.IsAbs(c.NetNSPath) || filepath.Clean(c.NetNSPath) == string(filepath.Separator)) {
		return time.Time{}, fmt.Errorf("netns_path must be a non-root absolute path")
	}
	deadline, err := time.Parse(time.RFC3339Nano, c.LeaseExpiresAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse lease_expires_at: %w", err)
	}
	return deadline, nil
}

// RegisterConsumer durably binds one host runtime and plugin liveness lease to
// a ready physical session. Re-registration may rotate only LeaseID and its
// deadline; immutable runtime paths cannot be replaced under the same writer.
func (m *Manager) RegisterConsumer(parent string, identity rootfshandoff.Identity, consumer ConsumerRegistration) error {
	consumer.LeaseID = strings.TrimSpace(consumer.LeaseID)
	consumer.ActiveKey = strings.TrimSpace(consumer.ActiveKey)
	consumer.ContainerID = strings.TrimSpace(consumer.ContainerID)
	consumer.StableMount = filepath.Clean(strings.TrimSpace(consumer.StableMount))
	consumer.HostMountNamespace = strings.TrimSpace(consumer.HostMountNamespace)
	if strings.TrimSpace(consumer.NetNSPath) != "" {
		consumer.NetNSPath = filepath.Clean(strings.TrimSpace(consumer.NetNSPath))
	}
	consumer.NetNSIdentity = strings.TrimSpace(consumer.NetNSIdentity)
	consumer.NetworkChain = strings.TrimSpace(consumer.NetworkChain)
	consumer.LeaseExpiresAt = strings.TrimSpace(consumer.LeaseExpiresAt)
	deadline, err := consumer.Validate()
	if err != nil || !deadline.After(time.Now()) {
		return fmt.Errorf("invalid RootFS consumer registration: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch || current.State != stateReady {
		return fmt.Errorf("RootFS consumer does not match a ready writer: %w", errdefs.ErrFailedPrecondition)
	}
	m.mu.Lock()
	_, live := m.live[parent]
	m.mu.Unlock()
	if !live {
		return fmt.Errorf("RootFS session has no live userspace device owner: %w", errdefs.ErrUnavailable)
	}
	if current.Consumer != nil && (current.Consumer.ActiveKey != consumer.ActiveKey ||
		current.Consumer.ContainerID != consumer.ContainerID || current.Consumer.StableMount != consumer.StableMount ||
		current.Consumer.HostMountNamespace != consumer.HostMountNamespace ||
		current.Consumer.NetNSPath != consumer.NetNSPath || current.Consumer.NetNSIdentity != consumer.NetNSIdentity ||
		current.Consumer.NetworkChain != consumer.NetworkChain) {
		return fmt.Errorf("RootFS session is bound to another host runtime consumer: %w", errdefs.ErrAlreadyExists)
	}
	current.Consumer = &consumer
	return m.save(current)
}

// RenewConsumer extends one exact daemon-issued liveness lease. A stale plugin
// instance cannot renew after re-registration rotates LeaseID.
func (m *Manager) RenewConsumer(parent string, identity rootfshandoff.Identity, leaseID string, expiresAt time.Time) error {
	leaseID = strings.TrimSpace(leaseID)
	if leaseID == "" || !expiresAt.After(time.Now()) {
		return fmt.Errorf("consumer lease and future expiry are required: %w", errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	current, err := m.load(parent)
	if err != nil {
		return err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch || current.Consumer == nil ||
		current.Consumer.LeaseID != leaseID || current.State != stateReady {
		return fmt.Errorf("RootFS consumer lease does not match the ready writer: %w", errdefs.ErrFailedPrecondition)
	}
	current.Consumer.LeaseExpiresAt = expiresAt.UTC().Format(time.RFC3339Nano)
	return m.save(current)
}

func (m *Manager) reconcileDeviceReservations() error {
	type reservation struct {
		owner        string
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
				if current.Version >= allocationSessionSchemaVersion && current.DeviceAllocationID != "" && !current.DeviceReservationReleased {
					current.DeviceReservationReleased = true
					changed = true
				}
			} else {
				if strings.TrimSpace(current.DeviceAllocationID) == "" {
					if current.Version != legacySessionSchemaVersion {
						return fmt.Errorf("current RootFS session %q lacks a device allocation identity", current.Parent)
					}
					current.DeviceAllocationID = legacyDeviceAllocationID(current.Parent, current.DevicePath)
					current.Version = allocationSessionSchemaVersion
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
					owner: current.Parent, devicePath: current.DevicePath, allocationID: current.DeviceAllocationID,
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
		rebases := tx.Bucket(rebaseBucket)
		if err := rebases.ForEach(func(key, payload []byte) error {
			if payload == nil {
				return nil
			}
			var current rebaseRecord
			if err := json.Unmarshal(payload, &current); err != nil {
				return fmt.Errorf("decode RootFS rebase %q: %w", key, err)
			}
			if err := m.validateRebaseRecord(current, string(key)); err != nil {
				return err
			}
			for _, resource := range current.Resources {
				if resource.DevicePath == "" || resource.DeviceReservationReleased {
					continue
				}
				owner := "rebase:" + current.OperationID + ":" + resource.Role
				if previous := owners[resource.DevicePath]; previous != "" && previous != owner {
					return fmt.Errorf(
						"NBD device %s is reserved by both %q and %q",
						resource.DevicePath, previous, owner,
					)
				}
				owners[resource.DevicePath] = owner
				reservations = append(reservations, reservation{
					owner: owner, devicePath: resource.DevicePath, allocationID: resource.DeviceAllocationID,
				})
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, reservation := range reservations {
		if err := m.runtime.AdoptDeviceReservation(reservation.devicePath, reservation.allocationID); err != nil {
			return fmt.Errorf("adopt device reservation for %q: %w", reservation.owner, err)
		}
	}
	return nil
}

// Reserve durably records the tokenless writer binding before the regional
// grant is consumed. A node daemon can therefore reconcile an attach whose
// caller disappears between grant consumption and physical device setup.
func (m *Manager) Reserve(request rootfshandoff.StageRequest) error {
	durable := request.WithoutWriterGrantToken()
	if err := durable.ValidateDurableBinding(); err != nil || durable.Generation == nil {
		return fmt.Errorf("invalid durable generation binding: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(durable.Parent)
	defer unlock()
	binding, err := durable.BindingDigest()
	if err != nil {
		return err
	}
	bindingText := hex.EncodeToString(binding[:])
	current, err := m.load(durable.Parent)
	if err == nil {
		if !sameBinding(current, durable, bindingText) {
			return fmt.Errorf("RootFS session parent is bound to another generation: %w", errdefs.ErrAlreadyExists)
		}
		return nil
	}
	if !errdefs.IsNotFound(err) {
		return err
	}
	current, err = m.newReservedRecord(durable, bindingText)
	if err != nil {
		return err
	}
	return m.saveNew(current)
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
		if current.State != stateReserved || current.DevicePath != "" {
			return Mount{}, fmt.Errorf("incomplete RootFS session requires startup reconciliation: %w", errdefs.ErrUnavailable)
		}
		if current.Stage == nil {
			stage := cloneDurableStage(durable)
			current.Stage = &stage
			current.Version = sessionSchemaVersion
			if err := m.save(current); err != nil {
				return Mount{}, err
			}
		}
	}
	if err != nil && !errdefs.IsNotFound(err) {
		return Mount{}, err
	}
	if errdefs.IsNotFound(err) {
		current, err = m.newReservedRecord(durable, bindingText)
		if err != nil {
			return Mount{}, err
		}
		if err := m.saveNew(current); err != nil {
			return Mount{}, err
		}
	}

	descriptor, err := rootfsblock.DecodeDescriptor(durable.Generation.Descriptor)
	if err != nil {
		return Mount{}, m.fail(current, fmt.Errorf("decode immutable generation: %w", err))
	}
	reader, err := rootfsblock.NewReaderWithCache(m.source, descriptor, m.readCache)
	if err != nil {
		return Mount{}, m.fail(current, fmt.Errorf("open immutable generation: %w", err))
	}
	branch, err := rootfsblock.OpenBranchWithOptions(current.BranchPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
		GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch,
		LogicalSizeBytes: int64(reader.Size()), BaseRootDigest: durable.Generation.CurrentBlockHead,
	}, reader, rootfsblock.BranchOptions{MaxDirtyTailBytes: m.maxDirty})
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

// ReclaimTerminalArtifacts deletes the node-local COW journal and boot-local mount
// directories only after the caller has made the corresponding planned
// publication or crash abandonment terminal at the regional authority. The
// durable session record and terminal proof remain available for idempotent
// retries and audit.
func (m *Manager) ReclaimTerminalArtifacts(parent string, identity rootfshandoff.Identity) error {
	if strings.TrimSpace(parent) == "" || strings.TrimSpace(identity.RootFSID) == "" || identity.WriterEpoch <= 0 {
		return fmt.Errorf("parent and writer identity are required: %w", errdefs.ErrInvalidArgument)
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
	terminal, err := terminalDeviceProof(current)
	if err != nil {
		return err
	}
	if current.State != stateTombstoned || !terminal {
		return fmt.Errorf("RootFS session has no terminal detach proof: %w", errdefs.ErrFailedPrecondition)
	}
	expected := sessionPaths(m.branchRoot, m.mountRoot, parent).branch
	if current.BranchPath != expected {
		return fmt.Errorf("RootFS branch path does not match its session identity: %w", errdefs.ErrFailedPrecondition)
	}
	if !current.BranchRemoved {
		if err := os.Remove(expected); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove terminal RootFS branch: %w", err)
		}
		current.BranchRemoved = true
		if err := m.save(current); err != nil {
			return err
		}
	}
	mountRoot := filepath.Dir(sessionPaths(m.branchRoot, m.mountRoot, parent).xfs)
	if filepath.Dir(mountRoot) != m.mountRoot {
		return fmt.Errorf("RootFS mount path does not match its session identity: %w", errdefs.ErrFailedPrecondition)
	}
	if err := os.RemoveAll(mountRoot); err != nil {
		return fmt.Errorf("remove terminal RootFS mount directory: %w", err)
	}
	return nil
}

// ForgetVerifiedTerminal removes one terminal session record after the caller
// has verified the exact writer binding as retired at the regional authority.
// Physical artifacts and the in-memory device owner must already be absent.
// Forgetting only after regional verification keeps retry safety in the
// durable authority while allowing the node journal to remain bounded by
// active and unreconciled sessions instead of lifetime churn.
func (m *Manager) ForgetVerifiedTerminal(parent string, identity rootfshandoff.Identity) error {
	if strings.TrimSpace(parent) == "" || strings.TrimSpace(identity.RootFSID) == "" || identity.WriterEpoch <= 0 {
		return fmt.Errorf("parent and writer identity are required: %w", errdefs.ErrInvalidArgument)
	}
	unlock := m.lock(parent)
	defer unlock()
	m.mu.Lock()
	_, live := m.live[parent]
	m.mu.Unlock()
	if live {
		return fmt.Errorf("RootFS session still has a live userspace owner: %w", errdefs.ErrFailedPrecondition)
	}
	current, err := m.load(parent)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if current.RootFSID != identity.RootFSID || current.WriterEpoch != identity.WriterEpoch {
		return fmt.Errorf("RootFS session belongs to another writer identity: %w", errdefs.ErrFailedPrecondition)
	}
	terminal, err := terminalDeviceProof(current)
	if err != nil {
		return err
	}
	if current.State != stateTombstoned || !terminal || !current.BranchRemoved {
		return fmt.Errorf("RootFS session has not reclaimed verified terminal artifacts: %w", errdefs.ErrFailedPrecondition)
	}
	paths := sessionPaths(m.branchRoot, m.mountRoot, parent)
	if current.BranchPath != paths.branch {
		return fmt.Errorf("RootFS branch path does not match its session identity: %w", errdefs.ErrFailedPrecondition)
	}
	for _, path := range []string{paths.branch, filepath.Dir(paths.xfs)} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("terminal RootFS artifact %q still exists: %w", path, errdefs.ErrFailedPrecondition)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect terminal RootFS artifact %q: %w", path, err)
		}
	}
	return m.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(sessionBucket)
		identityBucket := tx.Bucket(sessionIdentityBucket)
		payload := bucket.Get([]byte(parent))
		if payload == nil {
			return nil
		}
		var stored record
		if err := json.Unmarshal(payload, &stored); err != nil {
			return fmt.Errorf("decode RootFS session %q: %w", parent, err)
		}
		if stored.RootFSID != identity.RootFSID || stored.WriterEpoch != identity.WriterEpoch ||
			stored.State != stateTombstoned || !stored.BranchRemoved {
			return fmt.Errorf("RootFS session changed before terminal forget: %w", errdefs.ErrFailedPrecondition)
		}
		identityKey := writerIdentityKey(identity.RootFSID, identity.WriterEpoch)
		if indexed := identityBucket.Get(identityKey); string(indexed) != parent {
			return fmt.Errorf("RootFS writer identity index does not match terminal session: %w", errdefs.ErrFailedPrecondition)
		}
		if err := identityBucket.Delete(identityKey); err != nil {
			return err
		}
		return bucket.Delete([]byte(parent))
	})
}

// CrashFence durably proves that a non-cooperatively stopped session has no
// remaining userspace owner, mount, or NBD endpoint. It never seals or
// publishes the branch. The same operation is idempotent; a competing
// operation can never replace an existing intent.
func (m *Manager) CrashFence(
	request rootfshandoff.StageRequest,
	operationID string,
) (rootfshandoff.CrashFenceSessionObservation, error) {
	return m.crashFence(request, operationID, false)
}

// CrashFenceExternal records a physical terminal proof owned by a separate
// regional controller. The node daemon must not complete regional retirement
// for this intent because that controller binds a broader runtime-slot proof.
func (m *Manager) CrashFenceExternal(
	request rootfshandoff.StageRequest,
	operationID string,
) (rootfshandoff.CrashFenceSessionObservation, error) {
	return m.crashFence(request, operationID, true)
}

func (m *Manager) crashFence(
	request rootfshandoff.StageRequest,
	operationID string,
	external bool,
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
		if current.CrashFence.External != external {
			return rootfshandoff.CrashFenceSessionObservation{}, fmt.Errorf("RootFS crash fence authority owner changed: %w", errdefs.ErrFailedPrecondition)
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
			External:    external,
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
	} else if current.Version >= allocationSessionSchemaVersion && strings.TrimSpace(current.DeviceAllocationID) != "" {
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
			Version: allocationSessionSchemaVersion, Parent: parent, RootFSID: identity.RootFSID,
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
	if current.FreezeOperationID != "" {
		if err := m.runtime.ThawXFS(current.XFSRoot); err != nil {
			return fmt.Errorf("thaw RootFS before release: %w", err)
		}
		current.FreezeOperationID = ""
	}
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
		checkpoint := (*rootfsblock.BranchCheckpoint)(nil)
		closeBranch := false
		if live != nil {
			branch = live.branch
		} else {
			branch, releaseErr = m.reopenBranch(current)
			closeBranch = branch != nil
		}
		if releaseErr == nil {
			checkpoint, releaseErr = branch.Checkpoint()
			if releaseErr != nil {
				releaseErr = fmt.Errorf("checkpoint retiring branch: %w", releaseErr)
			}
		}
		if releaseErr == nil {
			base, decodeErr := rootfsblock.DecodeDescriptor(current.BaseDescriptor)
			if decodeErr != nil {
				releaseErr = fmt.Errorf("decode retiring base generation: %w", decodeErr)
			} else {
				sealed, payload, durability, buildErr := buildBranchCheckpoint(ctx, checkpoint, base, m.source, m.publisher)
				if buildErr != nil {
					releaseErr = fmt.Errorf("seal durable generation: %w", buildErr)
				} else {
					sealedDescriptor = payload
					sealedBlockHead = sealed.MappingRoot.RootDigest
					sealedDurability = durability
				}
			}
		}
		if checkpoint != nil {
			releaseErr = errors.Join(releaseErr, checkpoint.Close())
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
	branch, err := rootfsblock.OpenBranchWithOptions(current.BranchPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: current.RootFSID,
		GenerationID: current.GenerationID, WriterEpoch: current.WriterEpoch,
		LogicalSizeBytes: int64(reader.Size()), BaseRootDigest: descriptor.MappingRoot.RootDigest,
	}, reader, rootfsblock.BranchOptions{MaxDirtyTailBytes: m.maxDirty})
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
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return nil
	}
	m.closing = true
	m.mu.Unlock()
	m.cancel()
	m.rebaseWG.Wait()
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

func (m *Manager) newReservedRecord(durable rootfshandoff.StageRequest, bindingText string) (record, error) {
	paths := sessionPaths(m.branchRoot, m.mountRoot, durable.Parent)
	allocationID, err := newDeviceAllocationID()
	if err != nil {
		return record{}, fmt.Errorf("generate NBD allocation identity: %w", err)
	}
	stage := cloneDurableStage(durable)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return record{
		Version: sessionSchemaVersion, Parent: durable.Parent, BindingDigest: bindingText,
		RootFSID: durable.Identity.RootFSID, WriterEpoch: durable.Identity.WriterEpoch,
		GenerationID:   durable.InitialGeneration,
		BaseDescriptor: append([]byte(nil), durable.Generation.Descriptor...), BranchPath: paths.branch,
		DeviceAllocationID: allocationID, XFSRoot: paths.xfs, MergedRoot: paths.merged,
		State: stateReserved, CreatedAt: now, UpdatedAt: now, Stage: &stage,
	}, nil
}

func cloneDurableStage(stage rootfshandoff.StageRequest) rootfshandoff.StageRequest {
	stage.Identity.WriterGrantToken = ""
	if stage.Generation != nil {
		generation := *stage.Generation
		generation.Descriptor = append([]byte(nil), stage.Generation.Descriptor...)
		stage.Generation = &generation
	}
	if stage.Labels != nil {
		labels := make(map[string]string, len(stage.Labels))
		for key, value := range stage.Labels {
			labels[key] = value
		}
		stage.Labels = labels
	}
	return stage
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
	return version >= legacySessionSchemaVersion && version <= sessionSchemaVersion
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
