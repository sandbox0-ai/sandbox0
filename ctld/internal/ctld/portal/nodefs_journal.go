package portal

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
	"golang.org/x/sys/unix"
)

const (
	nodeFSJournalVersion = 1
	nodeFSStateDirName   = "nodefs"
	nodeFSStateFileName  = "state.json"
	nodeFSLockFileName   = "ctld.lock"
)

type nodeFSPortalPhase string

const (
	nodeFSPortalAllocating   nodeFSPortalPhase = "allocating"
	nodeFSPortalPublished    nodeFSPortalPhase = "published"
	nodeFSPortalUnpublishing nodeFSPortalPhase = "unpublishing"
)

type nodeFSJournal struct {
	Version              int                 `json:"version"`
	NodeIdentity         string              `json:"node_identity"`
	ConnectionGeneration string              `json:"connection_generation"`
	ShardCount           int                 `json:"shard_count"`
	RecoveryRequired     bool                `json:"recovery_required"`
	NextSlotByShard      []uint64            `json:"next_slot_by_shard"`
	Shards               []nodeFSShardState  `json:"shards"`
	Portals              []nodeFSPortalState `json:"portals"`
}

// ConfigureRecovery makes the recovery contract immutable once a shard has
// completed INIT or a portal has been allocated. This prevents a rollout from
// silently replacing recoverable connections with ordinary FUSE mounts.
func (s *nodeFSJournalStore) ConfigureRecovery(required bool) error {
	return s.Update(func(state *nodeFSJournal) error {
		if state.RecoveryRequired == required {
			return nil
		}
		for _, shard := range state.Shards {
			if len(shard.SessionState) > 0 {
				return fmt.Errorf("cannot change nodefs recovery mode after shard initialization")
			}
		}
		if len(state.Portals) > 0 {
			return fmt.Errorf("cannot change nodefs recovery mode with committed portals")
		}
		state.RecoveryRequired = required
		return nil
	})
}

type nodeFSShardState struct {
	Index        int             `json:"index"`
	Tag          string          `json:"tag"`
	MountPath    string          `json:"mount_path"`
	SessionState json.RawMessage `json:"session_state,omitempty"`
}

type nodeFSPortalState struct {
	PortalKey     string            `json:"portal_key"`
	PodUID        string            `json:"pod_uid"`
	Namespace     string            `json:"namespace"`
	PodName       string            `json:"pod_name"`
	Name          string            `json:"name"`
	MountPath     string            `json:"mount_path"`
	TargetPath    string            `json:"target_path"`
	Shard         int               `json:"shard"`
	Slot          uint64            `json:"slot"`
	RootFSBacking string            `json:"rootfs_backing_path"`
	Backend       string            `json:"backend"`
	VolumeID      string            `json:"volume_id,omitempty"`
	TeamID        string            `json:"team_id,omitempty"`
	MountedAt     time.Time         `json:"mounted_at,omitempty"`
	Phase         nodeFSPortalPhase `json:"phase"`
}

type nodeFSJournalStore struct {
	mu   sync.Mutex
	path string
	data nodeFSJournal
}

type nodeFSProcessLock struct {
	file *os.File
}

// openNodeFSJournal opens the node-local desired-state journal. A journal is
// bound to one node identity and one immutable shard count until the node is
// drained and the FUSE connection generation is replaced.
func openNodeFSJournal(rootDir, nodeIdentity string, shardCount int) (*nodeFSJournalStore, error) {
	rootDir = strings.TrimSpace(rootDir)
	nodeIdentity = strings.TrimSpace(nodeIdentity)
	if rootDir == "" {
		return nil, fmt.Errorf("nodefs journal root is required")
	}
	if nodeIdentity == "" {
		return nil, fmt.Errorf("nodefs node identity is required")
	}
	if shardCount <= 0 {
		return nil, fmt.Errorf("nodefs shard count must be positive")
	}

	dir := filepath.Join(rootDir, nodeFSStateDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create nodefs state directory: %w", err)
	}
	path := filepath.Join(dir, nodeFSStateFileName)
	data, err := loadNodeFSJournal(path)
	if errors.Is(err, os.ErrNotExist) {
		data = newNodeFSJournal(nodeIdentity, shardCount)
		if err := writeNodeFSJournal(path, data); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if data.NodeIdentity != nodeIdentity {
		return nil, fmt.Errorf("nodefs journal belongs to node %q, not %q", data.NodeIdentity, nodeIdentity)
	}
	if data.ShardCount != shardCount {
		return nil, fmt.Errorf("nodefs shard count changed from %d to %d; drain the node before changing it", data.ShardCount, shardCount)
	}
	if err := validateNodeFSJournal(data); err != nil {
		return nil, fmt.Errorf("validate nodefs journal: %w", err)
	}
	if err := validateNodeFSShardIdentity(data, rootDir); err != nil {
		return nil, fmt.Errorf("validate nodefs shard identity: %w", err)
	}
	return &nodeFSJournalStore{path: path, data: data}, nil
}

func newNodeFSJournal(nodeIdentity string, shardCount int) nodeFSJournal {
	nextSlots := make([]uint64, shardCount)
	for i := range nextSlots {
		nextSlots[i] = 1
	}
	return nodeFSJournal{
		Version:              nodeFSJournalVersion,
		NodeIdentity:         nodeIdentity,
		ConnectionGeneration: uuid.NewString(),
		ShardCount:           shardCount,
		NextSlotByShard:      nextSlots,
		Shards:               make([]nodeFSShardState, 0, shardCount),
		Portals:              []nodeFSPortalState{},
	}
}

func loadNodeFSJournal(path string) (nodeFSJournal, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nodeFSJournal{}, err
	}
	var state nodeFSJournal
	if err := json.Unmarshal(data, &state); err != nil {
		return nodeFSJournal{}, fmt.Errorf("decode nodefs journal %s: %w", path, err)
	}
	return state, nil
}

// Snapshot returns a detached copy so callers cannot mutate committed state
// without going through Update.
func (s *nodeFSJournalStore) Snapshot() nodeFSJournal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneNodeFSJournal(s.data)
}

// Update validates and atomically commits one lifecycle transition. It is not
// used by the file-operation hot path.
func (s *nodeFSJournalStore) Update(update func(*nodeFSJournal) error) error {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return fmt.Errorf("nodefs journal is not initialized")
	}
	if update == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	next := cloneNodeFSJournal(s.data)
	if err := update(&next); err != nil {
		return err
	}
	if err := validateNodeFSJournal(next); err != nil {
		return err
	}
	if err := writeNodeFSJournal(s.path, next); err != nil {
		return err
	}
	s.data = next
	return nil
}

// PrepareShards durably records the stable connection tags and host mount
// paths before any FUSE connection is created. A crash before the INIT state
// is committed is safe because portals cannot be allocated until all shards
// have completed initialization.
func (s *nodeFSJournalStore) PrepareShards(rootDir string) error {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return fmt.Errorf("nodefs shard root is required")
	}
	return s.Update(func(state *nodeFSJournal) error {
		if len(state.Shards) > 0 {
			if len(state.Shards) != state.ShardCount {
				return fmt.Errorf("nodefs journal has %d of %d shards", len(state.Shards), state.ShardCount)
			}
			return nil
		}
		state.Shards = make([]nodeFSShardState, state.ShardCount)
		for index := range state.Shards {
			state.Shards[index] = nodeFSShardState{
				Index:     index,
				Tag:       nodeFSConnectionTag(state.NodeIdentity, state.ConnectionGeneration, index),
				MountPath: nodeFSShardMountPath(rootDir, index),
			}
		}
		return nil
	})
}

func nodeFSShardMountPath(rootDir string, index int) string {
	return filepath.Clean(filepath.Join(rootDir, nodeFSStateDirName, "shards", fmt.Sprintf("%d", index), "mount"))
}

// validateNodeFSShardIdentity binds recovery tags and cleanup paths to this
// journal generation. Startup must reject modified state before attempting a
// recovery ioctl or unmounting any path from the journal.
func validateNodeFSShardIdentity(state nodeFSJournal, rootDir string) error {
	rootDir = filepath.Clean(strings.TrimSpace(rootDir))
	if rootDir == "." || !filepath.IsAbs(rootDir) {
		return fmt.Errorf("nodefs shard root must be absolute")
	}
	for _, shard := range state.Shards {
		expectedTag := nodeFSConnectionTag(state.NodeIdentity, state.ConnectionGeneration, shard.Index)
		if shard.Tag != expectedTag {
			return fmt.Errorf("shard %d recovery tag does not match journal generation", shard.Index)
		}
		expectedMountPath := nodeFSShardMountPath(rootDir, shard.Index)
		if shard.MountPath != expectedMountPath {
			return fmt.Errorf("shard %d mount path %q does not match %q", shard.Index, shard.MountPath, expectedMountPath)
		}
	}
	return nil
}

func nodeFSConnectionTag(nodeIdentity, generation string, shard int) string {
	digest := sha256.Sum256([]byte(nodeIdentity + "\x00" + generation + "\x00" + fmt.Sprint(shard)))
	return fmt.Sprintf("sandbox0-%x-%d", digest[:20], shard)
}

func (s *nodeFSJournalStore) CommitShardSession(index int, session json.RawMessage) error {
	if len(session) == 0 || !json.Valid(session) {
		return fmt.Errorf("nodefs shard %d session state is not valid JSON", index)
	}
	return s.updateShard(index, func(shard *nodeFSShardState) error {
		shard.SessionState = slices.Clone(session)
		return nil
	})
}

func (s *nodeFSJournalStore) ClearShardSession(index int) error {
	return s.Update(func(state *nodeFSJournal) error {
		for _, portal := range state.Portals {
			if portal.Shard == index {
				return fmt.Errorf("cannot replace nodefs shard %d with committed portals", index)
			}
		}
		for i := range state.Shards {
			if state.Shards[i].Index == index {
				state.Shards[i].SessionState = nil
				return nil
			}
		}
		return fmt.Errorf("nodefs shard %d is not prepared", index)
	})
}

func (s *nodeFSJournalStore) updateShard(index int, update func(*nodeFSShardState) error) error {
	return s.Update(func(state *nodeFSJournal) error {
		for i := range state.Shards {
			if state.Shards[i].Index == index {
				return update(&state.Shards[i])
			}
		}
		return fmt.Errorf("nodefs shard %d is not prepared", index)
	})
}

// AllocatePortal reserves a stable shard and slot before any mount becomes
// visible. The monotonically increasing slot high-water mark is never reduced
// within a connection generation, so stale kernel IDs cannot name a new portal.
func (s *nodeFSJournalStore) AllocatePortal(portal nodeFSPortalState) (nodeFSPortalState, error) {
	var allocated nodeFSPortalState
	err := s.Update(func(state *nodeFSJournal) error {
		for _, existing := range state.Portals {
			if existing.PortalKey != portal.PortalKey {
				continue
			}
			if existing.PodUID != portal.PodUID || existing.Name != portal.Name || existing.TargetPath != portal.TargetPath {
				return fmt.Errorf("portal %q allocation conflicts with committed state", portal.PortalKey)
			}
			allocated = existing
			return nil
		}

		shard := nodeFSShardForPortal(portal.PortalKey, state.ShardCount)
		initialized := false
		for _, shardState := range state.Shards {
			if shardState.Index == shard && len(shardState.SessionState) > 0 {
				initialized = true
				break
			}
		}
		if !initialized {
			return fmt.Errorf("nodefs shard %d is not initialized", shard)
		}
		slot := state.NextSlotByShard[shard]
		if slot == 0 || slot > uint64(nodefs.MaxSlot) {
			return fmt.Errorf("nodefs shard %d exhausted portal slots", shard)
		}
		state.NextSlotByShard[shard] = slot + 1
		portal.Shard = shard
		portal.Slot = slot
		portal.Phase = nodeFSPortalAllocating
		if strings.TrimSpace(portal.Backend) == "" {
			portal.Backend = "rootfs"
		}
		state.Portals = append(state.Portals, portal)
		slices.SortFunc(state.Portals, func(a, b nodeFSPortalState) int {
			if a.Shard != b.Shard {
				return a.Shard - b.Shard
			}
			if a.Slot < b.Slot {
				return -1
			}
			if a.Slot > b.Slot {
				return 1
			}
			return strings.Compare(a.PortalKey, b.PortalKey)
		})
		allocated = portal
		return nil
	})
	return allocated, err
}

func nodeFSShardForPortal(portalKey string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(portalKey))
	return int(hash.Sum32() % uint32(shardCount))
}

func (s *nodeFSJournalStore) MarkPortalPublished(portalKey string) error {
	return s.updatePortal(portalKey, func(portal *nodeFSPortalState) error {
		if portal.Phase == nodeFSPortalUnpublishing {
			return fmt.Errorf("portal %q is unpublishing", portalKey)
		}
		portal.Phase = nodeFSPortalPublished
		return nil
	})
}

// UpdatePortalBinding commits desired backend state before the in-memory
// router changes session, allowing recovery to finish an interrupted bind.
func (s *nodeFSJournalStore) UpdatePortalBinding(portalKey, backend, volumeID, teamID string, mountedAt time.Time) error {
	backend = strings.TrimSpace(backend)
	if backend == "" {
		return fmt.Errorf("portal backend is required")
	}
	return s.updatePortal(portalKey, func(portal *nodeFSPortalState) error {
		if portal.Phase != nodeFSPortalPublished {
			return fmt.Errorf("portal %q is not published", portalKey)
		}
		portal.Backend = backend
		portal.VolumeID = strings.TrimSpace(volumeID)
		portal.TeamID = strings.TrimSpace(teamID)
		portal.MountedAt = mountedAt.UTC()
		return nil
	})
}

func (s *nodeFSJournalStore) BeginPortalUnpublish(portalKey string) error {
	return s.updatePortal(portalKey, func(portal *nodeFSPortalState) error {
		portal.Phase = nodeFSPortalUnpublishing
		return nil
	})
}

func (s *nodeFSJournalStore) RemovePortal(portalKey string) error {
	return s.Update(func(state *nodeFSJournal) error {
		for i := range state.Portals {
			if state.Portals[i].PortalKey != portalKey {
				continue
			}
			if state.Portals[i].Phase != nodeFSPortalUnpublishing {
				return fmt.Errorf("portal %q must be unpublishing before removal", portalKey)
			}
			state.Portals = slices.Delete(state.Portals, i, i+1)
			return nil
		}
		return nil
	})
}

func (s *nodeFSJournalStore) Portal(portalKey string) (nodeFSPortalState, bool) {
	state := s.Snapshot()
	for _, portal := range state.Portals {
		if portal.PortalKey == portalKey {
			return portal, true
		}
	}
	return nodeFSPortalState{}, false
}

func (s *nodeFSJournalStore) updatePortal(portalKey string, update func(*nodeFSPortalState) error) error {
	portalKey = strings.TrimSpace(portalKey)
	if portalKey == "" {
		return fmt.Errorf("portal key is required")
	}
	return s.Update(func(state *nodeFSJournal) error {
		for i := range state.Portals {
			if state.Portals[i].PortalKey == portalKey {
				return update(&state.Portals[i])
			}
		}
		return fmt.Errorf("portal %q is not allocated", portalKey)
	})
}

func cloneNodeFSJournal(state nodeFSJournal) nodeFSJournal {
	cloned := state
	cloned.NextSlotByShard = slices.Clone(state.NextSlotByShard)
	cloned.Shards = slices.Clone(state.Shards)
	for i := range cloned.Shards {
		cloned.Shards[i].SessionState = slices.Clone(state.Shards[i].SessionState)
	}
	cloned.Portals = slices.Clone(state.Portals)
	return cloned
}

func validateNodeFSJournal(state nodeFSJournal) error {
	if state.Version != nodeFSJournalVersion {
		return fmt.Errorf("unsupported version %d", state.Version)
	}
	if strings.TrimSpace(state.NodeIdentity) == "" {
		return fmt.Errorf("node identity is required")
	}
	if strings.TrimSpace(state.ConnectionGeneration) == "" {
		return fmt.Errorf("connection generation is required")
	}
	if state.ShardCount <= 0 {
		return fmt.Errorf("shard count must be positive")
	}
	if len(state.NextSlotByShard) != state.ShardCount {
		return fmt.Errorf("next-slot vector has %d entries for %d shards", len(state.NextSlotByShard), state.ShardCount)
	}
	for shard, next := range state.NextSlotByShard {
		if next == 0 {
			return fmt.Errorf("next slot for shard %d must be positive", shard)
		}
	}

	shards := make(map[int]struct{}, len(state.Shards))
	for _, shard := range state.Shards {
		if shard.Index < 0 || shard.Index >= state.ShardCount {
			return fmt.Errorf("invalid shard index %d", shard.Index)
		}
		if _, exists := shards[shard.Index]; exists {
			return fmt.Errorf("duplicate shard index %d", shard.Index)
		}
		shards[shard.Index] = struct{}{}
		if strings.TrimSpace(shard.Tag) == "" || strings.TrimSpace(shard.MountPath) == "" {
			return fmt.Errorf("shard %d tag and mount path are required", shard.Index)
		}
		if len(shard.Tag) >= 128 || strings.ContainsAny(shard.Tag, "\x00,") {
			return fmt.Errorf("shard %d has invalid recovery tag", shard.Index)
		}
		if !filepath.IsAbs(filepath.Clean(shard.MountPath)) {
			return fmt.Errorf("shard %d mount path must be absolute", shard.Index)
		}
		if len(shard.SessionState) > 0 && !json.Valid(shard.SessionState) {
			return fmt.Errorf("shard %d has invalid session state", shard.Index)
		}
	}
	if len(state.Shards) != 0 && len(state.Shards) != state.ShardCount {
		return fmt.Errorf("journal has %d of %d shard records", len(state.Shards), state.ShardCount)
	}

	portalKeys := make(map[string]struct{}, len(state.Portals))
	targetPaths := make(map[string]string, len(state.Portals))
	slots := make(map[string]string, len(state.Portals))
	maxSlotByShard := make([]uint64, state.ShardCount)
	for _, portal := range state.Portals {
		if strings.TrimSpace(portal.PortalKey) == "" || strings.TrimSpace(portal.PodUID) == "" || strings.TrimSpace(portal.Name) == "" {
			return fmt.Errorf("portal key, pod UID, and name are required")
		}
		if _, exists := portalKeys[portal.PortalKey]; exists {
			return fmt.Errorf("duplicate portal key %q", portal.PortalKey)
		}
		portalKeys[portal.PortalKey] = struct{}{}
		targetPath := filepath.Clean(strings.TrimSpace(portal.TargetPath))
		if targetPath == "." || !filepath.IsAbs(targetPath) {
			return fmt.Errorf("portal %q has invalid target path %q", portal.PortalKey, portal.TargetPath)
		}
		if owner, exists := targetPaths[targetPath]; exists {
			return fmt.Errorf("portal target path %q is already owned by %q", targetPath, owner)
		}
		targetPaths[targetPath] = portal.PortalKey
		if portal.Shard < 0 || portal.Shard >= state.ShardCount || portal.Slot == 0 {
			return fmt.Errorf("portal %q has invalid shard/slot %d/%d", portal.PortalKey, portal.Shard, portal.Slot)
		}
		slotKey := fmt.Sprintf("%d/%d", portal.Shard, portal.Slot)
		if owner, exists := slots[slotKey]; exists {
			return fmt.Errorf("slot %s is shared by %q and %q", slotKey, owner, portal.PortalKey)
		}
		slots[slotKey] = portal.PortalKey
		maxSlotByShard[portal.Shard] = max(maxSlotByShard[portal.Shard], portal.Slot)
		switch portal.Phase {
		case nodeFSPortalAllocating, nodeFSPortalPublished, nodeFSPortalUnpublishing:
		default:
			return fmt.Errorf("portal %q has invalid phase %q", portal.PortalKey, portal.Phase)
		}
	}
	for shard, maximum := range maxSlotByShard {
		if state.NextSlotByShard[shard] <= maximum {
			return fmt.Errorf("next slot %d for shard %d would reuse allocated slot %d", state.NextSlotByShard[shard], shard, maximum)
		}
	}
	return nil
}

func writeNodeFSJournal(path string, state nodeFSJournal) (retErr error) {
	if err := validateNodeFSJournal(state); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode nodefs journal: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create nodefs journal temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if tmp != nil {
			if closeErr := tmp.Close(); retErr == nil && closeErr != nil {
				retErr = closeErr
			}
		}
		_ = os.Remove(tmpPath)
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod nodefs journal temporary file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fmt.Errorf("write nodefs journal temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync nodefs journal temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close nodefs journal temporary file: %w", err)
	}
	tmp = nil
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace nodefs journal: %w", err)
	}
	dirFile, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open nodefs journal directory: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return fmt.Errorf("sync nodefs journal directory: %w", err)
	}
	return nil
}

// acquireNodeFSProcessLock fences concurrent ctld processes on the same node.
// The kernel releases the flock automatically if ctld exits unexpectedly.
func acquireNodeFSProcessLock(rootDir string) (*nodeFSProcessLock, error) {
	rootDir = strings.TrimSpace(rootDir)
	if rootDir == "" {
		return nil, fmt.Errorf("nodefs lock root is required")
	}
	dir := filepath.Join(rootDir, nodeFSStateDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create nodefs lock directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, nodeFSLockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open nodefs process lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("another ctld process owns the nodefs lock")
		}
		return nil, fmt.Errorf("acquire nodefs process lock: %w", err)
	}
	return &nodeFSProcessLock{file: file}, nil
}

func (l *nodeFSProcessLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	file := l.file
	l.file = nil
	unlockErr := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(unlockErr, closeErr)
}
