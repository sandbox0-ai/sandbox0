package portal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal/nodefs"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumefuse"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

const nodeFSRootBackend = "rootfs"

type nodeFSRuntime struct {
	opMu    sync.Mutex
	journal *nodeFSJournalStore
	lock    *nodeFSProcessLock
	mounter nodeFSMounter
	shards  []nodeFSShardRuntime
	ready   atomic.Bool
}

type nodeFSShardRuntime struct {
	state      nodeFSShardState
	mux        *nodefs.SessionMux
	fs         *volumefuse.FileSystem
	connection *nodeFSConnection
	recovered  bool
}

func (r *nodeFSRuntime) shard(index int) (*nodeFSShardRuntime, error) {
	if r == nil || index < 0 || index >= len(r.shards) {
		return nil, fmt.Errorf("nodefs shard %d is unavailable", index)
	}
	return &r.shards[index], nil
}

func (m *Manager) requireNodeFS() (*nodeFSRuntime, error) {
	if m == nil || m.nodeFSShardCount <= 0 {
		return nil, fmt.Errorf("nodefs is disabled")
	}
	m.mu.Lock()
	runtime := m.nodeFS
	m.mu.Unlock()
	if runtime == nil || !runtime.ready.Load() {
		return nil, fmt.Errorf("nodefs is not initialized")
	}
	return runtime, nil
}

// Initialize establishes all recoverable shard connections and reconciles
// every committed portal before callers may perform stale CSI mount cleanup.
// It is a no-op when nodefs is disabled.
func (m *Manager) Initialize(ctx context.Context) error {
	if m == nil {
		return fmt.Errorf("portal manager is required")
	}
	if m.nodeFSShardCount < 0 || m.nodeFSShardCount > 64 {
		return fmt.Errorf("nodefs shard count %d is outside [0,64]", m.nodeFSShardCount)
	}
	if m.nodeFSShardCount == 0 {
		if m.nodeFSRequireRecovery {
			return fmt.Errorf("nodefs recovery requires a positive shard count")
		}
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(m.nodeName) == "" {
		return fmt.Errorf("nodefs requires a node name")
	}
	m.mu.Lock()
	existingRuntime := m.nodeFS
	m.mu.Unlock()
	if existingRuntime != nil {
		if existingRuntime.ready.Load() {
			return nil
		}
		return fmt.Errorf("nodefs initialization previously failed")
	}

	processLock, err := acquireNodeFSProcessLock(m.rootDir)
	if err != nil {
		return err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			_ = processLock.Close()
		}
	}()

	journal, err := openNodeFSJournal(m.rootDir, m.nodeName, m.nodeFSShardCount)
	if err != nil {
		return err
	}
	if err := journal.ConfigureRecovery(m.nodeFSRequireRecovery); err != nil {
		return err
	}
	if err := journal.PrepareShards(m.rootDir); err != nil {
		return err
	}
	state := journal.Snapshot()
	factory := m.nodeFSFactory
	if factory == nil {
		factory = systemNodeFSConnectionFactory{}
	}
	mounter := m.nodeFSMounter
	if mounter == nil {
		mounter = systemNodeFSMounter{kubeletPodsRoot: m.kubeletPodsRoot}
	}
	runtime := &nodeFSRuntime{
		journal: journal,
		lock:    processLock,
		mounter: mounter,
		shards:  make([]nodeFSShardRuntime, state.ShardCount),
	}
	for _, shard := range state.Shards {
		mux := nodefs.NewSessionMux()
		runtime.shards[shard.Index] = nodeFSShardRuntime{
			state: shard,
			mux:   mux,
			fs:    volumefuse.New(fmt.Sprintf("nodefs-shard-%d", shard.Index), time.Second, mux),
		}
	}

	// Keep the process lock reachable if any later step fails. Startup treats
	// such a failure as fatal, and releasing the fence while a shard server is
	// alive would allow two ctld processes to own the same connection.
	m.mu.Lock()
	m.nodeFS = runtime
	m.mu.Unlock()
	keepLock = true
	if err := m.restoreNodeFSPortals(ctx, runtime, state); err != nil {
		return err
	}
	portalCounts := make([]int, state.ShardCount)
	for _, portal := range state.Portals {
		portalCounts[portal.Shard]++
	}
	for index := range runtime.shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		shard := &runtime.shards[index]
		connection, recovered, err := startNodeFSConnection(
			journal,
			state,
			shard.state,
			portalCounts[index],
			shard.fs,
			factory,
		)
		if err != nil {
			return err
		}
		shard.connection = connection
		shard.recovered = recovered
	}
	if err := m.reconcileNodeFSPortals(ctx, runtime, state); err != nil {
		return err
	}
	runtime.ready.Store(true)
	return nil
}

func (m *Manager) restoreNodeFSPortals(ctx context.Context, runtime *nodeFSRuntime, state nodeFSJournal) error {
	for _, portal := range state.Portals {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.restoreNodeFSPortal(ctx, runtime, portal); err != nil {
			return fmt.Errorf("restore nodefs portal %q: %w", portal.PortalKey, err)
		}
	}
	return nil
}

func (m *Manager) restoreNodeFSPortal(ctx context.Context, runtime *nodeFSRuntime, state nodeFSPortalState) error {
	shard, err := runtime.shard(state.Shard)
	if err != nil {
		return err
	}
	rootfsBackingPath, err := m.validateNodeFSRootFSBacking(state.RootFSBacking, state.PodUID, state.Name)
	if err != nil {
		return err
	}
	var rootfsSession volumefuse.Session = unboundSession{}
	if state.Phase != nodeFSPortalUnpublishing {
		if state.Phase == nodeFSPortalAllocating {
			if err := ensureDirectory(rootfsBackingPath, 0o755); err != nil {
				return fmt.Errorf("restore allocating rootfs backing directory: %w", err)
			}
		} else if err := requireDirectory(rootfsBackingPath); err != nil {
			return fmt.Errorf("restore published rootfs backing directory: %w", err)
		}
		restored := newRootFSBackedSession(rootfsBackingPath)
		if err := restored.InitError(); err != nil {
			restored.Close()
			return fmt.Errorf("restore rootfs session: %w", err)
		}
		rootfsSession = restored
	}
	pm := &portalMount{
		namespace:         state.Namespace,
		podName:           state.PodName,
		podUID:            state.PodUID,
		name:              state.Name,
		mountPath:         state.MountPath,
		targetPath:        state.TargetPath,
		rootfsBackingPath: rootfsBackingPath,
		rootfsSession:     rootfsSession,
		nodeFSShard:       state.Shard,
		nodeFSSlot:        state.Slot,
		nodeFSRouteName:   nodeFSRouteName(state.Slot),
	}
	pm.nodeFSSourcePath = filepath.Join(shard.state.MountPath, pm.nodeFSRouteName)
	session := rootfsSession
	volumeID := state.PortalKey
	if state.Phase != nodeFSPortalUnpublishing && normalizeNodeFSBackend(state.Backend) != nodeFSRootBackend {
		bound, err := m.restoreNodeFSBoundVolume(ctx, state)
		if err != nil {
			return err
		}
		session = bound.session
		volumeID = bound.volumeID
		pm.volumeID = bound.volumeID
		pm.teamID = bound.teamID
		pm.mountedAt = state.MountedAt
	}
	slot, err := nodefs.NewSlot(state.Slot)
	if err != nil {
		return err
	}
	if err := shard.mux.RegisterPortal(nodefs.PortalSpec{
		Name:      pm.nodeFSRouteName,
		Slot:      slot,
		VolumeID:  volumeID,
		RootInode: 1,
		Session:   session,
	}); err != nil {
		return err
	}
	m.mu.Lock()
	m.portals[state.PortalKey] = pm
	m.portalsByTarget[state.TargetPath] = pm
	m.mu.Unlock()
	return nil
}

func (m *Manager) restoreNodeFSBoundVolume(ctx context.Context, state nodeFSPortalState) (*boundVolume, error) {
	if strings.TrimSpace(state.VolumeID) == "" || strings.TrimSpace(state.TeamID) == "" {
		return nil, fmt.Errorf("bound portal is missing volume identity")
	}
	if err := validateNodeFSRecoverableBackend(m.nodeFSRequireRecovery, state.Backend); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if existing := m.boundVolumes[state.VolumeID]; existing != nil {
		if existing.teamID != state.TeamID {
			m.mu.Unlock()
			return nil, fmt.Errorf("volume %s team changed from %s to %s", state.VolumeID, existing.teamID, state.TeamID)
		}
		if existing.access != volume.AccessModeROX && existing.refCount > 0 {
			m.mu.Unlock()
			return nil, fmt.Errorf("non-ROX volume %s is committed to multiple portals", state.VolumeID)
		}
		existing.refCount++
		m.mu.Unlock()
		return existing, nil
	}
	m.mu.Unlock()

	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{volumeID: state.VolumeID, teamID: state.TeamID})
	if err != nil {
		return nil, err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return nil, err
	}
	backend := volume.NormalizeBackend(volumeRecord.Backend)
	if backend != normalizeNodeFSBackend(state.Backend) {
		return nil, fmt.Errorf("volume %s backend changed from %s to %s", state.VolumeID, state.Backend, backend)
	}
	mountedAt := state.MountedAt
	if mountedAt.IsZero() {
		mountedAt = time.Now().UTC()
	}
	bound, cleanup, err := m.openBoundVolume(ctx, ctldapi.BindVolumePortalRequest{
		PodUID:          state.PodUID,
		PortalName:      state.Name,
		MountPath:       state.MountPath,
		SandboxVolumeID: state.VolumeID,
		TeamID:          state.TeamID,
	}, volumeRecord, accessMode, mountedAt)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if existing := m.boundVolumes[state.VolumeID]; existing != nil {
		m.mu.Unlock()
		cleanup()
		return nil, fmt.Errorf("volume %s was restored concurrently", state.VolumeID)
	}
	m.boundVolumes[state.VolumeID] = bound
	m.volumes.add(bound.volCtx)
	if err := m.registerOwner(ctx, bound); err != nil {
		delete(m.boundVolumes, state.VolumeID)
		m.volumes.remove(state.VolumeID)
		m.mu.Unlock()
		cleanup()
		return nil, fmt.Errorf("restore ctld volume owner: %w", err)
	}
	m.startMaterializer(bound)
	m.mu.Unlock()
	return bound, nil
}

func (m *Manager) reconcileNodeFSPortals(ctx context.Context, runtime *nodeFSRuntime, state nodeFSJournal) error {
	for _, portal := range state.Portals {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.mu.Lock()
		pm := m.portals[portal.PortalKey]
		m.mu.Unlock()
		if pm == nil {
			return fmt.Errorf("nodefs portal %q was not restored", portal.PortalKey)
		}
		if portal.Phase == nodeFSPortalUnpublishing {
			if err := m.completeNodeFSUnpublish(ctx, runtime, portal.PortalKey, pm); err != nil {
				return err
			}
			continue
		}
		if err := runtime.mounter.EnsureBind(pm.nodeFSSourcePath, pm.targetPath); err != nil {
			return fmt.Errorf("reconcile nodefs portal %q bind: %w", portal.PortalKey, err)
		}
		if portal.Phase == nodeFSPortalAllocating {
			if err := runtime.journal.MarkPortalPublished(portal.PortalKey); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) publishNodeFSPortal(ctx context.Context, req publishRequest) error {
	if req.TargetPath == "" {
		return fmt.Errorf("target path is required")
	}
	if req.PodUID == "" {
		return fmt.Errorf("pod uid is required")
	}
	if req.Name == "" {
		return fmt.Errorf("portal name is required")
	}
	runtime, err := m.requireNodeFS()
	if err != nil {
		return err
	}
	runtime.opMu.Lock()
	defer runtime.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	key := portalKey(req.PodUID, req.Name)
	m.mu.Lock()
	existing := m.portals[key]
	m.mu.Unlock()
	if existing != nil {
		if filepath.Clean(existing.targetPath) != filepath.Clean(req.TargetPath) {
			return fmt.Errorf("portal %q is already published at %s", key, existing.targetPath)
		}
		if err := runtime.mounter.EnsureBind(existing.nodeFSSourcePath, existing.targetPath); err != nil {
			return err
		}
		return runtime.journal.MarkPortalPublished(key)
	}

	rootfsBackingPath := m.unboundRootFSBackingPath(req.PodUID, req.Name)
	allocated, err := runtime.journal.AllocatePortal(nodeFSPortalState{
		PortalKey:     key,
		PodUID:        req.PodUID,
		Namespace:     req.Namespace,
		PodName:       req.PodName,
		Name:          req.Name,
		MountPath:     req.MountPath,
		TargetPath:    req.TargetPath,
		RootFSBacking: rootfsBackingPath,
		Backend:       nodeFSRootBackend,
	})
	if err != nil {
		return err
	}
	rootfsBackingPath, err = m.validateNodeFSRootFSBacking(allocated.RootFSBacking, allocated.PodUID, allocated.Name)
	if err != nil {
		return err
	}
	if err := ensureDirectory(rootfsBackingPath, 0o755); err != nil {
		return fmt.Errorf("create allocating rootfs backing directory: %w", err)
	}
	shard, err := runtime.shard(allocated.Shard)
	if err != nil {
		return err
	}
	rootfsSession := newRootFSBackedSession(rootfsBackingPath)
	if err := rootfsSession.InitError(); err != nil {
		rootfsSession.Close()
		return fmt.Errorf("initialize rootfs session: %w", err)
	}
	routeName := nodeFSRouteName(allocated.Slot)
	slot, err := nodefs.NewSlot(allocated.Slot)
	if err != nil {
		return err
	}
	if err := shard.mux.RegisterPortal(nodefs.PortalSpec{
		Name:      routeName,
		Slot:      slot,
		VolumeID:  key,
		RootInode: 1,
		Session:   rootfsSession,
	}); err != nil {
		return err
	}
	pm := &portalMount{
		namespace:         req.Namespace,
		podName:           req.PodName,
		podUID:            req.PodUID,
		name:              req.Name,
		mountPath:         req.MountPath,
		targetPath:        req.TargetPath,
		rootfsBackingPath: rootfsBackingPath,
		rootfsSession:     rootfsSession,
		nodeFSShard:       allocated.Shard,
		nodeFSSlot:        allocated.Slot,
		nodeFSRouteName:   routeName,
		nodeFSSourcePath:  filepath.Join(shard.state.MountPath, routeName),
	}
	m.mu.Lock()
	m.portals[key] = pm
	m.portalsByTarget[req.TargetPath] = pm
	m.mu.Unlock()
	if err := runtime.mounter.EnsureBind(pm.nodeFSSourcePath, req.TargetPath); err != nil {
		return err
	}
	return runtime.journal.MarkPortalPublished(key)
}

func (m *Manager) unpublishNodeFSPortal(ctx context.Context, targetPath string) error {
	runtime, err := m.requireNodeFS()
	if err != nil {
		return err
	}
	runtime.opMu.Lock()
	defer runtime.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	pm := m.portalsByTarget[targetPath]
	m.mu.Unlock()
	if pm == nil {
		for _, state := range runtime.journal.Snapshot().Portals {
			if filepath.Clean(state.TargetPath) == filepath.Clean(targetPath) {
				return fmt.Errorf("nodefs portal %q is committed but not restored", state.PortalKey)
			}
		}
		return m.cleanUnknownStaleMountTarget(targetPath)
	}
	key := portalKey(pm.podUID, pm.name)
	if err := runtime.journal.BeginPortalUnpublish(key); err != nil {
		return err
	}
	return m.completeNodeFSUnpublish(ctx, runtime, key, pm)
}

func (m *Manager) completeNodeFSUnpublish(ctx context.Context, runtime *nodeFSRuntime, key string, pm *portalMount) error {
	if err := runtime.mounter.Unmount(pm.targetPath); err != nil {
		return err
	}
	shard, err := runtime.shard(pm.nodeFSShard)
	if err != nil {
		return err
	}
	if _, err := shard.mux.DrainPortal(ctx, pm.nodeFSRouteName); err != nil && !errors.Is(err, nodefs.ErrPortalRouteMissing) {
		return err
	}
	// The portal is no longer reachable after the bind is detached and its
	// route drained. Remove desired state before best-effort local GC so a
	// cleanup failure cannot make restart depend on already-deleted backing.
	if err := runtime.journal.RemovePortal(key); err != nil {
		return err
	}

	m.mu.Lock()
	cleanup := m.unbindLockedSnapshot(pm)
	delete(m.portalsByTarget, pm.targetPath)
	delete(m.portals, key)
	m.mu.Unlock()
	if pm.rootfsSession != nil {
		pm.rootfsSession.Close()
	}
	backingErr := removeNodeFSRootFSBacking(m.rootDir, pm.rootfsBackingPath)
	cleanupErr := m.finishBoundVolumeCleanup(ctx, cleanup)
	return errors.Join(backingErr, cleanupErr)
}

func (m *Manager) bindNodeFSPortal(ctx context.Context, req ctldapi.BindVolumePortalRequest) (ctldapi.BindVolumePortalResponse, error) {
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if portalName == "" {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("portal name or mount path is required")
	}
	if req.PodUID == "" || req.SandboxVolumeID == "" || req.TeamID == "" {
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("pod_uid, sandboxvolume_id and team_id are required")
	}
	volumeRecord, err := m.validateBindableVolume(ctx, ctldBindContext{
		volumeID: req.SandboxVolumeID,
		teamID:   req.TeamID,
	})
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	accessMode, err := validateBindableAccessMode(volumeRecord.AccessMode)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	if err := validateNodeFSRecoverableBackend(m.nodeFSRequireRecovery, volumeRecord.Backend); err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	runtime, err := m.requireNodeFS()
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	runtime.opMu.Lock()
	defer runtime.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}

	key := portalKey(req.PodUID, portalName)
	m.mu.Lock()
	pm := m.portals[key]
	if pm == nil {
		m.mu.Unlock()
		return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal %s for pod %s is not published", portalName, req.PodUID)
	}
	if pm.volumeID != "" {
		response := boundResponse(pm)
		m.mu.Unlock()
		if response.SandboxVolumeID != req.SandboxVolumeID {
			return ctldapi.BindVolumePortalResponse{}, fmt.Errorf("volume portal already bound to %s", response.SandboxVolumeID)
		}
		return response, nil
	}
	m.mu.Unlock()

	bound, created, err := m.reserveNodeFSBoundVolume(ctx, req, volumeRecord, accessMode)
	if err != nil {
		return ctldapi.BindVolumePortalResponse{}, err
	}
	mountedAt := time.Now().UTC()
	backend := volume.NormalizeBackend(volumeRecord.Backend)
	if err := runtime.journal.UpdatePortalBinding(key, backend, req.SandboxVolumeID, req.TeamID, mountedAt); err != nil {
		cleanupErr := m.rollbackNodeFSBoundReservation(ctx, bound, created)
		return ctldapi.BindVolumePortalResponse{}, errors.Join(err, cleanupErr)
	}
	shard, err := runtime.shard(pm.nodeFSShard)
	if err != nil {
		_ = m.rollbackNodeFSBoundReservation(ctx, bound, false)
		return ctldapi.BindVolumePortalResponse{}, err
	}
	if _, err := shard.mux.UpdatePortalSession(pm.nodeFSRouteName, bound.volumeID, bound.session); err != nil {
		// The journal intentionally remains on the desired backend. A retry or
		// process recovery will complete the switch.
		_ = m.rollbackNodeFSBoundReservation(ctx, bound, false)
		return ctldapi.BindVolumePortalResponse{}, err
	}
	m.mu.Lock()
	pm.volumeID = bound.volumeID
	pm.teamID = bound.teamID
	pm.mountedAt = mountedAt
	m.mu.Unlock()
	return boundResponse(pm), nil
}

func (m *Manager) reserveNodeFSBoundVolume(
	ctx context.Context,
	req ctldapi.BindVolumePortalRequest,
	volumeRecord *db.SandboxVolume,
	accessMode volume.AccessMode,
) (*boundVolume, bool, error) {
	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if err := validateNodeFSBoundReservation(bound, req, accessMode); err != nil {
			m.mu.Unlock()
			return nil, false, err
		}
		bound.refCount++
		m.mu.Unlock()
		return bound, false, nil
	}
	if existing := findBoundPortalForVolume(m.portals, req.SandboxVolumeID, ""); existing != nil {
		conflictPath := existing.mountPath
		m.mu.Unlock()
		return nil, false, fmt.Errorf("volume %s is already bound to %s", req.SandboxVolumeID, conflictPath)
	}
	m.mu.Unlock()

	mountedAt := time.Now().UTC()
	newBound, cleanupNewBound, err := m.openBoundVolume(ctx, req, volumeRecord, accessMode, mountedAt)
	if err != nil {
		return nil, false, err
	}
	m.mu.Lock()
	if bound := m.boundVolumes[req.SandboxVolumeID]; bound != nil {
		if err := validateNodeFSBoundReservation(bound, req, accessMode); err != nil {
			m.mu.Unlock()
			cleanupNewBound()
			return nil, false, err
		}
		bound.refCount++
		m.mu.Unlock()
		cleanupNewBound()
		return bound, false, nil
	}
	m.boundVolumes[req.SandboxVolumeID] = newBound
	m.volumes.add(newBound.volCtx)
	if err := m.registerOwner(ctx, newBound); err != nil {
		delete(m.boundVolumes, req.SandboxVolumeID)
		m.volumes.remove(req.SandboxVolumeID)
		m.mu.Unlock()
		cleanupNewBound()
		return nil, false, fmt.Errorf("register ctld volume owner: %w", err)
	}
	m.startMaterializer(newBound)
	m.mu.Unlock()
	return newBound, true, nil
}

func validateNodeFSBoundReservation(bound *boundVolume, req ctldapi.BindVolumePortalRequest, accessMode volume.AccessMode) error {
	if bound == nil {
		return fmt.Errorf("volume %s binding is unavailable", req.SandboxVolumeID)
	}
	if bound.closing {
		return fmt.Errorf("volume %s is closing", req.SandboxVolumeID)
	}
	if bound.teamID != req.TeamID {
		return fmt.Errorf("volume %s belongs to team %s", req.SandboxVolumeID, bound.teamID)
	}
	if bound.access != accessMode {
		return fmt.Errorf("volume %s access mode changed from %s to %s", req.SandboxVolumeID, bound.access, accessMode)
	}
	if bound.refCount > 0 && accessMode != volume.AccessModeROX {
		return fmt.Errorf("volume %s is already bound", req.SandboxVolumeID)
	}
	if bound.session == nil {
		return fmt.Errorf("volume %s session is unavailable", req.SandboxVolumeID)
	}
	return nil
}

func (m *Manager) rollbackNodeFSBoundReservation(ctx context.Context, bound *boundVolume, removeIfUnused bool) error {
	if bound == nil {
		return nil
	}
	m.mu.Lock()
	if bound.refCount > 0 {
		bound.refCount--
	}
	var cleanup *boundVolumeCleanup
	if removeIfUnused && bound.refCount == 0 && m.boundVolumes[bound.volumeID] == bound && !bound.closing {
		cleanup = m.releaseOwnerOnlyVolumeLocked(bound.volumeID, bound)
	}
	m.mu.Unlock()
	return m.finishBoundVolumeCleanup(ctx, cleanup)
}

func (m *Manager) unbindNodeFSPortal(ctx context.Context, req ctldapi.UnbindVolumePortalRequest) (ctldapi.UnbindVolumePortalResponse, error) {
	portalName := volumeportal.NormalizePortalName(req.PortalName, req.MountPath)
	if req.PodUID == "" || portalName == "" {
		return ctldapi.UnbindVolumePortalResponse{}, fmt.Errorf("pod_uid and portal identity are required")
	}
	runtime, err := m.requireNodeFS()
	if err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	runtime.opMu.Lock()
	defer runtime.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	key := portalKey(req.PodUID, portalName)
	m.mu.Lock()
	pm := m.portals[key]
	if pm == nil || pm.volumeID == "" {
		m.mu.Unlock()
		return ctldapi.UnbindVolumePortalResponse{Unbound: true}, nil
	}
	m.mu.Unlock()
	if err := runtime.journal.UpdatePortalBinding(key, nodeFSRootBackend, "", "", time.Time{}); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	shard, err := runtime.shard(pm.nodeFSShard)
	if err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	if _, err := shard.mux.UpdatePortalSession(pm.nodeFSRouteName, key, pm.rootfsSession); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	m.mu.Lock()
	cleanup := m.unbindLockedSnapshot(pm)
	m.mu.Unlock()
	if err := m.finishBoundVolumeCleanup(ctx, cleanup); err != nil {
		return ctldapi.UnbindVolumePortalResponse{}, err
	}
	return ctldapi.UnbindVolumePortalResponse{Unbound: true}, nil
}

func (m *Manager) shutdownNodeFS(ctx context.Context) error {
	m.mu.Lock()
	runtime := m.nodeFS
	m.mu.Unlock()
	if runtime == nil {
		return nil
	}
	runtime.opMu.Lock()
	defer runtime.opMu.Unlock()

	type backgroundTasks struct {
		heartbeatCancel   context.CancelFunc
		heartbeatDone     <-chan struct{}
		materializeCancel context.CancelFunc
		materializeDone   <-chan struct{}
	}
	m.mu.Lock()
	tasks := make([]backgroundTasks, 0, len(m.boundVolumes))
	for _, bound := range m.boundVolumes {
		if bound == nil {
			continue
		}
		tasks = append(tasks, backgroundTasks{
			heartbeatCancel:   bound.heartbeatCancel,
			heartbeatDone:     bound.heartbeatDone,
			materializeCancel: bound.materializeCancel,
			materializeDone:   bound.materializeDone,
		})
		bound.heartbeatCancel = nil
		bound.heartbeatDone = nil
		bound.materializeCancel = nil
		bound.materializeDone = nil
	}
	m.mu.Unlock()
	for _, task := range tasks {
		if task.heartbeatCancel != nil {
			task.heartbeatCancel()
		}
		if task.materializeCancel != nil {
			task.materializeCancel()
		}
	}
	for _, task := range tasks {
		for _, done := range []<-chan struct{}{task.heartbeatDone, task.materializeDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	// Do not unmount shard or CSI targets, close backend sessions, or release
	// the process flock here. Process exit closes the serving descriptors and
	// lock while the recovery-enabled kernel connections remain mounted.
	return nil
}

func nodeFSRouteName(slot uint64) string {
	return fmt.Sprintf("p%05x", slot)
}

func normalizeNodeFSBackend(backend string) string {
	backend = strings.TrimSpace(strings.ToLower(backend))
	if backend == "" || backend == nodeFSRootBackend {
		return nodeFSRootBackend
	}
	return volume.NormalizeBackend(backend)
}

func validateNodeFSRecoverableBackend(recoveryRequired bool, backend string) error {
	if recoveryRequired && volume.NormalizeBackend(backend) == volume.BackendS3 {
		return fmt.Errorf("nodefs S3 session recovery is not enabled; refusing recoverable S3 bind")
	}
	return nil
}

func ensureDirectory(path string, mode uint32) error {
	return os.MkdirAll(path, os.FileMode(mode))
}

func requireDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func (m *Manager) validateNodeFSRootFSBacking(path, podUID, portalName string) (string, error) {
	trusted, err := trustedNodeFSRootFSPath(m.rootDir, path)
	if err != nil {
		return "", err
	}
	expected := filepath.Clean(m.unboundRootFSBackingPath(podUID, portalName))
	if trusted != expected {
		return "", fmt.Errorf("nodefs rootfs backing %q does not match portal identity", path)
	}
	return trusted, nil
}

func trustedNodeFSRootFSPath(rootDir, path string) (string, error) {
	root := filepath.Clean(filepath.Join(rootDir, "rootfs-portals"))
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "." || !filepath.IsAbs(path) {
		return "", fmt.Errorf("nodefs rootfs backing path %q is not absolute", path)
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", fmt.Errorf("resolve nodefs rootfs backing %q: %w", path, err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("nodefs rootfs backing %q escapes %s", path, root)
	}
	if err := rejectSymlinkPath(root, path); err != nil {
		return "", err
	}
	return path, nil
}

func rejectSymlinkPath(root, path string) error {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := root
	components := append([]string{""}, strings.Split(rel, string(filepath.Separator))...)
	for index, component := range components {
		if index > 0 {
			current = filepath.Join(current, component)
		}
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("inspect nodefs rootfs backing %s: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("nodefs rootfs backing contains symlink %s", current)
		}
	}
	return nil
}

func removeNodeFSRootFSBacking(rootDir, path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	trusted, err := trustedNodeFSRootFSPath(rootDir, path)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(trusted); err != nil {
		return fmt.Errorf("remove nodefs rootfs backing %s: %w", trusted, err)
	}
	return nil
}
