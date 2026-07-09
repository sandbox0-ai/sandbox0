package portal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const defaultKubeletPodsRoot = "/var/lib/kubelet/pods"
const kubeletCSIVolumeDir = "kubernetes.io~csi"
const sandboxVolumeNamePrefix = "sandbox0-volume-"
const runtimeRecoveryConcurrency = 16

type staleMountCleaner func(string) error
type staleMountChecker func(string) (bool, error)
type staleMountRecoverer func(context.Context, string, RecoverablePortal) error

// RuntimeRecoveryTarget identifies the procd container and durable state portal
// used after all of an active sandbox's CSI portals have been reconstructed.
type RuntimeRecoveryTarget struct {
	Namespace       string
	PodName         string
	PodUID          string
	ContainerName   string
	StateVolumeName string
	ReplayProcesses bool
}

// ActivePodRuntimeRecoverer replaces a stale procd container. portalsRecovered
// is true only when this reconciliation rebuilt at least one portal for the pod.
type ActivePodRuntimeRecoverer func(context.Context, RuntimeRecoveryTarget, bool) error

type runtimeRecoveryRequest struct {
	target           RuntimeRecoveryTarget
	portalsRecovered bool
}

// ActivePodUIDLister returns pod UIDs that still belong to live pods on this node.
type ActivePodUIDLister func(context.Context) (map[string]struct{}, error)

// RecoverablePortal contains the pod and binding metadata needed to recreate a
// CSI portal after ctld loses its in-memory mount registry.
type RecoverablePortal struct {
	VolumeName      string
	Namespace       string
	PodName         string
	PodUID          string
	PortalName      string
	MountPath       string
	TeamID          string
	SandboxVolumeID string
}

// ActivePodPortals contains recoverable CSI portals keyed by Kubernetes volume name.
type ActivePodPortals struct {
	Portals         map[string]RecoverablePortal
	RuntimeRecovery *RuntimeRecoveryTarget
	RecoveryError   error
}

// ActivePodPortalLister returns live pods and their sandbox0 CSI portal metadata.
type ActivePodPortalLister func(context.Context) (map[string]ActivePodPortals, error)

func defaultStaleMountCleaner(path string) error {
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil &&
		!errors.Is(err, unix.EINVAL) &&
		!errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("lazy unmount stale CSI mount: %w", err)
	}
	if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale CSI mount: %w", err)
	}
	return nil
}

func defaultStaleMountChecker(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EIO) || errors.Is(err, unix.ESTALE) {
		return true, nil
	}
	return false, fmt.Errorf("check CSI mount target: %w", err)
}

func (m *Manager) cleanUnknownStaleMountTarget(targetPath string) error {
	root := defaultKubeletPodsRoot
	if m != nil && strings.TrimSpace(m.kubeletPodsRoot) != "" {
		root = m.kubeletPodsRoot
	}
	if !isSandboxCSIMountPath(root, targetPath) {
		return fmt.Errorf("refusing to clean unknown CSI target outside sandbox0 kubelet volume paths: %s", targetPath)
	}
	return m.cleanStaleMountTarget(targetPath)
}

func (m *Manager) CleanupStaleCSIMounts(ctx context.Context) error {
	if m == nil || strings.TrimSpace(m.kubeletPodsRoot) == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Clean(m.kubeletPodsRoot)
	cleaner := m.staleMountCleaner
	if cleaner == nil {
		cleaner = defaultStaleMountCleaner
	}
	checker := m.staleMountChecker
	if checker == nil {
		checker = defaultStaleMountChecker
	}
	activePods, activePortals, activeReliable := m.activePodState(ctx)

	var firstErr error
	handledTargets, reconcileErr := m.reconcileActivePodMounts(ctx, root, cleaner, checker, activePortals, activeReliable)
	if reconcileErr != nil {
		firstErr = reconcileErr
	}
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if _, handled := handledTargets[path]; handled {
			return filepath.SkipDir
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if info, ok := sandboxCSIMountPathInfo(root, path); ok {
				if !shouldCleanSandboxCSIMount(info, activePods, activeReliable, true) {
					return filepath.SkipDir
				}
				if cleanupErr := cleaner(path); cleanupErr != nil {
					if firstErr == nil {
						firstErr = cleanupErr
					}
					if m.logger != nil {
						m.logger.Warn("ctld stale CSI mount cleanup failed", zap.String("path", path), zap.Error(cleanupErr))
					}
					return filepath.SkipDir
				}
				if m.logger != nil {
					m.logger.Info("ctld cleaned stale CSI mount", zap.String("path", path), zap.Error(err))
				}
				if recoverErr := m.recoverActivePodMount(ctx, path, info, activePods, activePortals, activeReliable); recoverErr != nil {
					if firstErr == nil {
						firstErr = recoverErr
					}
					if m.logger != nil {
						m.logger.Warn("ctld stale CSI mount recovery failed", zap.String("path", path), zap.Error(recoverErr))
					}
				}
				return filepath.SkipDir
			}
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount scan skipped path", zap.String("path", path), zap.Error(err))
			}
			return nil
		}
		if d == nil || !d.IsDir() || filepath.Base(path) != "mount" {
			return nil
		}
		if !isSandboxCSIMountPath(root, path) {
			return nil
		}
		info, _ := sandboxCSIMountPathInfo(root, path)
		broken, checkErr := checker(path)
		if checkErr != nil {
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount health check failed", zap.String("path", path), zap.Error(checkErr))
			}
			return filepath.SkipDir
		}
		if !shouldCleanSandboxCSIMount(info, activePods, activeReliable, broken) {
			return filepath.SkipDir
		}
		if err := cleaner(path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount cleanup failed", zap.String("path", path), zap.Error(err))
			}
			return filepath.SkipDir
		}
		if m.logger != nil {
			m.logger.Info("ctld cleaned stale CSI mount", zap.String("path", path))
		}
		if broken {
			if recoverErr := m.recoverActivePodMount(ctx, path, info, activePods, activePortals, activeReliable); recoverErr != nil {
				if firstErr == nil {
					firstErr = recoverErr
				}
				if m.logger != nil {
					m.logger.Warn("ctld stale CSI mount recovery failed", zap.String("path", path), zap.Error(recoverErr))
				}
			}
		}
		return filepath.SkipDir
	})
	if walkErr != nil {
		return walkErr
	}
	return firstErr
}

func (m *Manager) reconcileActivePodMounts(ctx context.Context, root string, cleaner staleMountCleaner, checker staleMountChecker, activePortals map[string]ActivePodPortals, activeReliable bool) (map[string]struct{}, error) {
	handled := map[string]struct{}{}
	if !activeReliable || activePortals == nil || m == nil || m.staleMountRecoverer == nil {
		return handled, nil
	}

	podUIDs := make([]string, 0, len(activePortals))
	for podUID := range activePortals {
		podUIDs = append(podUIDs, podUID)
	}
	sort.Strings(podUIDs)
	var errs []error
	runtimeRequests := make([]runtimeRecoveryRequest, 0)
	for _, podUID := range podUIDs {
		portalSet := activePortals[podUID]
		if portalSet.RecoveryError != nil {
			errs = append(errs, fmt.Errorf("read active pod recovery metadata %s: %w", podUID, portalSet.RecoveryError))
			continue
		}
		podErrorCount := len(errs)
		portalsRecovered := false
		volumeNames := make([]string, 0, len(portalSet.Portals))
		for volumeName := range portalSet.Portals {
			volumeNames = append(volumeNames, volumeName)
		}
		sort.Strings(volumeNames)
		for _, volumeName := range volumeNames {
			if err := ctx.Err(); err != nil {
				return handled, errors.Join(append(errs, err)...)
			}
			portal := portalSet.Portals[volumeName]
			targetPath := filepath.Join(root, podUID, "volumes", kubeletCSIVolumeDir, volumeName, "mount")
			if !isSandboxCSIMountPath(root, targetPath) {
				errs = append(errs, fmt.Errorf("refusing to recover invalid active portal path %s", targetPath))
				continue
			}
			mounted, mountErr := mountinfo.Mounted(targetPath)
			broken := false
			if mounted && mountErr == nil {
				broken, mountErr = checker(targetPath)
			}
			if mountErr == nil && mounted && !broken {
				continue
			}

			handled[targetPath] = struct{}{}
			if mounted || mountErr != nil {
				if err := cleaner(targetPath); err != nil {
					errs = append(errs, fmt.Errorf("clean active portal %s: %w", targetPath, err))
					continue
				}
			} else if _, err := os.Lstat(targetPath); err == nil {
				if err := cleaner(targetPath); err != nil {
					errs = append(errs, fmt.Errorf("clean unmounted active portal %s: %w", targetPath, err))
					continue
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				errs = append(errs, fmt.Errorf("inspect active portal %s: %w", targetPath, err))
				continue
			}
			if err := m.staleMountRecoverer(ctx, targetPath, portal); err != nil {
				errs = append(errs, fmt.Errorf("recover active portal %s: %w", targetPath, err))
				continue
			}
			portalsRecovered = true
			if m.logger != nil {
				m.logger.Info("ctld recovered active CSI mount", zap.String("path", targetPath), zap.String("pod_uid", podUID), zap.String("volume", volumeName))
			}
		}
		if portalSet.RuntimeRecovery != nil && m.activePodRuntimeRecoverer != nil && len(errs) == podErrorCount {
			runtimeRequests = append(runtimeRequests, runtimeRecoveryRequest{
				target:           *portalSet.RuntimeRecovery,
				portalsRecovered: portalsRecovered,
			})
		}
	}
	errs = append(errs, m.recoverActivePodRuntimes(ctx, runtimeRequests)...)
	return handled, errors.Join(errs...)
}

func (m *Manager) recoverActivePodRuntimes(ctx context.Context, requests []runtimeRecoveryRequest) []error {
	if m == nil || m.activePodRuntimeRecoverer == nil || len(requests) == 0 {
		return nil
	}
	errs := make([]error, len(requests))
	sem := make(chan struct{}, runtimeRecoveryConcurrency)
	var wg sync.WaitGroup
	for i := range requests {
		if err := ctx.Err(); err != nil {
			errs[i] = err
			continue
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }()
			request := requests[index]
			if err := m.activePodRuntimeRecoverer(ctx, request.target, request.portalsRecovered); err != nil {
				errs[index] = fmt.Errorf("recover active pod runtime %s: %w", request.target.PodUID, err)
				return
			}
			if request.portalsRecovered && m.logger != nil {
				m.logger.Info("ctld requested sandbox runtime recovery", zap.String("pod_uid", request.target.PodUID), zap.Bool("replay_processes", request.target.ReplayProcesses))
			}
		}(i)
	}
	wg.Wait()
	result := make([]error, 0)
	for _, err := range errs {
		if err != nil {
			result = append(result, err)
		}
	}
	return result
}

func (m *Manager) activePodState(ctx context.Context) (map[string]struct{}, map[string]ActivePodPortals, bool) {
	if m != nil && m.activePodPortalLister != nil {
		portals, err := m.activePodPortalLister(ctx)
		if err != nil {
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount recovery could not list active pod portals", zap.Error(err))
			}
			return nil, nil, false
		}
		if portals == nil {
			portals = map[string]ActivePodPortals{}
		}
		active := make(map[string]struct{}, len(portals))
		for podUID := range portals {
			active[podUID] = struct{}{}
		}
		return active, portals, true
	}
	active, reliable := m.activePodUIDs(ctx)
	return active, nil, reliable
}

func (m *Manager) recoverActivePodMount(ctx context.Context, targetPath string, info csiMountPathInfo, activePods map[string]struct{}, activePortals map[string]ActivePodPortals, activeReliable bool) error {
	if !activeReliable || activePortals == nil || m == nil || m.staleMountRecoverer == nil {
		return nil
	}
	if _, active := activePods[info.podUID]; !active {
		return nil
	}
	podPortals, ok := activePortals[info.podUID]
	if !ok {
		return fmt.Errorf("active pod %s has no portal recovery metadata", info.podUID)
	}
	portal, ok := podPortals.Portals[info.volumeName]
	if !ok {
		return fmt.Errorf("active pod %s has no recovery metadata for volume %s", info.podUID, info.volumeName)
	}
	return m.staleMountRecoverer(ctx, targetPath, portal)
}

func (m *Manager) recoverStaleMount(ctx context.Context, targetPath string, portal RecoverablePortal) error {
	if err := m.PublishPortal(ctx, publishRequest{
		Namespace:  portal.Namespace,
		PodName:    portal.PodName,
		PodUID:     portal.PodUID,
		Name:       portal.PortalName,
		MountPath:  portal.MountPath,
		TargetPath: targetPath,
	}); err != nil {
		return fmt.Errorf("republish portal: %w", err)
	}
	if strings.TrimSpace(portal.SandboxVolumeID) == "" {
		return nil
	}
	if _, err := m.Bind(ctx, ctldapi.BindVolumePortalRequest{
		Namespace:       portal.Namespace,
		PodName:         portal.PodName,
		PodUID:          portal.PodUID,
		PortalName:      portal.PortalName,
		MountPath:       portal.MountPath,
		SandboxID:       "",
		TeamID:          portal.TeamID,
		SandboxVolumeID: portal.SandboxVolumeID,
	}); err != nil {
		_ = m.unpublishPortalContext(ctx, targetPath, true, true)
		return fmt.Errorf("rebind portal: %w", err)
	}
	return nil
}

func (m *Manager) activePodUIDs(ctx context.Context) (map[string]struct{}, bool) {
	if m == nil || m.activePodUIDLister == nil {
		return nil, false
	}
	activePods, err := m.activePodUIDLister(ctx)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("ctld stale CSI mount cleanup could not list active pods", zap.Error(err))
		}
		return nil, false
	}
	if activePods == nil {
		activePods = map[string]struct{}{}
	}
	return activePods, true
}

type csiMountPathInfo struct {
	podUID     string
	volumeName string
}

func shouldCleanSandboxCSIMount(info csiMountPathInfo, activePods map[string]struct{}, activeReliable bool, broken bool) bool {
	if broken {
		return true
	}
	if info.podUID != "" && activeReliable {
		_, active := activePods[info.podUID]
		return !active
	}
	return false
}

func isSandboxCSIMountPath(root, path string) bool {
	_, ok := sandboxCSIMountPathInfo(root, path)
	return ok
}

func sandboxCSIMountPathInfo(root, path string) (csiMountPathInfo, bool) {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return csiMountPathInfo{}, false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 5 {
		return csiMountPathInfo{}, false
	}
	if parts[1] == "volumes" &&
		parts[2] == kubeletCSIVolumeDir &&
		strings.HasPrefix(parts[3], sandboxVolumeNamePrefix) &&
		parts[4] == "mount" {
		return csiMountPathInfo{podUID: parts[0], volumeName: parts[3]}, true
	}
	return csiMountPathInfo{}, false
}
