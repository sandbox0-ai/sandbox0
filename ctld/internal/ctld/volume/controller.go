package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const defaultStagingRoot = "/var/lib/sandbox0/volumes"
const defaultProcRoot = "/proc"

type Resolver interface {
	Resolve(r *http.Request, sandboxID string) (ctldpower.Target, error)
}

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) error
}

type StageProvider interface {
	EnsureStaged(ctx context.Context, req ctldapi.VolumeAttachRequest) (string, error)
	Release(ctx context.Context, req ctldapi.VolumeDetachRequest) error
}

type NamespaceOperator interface {
	EnsureMountPoint(ctx context.Context, mountNSPath, mountPoint string) error
	BindMount(ctx context.Context, mountNSPath, sourcePath, mountPoint string) error
	Unmount(ctx context.Context, mountNSPath, mountPoint string) error
}

type Controller struct {
	Resolver    Resolver
	StagingRoot string
	ProcRoot    string
	Stage       StageProvider
	Namespace   NamespaceOperator
}

func NewController(resolver Resolver, stagingRoot, procRoot string) *Controller {
	if strings.TrimSpace(stagingRoot) == "" {
		stagingRoot = defaultStagingRoot
	}
	if strings.TrimSpace(procRoot) == "" {
		procRoot = defaultProcRoot
	}
	return &Controller{
		Resolver:    resolver,
		StagingRoot: filepath.Clean(stagingRoot),
		ProcRoot:    filepath.Clean(procRoot),
		Namespace:   newNamespaceOperator(),
	}
}

func (c *Controller) AttachVolume(r *http.Request, sandboxID string, req ctldapi.VolumeAttachRequest) (ctldapi.VolumeAttachResponse, int) {
	if strings.TrimSpace(req.SandboxID) == "" {
		req.SandboxID = sandboxID
	}
	mountPoint, err := cleanMountPoint(req.MountPoint)
	if err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusBadRequest
	}
	source, err := c.stageProvider().EnsureStaged(requestContext(r), req)
	if err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusBadRequest
	}
	target, status, errMessage := c.resolveTarget(r, req.SandboxID)
	if status != http.StatusOK {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: errMessage}, status
	}
	if _, err := os.Stat(source); err != nil {
		if os.IsNotExist(err) {
			return ctldapi.VolumeAttachResponse{Attached: false, Error: "node-local staged volume is not mounted"}, http.StatusServiceUnavailable
		}
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	pid, err := c.sandboxProcessID(target)
	if err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	mountNS := c.mountNamespacePathForPID(pid)
	ctx := requestContext(r)
	ops := c.namespaceOperator()
	if err := ops.EnsureMountPoint(ctx, mountNS, mountPoint); err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: fmt.Sprintf("prepare mount target %s: %v", mountPoint, err)}, http.StatusInternalServerError
	}
	if err := ops.BindMount(ctx, mountNS, source, mountPoint); err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: fmt.Sprintf("bind mount %s -> %s: %v", source, mountPoint, err)}, http.StatusInternalServerError
	}
	attachmentID := attachmentID(req.SandboxID, req.SandboxVolumeID, mountPoint)
	return ctldapi.VolumeAttachResponse{
		Attached:       true,
		AttachmentID:   attachmentID,
		MountSessionID: attachmentID,
	}, http.StatusOK
}

func (c *Controller) DetachVolume(r *http.Request, sandboxID string, req ctldapi.VolumeDetachRequest) (ctldapi.VolumeDetachResponse, int) {
	if strings.TrimSpace(req.SandboxID) == "" {
		req.SandboxID = sandboxID
	}
	if _, err := cleanVolumeID(req.SandboxVolumeID); err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusBadRequest
	}
	mountPoint, err := cleanMountPoint(req.MountPoint)
	if err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusBadRequest
	}
	target, status, errMessage := c.resolveTarget(r, req.SandboxID)
	if status != http.StatusOK {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: errMessage}, status
	}
	pid, err := c.sandboxProcessID(target)
	if err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	if err := c.namespaceOperator().Unmount(requestContext(r), c.mountNamespacePathForPID(pid), mountPoint); err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	if err := c.stageProvider().Release(requestContext(r), req); err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	return ctldapi.VolumeDetachResponse{Detached: true}, http.StatusOK
}

func (c *Controller) resolveTarget(r *http.Request, sandboxID string) (ctldpower.Target, int, string) {
	if c == nil || c.Resolver == nil {
		return ctldpower.Target{}, http.StatusNotImplemented, "ctld volume resolver not configured"
	}
	target, err := c.Resolver.Resolve(r, sandboxID)
	if err != nil {
		if errors.Is(err, ctldpower.ErrSandboxNotFound) || errors.Is(err, ctldpower.ErrPodNotFound) {
			return ctldpower.Target{}, http.StatusNotFound, err.Error()
		}
		if errors.Is(err, ctldpower.ErrNotImplemented) {
			return ctldpower.Target{}, http.StatusNotImplemented, err.Error()
		}
		return ctldpower.Target{}, http.StatusInternalServerError, err.Error()
	}
	return target, http.StatusOK, ""
}

func (c *Controller) stagedVolumePath(volumeID string) (string, error) {
	cleaned, err := cleanVolumeID(volumeID)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.StagingRoot, cleaned), nil
}

func (c *Controller) stageProvider() StageProvider {
	if c != nil && c.Stage != nil {
		return c.Stage
	}
	return existingStageProvider{controller: c}
}

func (c *Controller) namespaceOperator() NamespaceOperator {
	if c != nil && c.Namespace != nil {
		return c.Namespace
	}
	return newNamespaceOperator()
}

func cleanVolumeID(volumeID string) (string, error) {
	cleaned := strings.TrimSpace(volumeID)
	if cleaned == "" {
		return "", fmt.Errorf("sandboxvolume_id is required")
	}
	if cleaned == "." || cleaned == ".." || strings.Contains(cleaned, "/") || strings.Contains(cleaned, "\\") {
		return "", fmt.Errorf("sandboxvolume_id contains invalid path separator")
	}
	return cleaned, nil
}

func cleanMountPoint(mountPoint string) (string, error) {
	cleaned := filepath.Clean(strings.TrimSpace(mountPoint))
	if cleaned == "." || !filepath.IsAbs(cleaned) || cleaned == "/" {
		return "", fmt.Errorf("mount_point must be an absolute non-root path")
	}
	return cleaned, nil
}

func (c *Controller) sandboxProcessID(target ctldpower.Target) (string, error) {
	procRoot := defaultProcRoot
	if c != nil && strings.TrimSpace(c.ProcRoot) != "" {
		procRoot = c.ProcRoot
	}
	return preferredProcessInCgroupTree(procRoot, target.CgroupDir)
}

func (c *Controller) mountNamespacePathForPID(pid string) string {
	return filepath.Join(c.ProcRoot, pid, "ns/mnt")
}

func preferredProcessInCgroupTree(procRoot, root string) (string, error) {
	root = filepath.Clean(root)
	procRoot = filepath.Clean(procRoot)
	var candidates []string
	seen := make(map[string]struct{})
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || d.Name() != "cgroup.procs" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, pid := range strings.Fields(string(data)) {
			if _, ok := seen[pid]; ok {
				continue
			}
			seen[pid] = struct{}{}
			candidates = append(candidates, pid)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no process found under cgroup %s", root)
	}
	if pid := selectPreferredProcessID(procRoot, candidates); pid != "" {
		return pid, nil
	}
	return candidates[0], nil
}

func selectPreferredProcessID(procRoot string, candidates []string) string {
	var fallback string
	var pause string
	for _, pid := range candidates {
		cmdline := processCommandLine(procRoot, pid)
		switch {
		case strings.Contains(cmdline, "/procd/bin/procd") || strings.HasPrefix(cmdline, "procd"):
			return pid
		case strings.Contains(cmdline, "/pause") || strings.HasPrefix(cmdline, "pause"):
			if pause == "" {
				pause = pid
			}
		case cmdline != "":
			if fallback == "" {
				fallback = pid
			}
		default:
			if fallback == "" {
				fallback = pid
			}
		}
	}
	if fallback != "" {
		return fallback
	}
	return pause
}

func processCommandLine(procRoot, pid string) string {
	data, err := os.ReadFile(filepath.Join(procRoot, pid, "cmdline"))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(strings.ReplaceAll(string(data), "\x00", " "))
}

func requestContext(r *http.Request) context.Context {
	if r == nil {
		return context.Background()
	}
	return r.Context()
}

func attachmentID(sandboxID, volumeID, mountPoint string) string {
	sum := sha256.Sum256([]byte(sandboxID + "\x00" + volumeID + "\x00" + mountPoint))
	return "ctld-" + hex.EncodeToString(sum[:16])
}
