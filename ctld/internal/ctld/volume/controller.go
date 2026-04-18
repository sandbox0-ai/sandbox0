package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
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

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

type Controller struct {
	Resolver    Resolver
	StagingRoot string
	ProcRoot    string
	Runner      CommandRunner
	Stage       StageProvider
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
		Runner:      execRunner{},
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
	mountNS, err := c.mountNamespacePath(target)
	if err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	ctx := requestContext(r)
	if err := c.runner().Run(ctx, "nsenter", "--mount="+mountNS, "--", "mkdir", "-p", mountPoint); err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	if err := c.runner().Run(ctx, "nsenter", "--mount="+mountNS, "--", "mount", "--bind", c.sourcePathForMountNamespace(source), mountPoint); err != nil {
		return ctldapi.VolumeAttachResponse{Attached: false, Error: err.Error()}, http.StatusInternalServerError
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
	mountNS, err := c.mountNamespacePath(target)
	if err != nil {
		return ctldapi.VolumeDetachResponse{Detached: false, Error: err.Error()}, http.StatusInternalServerError
	}
	if err := c.runner().Run(requestContext(r), "nsenter", "--mount="+mountNS, "--", "umount", mountPoint); err != nil {
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

func (c *Controller) sourcePathForMountNamespace(source string) string {
	procRoot := defaultProcRoot
	if c != nil && strings.TrimSpace(c.ProcRoot) != "" {
		procRoot = c.ProcRoot
	}
	return filepath.Join(procRoot, fmt.Sprintf("%d", os.Getpid()), "root", source)
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

func (c *Controller) mountNamespacePath(target ctldpower.Target) (string, error) {
	pid, err := firstProcessInCgroupTree(target.CgroupDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(c.ProcRoot, pid, "ns/mnt"), nil
}

func (c *Controller) runner() CommandRunner {
	if c != nil && c.Runner != nil {
		return c.Runner
	}
	return execRunner{}
}

func firstProcessInCgroupTree(root string) (string, error) {
	root = filepath.Clean(root)
	var pid string
	errStop := errors.New("stop")
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
		fields := strings.Fields(string(data))
		if len(fields) == 0 {
			return nil
		}
		pid = fields[0]
		return errStop
	})
	if err != nil && !errors.Is(err, errStop) {
		return "", err
	}
	if pid == "" {
		return "", fmt.Errorf("no process found under cgroup %s", root)
	}
	return pid, nil
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
