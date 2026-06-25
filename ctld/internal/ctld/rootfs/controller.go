package rootfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
)

var (
	ErrNotFound   = errors.New("rootfs target not found")
	ErrConflict   = errors.New("rootfs validation conflict")
	ErrBadRequest = errors.New("invalid rootfs request")
)

type Runtime interface {
	Inspect(ctx context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error)
	CommitS0FSRootFS(ctx context.Context, req S0FSCommitRequest) (ctldapi.RootFSHeadDescriptor, error)
	AttachS0FSRootFS(ctx context.Context, req S0FSAttachRequest) (ctldapi.RootFSHeadDescriptor, string, error)
}

type S0FSCommitRequest struct {
	Info          ctldapi.RootFSInfo
	Store         objectstore.Store
	SandboxID     string
	TeamID        string
	FilesystemID  string
	ParentHead    ctldapi.RootFSHeadDescriptor
	ExcludedPaths []string
	PortalPaths   []ctldapi.RootFSPortalPath
}

type S0FSAttachRequest struct {
	Info          ctldapi.RootFSInfo
	Store         objectstore.Store
	FilesystemID  string
	Head          ctldapi.RootFSHeadDescriptor
	ExcludedPaths []string
	PortalPaths   []ctldapi.RootFSPortalPath
}

type PortalResolver interface {
	RootFSPortalPaths(podUID string) []ctldapi.RootFSPortalPath
}

type Config struct {
	Runtime          Runtime
	Store            objectstore.Store
	OperationTimeout time.Duration
	PortalResolver   PortalResolver
}

type Controller struct {
	runtime          Runtime
	store            objectstore.Store
	operationTimeout time.Duration
	portalResolver   PortalResolver
}

func NewController(cfg Config) *Controller {
	timeout := cfg.OperationTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	return &Controller{
		runtime:          cfg.Runtime,
		store:            cfg.Store,
		operationTimeout: timeout,
		portalResolver:   cfg.PortalResolver,
	}
}

func (c *Controller) InspectRootFS(r *http.Request, req ctldapi.InspectRootFSRequest) (ctldapi.InspectRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.InspectRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	info, err := c.inspect(requestContext(r), req.Target)
	if err != nil {
		return ctldapi.InspectRootFSResponse{Error: err.Error()}, statusForError(err)
	}
	return ctldapi.InspectRootFSResponse{Info: info}, http.StatusOK
}

func (c *Controller) SaveRootFS(r *http.Request, req ctldapi.SaveRootFSRequest) (ctldapi.SaveRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.SaveRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if c.store == nil {
		return ctldapi.SaveRootFSResponse{Error: "rootfs object store is not configured"}, http.StatusNotImplemented
	}

	ctx, cancel := c.operationContext(requestContext(r))
	defer cancel()

	info, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.SaveRootFSResponse{Error: err.Error()}, statusForError(err)
	}
	if err := validateSupportedRuntime(info); err != nil {
		return ctldapi.SaveRootFSResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
	head, err := c.runtime.CommitS0FSRootFS(ctx, S0FSCommitRequest{
		Info:          info,
		Store:         c.store,
		SandboxID:     req.SandboxID,
		TeamID:        req.TeamID,
		FilesystemID:  req.FilesystemID,
		ParentHead:    req.ParentHead,
		ExcludedPaths: req.ExcludedPaths,
		PortalPaths:   portalPaths,
	})
	if err != nil {
		return ctldapi.SaveRootFSResponse{Info: info, Error: fmt.Sprintf("commit s0fs rootfs: %v", err)}, statusForError(err)
	}
	return ctldapi.SaveRootFSResponse{Info: info, Head: head}, http.StatusOK
}

func (c *Controller) ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if err := validateRootFSHeadDescriptor(req.Head); err != nil {
		return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if c.store == nil {
		return ctldapi.ApplyRootFSResponse{Error: "rootfs object store is not configured"}, http.StatusNotImplemented
	}

	ctx, cancel := c.operationContext(requestContext(r))
	defer cancel()

	info, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.ApplyRootFSResponse{Error: err.Error()}, statusForError(err)
	}
	if err := validateSupportedRuntime(info); err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	if err := validateExpectedBase(info, req); err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, http.StatusConflict
	}
	portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
	head, mountPath, err := c.runtime.AttachS0FSRootFS(ctx, S0FSAttachRequest{
		Info:          info,
		Store:         c.store,
		FilesystemID:  req.FilesystemID,
		Head:          req.Head,
		ExcludedPaths: req.ExcludedPaths,
		PortalPaths:   portalPaths,
	})
	if err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, statusForError(err)
	}
	return ctldapi.ApplyRootFSResponse{Info: info, Head: head, MountPath: mountPath, Applied: true}, http.StatusOK
}

func (c *Controller) portalPathsForRequest(info ctldapi.RootFSInfo, target ctldapi.RootFSContainerRef, excludedPaths []string, requested []ctldapi.RootFSPortalPath) []ctldapi.RootFSPortalPath {
	podUID := strings.TrimSpace(info.PodUID)
	if podUID == "" {
		podUID = strings.TrimSpace(target.PodUID)
	}
	paths := append([]ctldapi.RootFSPortalPath(nil), requested...)
	if podUID != "" && c != nil && c.portalResolver != nil {
		paths = append(paths, c.portalResolver.RootFSPortalPaths(podUID)...)
	}
	return filterRootFSPortalPaths(paths, excludedPaths)
}

func (c *Controller) inspect(ctx context.Context, target ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error) {
	if c == nil || c.runtime == nil {
		return ctldapi.RootFSInfo{}, fmt.Errorf("rootfs runtime is not configured")
	}
	return c.runtime.Inspect(ctx, target)
}

func (c *Controller) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := 2 * time.Minute
	if c != nil && c.operationTimeout > 0 {
		timeout = c.operationTimeout
	}
	return context.WithTimeout(parent, timeout)
}

func requestContext(r *http.Request) context.Context {
	if r != nil && r.Context() != nil {
		return r.Context()
	}
	return context.Background()
}

func validateTarget(target ctldapi.RootFSContainerRef) error {
	if strings.TrimSpace(target.Namespace) == "" {
		return fmt.Errorf("%w: namespace is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.PodName) == "" {
		return fmt.Errorf("%w: pod_name is required", ErrBadRequest)
	}
	if strings.TrimSpace(target.ContainerName) == "" {
		return fmt.Errorf("%w: container_name is required", ErrBadRequest)
	}
	return nil
}

func rootFSHeadDescriptorEmpty(head ctldapi.RootFSHeadDescriptor) bool {
	return strings.TrimSpace(head.Engine) == "" &&
		strings.TrimSpace(head.VolumeID) == "" &&
		strings.TrimSpace(head.ManifestKey) == "" &&
		head.ManifestSeq == 0
}

func validateRootFSHeadDescriptor(head ctldapi.RootFSHeadDescriptor) error {
	if strings.TrimSpace(head.Engine) != "" && strings.TrimSpace(head.Engine) != ctldapi.RootFSStorageEngineS0FS {
		return fmt.Errorf("%w: unsupported rootfs head engine %q", ErrBadRequest, head.Engine)
	}
	if strings.TrimSpace(head.VolumeID) == "" {
		return fmt.Errorf("%w: rootfs head volume_id is required", ErrBadRequest)
	}
	if strings.TrimSpace(head.ManifestKey) == "" {
		return fmt.Errorf("%w: rootfs head manifest_key is required", ErrBadRequest)
	}
	if head.ManifestSeq == 0 {
		return fmt.Errorf("%w: rootfs head manifest_seq is required", ErrBadRequest)
	}
	return nil
}

func validateSupportedRuntime(info ctldapi.RootFSInfo) error {
	runtime := strings.ToLower(strings.TrimSpace(info.Runtime))
	switch runtime {
	case "runc", "gvisor":
		return nil
	case "":
		return fmt.Errorf("%w: runtime is required", ErrBadRequest)
	default:
		return fmt.Errorf("%w: runtime %q is not supported for rootfs checkpoints", ErrBadRequest, info.Runtime)
	}
}

func validateExpectedBase(info ctldapi.RootFSInfo, req ctldapi.ApplyRootFSRequest) error {
	if expected := strings.TrimSpace(req.ExpectedRuntime); expected != "" && strings.TrimSpace(info.Runtime) != expected {
		return fmt.Errorf("%w: runtime mismatch: expected %s, got %s", ErrConflict, expected, info.Runtime)
	}
	if expected := strings.TrimSpace(req.ExpectedRuntimeHandler); expected != "" && strings.TrimSpace(info.RuntimeHandler) != expected {
		return fmt.Errorf("%w: runtime handler mismatch: expected %s, got %s", ErrConflict, expected, info.RuntimeHandler)
	}
	if expected := strings.TrimSpace(req.ExpectedSnapshotter); expected != "" && strings.TrimSpace(info.Snapshotter) != expected {
		return fmt.Errorf("%w: snapshotter mismatch: expected %s, got %s", ErrConflict, expected, info.Snapshotter)
	}
	return nil
}

func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if errors.Is(err, ErrBadRequest) {
		return http.StatusBadRequest
	}
	if errors.Is(err, ErrConflict) {
		return http.StatusConflict
	}
	if errors.Is(err, ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
