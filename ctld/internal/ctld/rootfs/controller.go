package rootfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	ctldpower "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/power"
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
	CreateDiff(ctx context.Context, info ctldapi.RootFSInfo) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error)
	ApplyDiff(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, content io.Reader) (ctldapi.RootFSDiffDescriptor, error)
}

type Config struct {
	Runtime          Runtime
	Store            objectstore.Store
	OperationTimeout time.Duration
}

type Controller struct {
	runtime          Runtime
	store            objectstore.Store
	operationTimeout time.Duration
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
	desc, reader, err := c.runtime.CreateDiff(ctx, info)
	if err != nil {
		return ctldapi.SaveRootFSResponse{Info: info, Error: fmt.Sprintf("create rootfs diff: %v", err)}, statusForError(err)
	}
	defer reader.Close()

	objectKey := strings.Trim(strings.TrimSpace(req.ObjectKey), "/")
	if objectKey == "" {
		objectKey, err = defaultObjectKey(req.TeamID, req.SandboxID, req.ExpectedRuntimeGeneration, desc.Digest)
		if err != nil {
			return ctldapi.SaveRootFSResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
		}
	}
	if err := c.store.Put(objectKey, reader); err != nil {
		return ctldapi.SaveRootFSResponse{Info: info, Error: fmt.Sprintf("upload rootfs diff: %v", err)}, http.StatusInternalServerError
	}
	desc.ObjectKey = objectKey
	return ctldapi.SaveRootFSResponse{Info: info, Descriptor: desc}, http.StatusOK
}

func (c *Controller) ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if err := validateDescriptor(req.Descriptor); err != nil {
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

	reader, err := c.store.Get(req.Descriptor.ObjectKey, 0, -1)
	if err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: fmt.Sprintf("download rootfs diff: %v", err)}, http.StatusInternalServerError
	}
	defer reader.Close()

	applied, err := c.runtime.ApplyDiff(ctx, info, req.Descriptor, reader)
	if err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: fmt.Sprintf("apply rootfs diff: %v", err)}, statusForError(err)
	}
	applied.ObjectKey = req.Descriptor.ObjectKey
	return ctldapi.ApplyRootFSResponse{Info: info, Descriptor: applied, Applied: true}, http.StatusOK
}

func (c *Controller) ReadRootFSDiff(r *http.Request, req ctldapi.ReadRootFSDiffRequest) (io.ReadCloser, ctldapi.RootFSDiffDescriptor, int, error) {
	if err := validateDescriptor(req.Descriptor); err != nil {
		return nil, ctldapi.RootFSDiffDescriptor{}, http.StatusBadRequest, err
	}
	objectKey := strings.Trim(strings.TrimSpace(req.Descriptor.ObjectKey), "/")
	if objectKey == "" {
		return nil, ctldapi.RootFSDiffDescriptor{}, http.StatusBadRequest, fmt.Errorf("%w: descriptor object_key is required", ErrBadRequest)
	}
	if !strings.HasPrefix(objectKey, "sandbox-rootfs/") {
		return nil, ctldapi.RootFSDiffDescriptor{}, http.StatusBadRequest, fmt.Errorf("%w: rootfs diff object key must be under sandbox-rootfs/", ErrBadRequest)
	}
	if c.store == nil {
		return nil, ctldapi.RootFSDiffDescriptor{}, http.StatusNotImplemented, fmt.Errorf("rootfs object store is not configured")
	}

	reader, err := c.store.Get(objectKey, 0, -1)
	if err != nil {
		return nil, ctldapi.RootFSDiffDescriptor{}, http.StatusInternalServerError, fmt.Errorf("download rootfs diff: %w", err)
	}
	desc := req.Descriptor
	desc.ObjectKey = objectKey
	return reader, desc, http.StatusOK, nil
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

func validateDescriptor(desc ctldapi.RootFSDiffDescriptor) error {
	if strings.TrimSpace(desc.Digest) == "" {
		return fmt.Errorf("%w: descriptor digest is required", ErrBadRequest)
	}
	if desc.Size < 0 {
		return fmt.Errorf("%w: descriptor size must be non-negative", ErrBadRequest)
	}
	if strings.TrimSpace(desc.ObjectKey) == "" {
		return fmt.Errorf("%w: descriptor object_key is required", ErrBadRequest)
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

func defaultObjectKey(teamID, sandboxID string, generation int64, digest string) (string, error) {
	teamID = strings.TrimSpace(teamID)
	sandboxID = strings.TrimSpace(sandboxID)
	if teamID == "" {
		return "", fmt.Errorf("%w: team_id is required when object_key is omitted", ErrBadRequest)
	}
	if sandboxID == "" {
		return "", fmt.Errorf("%w: sandbox_id is required when object_key is omitted", ErrBadRequest)
	}
	if containsPathSeparator(teamID) || containsPathSeparator(sandboxID) {
		return "", fmt.Errorf("%w: team_id and sandbox_id cannot contain '/'", ErrBadRequest)
	}
	digest = strings.TrimSpace(digest)
	parts := strings.SplitN(digest, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("%w: invalid diff digest %q", ErrBadRequest, digest)
	}
	return filepath.ToSlash(filepath.Join("sandbox-rootfs", teamID, sandboxID, strconv.FormatInt(generation, 10), parts[0], parts[1]+".tar")), nil
}

func containsPathSeparator(value string) bool {
	return strings.Contains(value, "/")
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
	if errors.Is(err, ErrNotFound) || errors.Is(err, ctldpower.ErrPodNotFound) || errors.Is(err, ctldpower.ErrSandboxNotFound) || errors.Is(err, ctldpower.ErrRuntimeTargetNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
