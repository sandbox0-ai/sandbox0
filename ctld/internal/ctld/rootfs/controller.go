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
	CreateDiff(ctx context.Context, info ctldapi.RootFSInfo, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error)
	CreateDiffFromBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error)
	ApplyDiff(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, content io.Reader, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, error)
	CaptureBaseline(ctx context.Context, info ctldapi.RootFSInfo, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) error
}

type S0FSRuntime interface {
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
	if s0fsRuntime, ok := c.runtime.(S0FSRuntime); ok {
		head, err := s0fsRuntime.CommitS0FSRootFS(ctx, S0FSCommitRequest{
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
	desc, reader, err := c.createDiff(ctx, info, strings.TrimSpace(req.ParentLayerID), req.ExcludedPaths, portalPaths)
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
	headed := !rootFSHeadDescriptorEmpty(req.Head)
	layered := len(req.Layers) > 0
	if headed {
		if err := validateRootFSHeadDescriptor(req.Head); err != nil {
			return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
		}
	} else if layered {
		if err := validateLayerDescriptors(req.Layers); err != nil {
			return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
		}
	} else if err := validateDescriptor(req.Descriptor); err != nil {
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
	if headed {
		s0fsRuntime, ok := c.runtime.(S0FSRuntime)
		if !ok {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: "s0fs rootfs attach is not supported by this runtime"}, http.StatusNotImplemented
		}
		portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
		head, mountPath, err := s0fsRuntime.AttachS0FSRootFS(ctx, S0FSAttachRequest{
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
	if layered {
		if err := validateStrictExpectedBase(info, req); err != nil {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, http.StatusConflict
		}
		if err := validateBaselineLayerID(req); err != nil {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
		}
		portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
		applied, err := c.applyLayers(ctx, info, req.Layers, req.ExcludedPaths, portalPaths)
		if err != nil {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, statusForError(err)
		}
		if req.BaselineLayerID != "" {
			if err := c.runtime.CaptureBaseline(ctx, info, req.BaselineLayerID, req.ExcludedPaths, portalPaths); err != nil {
				return ctldapi.ApplyRootFSResponse{Info: info, Error: fmt.Sprintf("capture rootfs baseline: %v", err)}, statusForError(err)
			}
		}
		return ctldapi.ApplyRootFSResponse{Info: info, Layers: applied, Applied: true}, http.StatusOK
	}

	portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
	applied, err := c.applyDescriptor(ctx, info, req.Descriptor, req.ExcludedPaths, portalPaths)
	if err != nil {
		return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, statusForError(err)
	}
	return ctldapi.ApplyRootFSResponse{Info: info, Descriptor: applied, Applied: true}, http.StatusOK
}

func (c *Controller) createDiff(ctx context.Context, info ctldapi.RootFSInfo, parentLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, io.ReadSeekCloser, error) {
	if parentLayerID != "" {
		return c.runtime.CreateDiffFromBaseline(ctx, info, parentLayerID, excludedPaths, portalPaths)
	}
	return c.runtime.CreateDiff(ctx, info, excludedPaths, portalPaths)
}

func (c *Controller) applyLayers(ctx context.Context, info ctldapi.RootFSInfo, layers []ctldapi.RootFSLayerDescriptor, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) ([]ctldapi.RootFSLayerDescriptor, error) {
	applied := make([]ctldapi.RootFSLayerDescriptor, 0, len(layers))
	for _, layer := range layers {
		desc, err := c.applyDescriptor(ctx, info, layer.Descriptor, excludedPaths, portalPaths)
		if err != nil {
			return nil, err
		}
		applied = append(applied, ctldapi.RootFSLayerDescriptor{
			LayerID:       layer.LayerID,
			ParentLayerID: layer.ParentLayerID,
			Descriptor:    desc,
		})
	}
	return applied, nil
}

func (c *Controller) applyDescriptor(ctx context.Context, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) (ctldapi.RootFSDiffDescriptor, error) {
	reader, err := c.store.Get(desc.ObjectKey, 0, -1)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("download rootfs diff: %w", err)
	}
	defer reader.Close()

	applied, err := c.runtime.ApplyDiff(ctx, info, desc, reader, excludedPaths, portalPaths)
	if err != nil {
		return ctldapi.RootFSDiffDescriptor{}, fmt.Errorf("apply rootfs diff: %w", err)
	}
	applied.ObjectKey = desc.ObjectKey
	return applied, nil
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

func validateLayerDescriptors(layers []ctldapi.RootFSLayerDescriptor) error {
	if len(layers) == 0 {
		return fmt.Errorf("%w: layers are required", ErrBadRequest)
	}
	seen := make(map[string]struct{}, len(layers))
	for i, layer := range layers {
		layerID := strings.TrimSpace(layer.LayerID)
		if layerID == "" {
			return fmt.Errorf("%w: layers[%d].layer_id is required", ErrBadRequest, i)
		}
		if _, ok := seen[layerID]; ok {
			return fmt.Errorf("%w: duplicate rootfs layer %q", ErrBadRequest, layerID)
		}
		seen[layerID] = struct{}{}
		if strings.TrimSpace(layer.ParentLayerID) == layerID {
			return fmt.Errorf("%w: layers[%d].parent_layer_id cannot reference itself", ErrBadRequest, i)
		}
		if i > 0 && strings.TrimSpace(layer.ParentLayerID) != strings.TrimSpace(layers[i-1].LayerID) {
			return fmt.Errorf("%w: layers[%d].parent_layer_id must reference previous layer", ErrBadRequest, i)
		}
		if err := validateDescriptor(layer.Descriptor); err != nil {
			return fmt.Errorf("%w: layers[%d]: %v", ErrBadRequest, i, err)
		}
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

func validateStrictExpectedBase(info ctldapi.RootFSInfo, req ctldapi.ApplyRootFSRequest) error {
	if expected := strings.TrimSpace(req.ExpectedBaseImageDigest); expected != "" && strings.TrimSpace(info.BaseImageDigest) != expected {
		return fmt.Errorf("%w: base image digest mismatch: expected %s, got %s", ErrConflict, expected, info.BaseImageDigest)
	}
	if expected := strings.TrimSpace(req.ExpectedSnapshotParent); expected != "" && strings.TrimSpace(info.SnapshotParent) != expected {
		return fmt.Errorf("%w: snapshot parent mismatch: expected %s, got %s", ErrConflict, expected, info.SnapshotParent)
	}
	if len(req.ExpectedSnapshotParentChain) > 0 && !equalStringSlices(req.ExpectedSnapshotParentChain, info.SnapshotParentChain) {
		return fmt.Errorf("%w: snapshot parent chain mismatch", ErrConflict)
	}
	return nil
}

func validateBaselineLayerID(req ctldapi.ApplyRootFSRequest) error {
	if strings.TrimSpace(req.BaselineLayerID) == "" {
		return nil
	}
	if len(req.Layers) == 0 {
		return fmt.Errorf("%w: baseline_layer_id requires layers", ErrBadRequest)
	}
	head := strings.TrimSpace(req.Layers[len(req.Layers)-1].LayerID)
	if strings.TrimSpace(req.BaselineLayerID) != head {
		return fmt.Errorf("%w: baseline_layer_id must match the head layer", ErrBadRequest)
	}
	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strings.TrimSpace(a[i]) != strings.TrimSpace(b[i]) {
			return false
		}
	}
	return true
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
	if errors.Is(err, ErrNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
