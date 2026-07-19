package rootfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
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
	CaptureBaseline(ctx context.Context, info ctldapi.RootFSInfo, teamID, sandboxID, baselineLayerID string, excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) error
}

type PortalResolver interface {
	RootFSPortalPaths(podUID string) []ctldapi.RootFSPortalPath
}

type Config struct {
	Runtime                        Runtime
	Store                          objectstore.Store
	OperationTimeout               time.Duration
	PortalResolver                 PortalResolver
	SnapshotDir                    string
	ObjectCache                    *ObjectCache
	PreparedSnapshotMaxBytes       int64
	PreparedSnapshotMaxTotalBytes  int64
	PreparedSnapshotMaxEntries     int
	PreparedSnapshotMaxTeamBytes   int64
	PreparedSnapshotMaxTeamEntries int
	PreparedSnapshotMinFreeBytes   int64
	PreparedSnapshotMaxConcurrent  int
	PreparedSnapshotMaxPerTeam     int
	PreparedSnapshotTTL            time.Duration
	PreparedSnapshotSweepInterval  time.Duration
}

type Controller struct {
	runtime                        Runtime
	store                          objectstore.Store
	operationTimeout               time.Duration
	portalResolver                 PortalResolver
	snapshotDir                    string
	objectCache                    *ObjectCache
	preparedSnapshotMaxBytes       int64
	preparedSnapshotMaxTotalBytes  int64
	preparedSnapshotMaxEntries     int
	preparedSnapshotMaxTeamBytes   int64
	preparedSnapshotMaxTeamEntries int
	preparedSnapshotMinFreeBytes   int64
	preparedSnapshotMaxConcurrent  int
	preparedSnapshotMaxPerTeam     int
	preparedSnapshotTTL            time.Duration
	preparedSnapshotSweepInterval  time.Duration
}

func NewController(cfg Config) *Controller {
	timeout := cfg.OperationTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	maxBytes := cfg.PreparedSnapshotMaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultPreparedSnapshotMaxBytes
	}
	maxTotalBytes := cfg.PreparedSnapshotMaxTotalBytes
	if maxTotalBytes <= 0 {
		maxTotalBytes = defaultPreparedSnapshotMaxTotalBytes
	}
	maxTeamBytes := cfg.PreparedSnapshotMaxTeamBytes
	if maxTeamBytes <= 0 {
		maxTeamBytes = maxBytes
	}
	maxEntries := cfg.PreparedSnapshotMaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultPreparedSnapshotMaxEntries
	}
	maxTeamEntries := cfg.PreparedSnapshotMaxTeamEntries
	if maxTeamEntries <= 0 {
		maxTeamEntries = defaultPreparedSnapshotMaxTeamEntries
	}
	maxConcurrent := cfg.PreparedSnapshotMaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = defaultPreparedSnapshotMaxConcurrent
	}
	maxPerTeam := cfg.PreparedSnapshotMaxPerTeam
	if maxPerTeam <= 0 {
		maxPerTeam = defaultPreparedSnapshotMaxPerTeam
	}
	ttl := cfg.PreparedSnapshotTTL
	if ttl <= 0 {
		ttl = defaultPreparedSnapshotTTL
	}
	sweepInterval := cfg.PreparedSnapshotSweepInterval
	if sweepInterval <= 0 {
		sweepInterval = defaultPreparedSnapshotSweepInterval
	}
	return &Controller{
		runtime:                        cfg.Runtime,
		store:                          cfg.Store,
		operationTimeout:               timeout,
		portalResolver:                 cfg.PortalResolver,
		snapshotDir:                    cfg.SnapshotDir,
		objectCache:                    cfg.ObjectCache,
		preparedSnapshotMaxBytes:       maxBytes,
		preparedSnapshotMaxTotalBytes:  maxTotalBytes,
		preparedSnapshotMaxEntries:     maxEntries,
		preparedSnapshotMaxTeamBytes:   maxTeamBytes,
		preparedSnapshotMaxTeamEntries: maxTeamEntries,
		preparedSnapshotMinFreeBytes:   max(cfg.PreparedSnapshotMinFreeBytes, 0),
		preparedSnapshotMaxConcurrent:  maxConcurrent,
		preparedSnapshotMaxPerTeam:     maxPerTeam,
		preparedSnapshotTTL:            ttl,
		preparedSnapshotSweepInterval:  sweepInterval,
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

func (c *Controller) PrepareRootFSSnapshot(r *http.Request, req ctldapi.PrepareRootFSSnapshotRequest) (ctldapi.PrepareRootFSSnapshotResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, http.StatusBadRequest
	}
	owner, err := validatePreparedSnapshotOwner(
		req.TeamID,
		req.SandboxID,
		req.ExpectedRuntimeGeneration,
		req.ExpiresAt,
		c.preparedSnapshotTTL,
	)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, http.StatusBadRequest
	}
	expiresAt := req.ExpiresAt.UTC()
	if nodeMaximum := time.Now().Add(c.preparedSnapshotTTL).UTC(); expiresAt.After(nodeMaximum) {
		expiresAt = nodeMaximum
	}
	handle, err := cleanPreparedSnapshotHandle(req.StageID)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: fmt.Sprintf("%v: stage_id is required", err)}, http.StatusBadRequest
	}
	if prepared, content, openErr := c.openPreparedSnapshot(handle); openErr == nil {
		_ = content.Close()
		expiryDifference := prepared.ExpiresAt.Sub(req.ExpiresAt)
		if expiryDifference < 0 {
			expiryDifference = -expiryDifference
		}
		if prepared.Owner != owner || expiryDifference > preparedSnapshotClockSkew {
			err := fmt.Errorf("%w: stage_id is already owned by another rootfs preparation", ErrConflict)
			return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, http.StatusConflict
		}
		return ctldapi.PrepareRootFSSnapshotResponse{
			Handle:     prepared.Handle,
			Info:       prepared.Info,
			Descriptor: prepared.Descriptor,
		}, http.StatusOK
	} else if !errors.Is(openErr, ErrNotFound) {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: openErr.Error()}, statusForError(openErr)
	}
	ctx, cancel := c.operationContext(requestContext(r))
	defer cancel()

	info, err := c.inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	if err := validateSupportedRuntime(info); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	reservation, err := c.reservePreparedSnapshot(handle, owner, expiresAt)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: fmt.Sprintf("reserve rootfs snapshot staging: %v", err)}, statusForError(err)
	}
	completed := false
	defer func() {
		if !completed {
			_ = c.discardPreparedSnapshot(handle, reservation)
		}
	}()

	portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
	ctx = withRootFSDiffMaxBytes(ctx, c.preparedSnapshotMaxBytes)
	desc, reader, err := c.createDiff(ctx, info, strings.TrimSpace(req.ParentLayerID), req.ExcludedPaths, portalPaths)
	if err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: fmt.Sprintf("create rootfs diff: %v", err)}, statusForError(err)
	}
	defer reader.Close()

	if err := c.writePreparedSnapshot(handle, owner, expiresAt, info, desc, reader); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: fmt.Sprintf("prepare rootfs snapshot: %v", err)}, statusForError(err)
	}
	if err := c.completePreparedSnapshotReservation(reservation); err != nil {
		return ctldapi.PrepareRootFSSnapshotResponse{Info: info, Error: fmt.Sprintf("complete rootfs snapshot staging reservation: %v", err)}, statusForError(err)
	}
	completed = true
	return ctldapi.PrepareRootFSSnapshotResponse{Handle: handle, Info: info, Descriptor: desc}, http.StatusOK
}

func (c *Controller) PublishRootFSSnapshot(r *http.Request, req ctldapi.PublishRootFSSnapshotRequest) (ctldapi.PublishRootFSSnapshotResponse, int) {
	if c.store == nil {
		return ctldapi.PublishRootFSSnapshotResponse{Error: "rootfs object store is not configured"}, http.StatusNotImplemented
	}
	prepared, content, err := c.openPreparedSnapshot(req.Handle)
	if err != nil {
		return ctldapi.PublishRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	defer content.Close()
	if strings.TrimSpace(req.TeamID) != prepared.Owner.TeamID ||
		strings.TrimSpace(req.SandboxID) != prepared.Owner.SandboxID ||
		req.ExpectedRuntimeGeneration != prepared.Owner.ExpectedRuntimeGeneration {
		return ctldapi.PublishRootFSSnapshotResponse{
			Info:  prepared.Info,
			Error: fmt.Sprintf("%v: prepared rootfs snapshot owner does not match publish request", ErrConflict),
		}, http.StatusConflict
	}
	objectKey := strings.Trim(strings.TrimSpace(req.ObjectKey), "/")
	if objectKey == "" {
		objectKey, err = defaultObjectKey(req.TeamID, req.SandboxID, req.ExpectedRuntimeGeneration, prepared.Descriptor.Digest)
		if err != nil {
			return ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Error: err.Error()}, http.StatusBadRequest
		}
	}
	if err := c.store.Put(objectKey, content); err != nil {
		return ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Error: fmt.Sprintf("upload rootfs diff: %v", err)}, http.StatusInternalServerError
	}
	desc := prepared.Descriptor
	desc.ObjectKey = objectKey
	if c.objectCache != nil {
		_ = c.objectCache.PutFile(requestContext(r), desc, c.preparedSnapshotContentPath(req.Handle))
	}
	_ = c.removePreparedSnapshot(req.Handle)
	return ctldapi.PublishRootFSSnapshotResponse{Info: prepared.Info, Descriptor: desc, Published: true}, http.StatusOK
}

func (c *Controller) AbortRootFSSnapshot(_ *http.Request, req ctldapi.AbortRootFSSnapshotRequest) (ctldapi.AbortRootFSSnapshotResponse, int) {
	owner, err := validateRootFSCacheOwner(req.TeamID, req.SandboxID)
	if err != nil {
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if req.ExpectedRuntimeGeneration < 0 {
		err := fmt.Errorf("%w: expected_runtime_generation must be non-negative", ErrBadRequest)
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, http.StatusBadRequest
	}
	owner.ExpectedRuntimeGeneration = req.ExpectedRuntimeGeneration
	prepared, content, err := c.openPreparedSnapshot(req.Handle)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ctldapi.AbortRootFSSnapshotResponse{Aborted: true}, http.StatusOK
		}
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	_ = content.Close()
	if prepared.Owner != owner {
		err := fmt.Errorf("%w: prepared rootfs snapshot owner does not match abort request", ErrConflict)
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, http.StatusConflict
	}
	if err := c.removePreparedSnapshot(req.Handle); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ctldapi.AbortRootFSSnapshotResponse{Error: err.Error()}, statusForError(err)
	}
	return ctldapi.AbortRootFSSnapshotResponse{Aborted: true}, http.StatusOK
}

func (c *Controller) ApplyRootFS(r *http.Request, req ctldapi.ApplyRootFSRequest) (ctldapi.ApplyRootFSResponse, int) {
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
	}
	layered := len(req.Layers) > 0
	if layered {
		if err := validateLayerDescriptors(req.Layers); err != nil {
			return ctldapi.ApplyRootFSResponse{Error: err.Error()}, http.StatusBadRequest
		}
		if _, err := validateRootFSCacheOwner(req.TeamID, req.SandboxID); err != nil {
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
	if layered {
		if err := validateBaselineLayerID(req); err != nil {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
		}
		// The layer chain represents the sandbox's writable rootfs state. Replaying
		// it across template image upgrades is valid; actual path-level conflicts
		// are surfaced by the runtime apply step below.
		portalPaths := c.portalPathsForRequest(info, req.Target, req.ExcludedPaths, req.PortalPaths)
		applied, err := c.applyLayers(ctx, info, req.Layers, req.ExcludedPaths, portalPaths)
		if err != nil {
			return ctldapi.ApplyRootFSResponse{Info: info, Error: err.Error()}, statusForError(err)
		}
		if req.BaselineLayerID != "" {
			if err := c.runtime.CaptureBaseline(ctx, info, req.TeamID, req.SandboxID, req.BaselineLayerID, req.ExcludedPaths, portalPaths); err != nil {
				// The baseline is a disposable node-local acceleration cache.
				// The durable layer was already applied successfully, so cache
				// pressure or capture failure must not turn resume into failure.
				fmt.Fprintf(os.Stderr, "capture disposable rootfs baseline cache failed: %v\n", err)
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
	var (
		reader io.ReadCloser
		err    error
	)
	if c.objectCache != nil {
		reader, _, err = c.objectCache.GetOrFetch(ctx, c.store, desc)
	} else {
		reader, err = c.store.Get(desc.ObjectKey, 0, -1)
	}
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

type preparedRootFSSnapshot struct {
	Handle     string                       `json:"handle"`
	Owner      preparedSnapshotOwner        `json:"owner"`
	ExpiresAt  time.Time                    `json:"expires_at"`
	Info       ctldapi.RootFSInfo           `json:"info"`
	Descriptor ctldapi.RootFSDiffDescriptor `json:"descriptor"`
	CreatedAt  time.Time                    `json:"created_at"`
}

func (c *Controller) writePreparedSnapshot(handle string, owner preparedSnapshotOwner, expiresAt time.Time, info ctldapi.RootFSInfo, desc ctldapi.RootFSDiffDescriptor, reader io.Reader) error {
	handle, err := cleanPreparedSnapshotHandle(handle)
	if err != nil {
		return err
	}
	if desc.Size < 0 {
		return fmt.Errorf("%w: rootfs snapshot descriptor size must be non-negative", ErrBadRequest)
	}
	if desc.Size > c.preparedSnapshotMaxBytes {
		return fmt.Errorf("%w: rootfs snapshot descriptor size %d exceeds %d bytes", ErrPreparedSnapshotTooLarge, desc.Size, c.preparedSnapshotMaxBytes)
	}
	if err := os.MkdirAll(c.preparedSnapshotDir(), 0o755); err != nil {
		return err
	}
	contentTmp := c.preparedSnapshotContentPath(handle) + ".tmp"
	content, err := os.OpenFile(contentTmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = content.Close()
			_ = c.removePreparedSnapshotFiles(handle)
		}
	}()
	limited := &io.LimitedReader{R: reader, N: c.preparedSnapshotMaxBytes + 1}
	written, err := io.Copy(content, limited)
	if err != nil {
		_ = content.Close()
		return err
	}
	if written > c.preparedSnapshotMaxBytes {
		_ = content.Close()
		return fmt.Errorf("%w: rootfs snapshot body exceeds %d bytes", ErrPreparedSnapshotTooLarge, c.preparedSnapshotMaxBytes)
	}
	if written != desc.Size {
		_ = content.Close()
		return fmt.Errorf("%w: rootfs snapshot body size %d does not match descriptor size %d", ErrConflict, written, desc.Size)
	}
	if err := content.Sync(); err != nil {
		_ = content.Close()
		return err
	}
	if err := content.Close(); err != nil {
		return err
	}
	if err := os.Rename(contentTmp, c.preparedSnapshotContentPath(handle)); err != nil {
		return err
	}

	meta := preparedRootFSSnapshot{
		Handle:     handle,
		Owner:      owner,
		ExpiresAt:  expiresAt.UTC(),
		Info:       info,
		Descriptor: desc,
		CreatedAt:  time.Now().UTC(),
	}
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	rawMeta = append(rawMeta, '\n')
	if len(rawMeta) > preparedSnapshotMetadataMaxBytes {
		return fmt.Errorf("%w: rootfs snapshot metadata exceeds %d bytes", ErrPreparedSnapshotTooLarge, preparedSnapshotMetadataMaxBytes)
	}
	metaTmp := c.preparedSnapshotMetaPath(handle) + ".tmp"
	metaFile, err := os.OpenFile(metaTmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := metaFile.Write(rawMeta); err != nil {
		_ = metaFile.Close()
		return err
	}
	if err := metaFile.Sync(); err != nil {
		_ = metaFile.Close()
		return err
	}
	if err := metaFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(metaTmp, c.preparedSnapshotMetaPath(handle)); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func (c *Controller) removePreparedSnapshot(handle string) error {
	handle, err := cleanPreparedSnapshotHandle(handle)
	if errors.Is(err, ErrBadRequest) && strings.TrimSpace(handle) == "" {
		return nil
	}
	if err != nil {
		return err
	}
	return c.removePreparedSnapshotHandle(handle)
}

func (c *Controller) preparedSnapshotContentPath(handle string) string {
	return filepath.Join(c.preparedSnapshotDir(), filepath.Base(handle)+".tar")
}

func (c *Controller) preparedSnapshotMetaPath(handle string) string {
	return filepath.Join(c.preparedSnapshotDir(), filepath.Base(handle)+".json")
}

func (c *Controller) preparedSnapshotDir() string {
	if c != nil && strings.TrimSpace(c.snapshotDir) != "" {
		return c.snapshotDir
	}
	return filepath.Join(os.TempDir(), "sandbox0-rootfs-snapshots")
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
	if errors.Is(err, ErrPreparedSnapshotTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	if errors.Is(err, ErrPreparedSnapshotBusy) {
		return http.StatusTooManyRequests
	}
	if errors.Is(err, ErrPreparedSnapshotCapacity) {
		return http.StatusInsufficientStorage
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout
	}
	return http.StatusInternalServerError
}
