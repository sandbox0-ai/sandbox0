package rootfs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfscow"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

type rootFSV3Runtime interface {
	Inspect(context.Context, ctldapi.RootFSContainerRef) (ctldapi.RootFSInfo, error)
	ActiveUpperdir(context.Context, ctldapi.RootFSInfo) (string, error)
	ActiveMergedRoot(context.Context, ctldapi.RootFSInfo, string) (string, error)
	BaseIdentityAndConfig(context.Context, ctldapi.RootFSInfo, *rootfshead.BaseIdentity) (rootfshead.BaseIdentity, []byte, error)
	EnsureBaseImage(context.Context, string) (rootfshead.BaseIdentity, error)
	MaterializeRootFSHead(context.Context, rootfshead.HeadReference, rootfshead.BaseIdentity, rootfshead.ImageReference, string, []byte, []byte) error
}

type rootFSSyncBinding struct {
	mu             sync.Mutex
	sandboxID      string
	teamID         string
	generation     int64
	parent         *rootfshead.HeadReference
	base           rootfshead.BaseIdentity
	baseConfig     []byte
	info           ctldapi.RootFSInfo
	writer         *rootfsstore.Writer
	session        *rootfscow.Session
	sealed         *ctldapi.SealRootFSHeadResponse
	ackedHeadID    string
	ackedPublished bool
}

type rootFSCaptureProtection struct {
	store      rootFSCaptureLeaseStore
	sandboxID  string
	teamID     string
	generation int64
}

func (p *rootFSCaptureProtection) Begin(ctx context.Context) error {
	return p.store.BeginCapture(ctx, p.sandboxID, p.teamID, p.generation)
}

func (p *rootFSCaptureProtection) Checkpoint(ctx context.Context, objects []rootfshead.Object) error {
	return p.store.CheckpointCapture(ctx, p.sandboxID, p.teamID, p.generation, objects)
}

func (p *rootFSCaptureProtection) Reset(ctx context.Context) error {
	return p.store.ResetCapture(ctx, p.sandboxID, p.teamID, p.generation)
}

func (c *Controller) BindRootFSSync(r *http.Request, req ctldapi.BindRootFSSyncRequest) (ctldapi.BindRootFSSyncResponse, int) {
	if c == nil || c.store == nil || c.v3Runtime == nil || c.captureLeases == nil {
		return ctldapi.BindRootFSSyncResponse{Error: "rootfs v3 runtime is not configured"}, http.StatusNotImplemented
	}
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, http.StatusBadRequest
	}
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.TeamID = strings.TrimSpace(req.TeamID)
	if req.SandboxID == "" || req.TeamID == "" || req.RuntimeGeneration <= 0 {
		return ctldapi.BindRootFSSyncResponse{Error: "sandbox_id, team_id, and runtime_generation are required"}, http.StatusBadRequest
	}
	key := rootFSSyncKey(req.SandboxID, req.RuntimeGeneration)
	unlockInitialization := c.lockRootFSSyncInitialization(key)
	defer unlockInitialization()
	c.v3Mu.Lock()
	existing := c.v3Sessions[key]
	c.v3Mu.Unlock()
	if existing != nil {
		existing.mu.Lock()
		defer existing.mu.Unlock()
		recoveringPublishedSeal := existing.sealed != nil && sameHeadReference(&existing.sealed.Reference, req.Parent)
		if existing.teamID != req.TeamID || (!sameHeadReference(existing.parent, req.Parent) && !recoveringPublishedSeal) {
			return ctldapi.BindRootFSSyncResponse{Error: "rootfs sync binding conflicts with the active generation"}, http.StatusConflict
		}
		return ctldapi.BindRootFSSyncResponse{Info: existing.info, Status: bindingStatusLocked(existing)}, http.StatusOK
	}
	ctx := requestContext(r)
	info, err := c.v3Runtime.Inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Error: err.Error()}, statusForError(err)
	}
	if info.Snapshotter != rootfshead.SnapshotterName {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: fmt.Sprintf("rootfs snapshotter is %q, expected %q", info.Snapshotter, rootfshead.SnapshotterName)}, http.StatusConflict
	}
	upperdir, err := c.v3Runtime.ActiveUpperdir(ctx, info)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, statusForError(err)
	}
	mergedRoot, err := c.v3Runtime.ActiveMergedRoot(ctx, info, upperdir)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, statusForError(err)
	}
	if c.portalBackings != nil {
		podUID := strings.TrimSpace(info.PodUID)
		if podUID == "" {
			podUID = strings.TrimSpace(req.Target.PodUID)
		}
		if err := c.portalBackings.AttachRootFSBackings(ctx, podUID, mergedRoot); err != nil {
			return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, statusForError(err)
		}
	}
	writer, err := rootfsstore.NewTeamWriter(c.store, req.TeamID)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	var parentHead *rootfshead.Head
	if req.Parent != nil {
		if err := rootfshead.ValidateReadableObjectScope(writer.Prefix(), req.Parent.Manifest); err != nil {
			return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusForbidden
		}
		head, err := rootfsstore.LoadHead(ctx, c.store, *req.Parent)
		if err != nil {
			return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
		}
		parentHead = &head
	}
	expectedBase := req.ExpectedBase
	if parentHead != nil {
		expectedBase = &parentHead.Base
	}
	base, baseConfig, err := c.v3Runtime.BaseIdentityAndConfig(ctx, info, expectedBase)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusConflict
	}
	editor, err := rootfscow.NewEditor(c.store, writer, parentHead)
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	excluded := normalizedRootFSExclusions(req.ExcludedPaths)
	capture, err := rootfscow.NewCapture(rootfscow.CaptureConfig{
		Root:          upperdir,
		GenerationID:  fmt.Sprintf("%s:%d", req.SandboxID, req.RuntimeGeneration),
		ExcludedPaths: excluded,
		Editor:        editor,
		Writer:        writer,
	})
	if err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusBadRequest
	}
	if _, err := c.captureLeases.EnsureCapture(ctx, req.SandboxID, req.TeamID, req.RuntimeGeneration); err != nil {
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: err.Error()}, http.StatusServiceUnavailable
	}
	session, err := rootfscow.StartSession(c.v3Context, rootfscow.SessionConfig{
		Capture:   capture,
		EventRoot: mergedRoot,
		Protection: &rootFSCaptureProtection{
			store: c.captureLeases, sandboxID: req.SandboxID,
			teamID: req.TeamID, generation: req.RuntimeGeneration,
		},
		WatchFenceRoot: c.watchFenceRoot,
	})
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		releaseErr := c.captureLeases.ReleaseCapture(cleanupCtx, req.SandboxID, req.TeamID, req.RuntimeGeneration)
		cancel()
		return ctldapi.BindRootFSSyncResponse{Info: info, Error: errors.Join(err, releaseErr).Error()}, http.StatusInternalServerError
	}
	binding := &rootFSSyncBinding{
		sandboxID:  req.SandboxID,
		teamID:     req.TeamID,
		generation: req.RuntimeGeneration,
		parent:     cloneHeadReference(req.Parent),
		base:       base,
		baseConfig: append([]byte(nil), baseConfig...),
		info:       info,
		writer:     writer,
		session:    session,
	}
	c.v3Mu.Lock()
	c.v3Sessions[key] = binding
	staleBindings := c.pruneOlderRootFSSyncBindingsLocked(req.SandboxID, req.RuntimeGeneration)
	c.v3Mu.Unlock()
	for _, stale := range staleBindings {
		_ = stale.session.Close()
		_ = c.captureLeases.ReleaseCapture(c.v3Context, stale.sandboxID, stale.teamID, stale.generation)
	}
	return ctldapi.BindRootFSSyncResponse{Info: info, Status: bindingStatus(binding)}, http.StatusOK
}

func (c *Controller) GetRootFSSyncStatus(_ *http.Request, req ctldapi.GetRootFSSyncStatusRequest) (ctldapi.GetRootFSSyncStatusResponse, int) {
	binding := c.rootFSSyncBinding(req.SandboxID, req.RuntimeGeneration)
	if binding == nil {
		return ctldapi.GetRootFSSyncStatusResponse{Error: "rootfs sync binding not found"}, http.StatusNotFound
	}
	return ctldapi.GetRootFSSyncStatusResponse{Status: bindingStatus(binding)}, http.StatusOK
}

func (c *Controller) SealRootFSHead(r *http.Request, req ctldapi.SealRootFSHeadRequest) (ctldapi.SealRootFSHeadResponse, int) {
	binding := c.rootFSSyncBinding(req.SandboxID, req.ExpectedRuntimeGeneration)
	if binding == nil {
		return ctldapi.SealRootFSHeadResponse{Error: "rootfs sync binding not found"}, http.StatusNotFound
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	if binding.teamID != strings.TrimSpace(req.TeamID) || !sameHeadReference(binding.parent, req.ExpectedParent) {
		return ctldapi.SealRootFSHeadResponse{Error: "rootfs seal team or expected parent conflict"}, http.StatusConflict
	}
	if binding.sealed != nil {
		if binding.sealed.Reference.HeadID != strings.TrimSpace(req.HeadID) {
			return ctldapi.SealRootFSHeadResponse{Error: rootfscow.ErrSessionSealed.Error()}, http.StatusConflict
		}
		return *cloneSealResponse(binding.sealed), http.StatusOK
	}
	result, err := binding.session.Seal(requestContext(r), rootfscow.SealRequest{HeadID: req.HeadID, Base: binding.base})
	if err != nil {
		status := statusForError(err)
		if errors.Is(err, rootfscow.ErrInitialScanIncomplete) {
			status = http.StatusConflict
		}
		return ctldapi.SealRootFSHeadResponse{Error: err.Error()}, status
	}
	response := &ctldapi.SealRootFSHeadResponse{
		Reference:      result.Reference,
		Head:           result.Head,
		CreatedBytes:   result.CreatedBytes,
		CreatedObjects: result.CreatedObjects,
		Timings: ctldapi.RootFSSealTimings{
			Reconcile: result.ReconcileDuration,
			Flush:     result.FlushDuration,
			Total:     result.TotalDuration,
		},
	}
	composed, err := rootfshead.ComposeImage(binding.writer.Prefix(), result.Reference, binding.baseConfig)
	if err != nil {
		response.Error = err.Error()
		return *response, http.StatusInternalServerError
	}
	if _, err := binding.writer.PutObject(requestContext(r), composed.Reference.Marker, composed.MarkerPayload); err != nil {
		response.Error = err.Error()
		return *response, statusForError(err)
	}
	if _, err := binding.writer.PutObject(requestContext(r), composed.Reference.Envelope, composed.EnvelopePayload); err != nil {
		response.Error = err.Error()
		return *response, statusForError(err)
	}
	createdBytes, createdObjects := binding.writer.CreatedMetrics()
	response.Image = composed.Reference
	response.CreatedBytes = createdBytes
	response.CreatedObjects = createdObjects
	binding.sealed = cloneSealResponse(response)
	return *cloneSealResponse(response), http.StatusOK
}

func (c *Controller) AcknowledgeRootFSHead(r *http.Request, req ctldapi.AcknowledgeRootFSHeadRequest) (ctldapi.AcknowledgeRootFSHeadResponse, int) {
	req.SandboxID = strings.TrimSpace(req.SandboxID)
	req.TeamID = strings.TrimSpace(req.TeamID)
	req.HeadID = strings.TrimSpace(req.HeadID)
	if req.SandboxID == "" || req.TeamID == "" || req.HeadID == "" || req.RuntimeGeneration <= 0 {
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: "sandbox_id, team_id, head_id, and runtime_generation are required"}, http.StatusBadRequest
	}
	binding := c.rootFSSyncBinding(req.SandboxID, req.RuntimeGeneration)
	if binding == nil {
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: "rootfs sync binding not found"}, http.StatusNotFound
	}
	binding.mu.Lock()
	if binding.teamID != req.TeamID {
		binding.mu.Unlock()
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: "rootfs acknowledgement team conflict"}, http.StatusConflict
	}
	if binding.sealed == nil {
		if binding.ackedHeadID == req.HeadID && binding.ackedPublished == req.Published {
			status := bindingStatusLocked(binding)
			binding.mu.Unlock()
			return ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true, Status: status}, http.StatusOK
		}
		if req.Published {
			binding.mu.Unlock()
			return ctldapi.AcknowledgeRootFSHeadResponse{Error: "rootfs sealed Head has no complete publication response"}, http.StatusConflict
		}
		if !req.RuntimeContinues {
			_ = binding.session.Close()
			if err := c.captureLeases.ReleaseCapture(requestContext(r), req.SandboxID, req.TeamID, req.RuntimeGeneration); err != nil {
				binding.mu.Unlock()
				return ctldapi.AcknowledgeRootFSHeadResponse{Error: err.Error()}, http.StatusServiceUnavailable
			}
		}
		if err := binding.session.Acknowledge(requestContext(r), req.HeadID, false, req.RuntimeContinues); err != nil {
			binding.mu.Unlock()
			return ctldapi.AcknowledgeRootFSHeadResponse{Error: err.Error()}, http.StatusConflict
		}
		binding.ackedHeadID = req.HeadID
		binding.ackedPublished = false
		status := bindingStatusLocked(binding)
		binding.mu.Unlock()
		if !req.RuntimeContinues {
			c.removeRootFSSyncBinding(req.SandboxID, req.RuntimeGeneration, binding)
		}
		return ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true, Status: status}, http.StatusOK
	}
	if binding.sealed.Reference.HeadID != req.HeadID {
		binding.mu.Unlock()
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: "rootfs acknowledgement Head conflict"}, http.StatusConflict
	}
	if !req.RuntimeContinues {
		_ = binding.session.Close()
		if err := c.captureLeases.ReleaseCapture(requestContext(r), req.SandboxID, req.TeamID, req.RuntimeGeneration); err != nil {
			binding.mu.Unlock()
			return ctldapi.AcknowledgeRootFSHeadResponse{Error: err.Error()}, http.StatusServiceUnavailable
		}
	}
	if err := binding.session.Acknowledge(requestContext(r), req.HeadID, req.Published, req.RuntimeContinues); err != nil {
		binding.mu.Unlock()
		return ctldapi.AcknowledgeRootFSHeadResponse{Error: err.Error()}, http.StatusConflict
	}
	if req.Published {
		binding.parent = cloneHeadReference(&binding.sealed.Reference)
	}
	binding.ackedHeadID = req.HeadID
	binding.ackedPublished = req.Published
	binding.sealed = nil
	status := bindingStatusLocked(binding)
	binding.mu.Unlock()
	if !req.RuntimeContinues {
		c.removeRootFSSyncBinding(req.SandboxID, req.RuntimeGeneration, binding)
	}
	return ctldapi.AcknowledgeRootFSHeadResponse{Acknowledged: true, Status: status}, http.StatusOK
}

func (c *Controller) removeRootFSSyncBinding(sandboxID string, generation int64, binding *rootFSSyncBinding) {
	key := rootFSSyncKey(sandboxID, generation)
	c.v3Mu.Lock()
	if c.v3Sessions[key] == binding {
		delete(c.v3Sessions, key)
	}
	c.v3Mu.Unlock()
}

func (c *Controller) MaterializeRootFSHead(r *http.Request, req ctldapi.MaterializeRootFSHeadRequest) (ctldapi.MaterializeRootFSHeadResponse, int) {
	if c == nil || c.store == nil || c.v3Runtime == nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: "rootfs v3 runtime is not configured"}, http.StatusNotImplemented
	}
	if err := req.Reference.Validate(); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if err := req.Image.Validate(); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadRequest
	}
	targetImageName := strings.TrimSpace(req.TargetImageName)
	if targetImageName == "" {
		targetImageName = req.Image.Name
	} else if err := carrier.ValidateMarkerImage(req.CarrierSlot, targetImageName); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadRequest
	}
	prefix, err := rootfsstore.PrefixFromObject(req.Reference.Manifest)
	if err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if err := rootfshead.ValidateObjectScope(prefix, req.Image.Marker); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusForbidden
	}
	if err := rootfshead.ValidateObjectScope(prefix, req.Image.Envelope); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusForbidden
	}
	head, err := rootfsstore.LoadHead(requestContext(r), c.store, req.Reference)
	if err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadGateway
	}
	marker, err := rootfsstore.Read(requestContext(r), c.store, prefix, req.Image.Marker)
	if err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadGateway
	}
	envelopePayload, err := rootfsstore.Read(requestContext(r), c.store, prefix, req.Image.Envelope)
	if err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, http.StatusBadGateway
	}
	if err := c.v3Runtime.MaterializeRootFSHead(requestContext(r), req.Reference, head.Base, req.Image, targetImageName, envelopePayload, marker); err != nil {
		return ctldapi.MaterializeRootFSHeadResponse{Error: err.Error()}, statusForError(err)
	}
	return ctldapi.MaterializeRootFSHeadResponse{ImageName: targetImageName, Materialized: true}, http.StatusOK
}

func (c *Controller) rootFSSyncBinding(sandboxID string, generation int64) *rootFSSyncBinding {
	if c == nil {
		return nil
	}
	c.v3Mu.Lock()
	defer c.v3Mu.Unlock()
	return c.v3Sessions[rootFSSyncKey(strings.TrimSpace(sandboxID), generation)]
}

func rootFSSyncKey(sandboxID string, generation int64) string {
	return fmt.Sprintf("%s:%d", strings.TrimSpace(sandboxID), generation)
}

func (c *Controller) pruneOlderRootFSSyncBindingsLocked(sandboxID string, generation int64) []*rootFSSyncBinding {
	var stale []*rootFSSyncBinding
	for key, binding := range c.v3Sessions {
		if binding == nil || binding.sandboxID != sandboxID || binding.generation >= generation {
			continue
		}
		delete(c.v3Sessions, key)
		stale = append(stale, binding)
	}
	return stale
}

func bindingStatus(binding *rootFSSyncBinding) ctldapi.RootFSSyncStatus {
	if binding == nil {
		return ctldapi.RootFSSyncStatus{}
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	return bindingStatusLocked(binding)
}

func bindingStatusLocked(binding *rootFSSyncBinding) ctldapi.RootFSSyncStatus {
	if binding == nil || binding.session == nil {
		return ctldapi.RootFSSyncStatus{}
	}
	status := binding.session.Status()
	result := ctldapi.RootFSSyncStatus{
		SandboxID:           binding.sandboxID,
		RuntimeGeneration:   binding.generation,
		InitialScanComplete: status.InitialScanComplete,
		DirtyPaths:          status.DirtyPaths,
		DirtyBytes:          status.DirtyBytes,
		ActiveCaptures:      status.ActiveCaptures,
		WatcherErrors:       status.WatcherErrors,
		Reconciliations:     status.Reconciliations,
		NeedsFullReconcile:  status.NeedsFullReconcile,
		LastError:           status.LastError,
		Sealing:             status.Sealing,
		Sealed:              status.Sealed,
	}
	if binding.sealed != nil {
		result.SealedReference = cloneHeadReference(&binding.sealed.Reference)
	} else if pending := binding.session.PendingSealReference(); pending != nil {
		result.SealedReference = pending
	}
	return result
}

func normalizedRootFSExclusions(values []string) []string {
	values = append(append([]string(nil), values...), "/tmp", "/procd", "/procd-image")
	cleaned := values[:0]
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			cleaned = append(cleaned, value)
		}
	}
	slices.Sort(cleaned)
	return slices.Compact(cleaned)
}

func sameHeadReference(left, right *rootfshead.HeadReference) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Version == right.Version && left.HeadID == right.HeadID && left.Manifest == right.Manifest
}

func cloneHeadReference(value *rootfshead.HeadReference) *rootfshead.HeadReference {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneSealResponse(value *ctldapi.SealRootFSHeadResponse) *ctldapi.SealRootFSHeadResponse {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
