package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const (
	publishPathSuffix              = "/terminal/publish"
	crashAbandonBeginPathSuffix    = "/terminal/crash-abandon/begin"
	crashAbandonCompletePathSuffix = "/terminal/crash-abandon/complete"
	maxLifecycleRequestBytes       = 256 << 10
)

// LifecycleStore is the regional transaction boundary required to publish or
// abandon a node-local RootFS writer branch.
type LifecycleStore interface {
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
	BeginRootFSWriterCrashAbandon(context.Context, *sandboxstore.BeginRootFSWriterCrashAbandonRequest) (*sandboxstore.RootFSWriterGrant, error)
	GetRootFSGeneration(context.Context, string) (*sandboxstore.RootFSGeneration, error)
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
	WithSandboxLock(context.Context, string, func(context.Context, sandboxstore.SandboxStoreTx, *sandboxstore.SandboxRecord) error) error
}

// NewLifecycleHandler wraps the writer grant handler with terminal publish and
// crash-abandon routes that require the regional sandbox transaction store.
func NewLifecycleHandler(
	verifier CallerVerifier,
	store LifecycleStore,
	next http.Handler,
) (http.Handler, error) {
	if verifier == nil || store == nil || next == nil {
		return nil, fmt.Errorf("writer lifecycle verifier, store, and base handler are required")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		path := request.URL.EscapedPath()
		switch {
		case isLifecyclePath(path, publishPathSuffix):
			servePublish(verifier, store, writer, request)
			return
		case isLifecyclePath(path, crashAbandonBeginPathSuffix):
			serveCrashAbandonBegin(verifier, store, writer, request)
			return
		case isLifecyclePath(path, crashAbandonCompletePathSuffix):
			serveCrashAbandonComplete(verifier, store, writer, request)
			return
		}
		next.ServeHTTP(writer, request)
	}), nil
}

func serveCrashAbandonBegin(
	verifier CallerVerifier,
	store LifecycleStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	grantID, caller, body, grant, ok := decodeCrashAbandonBegin(
		verifier, store, writer, request, crashAbandonBeginPathSuffix,
	)
	if !ok {
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired &&
		grant.RetireKind == sandboxstore.RootFSWriterRetireKindCrashAbandon &&
		grant.RetireOperationID == body.OperationID {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if err := ensureCrashLifecycle(request.Context(), store, grant, body); err != nil {
		http.Error(writer, "prepare crash lifecycle: "+err.Error(), http.StatusConflict)
		return
	}
	binding, _ := hex.DecodeString(body.BindingDigest)
	if _, err := store.BeginRootFSWriterCrashAbandon(request.Context(), &sandboxstore.BeginRootFSWriterCrashAbandonRequest{
		GrantID: grantID, WriterEpoch: body.WriterEpoch, OperationID: body.OperationID,
		BindingVersion: body.BindingVersion, BindingDigest: binding,
		NodeUID: caller.NodeUID, NodeBootID: grant.NodeBootID,
		ExpectedOldGenerationID: body.ExpectedOldGenerationID,
	}); err != nil {
		http.Error(writer, "begin crash abandon: "+err.Error(), http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func serveCrashAbandonComplete(
	verifier CallerVerifier,
	store LifecycleStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	grantID, caller, begin, grant, ok := decodeCrashAbandonComplete(verifier, store, writer, request)
	if !ok {
		return
	}
	proof := begin.Proof
	proofDigest, err := proof.Digest()
	if err != nil {
		http.Error(writer, "invalid crash fence proof: "+err.Error(), http.StatusBadRequest)
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		if grant.RetireKind == sandboxstore.RootFSWriterRetireKindCrashAbandon &&
			grant.RetireOperationID == begin.OperationID && bytes.Equal(grant.RetireProofDigest, proofDigest[:]) {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(writer, "writer grant has a different terminal result", http.StatusConflict)
		return
	}
	if proof.OperationID != begin.OperationID || proof.WriterGrantID != grantID ||
		proof.WriterEpoch != grant.WriterEpoch || proof.BindingVersion != grant.BindingVersion ||
		proof.BindingDigest != begin.BindingDigest || proof.Parent != grant.GateParent ||
		proof.ClaimID != grant.ClaimID || proof.NodeUID != caller.NodeUID ||
		proof.BootID != grant.NodeBootID || proof.InitialGeneration != grant.InitialGenerationID {
		http.Error(writer, "crash proof does not match writer grant", http.StatusConflict)
		return
	}
	generation, err := store.GetRootFSGeneration(request.Context(), grant.InitialGenerationID)
	if err != nil || generation.CurrentBlockHead != proof.InitialBlockHead {
		http.Error(writer, "crash proof does not match durable generation", http.StatusConflict)
		return
	}
	record, err := store.GetSandbox(request.Context(), grant.SandboxID)
	if err != nil || record.CurrentPodName != proof.PodUID {
		http.Error(writer, "crash proof does not match Nomad allocation", http.StatusConflict)
		return
	}
	binding, _ := hex.DecodeString(begin.BindingDigest)
	err = store.WithSandboxLock(request.Context(), grant.SandboxID, func(
		ctx context.Context,
		tx sandboxstore.SandboxStoreTx,
		_ *sandboxstore.SandboxRecord,
	) error {
		crashTx, ok := tx.(sandboxstore.RootFSWriterCrashAbandonTx)
		if !ok {
			return fmt.Errorf("sandbox transaction cannot abandon rootfs writers")
		}
		_, err := crashTx.CompleteRootFSWriterCrashAbandon(ctx, &sandboxstore.CompleteRootFSWriterCrashAbandonRequest{
			LifecycleTxnID: begin.OperationID, GrantID: grantID, WriterEpoch: grant.WriterEpoch,
			OperationID: begin.OperationID, BindingVersion: grant.BindingVersion, BindingDigest: binding,
			ProofVersion: sandboxstore.RootFSWriterCrashAbandonProofVersion, ProofDigest: proofDigest[:],
			NodeUID: caller.NodeUID, NodeBootID: grant.NodeBootID,
			ExpectedOldGenerationID: grant.InitialGenerationID,
		})
		return err
	})
	if err != nil {
		http.Error(writer, "complete crash abandon: "+err.Error(), http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func decodeCrashAbandonBegin(
	verifier CallerVerifier,
	store LifecycleStore,
	writer http.ResponseWriter,
	request *http.Request,
	suffix string,
) (string, CallerIdentity, CrashAbandonBeginRequest, *sandboxstore.RootFSWriterGrant, bool) {
	var body CrashAbandonBeginRequest
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return "", CallerIdentity{}, body, nil, false
	}
	grantID, err := crashAbandonGrantID(request.URL.EscapedPath(), suffix)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return "", CallerIdentity{}, body, nil, false
	}
	if err := decodeRequestLimit(writer, request, &body, maxLifecycleRequestBytes); err != nil {
		http.Error(writer, "invalid crash abandon request", http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	if body.WriterEpoch <= 0 || body.BindingVersion != sandboxstore.RootFSWriterBindingVersion ||
		strings.TrimSpace(body.OperationID) == "" || strings.TrimSpace(body.ExpectedOldGenerationID) == "" {
		http.Error(writer, "invalid crash abandon binding", http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	binding, err := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || len(binding) != 32 || hex.EncodeToString(binding) != body.BindingDigest {
		http.Error(writer, "binding_digest must be canonical", http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil || grant == nil || grant.NodeUID != caller.NodeUID ||
		grant.WriterEpoch != body.WriterEpoch || grant.BindingVersion != body.BindingVersion ||
		!bytes.Equal(grant.BindingDigest, binding) || grant.InitialGenerationID != body.ExpectedOldGenerationID {
		http.Error(writer, "crash abandon request does not match writer grant", http.StatusConflict)
		return "", CallerIdentity{}, body, nil, false
	}
	return grantID, caller, body, grant, true
}

func decodeCrashAbandonComplete(
	verifier CallerVerifier,
	store LifecycleStore,
	writer http.ResponseWriter,
	request *http.Request,
) (string, CallerIdentity, CrashAbandonCompleteRequest, *sandboxstore.RootFSWriterGrant, bool) {
	var body CrashAbandonCompleteRequest
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return "", CallerIdentity{}, body, nil, false
	}
	grantID, err := crashAbandonGrantID(request.URL.EscapedPath(), crashAbandonCompletePathSuffix)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return "", CallerIdentity{}, body, nil, false
	}
	if err := decodeRequestLimit(writer, request, &body, maxLifecycleRequestBytes); err != nil {
		http.Error(writer, "invalid crash abandon completion", http.StatusBadRequest)
		return "", CallerIdentity{}, body, nil, false
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	binding, bindingErr := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || grant == nil || bindingErr != nil || len(binding) != 32 ||
		grant.NodeUID != caller.NodeUID || grant.WriterEpoch != body.WriterEpoch ||
		grant.BindingVersion != body.BindingVersion || !bytes.Equal(grant.BindingDigest, binding) ||
		grant.InitialGenerationID != body.ExpectedOldGenerationID || strings.TrimSpace(body.OperationID) == "" {
		http.Error(writer, "crash abandon completion does not match writer grant", http.StatusConflict)
		return "", CallerIdentity{}, body, nil, false
	}
	return grantID, caller, body, grant, true
}

func crashAbandonGrantID(path, suffix string) (string, error) {
	relative := strings.TrimPrefix(path, protocol.ConsumePathPrefix)
	if relative == path || !strings.HasSuffix(relative, suffix) {
		return "", fmt.Errorf("invalid crash abandon path")
	}
	grantID, err := url.PathUnescape(strings.TrimSuffix(relative, suffix))
	if err != nil || grantID == "" || strings.Contains(grantID, "/") {
		return "", fmt.Errorf("invalid writer grant")
	}
	return grantID, nil
}

func ensureCrashLifecycle(
	ctx context.Context,
	store LifecycleStore,
	grant *sandboxstore.RootFSWriterGrant,
	body CrashAbandonBeginRequest,
) error {
	runtimeGeneration, err := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if err != nil || runtimeGeneration <= 0 {
		return fmt.Errorf("invalid writer runtime generation")
	}
	return store.WithSandboxLock(ctx, grant.SandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		record *sandboxstore.SandboxRecord,
	) error {
		active, activeErr := tx.GetActiveLifecycleTxn(lockCtx, grant.SandboxID)
		if activeErr != nil {
			return activeErr
		}
		if active != nil {
			if active.ID == body.OperationID && active.Kind == sandboxstore.SandboxLifecycleKindPause &&
				active.Source == sandboxstore.SandboxLifecycleSourceCrash &&
				active.ExpectedHeadLayerID == body.ExpectedOldGenerationID {
				return nil
			}
			return fmt.Errorf("another lifecycle transaction %s is active", active.ID)
		}
		if record == nil || record.DesiredState != sandboxstore.SandboxDesiredStateActive ||
			record.RuntimeGeneration != runtimeGeneration || record.CurrentPodNamespace == "" || record.CurrentPodName == "" {
			return fmt.Errorf("sandbox runtime does not match crashed writer")
		}
		return tx.BeginLifecycleTxn(lockCtx, &sandboxstore.SandboxLifecycleTxn{
			ID: body.OperationID, SandboxID: grant.SandboxID, Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, Source: sandboxstore.SandboxLifecycleSourceCrash,
			Cancelable: false, FromGeneration: runtimeGeneration,
			FromPodNamespace: record.CurrentPodNamespace, FromPodName: record.CurrentPodName,
			ExpectedHeadLayerID: body.ExpectedOldGenerationID,
		})
	})
}

func servePublish(
	verifier CallerVerifier,
	store LifecycleStore,
	writer http.ResponseWriter,
	request *http.Request,
) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	relative := strings.TrimPrefix(request.URL.EscapedPath(), protocol.ConsumePathPrefix)
	grantID, err := url.PathUnescape(strings.TrimSuffix(relative, publishPathSuffix))
	if err != nil || grantID == "" || strings.Contains(grantID, "/") {
		http.Error(writer, "invalid writer grant", http.StatusBadRequest)
		return
	}
	caller, err := verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil {
		http.Error(writer, "unknown writer authority client", http.StatusUnauthorized)
		return
	}
	var body PublishGenerationRequest
	if err := decodeRequestLimit(writer, request, &body, maxLifecycleRequestBytes); err != nil {
		http.Error(writer, "invalid publish request", http.StatusBadRequest)
		return
	}
	proof, err := hex.DecodeString(strings.TrimSpace(body.ProofDigest))
	if err != nil || len(proof) != 32 || hex.EncodeToString(proof) != strings.TrimSpace(body.ProofDigest) {
		http.Error(writer, "proof_digest must be a canonical SHA-256 digest", http.StatusBadRequest)
		return
	}
	grant, err := store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil || grant == nil || grant.NodeUID != caller.NodeUID ||
		grant.WriterEpoch != body.WriterEpoch || grant.BindingVersion != body.BindingVersion {
		http.Error(writer, "publish request does not match writer grant", http.StatusConflict)
		return
	}
	binding, err := hex.DecodeString(strings.TrimSpace(body.BindingDigest))
	if err != nil || !bytes.Equal(grant.BindingDigest, binding) {
		http.Error(writer, "publish binding does not match writer grant", http.StatusConflict)
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	oldGenerationID := grant.InitialGenerationID
	filesystemID := grant.FilesystemID
	generation := body.Generation
	generation.FilesystemID = filesystemID
	generation.ParentGenerationID = oldGenerationID
	generation.WriterEpoch = grant.WriterEpoch
	if generation.ID == "" || generation.Descriptor == nil || generation.CurrentBlockHead == "" {
		http.Error(writer, "sealed generation is incomplete", http.StatusBadRequest)
		return
	}
	runtimeGeneration, err := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if err != nil {
		runtimeGeneration = 1
	}
	err = store.WithSandboxLock(request.Context(), grant.SandboxID, func(ctx context.Context, tx sandboxstore.SandboxStoreTx, record *sandboxstore.SandboxRecord) error {
		if err := preparePlannedPublishLifecycle(ctx, tx, record, grant, body.OperationID, oldGenerationID, runtimeGeneration); err != nil {
			return err
		}
		writerTx, ok := tx.(sandboxstore.RootFSWriterGrantTx)
		if !ok {
			return fmt.Errorf("sandbox transaction cannot publish rootfs generations")
		}
		if _, err := writerTx.BeginRootFSWriterRetire(ctx, &sandboxstore.BeginRootFSWriterRetireRequest{
			GrantID: grantID, WriterEpoch: grant.WriterEpoch, OperationID: body.OperationID,
			BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest,
			ExpectedOldHeadLayerID: oldGenerationID,
		}); err != nil {
			return err
		}
		if _, err := writerTx.CompleteRootFSWriterRetireAndPublishGeneration(ctx, &sandboxstore.CompleteRootFSWriterRetireAndPublishGenerationRequest{
			LifecycleTxnID: body.OperationID, GrantID: grantID, WriterEpoch: grant.WriterEpoch,
			OperationID: body.OperationID, BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest,
			ProofDigest: proof, ExpectedOldGenerationID: oldGenerationID, Generation: &generation,
		}); err != nil {
			return err
		}
		return tx.MarkRuntimePaused(ctx, grant.SandboxID, runtimeGeneration, time.Now().UTC())
	})
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, sandboxstore.ErrRootFSCompositeBacklogExhausted) {
			status = http.StatusInsufficientStorage
			writer.Header().Set("Retry-After", "1")
		}
		http.Error(writer, "publish regional retire: "+err.Error(), status)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func preparePlannedPublishLifecycle(
	ctx context.Context,
	tx sandboxstore.SandboxStoreTx,
	record *sandboxstore.SandboxRecord,
	grant *sandboxstore.RootFSWriterGrant,
	operationID string,
	expectedHead string,
	runtimeGeneration int64,
) error {
	active, err := tx.GetActiveLifecycleTxn(ctx, grant.SandboxID)
	if err != nil {
		return err
	}
	if active == nil {
		return tx.BeginLifecycleTxn(ctx, &sandboxstore.SandboxLifecycleTxn{
			ID: operationID, SandboxID: grant.SandboxID, Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase: sandboxstore.SandboxLifecyclePhasePublishing, ExpectedHeadLayerID: expectedHead,
		})
	}
	if record == nil || active.ID != operationID || active.SandboxID != grant.SandboxID ||
		active.Kind != sandboxstore.SandboxLifecycleKindPause ||
		(active.Source != sandboxstore.SandboxLifecycleSourceManual && active.Source != sandboxstore.SandboxLifecycleSourceAuto) ||
		active.Cancelable || !active.CancelRequestedAt.IsZero() || active.FromGeneration != runtimeGeneration ||
		active.FromPodNamespace != record.CurrentPodNamespace || active.FromPodName != record.CurrentPodName ||
		active.ExpectedHeadLayerID != expectedHead || active.PreparedHeadLayerID != "" {
		return fmt.Errorf("pre-existing planned pause lifecycle does not match writer grant")
	}
	switch active.Phase {
	case sandboxstore.SandboxLifecyclePhasePreparing, sandboxstore.SandboxLifecyclePhaseBarriered:
		return tx.UpdateLifecycleTxnPhase(ctx, operationID, sandboxstore.SandboxLifecyclePhasePublishing)
	case sandboxstore.SandboxLifecyclePhasePublishing, sandboxstore.SandboxLifecyclePhaseCommitting:
		return nil
	default:
		return fmt.Errorf("pre-existing planned pause lifecycle is %s", active.Phase)
	}
}

func isLifecyclePath(path, suffix string) bool {
	return strings.HasPrefix(path, protocol.ConsumePathPrefix) && strings.HasSuffix(path, suffix)
}
