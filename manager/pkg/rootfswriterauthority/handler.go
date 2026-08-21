package rootfswriterauthority

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const (
	maxRequestBytes            = 64 << 10
	maxRunningForkRequestBytes = 128 << 10
	renewPathSuffix            = "/renew"
	pressurePathSuffix         = "/pressure"
	runningForkPathSuffix      = "/fork-running"
	terminalPathSuffix         = "/terminal"
	preconsumeAbortPathSuffix  = "/terminal/preconsume-abort"
)

type CallerIdentity = nodeauth.Identity
type CallerVerifier = nodeauth.Verifier

type GrantStore interface {
	ConsumeRootFSWriterGrant(context.Context, *sandboxstore.ConsumeRootFSWriterGrantRequest) (*sandboxstore.RootFSWriterGrant, error)
	CancelRootFSWriterGrant(context.Context, *sandboxstore.CancelRootFSWriterGrantRequest) (*sandboxstore.RootFSWriterGrant, error)
	RenewRootFSWriterGrant(context.Context, *sandboxstore.RenewRootFSWriterGrantRequest, sandboxstore.RootFSWriterLeaseRenewalPolicy) (*sandboxstore.RootFSWriterGrant, error)
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
}

type batchGrantStore interface {
	RenewRootFSWriterGrants(context.Context, []*sandboxstore.RenewRootFSWriterGrantRequest, sandboxstore.RootFSWriterLeaseRenewalPolicy) ([]sandboxstore.RenewRootFSWriterGrantResult, error)
}

type terminalGrantProofStore interface {
	GetRootFSWriterTerminalProof(context.Context, string) (*sandboxstore.RootFSWriterTerminalProof, error)
}

type runningForkGrantStore interface {
	ForkRunningRootFSFilesystem(context.Context, *sandboxstore.ForkRunningRootFSFilesystemRequest) (*sandboxstore.RootFSFilesystem, error)
}

// PressurePauser persists a planned pause for one exact writer and returns
// before external Nomad stop side effects. This ordering lets the node journal
// the same operation before its plugin-independent reconciler fences runtime.
type PressurePauser interface {
	RequestRootFSWriterPressurePause(
		context.Context,
		*sandboxstore.RootFSWriterPressurePauseRequest,
	) (string, error)
}

type HandlerConfig struct {
	Verifier       CallerVerifier
	Store          GrantStore
	LeaseTTL       time.Duration
	RenewalPolicy  sandboxstore.RootFSWriterLeaseRenewalPolicy
	PressurePauser PressurePauser
}

func NewHandler(config HandlerConfig) (http.Handler, error) {
	if config.Verifier == nil || config.Store == nil {
		return nil, fmt.Errorf("writer authority verifier and store are required")
	}
	if config.LeaseTTL <= 0 {
		return nil, fmt.Errorf("writer authority lease TTL must be positive")
	}
	if config.RenewalPolicy.LeaseTTL == 0 {
		config.RenewalPolicy.LeaseTTL = config.LeaseTTL
	}
	if config.RenewalPolicy.LeaseTTL < time.Millisecond {
		return nil, fmt.Errorf("writer authority renewal lease TTL must be at least one millisecond")
	}
	if config.RenewalPolicy.GracePeriod < 0 || config.RenewalPolicy.GracePeriod > sandboxstore.RootFSWriterMaxRenewGrace {
		return nil, fmt.Errorf("writer authority renewal grace must be between zero and %s", sandboxstore.RootFSWriterMaxRenewGrace)
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.EscapedPath() == protocol.BatchRenewPath {
			serveBatchRenew(config, writer, request)
			return
		}
		if isPreconsumeAbortPath(request.URL.EscapedPath()) {
			servePreconsumeAbort(config, writer, request)
			return
		}
		if isPressurePath(request.URL.EscapedPath()) {
			servePressure(config, writer, request)
			return
		}
		if isRunningForkPath(request.URL.EscapedPath()) {
			serveRunningFork(config, writer, request)
			return
		}
		if isTerminalPath(request.URL.EscapedPath()) {
			serveTerminal(config, writer, request)
			return
		}
		if isRenewPath(request.URL.EscapedPath()) {
			serveRenew(config, writer, request)
			return
		}
		serveConsume(config, writer, request)
	}), nil
}

func servePressure(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parsePressureGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.DirtyTailPressureRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil || body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "invalid dirty-tail pressure binding")
		return
	}
	grant, err := config.Store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	if err := verifyWriterGrantBinding(grant, grantID, body.TerminalRequest, caller); err != nil {
		writeClassifiedError(writer, err)
		return
	}
	expectedOperationID := rootfshandoff.PlannedRetireOperationID(grant.GateParent, grant.ID, grant.WriterEpoch)
	if grant.State == sandboxstore.RootFSWriterGrantStateRetiring || grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		if grant.RetireKind != sandboxstore.RootFSWriterRetireKindPlannedPublish ||
			grant.RetireOperationID != expectedOperationID {
			writeClassifiedError(writer, fmt.Errorf("writer is owned by another retirement: %w", sandboxstore.ErrRootFSWriterGrantInvalidState))
			return
		}
		writePressureResponse(writer, expectedOperationID)
		return
	}
	if grant.State != sandboxstore.RootFSWriterGrantStateConsumed {
		writeClassifiedError(writer, fmt.Errorf("writer is not active: %w", sandboxstore.ErrRootFSWriterGrantInvalidState))
		return
	}
	if config.PressurePauser == nil {
		writeClassifiedError(writer, fmt.Errorf("writer pressure pauser is unavailable: %w", errdefs.ErrUnavailable))
		return
	}
	binding, _ := body.DecodedBindingDigest()
	operationID, err := config.PressurePauser.RequestRootFSWriterPressurePause(
		request.Context(),
		&sandboxstore.RootFSWriterPressurePauseRequest{
			SandboxID: grant.SandboxID, GrantID: grant.ID, WriterEpoch: grant.WriterEpoch,
			BindingVersion: grant.BindingVersion, BindingDigest: binding, NodeUID: caller.NodeUID,
		},
	)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	if operationID != expectedOperationID {
		writeClassifiedError(writer, fmt.Errorf("writer pressure operation does not match the durable binding: %w", errdefs.ErrUnavailable))
		return
	}
	writePressureResponse(writer, operationID)
}

func writePressureResponse(writer http.ResponseWriter, operationID string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(protocol.DirtyTailPressureResponse{OperationID: operationID})
}

func serveRunningFork(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parseRunningForkGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body PublishRunningForkRequest
	if err := decodeRequestLimit(writer, request, &body, maxRunningForkRequestBytes); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if body.WriterEpoch <= 0 || body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "invalid running fork writer binding")
		return
	}
	binding, err := protocol.TerminalRequest{
		WriterEpoch: body.WriterEpoch, BindingVersion: body.BindingVersion, BindingDigest: body.BindingDigest,
	}.DecodedBindingDigest()
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Checkpoint.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid running fork checkpoint: "+err.Error())
		return
	}
	grant, err := config.Store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	if err := verifyWriterGrantBinding(grant, grantID, protocol.TerminalRequest{
		WriterEpoch: body.WriterEpoch, BindingVersion: body.BindingVersion, BindingDigest: body.BindingDigest,
	}, caller); err != nil {
		writeClassifiedError(writer, err)
		return
	}
	proof := body.Checkpoint.Proof
	if proof.SourceWriterGrantID != grantID || proof.SourceSandboxID != grant.SandboxID ||
		proof.SourceFilesystemID != grant.FilesystemID || proof.SourceWriterEpoch != grant.WriterEpoch ||
		proof.BindingVersion != grant.BindingVersion || proof.BindingDigest != body.BindingDigest ||
		proof.ExpectedSourceGenerationID != grant.InitialGenerationID {
		writeClassifiedError(writer, fmt.Errorf("running fork proof does not match the source grant: %w", sandboxstore.ErrRootFSWriterGrantConflict))
		return
	}
	store, ok := config.Store.(runningForkGrantStore)
	if !ok {
		writeClassifiedError(writer, fmt.Errorf("running fork store is unavailable: %w", errdefs.ErrUnavailable))
		return
	}
	generation := runningForkGeneration(body.Checkpoint.Generation, proof.ExpectedSourceGenerationID)
	proofDigest, _ := hex.DecodeString(body.Checkpoint.ProofDigest)
	_, err = store.ForkRunningRootFSFilesystem(request.Context(), &sandboxstore.ForkRunningRootFSFilesystemRequest{
		OperationID: proof.OperationID, SourceSandboxID: proof.SourceSandboxID,
		TargetSandboxID: proof.TargetSandboxID, SourceGrantID: grantID,
		SourceWriterEpoch: grant.WriterEpoch, BindingVersion: grant.BindingVersion, BindingDigest: binding,
		CheckpointProof: proof, CheckpointProofDigest: proofDigest,
		ExpectedSourceGenerationID: grant.InitialGenerationID, Generation: &generation,
	})
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRootFSCompositeBacklogExhausted) {
			writer.Header().Set("Retry-After", "1")
			writeError(writer, http.StatusInsufficientStorage, err.Error())
			return
		}
		if errors.Is(err, sandboxstore.ErrRootFSFilesystemNotFound) {
			writeError(writer, http.StatusNotFound, err.Error())
			return
		}
		if errors.Is(err, sandboxstore.ErrRootFSFilesystemConflict) ||
			errors.Is(err, sandboxstore.ErrRootFSGenerationConflict) {
			writeError(writer, http.StatusPreconditionFailed, err.Error())
			return
		}
		writeClassifiedError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func runningForkGeneration(descriptor rootfshandoff.GenerationDescriptor, parent string) sandboxstore.RootFSGeneration {
	return sandboxstore.RootFSGeneration{
		ID: descriptor.GenerationID, FilesystemID: descriptor.FilesystemID,
		ParentGenerationID: parent, SourceOCIDigest: descriptor.SourceOCIDigest,
		BaseArtifactDigest: descriptor.BaseArtifactDigest, BaseBlockRoot: descriptor.BaseBlockRoot,
		CurrentBlockHead: descriptor.CurrentBlockHead, WriterEpoch: descriptor.WriterEpoch,
		FormatGeneration: descriptor.FormatGeneration, DurabilityState: descriptor.DurabilityState,
		LocatorVersion: descriptor.LocatorVersion, Descriptor: append([]byte(nil), descriptor.Descriptor...),
	}
}

func servePreconsumeAbort(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parsePreconsumeAbortGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.TerminalRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil || body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "invalid preconsume abort binding")
		return
	}
	grant, err := config.Store.GetRootFSWriterGrant(request.Context(), grantID)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	if err := verifyWriterGrantBinding(grant, grantID, body, caller); err != nil {
		writeClassifiedError(writer, err)
		return
	}
	if grant.State == sandboxstore.RootFSWriterGrantStateCanceled || grant.State == sandboxstore.RootFSWriterGrantStateRetired {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if grant.State != sandboxstore.RootFSWriterGrantStateIssued {
		writeClassifiedError(writer, fmt.Errorf("writer grant was already consumed: %w", sandboxstore.ErrRootFSWriterGrantInvalidState))
		return
	}
	digest, _ := body.DecodedBindingDigest()
	if _, err := config.Store.CancelRootFSWriterGrant(request.Context(), &sandboxstore.CancelRootFSWriterGrantRequest{
		GrantID: grant.ID, WriterEpoch: grant.WriterEpoch, OperationID: grant.IssueOperationID,
		BindingVersion: body.BindingVersion, BindingDigest: digest,
	}); err != nil {
		writeClassifiedError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func serveConsume(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parseGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.ConsumeRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "unsupported binding_version")
		return
	}
	digest, _ := body.DecodedBindingDigest()
	grant, err := config.Store.ConsumeRootFSWriterGrant(request.Context(), &sandboxstore.ConsumeRootFSWriterGrantRequest{
		GrantID: grantID, WriterEpoch: body.WriterEpoch,
		BindingVersion: body.BindingVersion,
		RawToken:       body.WriterToken, BindingDigest: digest,
		ConsumerNodeUID: caller.NodeUID, ConsumerCtldPodUID: caller.PodUID,
		LeaseTTL: config.LeaseTTL,
	})
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	writeLeaseObservation(writer, grant)
}

func serveTerminal(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parseTerminalGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.TerminalRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "unsupported binding_version")
		return
	}
	grant, err := getTerminalWriterGrant(request.Context(), config.Store, grantID)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound) {
			writeClassifiedError(writer, fmt.Errorf("terminal proof does not match a writer grant: %w", sandboxstore.ErrRootFSWriterGrantConflict))
		} else {
			writeClassifiedError(writer, fmt.Errorf("terminal writer grant lookup is unavailable: %w", errdefs.ErrUnavailable))
		}
		return
	}
	if err := verifyTerminalGrant(grant, grantID, body, caller); err != nil {
		writeClassifiedError(writer, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func getTerminalWriterGrant(ctx context.Context, store GrantStore, grantID string) (*sandboxstore.RootFSWriterGrant, error) {
	grant, err := store.GetRootFSWriterGrant(ctx, grantID)
	if err == nil || !errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound) {
		return grant, err
	}
	proofStore, ok := store.(terminalGrantProofStore)
	if !ok {
		return nil, err
	}
	proof, proofErr := proofStore.GetRootFSWriterTerminalProof(ctx, grantID)
	if proofErr != nil {
		return nil, proofErr
	}
	if proof == nil {
		return nil, fmt.Errorf("terminal writer proof lookup returned no record: %w", errdefs.ErrUnavailable)
	}
	return &sandboxstore.RootFSWriterGrant{
		ID: proof.GrantID, SandboxID: proof.SandboxID,
		WriterEpoch: proof.WriterEpoch, BindingVersion: proof.BindingVersion,
		BindingDigest: append([]byte(nil), proof.BindingDigest...),
		NodeUID:       proof.NodeUID, State: proof.State,
	}, nil
}

func verifyTerminalGrant(
	grant *sandboxstore.RootFSWriterGrant,
	grantID string,
	request protocol.TerminalRequest,
	caller CallerIdentity,
) error {
	if err := verifyWriterGrantBinding(grant, grantID, request, caller); err != nil {
		return err
	}
	if grant.State != sandboxstore.RootFSWriterGrantStateRetired && grant.State != sandboxstore.RootFSWriterGrantStateCanceled {
		return fmt.Errorf("writer grant is not terminal: %w", sandboxstore.ErrRootFSWriterGrantInvalidState)
	}
	return nil
}

func verifyWriterGrantBinding(
	grant *sandboxstore.RootFSWriterGrant,
	grantID string,
	request protocol.TerminalRequest,
	caller CallerIdentity,
) error {
	if grant == nil {
		return fmt.Errorf("terminal writer grant lookup returned no record: %w", errdefs.ErrUnavailable)
	}
	if grant.ID != grantID || grant.NodeUID != caller.NodeUID {
		return fmt.Errorf("terminal proof does not match the authenticated grant owner: %w", sandboxstore.ErrRootFSWriterGrantConflict)
	}
	if grant.WriterEpoch != request.WriterEpoch {
		return fmt.Errorf("terminal proof writer epoch does not match: %w", sandboxstore.ErrRootFSWriterEpochConflict)
	}
	digest, _ := request.DecodedBindingDigest()
	if grant.BindingVersion != request.BindingVersion || !bytes.Equal(grant.BindingDigest, digest) {
		return fmt.Errorf("terminal proof does not match the immutable writer binding: %w", sandboxstore.ErrRootFSWriterGrantConflict)
	}
	return nil
}

func serveRenew(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parseRenewGrantID(request.URL.EscapedPath())
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.RenewRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if body.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(writer, http.StatusBadRequest, "unsupported binding_version")
		return
	}
	digest, _ := body.DecodedBindingDigest()
	grant, err := config.Store.RenewRootFSWriterGrant(request.Context(), &sandboxstore.RenewRootFSWriterGrantRequest{
		GrantID: grantID, WriterEpoch: body.WriterEpoch,
		BindingVersion: body.BindingVersion, BindingDigest: digest, ConsumerNodeUID: caller.NodeUID,
	}, config.RenewalPolicy)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	writeLeaseObservation(writer, grant)
}

func serveBatchRenew(config HandlerConfig, writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPut {
		writer.Header().Set("Allow", http.MethodPut)
		writeError(writer, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	bearer, err := bearerToken(request.Header.Get("Authorization"))
	if err != nil {
		writeError(writer, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(request.Context(), bearer)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	var body protocol.BatchRenewRequest
	if err := decodeRequest(writer, request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if err := body.Validate(); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	response := protocol.BatchRenewResponse{Results: make([]protocol.BatchRenewResult, len(body.Items))}
	validRequests := make([]*sandboxstore.RenewRootFSWriterGrantRequest, 0, len(body.Items))
	validIndexes := make([]int, 0, len(body.Items))
	for index, item := range body.Items {
		response.Results[index].GrantID = item.GrantID
		if item.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
			response.Results[index].ErrorCode = protocol.RenewErrorInvalidArgument
			response.Results[index].Error = "unsupported binding_version"
			continue
		}
		digest, _ := item.DecodedBindingDigest()
		validRequests = append(validRequests, &sandboxstore.RenewRootFSWriterGrantRequest{
			GrantID: item.GrantID, WriterEpoch: item.WriterEpoch,
			BindingVersion: item.BindingVersion, BindingDigest: digest, ConsumerNodeUID: caller.NodeUID,
		})
		validIndexes = append(validIndexes, index)
	}
	if store, ok := config.Store.(batchGrantStore); ok && len(validRequests) != 0 {
		results, batchErr := store.RenewRootFSWriterGrants(request.Context(), validRequests, config.RenewalPolicy)
		if batchErr != nil || len(results) != len(validRequests) {
			for _, index := range validIndexes {
				setBatchRenewError(&response.Results[index], errdefs.ErrUnavailable)
			}
		} else {
			for resultIndex, result := range results {
				setBatchRenewResult(&response.Results[validIndexes[resultIndex]], result.Grant, result.Err)
			}
		}
	} else {
		for resultIndex, renewalRequest := range validRequests {
			grant, renewErr := config.Store.RenewRootFSWriterGrant(request.Context(), renewalRequest, config.RenewalPolicy)
			setBatchRenewResult(&response.Results[validIndexes[resultIndex]], grant, renewErr)
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(response)
}

func setBatchRenewResult(result *protocol.BatchRenewResult, grant *sandboxstore.RootFSWriterGrant, renewErr error) {
	if renewErr != nil {
		setBatchRenewError(result, renewErr)
		return
	}
	observation, observationErr := leaseObservation(grant)
	if observationErr != nil {
		setBatchRenewError(result, observationErr)
		return
	}
	result.Observation = &observation
}

func setBatchRenewError(result *protocol.BatchRenewResult, err error) {
	result.Observation = nil
	result.ErrorCode = renewErrorCode(err)
	result.Error = result.ErrorCode
}

func writeLeaseObservation(writer http.ResponseWriter, grant *sandboxstore.RootFSWriterGrant) {
	observation, err := leaseObservation(grant)
	if err != nil {
		writeClassifiedError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(observation)
}

func leaseObservation(grant *sandboxstore.RootFSWriterGrant) (protocol.LeaseObservation, error) {
	if grant == nil || grant.AuthorityObservedAt.IsZero() || grant.LeaseExpiresAt.IsZero() ||
		!grant.LeaseExpiresAt.After(grant.AuthorityObservedAt) {
		return protocol.LeaseObservation{}, fmt.Errorf("writer authority returned an invalid lease: %w", errdefs.ErrUnavailable)
	}
	remaining := grant.LeaseExpiresAt.Sub(grant.AuthorityObservedAt)
	observation := protocol.LeaseObservation{
		ServerTime: grant.AuthorityObservedAt, LeaseExpiresAt: grant.LeaseExpiresAt,
		RenewAfter: grant.AuthorityObservedAt.Add(remaining / 2),
	}
	if err := observation.Validate(); err != nil {
		return protocol.LeaseObservation{}, fmt.Errorf("writer authority returned an invalid lease: %w: %w", err, errdefs.ErrUnavailable)
	}
	return observation, nil
}

func renewErrorCode(err error) string {
	switch {
	case errdefs.IsInvalidArgument(err):
		return protocol.RenewErrorInvalidArgument
	case errdefs.IsPermissionDenied(err), errors.Is(err, sandboxstore.ErrRootFSWriterGrantConflict):
		return protocol.RenewErrorPermissionDenied
	case errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound):
		return protocol.RenewErrorNotFound
	case errors.Is(err, context.DeadlineExceeded):
		return protocol.RenewErrorDeadlineExceeded
	case errors.Is(err, sandboxstore.ErrRootFSWriterGrantExpired),
		errors.Is(err, sandboxstore.ErrRootFSWriterLeaseExpired),
		errors.Is(err, sandboxstore.ErrRootFSWriterEpochConflict),
		errors.Is(err, sandboxstore.ErrRootFSWriterGrantInvalidState):
		return protocol.RenewErrorFailedPrecondition
	default:
		return protocol.RenewErrorUnavailable
	}
}

func decodeRequest(writer http.ResponseWriter, request *http.Request, target any) error {
	return decodeRequestLimit(writer, request, target, maxRequestBytes)
}

func decodeRequestLimit(writer http.ResponseWriter, request *http.Request, target any, limit int64) error {
	request.Body = http.MaxBytesReader(writer, request.Body, limit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain one JSON value")
	}
	return nil
}

func parseGrantID(path string) (string, error) {
	if !strings.HasPrefix(path, protocol.ConsumePathPrefix) {
		return "", fmt.Errorf("invalid writer grant path")
	}
	value, err := url.PathUnescape(strings.TrimPrefix(path, protocol.ConsumePathPrefix))
	if err != nil || strings.TrimSpace(value) == "" || strings.Contains(value, "/") {
		return "", fmt.Errorf("invalid writer grant ID")
	}
	return value, nil
}

func parseRenewGrantID(path string) (string, error) {
	if !strings.HasSuffix(path, renewPathSuffix) {
		return "", fmt.Errorf("invalid writer grant renewal path")
	}
	return parseGrantID(strings.TrimSuffix(path, renewPathSuffix))
}

func parsePressureGrantID(path string) (string, error) {
	if !strings.HasSuffix(path, pressurePathSuffix) {
		return "", fmt.Errorf("invalid writer grant pressure path")
	}
	return parseGrantID(strings.TrimSuffix(path, pressurePathSuffix))
}

func parseRunningForkGrantID(path string) (string, error) {
	if !strings.HasSuffix(path, runningForkPathSuffix) {
		return "", fmt.Errorf("invalid writer grant running fork path")
	}
	return parseGrantID(strings.TrimSuffix(path, runningForkPathSuffix))
}

func parseTerminalGrantID(path string) (string, error) {
	if !strings.HasSuffix(path, terminalPathSuffix) {
		return "", fmt.Errorf("invalid writer grant terminal path")
	}
	return parseGrantID(strings.TrimSuffix(path, terminalPathSuffix))
}

func parsePreconsumeAbortGrantID(path string) (string, error) {
	if !strings.HasSuffix(path, preconsumeAbortPathSuffix) {
		return "", fmt.Errorf("invalid writer grant preconsume abort path")
	}
	return parseGrantID(strings.TrimSuffix(path, preconsumeAbortPathSuffix))
}

func isRenewPath(path string) bool {
	return isGrantActionPath(path, renewPathSuffix)
}

func isPressurePath(path string) bool {
	return isGrantActionPath(path, pressurePathSuffix)
}

func isRunningForkPath(path string) bool {
	return isGrantActionPath(path, runningForkPathSuffix)
}

func isTerminalPath(path string) bool {
	return isGrantActionPath(path, terminalPathSuffix)
}

func isPreconsumeAbortPath(path string) bool {
	return isGrantActionPath(path, preconsumeAbortPathSuffix)
}

func isGrantActionPath(path, suffix string) bool {
	if !strings.HasPrefix(path, protocol.ConsumePathPrefix) {
		return false
	}
	relative := strings.TrimPrefix(path, protocol.ConsumePathPrefix)
	if !strings.HasSuffix(relative, suffix) {
		return false
	}
	grantID := strings.TrimSuffix(relative, suffix)
	return grantID != "" && !strings.Contains(grantID, "/")
}

func bearerToken(value string) (string, error) {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("a bearer token is required")
	}
	return parts[1], nil
}

func writeClassifiedError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errdefs.IsInvalidArgument(err):
		status = http.StatusBadRequest
	case errdefs.IsPermissionDenied(err):
		status = http.StatusForbidden
	case errors.Is(err, sandboxstore.ErrRootFSWriterGrantNotFound):
		status = http.StatusNotFound
	case errors.Is(err, sandboxstore.ErrRootFSWriterGrantConflict):
		status = http.StatusForbidden
	case errors.Is(err, sandboxstore.ErrRootFSWriterGrantExpired),
		errors.Is(err, sandboxstore.ErrRootFSWriterLeaseExpired),
		errors.Is(err, sandboxstore.ErrRootFSWriterEpochConflict),
		errors.Is(err, sandboxstore.ErrRootFSWriterGrantInvalidState):
		status = http.StatusPreconditionFailed
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errdefs.IsUnavailable(err):
		status = http.StatusServiceUnavailable
	}
	writeError(writer, status, err.Error())
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"error": message})
}
