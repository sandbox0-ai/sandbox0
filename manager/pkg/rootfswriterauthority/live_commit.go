package rootfswriterauthority

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const liveCommitSuffix = "/live-commit"
const liveInstallSuffix = "/live-commit/installed"

type PublishLiveCommitRequest struct {
	protocol.TerminalRequest
	OperationID                   string                        `json:"operation_id"`
	ExpectedCommittedGenerationID string                        `json:"expected_committed_generation_id"`
	BaseGenerationID              string                        `json:"base_generation_id"`
	CutSequence                   uint64                        `json:"cut_sequence"`
	Generation                    sandboxstore.RootFSGeneration `json:"generation"`
}

type InstallLiveCommitRequest struct {
	protocol.TerminalRequest
	OperationID  string `json:"operation_id"`
	GenerationID string `json:"generation_id"`
	CutSequence  uint64 `json:"cut_sequence"`
}

type liveCommitStore interface {
	PublishRootFSLiveCommit(context.Context, *sandboxstore.PublishRootFSLiveCommitRequest) (*sandboxstore.RootFSLiveProgress, error)
	InstallRootFSLiveCommit(context.Context, *sandboxstore.InstallRootFSLiveCommitRequest) (*sandboxstore.RootFSLiveProgress, error)
}

func validLiveCommitIdentity(operation string, sequence uint64) bool {
	return strings.HasPrefix(operation, "live/") && len(operation) <= 512 && strings.TrimSpace(operation) == operation && sequence > 0 && sequence <= math.MaxInt64
}

func serveLiveCommit(config HandlerConfig, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.Header().Set("Allow", http.MethodPut)
		writeError(w, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	install := strings.HasSuffix(r.URL.EscapedPath(), liveInstallSuffix)
	suffix := liveCommitSuffix
	if install {
		suffix = liveInstallSuffix
	}
	grantID, err := parseRenewGrantID(strings.TrimSuffix(r.URL.EscapedPath(), suffix) + renewPathSuffix)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	bearer, err := bearerToken(r.Header.Get("Authorization"))
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	caller, err := config.Verifier.Verify(r.Context(), bearer)
	if err != nil {
		writeClassifiedError(w, err)
		return
	}
	var publish PublishLiveCommitRequest
	var installed InstallLiveCommitRequest
	var binding protocol.TerminalRequest
	if install {
		err = decodeRequest(w, r, &installed)
		binding = installed.TerminalRequest
		if err == nil && (!validLiveCommitIdentity(installed.OperationID, installed.CutSequence) || installed.GenerationID == "") {
			err = fmt.Errorf("invalid live installation identity")
		}
	} else {
		err = decodeRequestLimit(w, r, &publish, maxRunningForkRequestBytes)
		binding = publish.TerminalRequest
		if err == nil && (!validLiveCommitIdentity(publish.OperationID, publish.CutSequence) || publish.ExpectedCommittedGenerationID == "" || publish.BaseGenerationID == "" || publish.Generation.ID == "" || len(publish.Generation.Descriptor) == 0) {
			err = fmt.Errorf("invalid live commit identity")
		}
	}
	if err == nil {
		err = binding.Validate()
	}
	if err != nil || binding.BindingVersion != sandboxstore.RootFSWriterBindingVersion {
		writeError(w, http.StatusBadRequest, "invalid live writer request")
		return
	}
	operation, cutSequence := publish.OperationID, publish.CutSequence
	if install {
		operation, cutSequence = installed.OperationID, installed.CutSequence
	}
	sequence, err := protocol.LiveCommitSequence(grantID, operation)
	if err != nil || sequence != cutSequence {
		writeError(w, http.StatusBadRequest, "live operation does not match its grant and cut")
		return
	}
	grant, err := config.Store.GetRootFSWriterGrant(r.Context(), grantID)
	if err != nil {
		writeClassifiedError(w, err)
		return
	}
	if err = verifyWriterGrantBinding(grant, grantID, binding, caller); err != nil {
		writeClassifiedError(w, err)
		return
	}
	store, ok := config.Store.(liveCommitStore)
	if !ok {
		writeClassifiedError(w, fmt.Errorf("live commit store unavailable: %w", errdefs.ErrUnavailable))
		return
	}
	digest, _ := binding.DecodedBindingDigest()
	var progress *sandboxstore.RootFSLiveProgress
	if install {
		progress, err = store.InstallRootFSLiveCommit(r.Context(), &sandboxstore.InstallRootFSLiveCommitRequest{
			GrantID: grantID, NodeUID: caller.NodeUID, WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion, BindingDigest: digest,
			OperationID: installed.OperationID, GenerationID: installed.GenerationID, CutSequence: installed.CutSequence,
		})
	} else {
		progress, err = store.PublishRootFSLiveCommit(r.Context(), &sandboxstore.PublishRootFSLiveCommitRequest{
			GrantID: grantID, NodeUID: caller.NodeUID, WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion, BindingDigest: digest,
			OperationID: publish.OperationID, ExpectedCommittedGenerationID: publish.ExpectedCommittedGenerationID, BaseGenerationID: publish.BaseGenerationID,
			CutSequence: publish.CutSequence, Generation: &publish.Generation,
		})
	}
	if err != nil {
		writeClassifiedError(w, err)
		return
	}
	if progress == nil {
		writeClassifiedError(w, errdefs.ErrUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(progress)
}

func (c *ManagerClient) PublishLiveCommit(ctx context.Context, stage rootfshandoff.StageRequest, request PublishLiveCommitRequest) (sandboxstore.RootFSLiveProgress, error) {
	var progress sandboxstore.RootFSLiveProgress
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return progress, err
	}
	request.TerminalRequest = protocol.TerminalRequest{WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion, BindingDigest: binding.BindingDigest}
	if !validLiveCommitIdentity(request.OperationID, request.CutSequence) || request.Generation.ID == "" || request.ExpectedCommittedGenerationID == "" || request.BaseGenerationID == "" {
		return progress, errdefs.ErrInvalidArgument
	}
	sequence, err := protocol.LiveCommitSequence(stage.Identity.WriterGrantID, request.OperationID)
	if err != nil || sequence != request.CutSequence {
		return progress, errdefs.ErrInvalidArgument
	}
	err = c.putWriterGrant(ctx, "publish live checkpoint", protocol.ConsumePath(stage.Identity.WriterGrantID)+liveCommitSuffix, request, &progress)
	if err == nil {
		err = validateLiveProgress(progress, request.OperationID, request.Generation.ID, request.CutSequence, false)
	}
	return progress, err
}

func (c *ManagerClient) InstallLiveCommit(ctx context.Context, stage rootfshandoff.StageRequest, request InstallLiveCommitRequest) (sandboxstore.RootFSLiveProgress, error) {
	var progress sandboxstore.RootFSLiveProgress
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return progress, err
	}
	request.TerminalRequest = protocol.TerminalRequest{WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion, BindingDigest: binding.BindingDigest}
	if !validLiveCommitIdentity(request.OperationID, request.CutSequence) || request.GenerationID == "" {
		return progress, errdefs.ErrInvalidArgument
	}
	sequence, err := protocol.LiveCommitSequence(stage.Identity.WriterGrantID, request.OperationID)
	if err != nil || sequence != request.CutSequence {
		return progress, errdefs.ErrInvalidArgument
	}
	err = c.putWriterGrant(ctx, "acknowledge installed checkpoint", protocol.ConsumePath(stage.Identity.WriterGrantID)+liveInstallSuffix, request, &progress)
	if err == nil {
		err = validateLiveProgress(progress, request.OperationID, request.GenerationID, request.CutSequence, true)
	}
	return progress, err
}

func validateLiveProgress(p sandboxstore.RootFSLiveProgress, operation, generation string, sequence uint64, installed bool) error {
	if p.OperationID != operation || p.CommittedGenerationID != generation || p.CommittedSequence != sequence || p.InstalledGenerationID == "" || p.InstalledSequence > p.CommittedSequence ||
		installed && (p.InstalledGenerationID != generation || p.InstalledSequence != sequence) {
		return fmt.Errorf("live commit acknowledgement does not match the durable cut: %w", errdefs.ErrUnavailable)
	}
	return nil
}
