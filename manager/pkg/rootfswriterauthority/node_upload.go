package rootfswriterauthority

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

const nodeUploadSuffix = "/objects"

type nodeUploadRequest struct {
	protocol.TerminalRequest
	OperationID       string                      `json:"operation_id"`
	Reference         rootfsblock.ObjectReference `json:"reference"`
	Uploaded          bool                        `json:"uploaded"`
	RebaseOperationID string                      `json:"rebase_operation_id,omitempty"`
}

type nodeUploadStore interface {
	RecordRootFSNodeUpload(context.Context, *sandboxstore.RootFSNodeUploadRequest) error
}

func serveNodeUpload(config HandlerConfig, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.Header().Set("Allow", http.MethodPut)
		writeError(w, http.StatusMethodNotAllowed, "method is not supported")
		return
	}
	grantID, err := parseRenewGrantID(strings.TrimSuffix(r.URL.EscapedPath(), nodeUploadSuffix) + renewPathSuffix)
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
	var body nodeUploadRequest
	if err = decodeRequest(w, r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.RebaseOperationID == "" {
		if err = body.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	} else if body.RebaseOperationID != body.OperationID || grantID != rootfsrebase.UploadOwnerID(body.OperationID) || body.WriterEpoch != 0 || body.BindingVersion != 0 || body.BindingDigest != "" {
		writeError(w, http.StatusBadRequest, "invalid offline upload binding")
		return
	}
	if body.OperationID == "" || strings.TrimSpace(body.OperationID) != body.OperationID || len(body.OperationID) > 512 {
		writeError(w, http.StatusBadRequest, "invalid upload operation identity")
		return
	}
	if err = rootfsblock.ValidateObjectReference(body.Reference); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	store, ok := config.Store.(nodeUploadStore)
	if !ok {
		writeClassifiedError(w, fmt.Errorf("node upload store unavailable: %w", errdefs.ErrUnavailable))
		return
	}
	digest, _ := body.DecodedBindingDigest()
	err = store.RecordRootFSNodeUpload(r.Context(), &sandboxstore.RootFSNodeUploadRequest{GrantID: grantID, NodeUID: caller.NodeUID,
		OperationID: body.OperationID, RebaseOperationID: body.RebaseOperationID, WriterEpoch: body.WriterEpoch, BindingVersion: body.BindingVersion, BindingDigest: digest, Reference: body.Reference, Uploaded: body.Uploaded})
	if err != nil {
		writeClassifiedError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// RecordRootFSNodeUpload uses the persisted binding; the one-use consume token
// is intentionally unavailable during checkpoint or crash recovery.
func (c *ManagerClient) RecordRootFSNodeUpload(ctx context.Context, stage rootfshandoff.StageRequest, operationID string, reference rootfsblock.ObjectReference, uploaded bool) error {
	binding, err := durableWriterGrantBinding(stage)
	if err != nil {
		return err
	}
	body := nodeUploadRequest{TerminalRequest: protocol.TerminalRequest{WriterEpoch: binding.WriterEpoch, BindingVersion: binding.BindingVersion, BindingDigest: binding.BindingDigest}, OperationID: operationID, Reference: reference, Uploaded: uploaded}
	return c.putWriterGrant(ctx, "record object upload", protocol.ConsumePath(stage.Identity.WriterGrantID)+nodeUploadSuffix, body, nil)
}

func (c *ManagerClient) RecordRootFSRebaseNodeUpload(ctx context.Context, operationID string, reference rootfsblock.ObjectReference, uploaded bool) error {
	body := nodeUploadRequest{OperationID: operationID, RebaseOperationID: operationID, Reference: reference, Uploaded: uploaded}
	return c.putWriterGrant(ctx, "record rebase object upload", protocol.ConsumePath(rootfsrebase.UploadOwnerID(operationID))+nodeUploadSuffix, body, nil)
}
