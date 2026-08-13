package rootfs

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfscow"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

// ImportRootFSImage captures a quiescent import container's complete merged
// OCI filesystem. The opaque root guarantees that no file from the fixed
// platform carrier base is observable in the resulting ImageFS.
func (c *Controller) ImportRootFSImage(r *http.Request, req ctldapi.ImportRootFSImageRequest) (ctldapi.ImportRootFSImageResponse, int) {
	started := time.Now()
	if c == nil || c.store == nil || c.v3Runtime == nil {
		return ctldapi.ImportRootFSImageResponse{Error: "S0FS ImageFS importer is not configured"}, http.StatusNotImplemented
	}
	if err := validateTarget(req.Target); err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadRequest
	}
	req.RevisionID = strings.TrimSpace(req.RevisionID)
	req.TeamID = strings.TrimSpace(req.TeamID)
	req.HeadID = strings.TrimSpace(req.HeadID)
	req.BaseImageRef = strings.TrimSpace(req.BaseImageRef)
	if req.RevisionID == "" || req.TeamID == "" || req.HeadID == "" || req.BaseImageRef == "" {
		return ctldapi.ImportRootFSImageResponse{Error: "revision_id, team_id, head_id, and base_image_ref are required"}, http.StatusBadRequest
	}
	ctx := requestContext(r)
	info, err := c.v3Runtime.Inspect(ctx, req.Target)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, statusForError(err)
	}
	upperdir, err := c.v3Runtime.ActiveUpperdir(ctx, info)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, statusForError(err)
	}
	mergedRoot, err := c.v3Runtime.ActiveMergedRoot(ctx, info, upperdir)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, statusForError(err)
	}
	_, sourceConfig, err := c.v3Runtime.BaseIdentityAndConfig(ctx, info, nil)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusConflict
	}
	base, err := c.v3Runtime.EnsureBaseImage(ctx, req.BaseImageRef)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusConflict
	}
	writer, err := rootfsstore.NewTeamWriter(c.store, req.TeamID)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadRequest
	}
	editor, err := rootfscow.NewEditor(c.store, writer, nil)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusInternalServerError
	}
	capture, err := rootfscow.NewCapture(rootfscow.CaptureConfig{
		Root:         mergedRoot,
		GenerationID: "imagefs:" + req.RevisionID,
		Editor:       editor,
		Writer:       writer,
		OpaqueRoot:   true,
	})
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadRequest
	}
	if err := capture.CaptureTree(ctx); err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: fmt.Sprintf("capture complete OCI rootfs: %v", err)}, statusForError(err)
	}
	root, err := editor.Flush(ctx)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusInternalServerError
	}
	head := rootfshead.Head{Version: rootfshead.Version, HeadID: req.HeadID, Base: base, Root: root}
	payload, err := rootfshead.EncodeHead(head)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusInternalServerError
	}
	manifest, err := writer.Put(ctx, rootfshead.HeadMediaType, payload)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadGateway
	}
	reference := rootfshead.HeadReference{Version: rootfshead.Version, HeadID: req.HeadID, Manifest: manifest}
	composed, err := rootfshead.ComposeImage(writer.Prefix(), reference, sourceConfig)
	if err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusInternalServerError
	}
	if _, err := writer.PutObject(ctx, composed.Reference.Marker, composed.MarkerPayload); err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadGateway
	}
	if _, err := writer.PutObject(ctx, composed.Reference.Envelope, composed.EnvelopePayload); err != nil {
		return ctldapi.ImportRootFSImageResponse{Error: err.Error()}, http.StatusBadGateway
	}
	createdBytes, createdObjects := writer.CreatedMetrics()
	return ctldapi.ImportRootFSImageResponse{
		Reference: reference, Head: head, Image: composed.Reference,
		SourceDigest: info.BaseImageDigest, OCIConfig: append([]byte(nil), sourceConfig...),
		CreatedBytes: createdBytes, CreatedObjects: createdObjects, Duration: time.Since(started),
	}, http.StatusOK
}
