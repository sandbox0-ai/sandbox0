package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) CreateSandboxVolume(ctx context.Context, t ContractT, req apispec.CreateSandboxVolumeRequest) (*apispec.SandboxVolume, int, error) {
	specPath := "/api/v1/sandboxvolumes"
	requestPath := "/api/v1/sandboxvolumes"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create sandbox volume failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxVolumeResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create sandbox volume response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSandboxVolumes(ctx context.Context, t ContractT) ([]apispec.SandboxVolume, int, *apispec.ErrorEnvelope, error) {
	specPath := "/api/v1/sandboxvolumes"
	requestPath := "/api/v1/sandboxvolumes"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, nil, err
	}
	if status != http.StatusOK {
		apiErr, err := decodeErrorEnvelope(body)
		if err != nil {
			return nil, status, nil, fmt.Errorf("list sandbox volumes failed with status %d: %s", status, formatAPIError(body))
		}
		return nil, status, apiErr, nil
	}
	var resp apispec.SuccessSandboxVolumeListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil, nil
	}
	return *resp.Data, status, nil, nil
}

func (s *Session) DeleteSandboxVolume(ctx context.Context, t ContractT, volumeID string) (int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete sandbox volume failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) DeleteSandboxVolumeEventually(ctx context.Context, t ContractT, volumeID string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.Now().Add(timeout)
	for {
		status, err := s.DeleteSandboxVolume(ctx, t, volumeID)
		if err == nil || status == http.StatusNotFound {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Session) CreateSnapshot(ctx context.Context, t ContractT, volumeID string, req apispec.CreateSnapshotRequest) (*apispec.Snapshot, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/snapshots"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSnapshotResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSnapshots(ctx context.Context, t ContractT, volumeID string) ([]apispec.Snapshot, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/snapshots"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list snapshots failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSnapshotListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil
	}
	return *resp.Data, status, nil
}

func (s *Session) RestoreSnapshot(ctx context.Context, t ContractT, volumeID, snapshotID string) (int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/snapshots/{snapshot_id}/restore"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/snapshots/" + snapshotID + "/restore"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("restore snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) DeleteSnapshot(ctx context.Context, t ContractT, volumeID, snapshotID string) (int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/snapshots/{snapshot_id}"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/snapshots/" + snapshotID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) WriteVolumeFile(ctx context.Context, t ContractT, volumeID, filePath string, content []byte, contentType string) (int, error) {
	if contentType == "" {
		contentType = contentTypeBinary
	}
	specPath := "/api/v1/sandboxvolumes/{id}/files"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/files?path=" + url.QueryEscape(filePath)
	status, body, err := s.doRawSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, content, contentType, defaultContentType, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("write volume file failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) ReadVolumeFile(ctx context.Context, t ContractT, volumeID, filePath string) ([]byte, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/files"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/files?path=" + url.QueryEscape(filePath)
	status, body, err := s.doRawSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, "", contentTypeBinary, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("read volume file failed with status %d: %s", status, formatAPIError(body))
	}
	return body, status, nil
}

func (s *Session) CloneVolumeFiles(ctx context.Context, t ContractT, volumeID string, req apispec.CloneVolumeFilesRequest) ([]apispec.CloneVolumeFileResult, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/files/clone"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/files/clone"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("clone volume files failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessCloneVolumeFilesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !bool(resp.Success) || resp.Data == nil || resp.Data.Entries == nil {
		return nil, status, fmt.Errorf("clone volume files response missing entries")
	}
	return *resp.Data.Entries, status, nil
}
