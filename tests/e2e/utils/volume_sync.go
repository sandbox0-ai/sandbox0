package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) UpsertSyncReplica(ctx context.Context, t ContractT, volumeID, replicaID string, req apispec.UpsertSyncReplicaRequest) (*apispec.VolumeSyncReplicaEnvelope, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/replicas/{replica_id}"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/replicas/" + replicaID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("upsert sync replica failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncReplicaResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("upsert sync replica response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) GetSyncReplica(ctx context.Context, t ContractT, volumeID, replicaID string) (*apispec.VolumeSyncReplicaEnvelope, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/replicas/{replica_id}"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/replicas/" + replicaID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sync replica failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncReplicaResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sync replica response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) AppendSyncReplicaChanges(ctx context.Context, t ContractT, volumeID, replicaID string, req apispec.AppendReplicaChangesRequest) (*apispec.AppendReplicaChangesResponse, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/replicas/{replica_id}/changes"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/replicas/" + replicaID + "/changes"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("append sync changes failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncAppendResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("append sync changes response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) UpdateSyncReplicaCursor(ctx context.Context, t ContractT, volumeID, replicaID string, req apispec.UpdateSyncReplicaCursorRequest) (*apispec.VolumeSyncReplicaEnvelope, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/replicas/{replica_id}/cursor"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/replicas/" + replicaID + "/cursor"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("update sync cursor failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncReplicaResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("update sync cursor response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSyncChanges(ctx context.Context, t ContractT, volumeID string, after int64, limit *int) (*apispec.ListVolumeSyncChangesResponse, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/changes"
	query := url.Values{}
	query.Set("after", fmt.Sprintf("%d", after))
	if limit != nil {
		query.Set("limit", fmt.Sprintf("%d", *limit))
	}
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/changes?" + query.Encode()
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sync changes failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncChangeListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("list sync changes response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSyncConflicts(ctx context.Context, t ContractT, volumeID, statusFilter string, limit *int) (*apispec.ListVolumeSyncConflictsResponse, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/conflicts"
	query := url.Values{}
	if statusFilter != "" {
		query.Set("status", statusFilter)
	}
	if limit != nil {
		query.Set("limit", fmt.Sprintf("%d", *limit))
	}
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/conflicts"
	if encoded := query.Encode(); encoded != "" {
		requestPath += "?" + encoded
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sync conflicts failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncConflictListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("list sync conflicts response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ResolveSyncConflict(ctx context.Context, t ContractT, volumeID, conflictID string, req apispec.ResolveVolumeSyncConflictRequest) (*apispec.SyncConflict, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/conflicts/{conflict_id}"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/conflicts/" + conflictID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPut, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("resolve sync conflict failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncConflictResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("resolve sync conflict response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) CreateSyncBootstrap(ctx context.Context, t ContractT, volumeID string, req *apispec.CreateVolumeSyncBootstrapRequest) (*apispec.VolumeSyncBootstrap, int, *apispec.VolumeSyncBootstrapConflictErrorEnvelope, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/bootstrap"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/bootstrap"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, nil, err
	}
	if status == http.StatusConflict {
		var conflict apispec.VolumeSyncBootstrapConflictErrorEnvelope
		if err := json.Unmarshal(body, &conflict); err != nil {
			return nil, status, nil, err
		}
		return nil, status, &conflict, nil
	}
	if status != http.StatusCreated {
		return nil, status, nil, fmt.Errorf("create sync bootstrap failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessVolumeSyncBootstrapResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, nil, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil, fmt.Errorf("create sync bootstrap response missing data")
	}
	return resp.Data, status, nil, nil
}

func (s *Session) DownloadSyncBootstrapArchive(ctx context.Context, t ContractT, volumeID, snapshotID string) ([]byte, int, error) {
	specPath := "/api/v1/sandboxvolumes/{id}/sync/bootstrap/archive"
	requestPath := "/api/v1/sandboxvolumes/" + volumeID + "/sync/bootstrap/archive?snapshot_id=" + url.QueryEscape(snapshotID)
	status, body, err := s.doRawSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, "", "application/gzip", true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("download sync bootstrap archive failed with status %d: %s", status, formatAPIError(body))
	}
	return body, status, nil
}
