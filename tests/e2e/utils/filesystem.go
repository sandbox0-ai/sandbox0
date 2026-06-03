package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) CreateSandboxFilesystem(ctx context.Context, t ContractT, req apispec.CreateSandboxFilesystemRequest) (*apispec.SandboxFilesystem, int, error) {
	specPath := "/api/v1/sandboxfilesystems"
	requestPath := "/api/v1/sandboxfilesystems"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create sandbox filesystem failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create sandbox filesystem response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSandboxFilesystems(ctx context.Context, t ContractT) ([]apispec.SandboxFilesystem, int, error) {
	specPath := "/api/v1/sandboxfilesystems"
	requestPath := "/api/v1/sandboxfilesystems"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sandbox filesystems failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil
	}
	return *resp.Data, status, nil
}

func (s *Session) GetSandboxFilesystem(ctx context.Context, t ContractT, filesystemID string) (*apispec.SandboxFilesystem, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sandbox filesystem failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sandbox filesystem response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) DeleteSandboxFilesystem(ctx context.Context, t ContractT, filesystemID string) (int, error) {
	if filesystemID == "" {
		return http.StatusNotFound, nil
	}
	specPath := "/api/v1/sandboxfilesystems/{id}"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete sandbox filesystem failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

func (s *Session) ForkSandboxFilesystem(ctx context.Context, t ContractT, filesystemID string, req apispec.ForkSandboxFilesystemRequest) (*apispec.SandboxFilesystem, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/fork"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/fork"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("fork sandbox filesystem failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("fork sandbox filesystem response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) CreateSandboxFilesystemSnapshot(ctx context.Context, t ContractT, filesystemID string, req apispec.CreateSandboxFilesystemSnapshotRequest) (*apispec.SandboxFilesystemSnapshot, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/snapshots"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create sandbox filesystem snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemSnapshotResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("create sandbox filesystem snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) ListSandboxFilesystemSnapshots(ctx context.Context, t ContractT, filesystemID string) ([]apispec.SandboxFilesystemSnapshot, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/snapshots"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/snapshots"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list sandbox filesystem snapshots failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemSnapshotListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, nil
	}
	return *resp.Data, status, nil
}

func (s *Session) GetSandboxFilesystemSnapshot(ctx context.Context, t ContractT, filesystemID, snapshotID string) (*apispec.SandboxFilesystemSnapshot, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/snapshots/{snapshot_id}"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/snapshots/" + snapshotID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get sandbox filesystem snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemSnapshotResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("get sandbox filesystem snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) RestoreSandboxFilesystemSnapshot(ctx context.Context, t ContractT, filesystemID, snapshotID string) (*apispec.SandboxFilesystem, int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/snapshots/{snapshot_id}/restore"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/snapshots/" + snapshotID + "/restore"
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, specPath, requestPath, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("restore sandbox filesystem snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessSandboxFilesystemResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil {
		return nil, status, fmt.Errorf("restore sandbox filesystem snapshot response missing data")
	}
	return resp.Data, status, nil
}

func (s *Session) DeleteSandboxFilesystemSnapshot(ctx context.Context, t ContractT, filesystemID, snapshotID string) (int, error) {
	specPath := "/api/v1/sandboxfilesystems/{id}/snapshots/{snapshot_id}"
	requestPath := "/api/v1/sandboxfilesystems/" + filesystemID + "/snapshots/" + snapshotID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete sandbox filesystem snapshot failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}
