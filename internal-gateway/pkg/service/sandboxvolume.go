package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sandbox0-ai/infra/internal-gateway/pkg/db"
	"go.uber.org/zap"
)

var (
	ErrSandboxNotFound   = errors.New("sandbox not found")
	ErrVolumeNotFound    = errors.New("sandboxvolume not found")
	ErrAttachFailed      = errors.New("failed to attach sandboxvolume")
	ErrDetachFailed      = errors.New("failed to detach sandboxvolume")
	ErrMountFailed       = errors.New("failed to mount sandboxvolume")
	ErrUnmountFailed     = errors.New("failed to unmount sandboxvolume")
	ErrStorageProxyError = errors.New("storage proxy error")
	ErrProcdError        = errors.New("procd error")
)

// SandboxVolumeService handles SandboxVolume attach/detach coordination
type SandboxVolumeService struct {
	repo            *db.Repository
	storageProxyURL string
	logger          *zap.Logger
	httpClient      *http.Client
}

// NewSandboxVolumeService creates a new SandboxVolume service
func NewSandboxVolumeService(repo *db.Repository, storageProxyURL string, logger *zap.Logger) *SandboxVolumeService {
	return &SandboxVolumeService{
		repo:            repo,
		storageProxyURL: storageProxyURL,
		logger:          logger,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// AttachRequest represents a request to attach a SandboxVolume
type AttachRequest struct {
	SandboxID  string `json:"sandbox_id" binding:"required"`
	MountPoint string `json:"mount_point" binding:"required"`
	ReadOnly   bool   `json:"read_only"`
	SnapshotID string `json:"snapshot_id,omitempty"`
}

// AttachResponse represents the response from attach operation
type AttachResponse struct {
	SandboxVolumeID string `json:"sandboxvolume_id"`
	SandboxID       string `json:"sandbox_id"`
	MountPoint      string `json:"mount_point"`
	MountedAt       string `json:"mounted_at"`
}

// DetachRequest represents a request to detach a SandboxVolume
type DetachRequest struct {
	SandboxID string `json:"sandbox_id" binding:"required"`
}

// DetachResponse represents the response from detach operation
type DetachResponse struct {
	Detached    bool   `json:"detached"`
	UnmountedAt string `json:"unmounted_at"`
}

// StorageProxyAttachResponse is the response from Storage Proxy attach API
type StorageProxyAttachResponse struct {
	SandboxVolumeID     string `json:"sandboxvolume_id"`
	SandboxID           string `json:"sandbox_id"`
	MountPoint          string `json:"mount_point"`
	Token               string `json:"token"`
	StorageProxyAddress string `json:"storage_proxy_address"`
}

// Attach coordinates attaching a SandboxVolume to a sandbox
// Flow:
// 1. Call Storage Proxy to prepare mount (get token)
// 2. Call Procd to mount with token
// 3. If Procd mount fails, call Storage Proxy to detach (rollback)
func (s *SandboxVolumeService) Attach(ctx context.Context, sandboxVolumeID string, req *AttachRequest) (*AttachResponse, error) {
	s.logger.Info("Starting sandboxvolume attach",
		zap.String("sandboxvolume_id", sandboxVolumeID),
		zap.String("sandbox_id", req.SandboxID),
		zap.String("mount_point", req.MountPoint),
	)

	// Step 1: Get sandbox info to find procd address
	sandbox, err := s.repo.GetSandbox(ctx, req.SandboxID)
	if err != nil {
		if err == db.ErrNotFound {
			return nil, ErrSandboxNotFound
		}
		return nil, fmt.Errorf("get sandbox: %w", err)
	}

	// Step 2: Call Storage Proxy to prepare mount (get token)
	spResp, err := s.callStorageProxyAttach(ctx, sandboxVolumeID, req)
	if err != nil {
		s.logger.Error("Storage Proxy attach failed",
			zap.String("sandboxvolume_id", sandboxVolumeID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("%w: %v", ErrStorageProxyError, err)
	}

	// Step 3: Call Procd to mount with token
	err = s.callProcdMount(ctx, sandbox.ProcdAddress, sandboxVolumeID, req, spResp)
	if err != nil {
		s.logger.Error("Procd mount failed, initiating rollback",
			zap.String("sandboxvolume_id", sandboxVolumeID),
			zap.String("sandbox_id", req.SandboxID),
			zap.Error(err),
		)

		// Rollback: call Storage Proxy detach
		if rollbackErr := s.callStorageProxyDetach(ctx, sandboxVolumeID, req.SandboxID); rollbackErr != nil {
			s.logger.Error("Rollback detach failed",
				zap.String("sandboxvolume_id", sandboxVolumeID),
				zap.Error(rollbackErr),
			)
		}

		return nil, fmt.Errorf("%w: %v", ErrMountFailed, err)
	}

	s.logger.Info("SandboxVolume attached successfully",
		zap.String("sandboxvolume_id", sandboxVolumeID),
		zap.String("sandbox_id", req.SandboxID),
	)

	return &AttachResponse{
		SandboxVolumeID: sandboxVolumeID,
		SandboxID:       req.SandboxID,
		MountPoint:      req.MountPoint,
		MountedAt:       time.Now().Format(time.RFC3339),
	}, nil
}

// Detach coordinates detaching a SandboxVolume from a sandbox
// Flow:
// 1. Call Procd to unmount first
// 2. Call Storage Proxy to detach record
func (s *SandboxVolumeService) Detach(ctx context.Context, sandboxVolumeID string, req *DetachRequest) (*DetachResponse, error) {
	s.logger.Info("Starting sandboxvolume detach",
		zap.String("sandboxvolume_id", sandboxVolumeID),
		zap.String("sandbox_id", req.SandboxID),
	)

	// Step 1: Get sandbox info to find procd address
	sandbox, err := s.repo.GetSandbox(ctx, req.SandboxID)
	if err != nil {
		if err == db.ErrNotFound {
			// Sandbox not found, skip procd unmount and just detach from storage proxy
			s.logger.Warn("Sandbox not found, skipping procd unmount",
				zap.String("sandbox_id", req.SandboxID),
			)
			goto detachStorageProxy
		}
		return nil, fmt.Errorf("get sandbox: %w", err)
	}

	// Step 2: Call Procd to unmount first
	err = s.callProcdUnmount(ctx, sandbox.ProcdAddress, sandboxVolumeID)
	if err != nil {
		s.logger.Warn("Procd unmount failed, continuing with storage proxy detach",
			zap.String("sandboxvolume_id", sandboxVolumeID),
			zap.Error(err),
		)
		// Continue with storage proxy detach even if procd unmount fails
		// This ensures we don't leave orphaned attach records
	}

detachStorageProxy:
	// Step 3: Call Storage Proxy to detach record
	err = s.callStorageProxyDetach(ctx, sandboxVolumeID, req.SandboxID)
	if err != nil {
		s.logger.Error("Storage Proxy detach failed",
			zap.String("sandboxvolume_id", sandboxVolumeID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("%w: %v", ErrStorageProxyError, err)
	}

	s.logger.Info("SandboxVolume detached successfully",
		zap.String("sandboxvolume_id", sandboxVolumeID),
		zap.String("sandbox_id", req.SandboxID),
	)

	return &DetachResponse{
		Detached:    true,
		UnmountedAt: time.Now().Format(time.RFC3339),
	}, nil
}

// callStorageProxyAttach calls Storage Proxy attach API
func (s *SandboxVolumeService) callStorageProxyAttach(ctx context.Context, sandboxVolumeID string, req *AttachRequest) (*StorageProxyAttachResponse, error) {
	url := fmt.Sprintf("%s/api/v1/sandboxvolumes/%s/attach", s.storageProxyURL, sandboxVolumeID)

	body := map[string]interface{}{
		"sandbox_id":  req.SandboxID,
		"mount_point": req.MountPoint,
		"read_only":   req.ReadOnly,
	}
	if req.SnapshotID != "" {
		body["snapshot_id"] = req.SnapshotID
	}

	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("storage proxy returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var spResp StorageProxyAttachResponse
	if err := json.NewDecoder(resp.Body).Decode(&spResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &spResp, nil
}

// callStorageProxyDetach calls Storage Proxy detach API
func (s *SandboxVolumeService) callStorageProxyDetach(ctx context.Context, sandboxVolumeID, sandboxID string) error {
	url := fmt.Sprintf("%s/api/v1/sandboxvolumes/%s/detach", s.storageProxyURL, sandboxVolumeID)

	body := map[string]string{
		"sandbox_id": sandboxID,
	}
	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("storage proxy returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// callProcdMount calls Procd mount API
func (s *SandboxVolumeService) callProcdMount(ctx context.Context, procdAddress, sandboxVolumeID string, req *AttachRequest, spResp *StorageProxyAttachResponse) error {
	url := fmt.Sprintf("%s/api/v1/sandboxvolumes/mount", procdAddress)

	body := map[string]interface{}{
		"sandboxvolume_id":      sandboxVolumeID,
		"sandbox_id":            req.SandboxID,
		"mount_point":           req.MountPoint,
		"token":                 spResp.Token,
		"storage_proxy_address": spResp.StorageProxyAddress,
		"read_only":             req.ReadOnly,
	}
	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("procd returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// callProcdUnmount calls Procd unmount API
func (s *SandboxVolumeService) callProcdUnmount(ctx context.Context, procdAddress, sandboxVolumeID string) error {
	url := fmt.Sprintf("%s/api/v1/sandboxvolumes/unmount", procdAddress)

	body := map[string]string{
		"sandboxvolume_id": sandboxVolumeID,
	}
	jsonBody, _ := json.Marshal(body)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("procd returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}
