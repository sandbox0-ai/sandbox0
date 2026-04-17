package service

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

const (
	webhookStateMountPoint = "/var/lib/sandbox0/procd"
	webhookStateVolumeKind = "webhook-state"
)

// SandboxSystemVolumeClient creates and deletes manager-owned sandbox volumes.
type SandboxSystemVolumeClient interface {
	Create(ctx context.Context, teamID, userID, sandboxID, kind string) (string, error)
	Delete(ctx context.Context, teamID, userID, sandboxID, volumeID string) error
}

type StorageProxyVolumeClient struct {
	baseURL        string
	httpClient     *http.Client
	tokenGenerator TokenGenerator
}

type StorageProxyVolumeClientConfig struct {
	BaseURL        string
	HTTPClient     *http.Client
	TokenGenerator TokenGenerator
}

func NewStorageProxyVolumeClient(cfg StorageProxyVolumeClientConfig) *StorageProxyVolumeClient {
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &StorageProxyVolumeClient{
		baseURL:        strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/"),
		httpClient:     httpClient,
		tokenGenerator: cfg.TokenGenerator,
	}
}

func (c *StorageProxyVolumeClient) Create(ctx context.Context, teamID, userID, sandboxID, kind string) (string, error) {
	if c == nil || c.baseURL == "" {
		return "", fmt.Errorf("storage-proxy volume client is not configured")
	}
	token, err := c.generateToken(teamID, userID, sandboxID)
	if err != nil {
		return "", err
	}
	body := map[string]any{
		"cache_size":  "64M",
		"buffer_size": "8M",
		"access_mode": "RWO",
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/sandboxvolumes", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("create %s volume: %w", kind, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read create volume response: %w", err)
	}
	volume, apiErr, err := spec.DecodeResponse[struct {
		ID string `json:"id"`
	}](bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("decode create volume response: %w", err)
	}
	if apiErr != nil {
		return "", fmt.Errorf("create %s volume failed: %s", kind, apiErr.Message)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("create %s volume failed with status %d", kind, resp.StatusCode)
	}
	if volume == nil || strings.TrimSpace(volume.ID) == "" {
		return "", fmt.Errorf("create %s volume returned no id", kind)
	}
	return volume.ID, nil
}

func (c *StorageProxyVolumeClient) Delete(ctx context.Context, teamID, userID, sandboxID, volumeID string) error {
	if c == nil || c.baseURL == "" || strings.TrimSpace(volumeID) == "" {
		return nil
	}
	token, err := c.generateToken(teamID, userID, sandboxID)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/sandboxvolumes/"+volumeID+"?force=true", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Internal-Token", token)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("delete webhook state volume: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	data, _ := io.ReadAll(resp.Body)
	_, apiErr, decodeErr := spec.DecodeResponse[map[string]bool](bytes.NewReader(data))
	if decodeErr == nil && apiErr != nil {
		return fmt.Errorf("delete webhook state volume failed: %s", apiErr.Message)
	}
	return fmt.Errorf("delete webhook state volume failed with status %d", resp.StatusCode)
}

func (c *StorageProxyVolumeClient) generateToken(teamID, userID, sandboxID string) (string, error) {
	if c.tokenGenerator == nil {
		return "", fmt.Errorf("storage-proxy token generator not configured")
	}
	return c.tokenGenerator.GenerateToken(teamID, userID, sandboxID)
}

type webhookStateVolume struct {
	VolumeID string
	Mount    ClaimMount
}

func (s *SandboxService) prepareWebhookStateVolume(ctx context.Context, req *ClaimRequest, sandboxID string) (*webhookStateVolume, error) {
	if s == nil || s.getWebhookInfo(req) == nil {
		return nil, nil
	}
	if s.webhookStateVolumes == nil {
		return nil, fmt.Errorf("webhook state volume client is not configured")
	}
	volumeID, err := s.webhookStateVolumes.Create(ctx, req.TeamID, req.UserID, sandboxID, webhookStateVolumeKind)
	if err != nil {
		return nil, err
	}
	return &webhookStateVolume{
		VolumeID: volumeID,
		Mount: ClaimMount{
			SandboxVolumeID: volumeID,
			MountPoint:      webhookStateMountPoint,
		},
	}, nil
}

func (s *SandboxService) deleteWebhookStateVolume(ctx context.Context, info SandboxLifecycleInfo) error {
	volumeID := strings.TrimSpace(info.WebhookStateVolumeID)
	if volumeID == "" || s == nil || s.webhookStateVolumes == nil {
		return nil
	}
	return s.webhookStateVolumes.Delete(ctx, info.TeamID, info.UserID, info.SandboxID, volumeID)
}

func appendWebhookStateMount(mounts []ClaimMount, state *webhookStateVolume) []ClaimMount {
	if state == nil {
		return mounts
	}
	out := make([]ClaimMount, 0, len(mounts)+1)
	out = append(out, mounts...)
	out = append(out, state.Mount)
	return out
}

// SandboxDeletionWebhookEmitter emits manager-owned sandbox deletion lifecycle events.
type SandboxDeletionWebhookEmitter interface {
	EmitSandboxDeleted(ctx context.Context, info SandboxLifecycleInfo) error
}

type HTTPSandboxDeletionWebhookEmitter struct {
	httpClient *http.Client
}

func NewHTTPSandboxDeletionWebhookEmitter(httpClient *http.Client) *HTTPSandboxDeletionWebhookEmitter {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPSandboxDeletionWebhookEmitter{httpClient: httpClient}
}

func (e *HTTPSandboxDeletionWebhookEmitter) EmitSandboxDeleted(ctx context.Context, info SandboxLifecycleInfo) error {
	url := strings.TrimSpace(info.WebhookURL)
	if e == nil || url == "" {
		return nil
	}
	event := map[string]any{
		"event_id":   "evt_sandbox_deleted_" + info.SandboxID,
		"event_type": "sandbox.deleted",
		"timestamp":  time.Now().UTC(),
		"sandbox_id": info.SandboxID,
		"team_id":    info.TeamID,
		"payload": map[string]any{
			"reason": "pod_deleted",
		},
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if info.WebhookSecret != "" {
		req.Header.Set("X-Sandbox0-Signature", signWebhookPayload(info.WebhookSecret, body))
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("emit sandbox.deleted webhook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("emit sandbox.deleted webhook failed with status %d", resp.StatusCode)
	}
	return nil
}

func signWebhookPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
