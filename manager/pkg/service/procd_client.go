package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// ProcdClientConfig holds configuration for ProcdClient
type ProcdClientConfig struct {
	Timeout time.Duration
}

// ProcdClient is an HTTP client for calling procd APIs.
type ProcdClient struct {
	httpClient *http.Client
}

// NewProcdClient creates a new procd client.
func NewProcdClient(config ProcdClientConfig) *ProcdClient {
	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &ProcdClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// NewProcdClientWithHTTPClient creates a procd client with a custom HTTP client.
func NewProcdClientWithHTTPClient(httpClient *http.Client) *ProcdClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &ProcdClient{
		httpClient: httpClient,
	}
}

// ResourceUsage represents resource consumption from procd.
type ResourceUsage struct {
	CPUPercent                float64 `json:"cpu_percent"`
	MemoryRSS                 int64   `json:"memory_rss"`
	MemoryVMS                 int64   `json:"memory_vms"`
	OpenFiles                 int     `json:"open_files"`
	ThreadCount               int     `json:"thread_count"`
	ContainerMemoryUsage      int64   `json:"container_memory_usage,omitempty"`
	ContainerMemoryLimit      int64   `json:"container_memory_limit,omitempty"`
	ContainerMemoryWorkingSet int64   `json:"container_memory_working_set,omitempty"`
	IOReadBytes               int64   `json:"io_read_bytes,omitempty"`
	IOWriteBytes              int64   `json:"io_write_bytes,omitempty"`
	MemoryBytes               int64   `json:"memory_bytes"`
}

// ContextResourceUsage represents resource usage for a single context.
type ContextResourceUsage struct {
	ContextID string        `json:"context_id"`
	Type      string        `json:"type"`
	Language  string        `json:"language"`
	Running   bool          `json:"running"`
	Paused    bool          `json:"paused"`
	Usage     ResourceUsage `json:"usage"`
}

// SandboxResourceUsage represents aggregated resource usage for the entire sandbox.
type SandboxResourceUsage struct {
	ContainerMemoryUsage      int64 `json:"container_memory_usage"`
	ContainerMemoryLimit      int64 `json:"container_memory_limit"`
	ContainerMemoryWorkingSet int64 `json:"container_memory_working_set"`
	TotalMemoryRSS            int64 `json:"total_memory_rss"`
	TotalMemoryVMS            int64 `json:"total_memory_vms"`
	TotalOpenFiles            int   `json:"total_open_files"`
	TotalThreadCount          int   `json:"total_thread_count"`
	TotalIOReadBytes          int64 `json:"total_io_read_bytes"`
	TotalIOWriteBytes         int64 `json:"total_io_write_bytes"`
	ContextCount              int   `json:"context_count"`
	RunningContextCount       int   `json:"running_context_count"`
	PausedContextCount        int   `json:"paused_context_count"`

	Contexts []ContextResourceUsage `json:"contexts"`
}

// PauseResponse represents the response from procd pause API.
type PauseResponse struct {
	Paused        bool                  `json:"paused"`
	Error         string                `json:"error,omitempty"`
	ResourceUsage *SandboxResourceUsage `json:"resource_usage,omitempty"`
}

// ResumeResponse represents the response from procd resume API.
type ResumeResponse struct {
	Resumed bool   `json:"resumed"`
	Error   string `json:"error,omitempty"`
}

// StatsResponse represents the response from procd stats API.
type StatsResponse struct {
	SandboxResourceUsage
}

// InitializeRequest represents the procd initialize request.
type InitializeRequest struct {
	SandboxID          string                   `json:"sandbox_id"`
	TeamID             string                   `json:"team_id,omitempty"`
	Webhook            *InitializeWebhook       `json:"webhook,omitempty"`
	Mounts             []InitializeMountRequest `json:"mounts,omitempty"`
	WaitForMounts      bool                     `json:"wait_for_mounts,omitempty"`
	MountWaitTimeoutMs int32                    `json:"mount_wait_timeout_ms,omitempty"`
}

type InitializeMountRequest struct {
	SandboxVolumeID string             `json:"sandboxvolume_id"`
	MountPoint      string             `json:"mount_point"`
	VolumeConfig    *MountVolumeConfig `json:"volume_config,omitempty"`
}

// InitializeWebhook represents webhook configuration for initialization.
type InitializeWebhook struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// InitializeResponse represents the response from procd initialize API.
type InitializeResponse struct {
	SandboxID       string                 `json:"sandbox_id"`
	TeamID          string                 `json:"team_id,omitempty"`
	BootstrapMounts []BootstrapMountStatus `json:"bootstrap_mounts,omitempty"`
}

type MountStatusResponse struct {
	Mounts []BootstrapMountStatus `json:"mounts,omitempty"`
}

// Pause calls the procd pause API and returns resource usage.
func (c *ProcdClient) Pause(ctx context.Context, procdAddress, internalToken, procdStorageToken string) (*PauseResponse, error) {
	url := procdAddress + "/api/v1/sandbox/pause"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", internalToken)
	req.Header.Set("X-Token-For-Procd", procdStorageToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[PauseResponse](body)
	if err != nil {
		return nil, fmt.Errorf("decode pause response: %w", err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("pause failed: %s", errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pause failed with status %d", resp.StatusCode)
	}

	return result, nil
}

// Resume calls the procd resume API.
func (c *ProcdClient) Resume(ctx context.Context, procdAddress, internalToken, procdStorageToken string) (*ResumeResponse, error) {
	url := procdAddress + "/api/v1/sandbox/resume"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", internalToken)
	req.Header.Set("X-Token-For-Procd", procdStorageToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[ResumeResponse](body)
	if err != nil {
		return nil, fmt.Errorf("decode resume response: %w", err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("resume failed: %s", errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resume failed with status %d", resp.StatusCode)
	}

	return result, nil
}

// Stats calls the procd stats API.
func (c *ProcdClient) Stats(ctx context.Context, procdAddress, internalToken, procdStorageToken string) (*StatsResponse, error) {
	url := procdAddress + "/api/v1/sandbox/stats"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", internalToken)
	req.Header.Set("X-Token-For-Procd", procdStorageToken)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[StatsResponse](body)
	if err != nil {
		return nil, fmt.Errorf("decode stats response: %w", err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("stats failed: %s", errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats failed with status %d", resp.StatusCode)
	}

	return result, nil
}

// Initialize calls the procd initialize API.
func (c *ProcdClient) Initialize(ctx context.Context, procdAddress string, req InitializeRequest, internalToken, procdStorageToken string) (*InitializeResponse, error) {
	url := procdAddress + "/api/v1/initialize"

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal body: %w", err)
	}
	reqBody := bytes.NewReader(jsonBody)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Internal-Token", internalToken)
	httpReq.Header.Set("X-Token-For-Procd", procdStorageToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[InitializeResponse](respBody)
	if err != nil {
		return nil, fmt.Errorf("decode initialize response: %w", err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("initialize failed: %s", errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("initialize failed with status %d", resp.StatusCode)
	}

	return result, nil
}

// MountStatus calls the procd sandbox volume status API.
func (c *ProcdClient) MountStatus(ctx context.Context, procdAddress, internalToken, procdStorageToken string) (*MountStatusResponse, error) {
	url := procdAddress + "/api/v1/sandboxvolumes/status"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Internal-Token", internalToken)
	httpReq.Header.Set("X-Token-For-Procd", procdStorageToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[MountStatusResponse](respBody)
	if err != nil {
		return nil, fmt.Errorf("decode mount status response: %w", err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("mount status failed: %s", errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mount status failed with status %d", resp.StatusCode)
	}

	return result, nil
}

func decodeProcdResponse[T any](body []byte) (*T, *spec.Error, error) {
	if len(body) == 0 {
		return nil, nil, fmt.Errorf("empty response body")
	}
	return spec.DecodeResponse[T](bytes.NewReader(body))
}
