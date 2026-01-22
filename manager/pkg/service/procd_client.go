package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
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
	SandboxID string             `json:"sandbox_id"`
	TeamID    string             `json:"team_id,omitempty"`
	Webhook   *InitializeWebhook `json:"webhook,omitempty"`
}

// InitializeWebhook represents webhook configuration for initialization.
type InitializeWebhook struct {
	URL    string `json:"url"`
	Secret string `json:"secret,omitempty"`
}

// InitializeResponse represents the response from procd initialize API.
type InitializeResponse struct {
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id,omitempty"`
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pause failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result PauseResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("resume failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result ResumeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
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

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stats failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result StatsResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

// Initialize calls the procd initialize API.
func (c *ProcdClient) Initialize(ctx context.Context, procdAddress string, req InitializeRequest, internalToken, procdStorageToken string) (*InitializeResponse, error) {
	url := procdAddress + "/api/v1/initialize"

	respBody, err := c.doRequest(ctx, http.MethodPost, url, req, internalToken, procdStorageToken)
	if err != nil {
		return nil, err
	}

	var result InitializeResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &result, nil
}

// doRequest is a helper for making HTTP requests.
func (c *ProcdClient) doRequest(ctx context.Context, method, url string, body any, internalToken, procdStorageToken string) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
