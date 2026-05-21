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
	SandboxID string             `json:"sandbox_id"`
	TeamID    string             `json:"team_id,omitempty"`
	Webhook   *InitializeWebhook `json:"webhook,omitempty"`
}

// InitializeWebhook represents webhook configuration for initialization.
type InitializeWebhook struct {
	URL      string `json:"url"`
	Secret   string `json:"secret,omitempty"`
	WatchDir string `json:"watch_dir,omitempty"`
}

// InitializeResponse represents the response from procd initialize API.
type InitializeResponse struct {
	SandboxID string `json:"sandbox_id"`
	TeamID    string `json:"team_id,omitempty"`
}

// Pause calls the procd pause API and returns resource usage.
func (c *ProcdClient) Pause(ctx context.Context, procdAddress, internalToken string) (*PauseResponse, error) {
	url := procdAddress + "/api/v1/sandbox/pause"
	return doProcdRequest[PauseResponse](ctx, c.httpClient, http.MethodPost, url, internalToken, "pause", nil)
}

// Resume calls the procd resume API.
func (c *ProcdClient) Resume(ctx context.Context, procdAddress, internalToken string) (*ResumeResponse, error) {
	url := procdAddress + "/api/v1/sandbox/resume"
	return doProcdRequest[ResumeResponse](ctx, c.httpClient, http.MethodPost, url, internalToken, "resume", nil)
}

// Stats calls the procd stats API.
func (c *ProcdClient) Stats(ctx context.Context, procdAddress, internalToken string) (*StatsResponse, error) {
	url := procdAddress + "/api/v1/sandbox/stats"
	return doProcdRequest[StatsResponse](ctx, c.httpClient, http.MethodGet, url, internalToken, "stats", nil)
}

// Initialize calls the procd initialize API.
func (c *ProcdClient) Initialize(ctx context.Context, procdAddress string, req InitializeRequest, internalToken string) (*InitializeResponse, error) {
	url := procdAddress + "/api/v1/initialize"
	return doProcdRequest[InitializeResponse](ctx, c.httpClient, http.MethodPost, url, internalToken, "initialize", req)
}

func doProcdRequest[T any](ctx context.Context, httpClient *http.Client, method, url, internalToken, action string, request any) (*T, error) {
	var body io.Reader
	if request != nil {
		jsonBody, err := json.Marshal(request)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		body = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", internalToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	result, errInfo, err := decodeProcdResponse[T](respBody)
	if err != nil {
		return nil, fmt.Errorf("decode %s response: %w", action, err)
	}
	if errInfo != nil {
		return nil, fmt.Errorf("%s failed: %s", action, errInfo.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s failed with status %d", action, resp.StatusCode)
	}

	return result, nil
}

func decodeProcdResponse[T any](body []byte) (*T, *spec.Error, error) {
	if len(body) == 0 {
		return nil, nil, fmt.Errorf("empty response body")
	}
	return spec.DecodeResponse[T](bytes.NewReader(body))
}
