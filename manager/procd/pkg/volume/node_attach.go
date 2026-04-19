package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const (
	MountBackendNodeLocal = "ctld-bind"
)

type CtldVolumeClient interface {
	Attach(ctx context.Context, req *ctldapi.VolumeAttachRequest) (*ctldapi.VolumeAttachResponse, error)
	Detach(ctx context.Context, req *ctldapi.VolumeDetachRequest) error
}

type HTTPCtldVolumeClient struct {
	baseURL       *url.URL
	httpClient    *http.Client
	tokenProvider TokenProvider
}

func NewHTTPCtldVolumeClient(baseURL string, timeout time.Duration, tokenProvider TokenProvider) (*HTTPCtldVolumeClient, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return nil, fmt.Errorf("parse ctld base url: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("ctld base url must include scheme and host")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &HTTPCtldVolumeClient{
		baseURL:       parsed,
		httpClient:    &http.Client{Timeout: timeout},
		tokenProvider: tokenProvider,
	}, nil
}

func (c *HTTPCtldVolumeClient) Attach(ctx context.Context, req *ctldapi.VolumeAttachRequest) (*ctldapi.VolumeAttachResponse, error) {
	if req == nil || strings.TrimSpace(req.SandboxID) == "" {
		return nil, fmt.Errorf("missing sandbox id for ctld volume attach")
	}
	var resp ctldapi.VolumeAttachResponse
	requestPath := path.Join("/api/v1/sandboxes", url.PathEscape(req.SandboxID), "volumes/attach")
	if err := c.doJSON(ctx, http.MethodPost, requestPath, req, &resp); err != nil {
		return nil, err
	}
	if !resp.Attached {
		if strings.TrimSpace(resp.Error) != "" {
			return nil, fmt.Errorf("ctld volume attach failed: %s", resp.Error)
		}
		return nil, fmt.Errorf("ctld volume attach failed")
	}
	if strings.TrimSpace(resp.AttachmentID) == "" {
		return nil, fmt.Errorf("ctld volume attach response missing attachment_id")
	}
	return &resp, nil
}

func (c *HTTPCtldVolumeClient) Detach(ctx context.Context, req *ctldapi.VolumeDetachRequest) error {
	if req == nil || strings.TrimSpace(req.SandboxID) == "" {
		return fmt.Errorf("missing sandbox id for ctld volume detach")
	}
	requestPath := path.Join("/api/v1/sandboxes", url.PathEscape(req.SandboxID), "volumes/detach")
	var resp ctldapi.VolumeDetachResponse
	if err := c.doJSON(ctx, http.MethodPost, requestPath, req, &resp); err != nil {
		return err
	}
	if !resp.Detached {
		if strings.TrimSpace(resp.Error) != "" {
			return fmt.Errorf("ctld volume detach failed: %s", resp.Error)
		}
		return fmt.Errorf("ctld volume detach failed")
	}
	return nil
}

func (c *HTTPCtldVolumeClient) doJSON(ctx context.Context, method, requestPath string, in any, out any) error {
	if c == nil || c.baseURL == nil || c.httpClient == nil {
		return ErrNodeLocalMountUnavailable
	}
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal ctld volume request: %w", err)
	}
	endpoint := *c.baseURL
	endpoint.Path = path.Join(endpoint.Path, requestPath)
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build ctld volume request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	if c.tokenProvider != nil {
		if token := strings.TrimSpace(c.tokenProvider.GetInternalToken()); token != "" {
			httpReq.Header.Set("x-internal-token", token)
		}
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("call ctld volume API: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 64*1024))
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		message := strings.TrimSpace(string(respBody))
		if message == "" {
			message = httpResp.Status
		}
		return fmt.Errorf("ctld volume API returned %d: %s", httpResp.StatusCode, message)
	}
	if out == nil {
		return nil
	}
	if len(respBody) == 0 {
		return fmt.Errorf("ctld volume API returned empty response")
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode ctld volume response: %w", err)
	}
	return nil
}
