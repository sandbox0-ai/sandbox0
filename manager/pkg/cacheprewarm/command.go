package cacheprewarm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
)

// CommandClient is manager-only. Keeping this transport outside procdapi
// preserves the exact deployed procd binary digest during control-only rollouts.
type CommandClient struct{ httpClient *http.Client }

func NewCommandClient(client *http.Client) *CommandClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &CommandClient{httpClient: client}
}

func (c *CommandClient) CreateCommand(ctx context.Context, address, token string, command []string) (*procdapi.ContextResponse, error) {
	if len(command) == 0 {
		return nil, fmt.Errorf("command is required")
	}
	body, err := json.Marshal(procdapi.CreateContextRequest{Type: procdapi.ProcessTypeCMD,
		Cmd: &procdapi.CreateCMDContextRequest{Command: command}, WaitUntilDone: true})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, address+procdapi.ContextsPath, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Token", token)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > 1<<20 {
		return nil, fmt.Errorf("command response exceeds 1 MiB")
	}
	result, apiErr, err := spec.DecodeResponse[procdapi.ContextResponse](bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if apiErr != nil {
		return nil, fmt.Errorf("command failed: %s", apiErr.Message)
	}
	if response.StatusCode != http.StatusCreated || result == nil || result.ExitCode == nil || *result.ExitCode != 0 {
		return nil, fmt.Errorf("command did not complete successfully (status %d)", response.StatusCode)
	}
	return result, nil
}
