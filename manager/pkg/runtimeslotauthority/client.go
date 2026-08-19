package runtimeslotauthority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const maxResponseBytes = 64 << 10

type ClientConfig struct {
	BaseURL        string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	Timeout        time.Duration
}

type Client struct {
	transport *nodeauth.HTTPSClient
}

func NewClient(config ClientConfig) (*Client, error) {
	transport, err := nodeauth.NewHTTPSClient(nodeauth.HTTPSClientConfig{
		Authority: "runtime slot authority", BaseURL: config.BaseURL, CAFile: config.CAFile,
		ClientCertFile: config.ClientCertFile, ClientKeyFile: config.ClientKeyFile,
		TokenFile: config.TokenFile, Timeout: config.Timeout,
	})
	if err != nil {
		return nil, err
	}
	return &Client{transport: transport}, nil
}

func (c *Client) Register(ctx context.Context, slotID string, request protocol.RegistrationRequest) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	if err := request.Validate(); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodPut, protocol.SlotPath(slotID), request, slotID)
}

func (c *Client) Observe(ctx context.Context, slotID string) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodGet, protocol.SlotPath(slotID), nil, slotID)
}

func (c *Client) Ready(ctx context.Context, slotID string, request protocol.ReadinessRequest) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	if err := request.Validate(); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodPut, protocol.ReadyPath(slotID), request, slotID)
}

func (c *Client) Heartbeat(ctx context.Context, slotID string, request protocol.HeartbeatRequest) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	if err := request.Validate(); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodPut, protocol.HeartbeatPath(slotID), request, slotID)
}

func (c *Client) Starting(ctx context.Context, slotID string, request protocol.StartingRequest) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	if err := request.Validate(); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodPut, protocol.StartingPath(slotID), request, slotID)
}

func (c *Client) CommandReady(ctx context.Context, slotID string, request protocol.CommandReadyRequest) (protocol.Observation, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	if err := request.Validate(); err != nil {
		return protocol.Observation{}, invalidRequest(err)
	}
	return c.exchange(ctx, http.MethodPut, protocol.CommandReadyPath(slotID), request, slotID)
}

func (c *Client) exchange(
	ctx context.Context,
	method, path string,
	body any,
	expectedSlotID string,
) (protocol.Observation, error) {
	if c == nil || c.transport == nil {
		return protocol.Observation{}, fmt.Errorf("runtime slot authority client is not initialized: %w", errdefs.ErrUnavailable)
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return protocol.Observation{}, invalidRequest(err)
		}
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(payload)
	}
	request, err := c.transport.NewRequest(ctx, method, path, reader)
	if err != nil {
		return protocol.Observation{}, err
	}
	response, err := c.transport.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return protocol.Observation{}, err
		}
		return protocol.Observation{}, fmt.Errorf("call runtime slot authority: %w: %w", err, errdefs.ErrUnavailable)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return protocol.Observation{}, decodeResponseError(response)
	}
	responsePayload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(responsePayload) > maxResponseBytes {
		return protocol.Observation{}, fmt.Errorf("runtime slot observation exceeds the response limit: %w", errdefs.ErrUnavailable)
	}
	var observation protocol.Observation
	decoder := json.NewDecoder(bytes.NewReader(responsePayload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&observation); err != nil {
		return protocol.Observation{}, fmt.Errorf("decode runtime slot observation: %w: %w", err, errdefs.ErrUnavailable)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return protocol.Observation{}, fmt.Errorf("runtime slot response contains trailing data: %w", errdefs.ErrUnavailable)
	}
	if err := observation.Validate(); err != nil {
		return protocol.Observation{}, fmt.Errorf("validate runtime slot observation: %w: %w", err, errdefs.ErrUnavailable)
	}
	if observation.SlotID != expectedSlotID {
		return protocol.Observation{}, fmt.Errorf("runtime slot authority returned another slot: %w", errdefs.ErrUnavailable)
	}
	return observation, nil
}

func decodeResponseError(response *http.Response) error {
	payload, _ := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	var body protocol.ErrorResponse
	if err := json.Unmarshal(payload, &body); err != nil || body.Validate() != nil ||
		!errorCodeMatchesStatus(body.Code, response.StatusCode) {
		body = protocol.ErrorResponse{Code: protocol.ErrorUnavailable, Message: strings.TrimSpace(string(payload))}
		if body.Message == "" {
			body.Message = response.Status
		}
	}
	message := fmt.Sprintf("runtime slot authority returned %s: %s", response.Status, body.Message)
	switch body.Code {
	case protocol.ErrorInvalidArgument:
		return fmt.Errorf("%s: %w", message, errdefs.ErrInvalidArgument)
	case protocol.ErrorUnauthenticated, protocol.ErrorPermissionDenied:
		return fmt.Errorf("%s: %w", message, errdefs.ErrPermissionDenied)
	case protocol.ErrorNotFound:
		return fmt.Errorf("%s: %w", message, errdefs.ErrNotFound)
	case protocol.ErrorConflict, protocol.ErrorFailedPrecondition:
		return fmt.Errorf("%s: %w", message, errdefs.ErrFailedPrecondition)
	default:
		return fmt.Errorf("%s: %w", message, errdefs.ErrUnavailable)
	}
}

func errorCodeMatchesStatus(code string, status int) bool {
	switch code {
	case protocol.ErrorInvalidArgument:
		return status == http.StatusBadRequest || status == http.StatusMethodNotAllowed
	case protocol.ErrorUnauthenticated:
		return status == http.StatusUnauthorized
	case protocol.ErrorPermissionDenied:
		return status == http.StatusForbidden
	case protocol.ErrorNotFound:
		return status == http.StatusNotFound
	case protocol.ErrorConflict:
		return status == http.StatusConflict
	case protocol.ErrorFailedPrecondition:
		return status == http.StatusPreconditionFailed
	case protocol.ErrorUnavailable:
		return status >= 500
	default:
		return false
	}
}

func invalidRequest(err error) error {
	return fmt.Errorf("invalid runtime slot request: %w: %w", err, errdefs.ErrInvalidArgument)
}
