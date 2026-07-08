package utils

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"

	gatewayspec "github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func (s *Session) GetMeteringStatus(ctx context.Context) (*metering.Status, int, error) {
	status, body, err := s.doJSONRequest(ctx, http.MethodGet, "/internal/v1/metering/status", nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("get metering status failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[metering.Status](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("get metering status failed: %s", apiErr.Message)
	}
	return resp, status, nil
}

func (s *Session) ListMeteringEvents(ctx context.Context, cursor string, limit int) ([]*metering.Event, string, int, error) {
	path := "/internal/v1/metering/events"
	query := url.Values{}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	status, body, err := s.doJSONRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("list metering events failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[struct {
		Events     []*metering.Event `json:"events"`
		NextCursor string            `json:"next_cursor"`
	}](bytes.NewReader(body))
	if err != nil {
		return nil, "", status, err
	}
	if apiErr != nil {
		return nil, "", status, fmt.Errorf("list metering events failed: %s", apiErr.Message)
	}
	return resp.Events, resp.NextCursor, status, nil
}

func (s *Session) ListMeteringWindows(ctx context.Context, cursor string, limit int) ([]*metering.Window, string, int, error) {
	path := "/internal/v1/metering/windows"
	query := url.Values{}
	if cursor != "" {
		query.Set("cursor", cursor)
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	status, body, err := s.doJSONRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, "", status, err
	}
	if status != http.StatusOK {
		return nil, "", status, fmt.Errorf("list metering windows failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[struct {
		Windows    []*metering.Window `json:"windows"`
		NextCursor string             `json:"next_cursor"`
	}](bytes.NewReader(body))
	if err != nil {
		return nil, "", status, err
	}
	if apiErr != nil {
		return nil, "", status, fmt.Errorf("list metering windows failed: %s", apiErr.Message)
	}
	return resp.Windows, resp.NextCursor, status, nil
}

func (s *Session) ListAllMeteringEvents(ctx context.Context, batchSize int) ([]*metering.Event, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	var (
		cursor string
		out    []*metering.Event
	)
	for {
		events, nextCursor, _, err := s.ListMeteringEvents(ctx, cursor, batchSize)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return out, nil
		}
		out = append(out, events...)
		if nextCursor == "" || nextCursor == cursor {
			return out, nil
		}
		cursor = nextCursor
	}
}

func (s *Session) ListAllMeteringWindows(ctx context.Context, batchSize int) ([]*metering.Window, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	var (
		cursor string
		out    []*metering.Window
	)
	for {
		windows, nextCursor, _, err := s.ListMeteringWindows(ctx, cursor, batchSize)
		if err != nil {
			return nil, err
		}
		if len(windows) == 0 {
			return out, nil
		}
		out = append(out, windows...)
		if nextCursor == "" || nextCursor == cursor {
			return out, nil
		}
		cursor = nextCursor
	}
}
