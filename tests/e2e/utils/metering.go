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

func (s *Session) ListMeteringEvents(ctx context.Context, afterSequence int64, limit int) ([]*metering.Event, int, error) {
	path := "/internal/v1/metering/events"
	query := url.Values{}
	if afterSequence > 0 {
		query.Set("after_sequence", fmt.Sprintf("%d", afterSequence))
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	status, body, err := s.doJSONRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list metering events failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[struct {
		Events []*metering.Event `json:"events"`
	}](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("list metering events failed: %s", apiErr.Message)
	}
	return resp.Events, status, nil
}

func (s *Session) ListMeteringWindows(ctx context.Context, afterSequence int64, limit int) ([]*metering.Window, int, error) {
	path := "/internal/v1/metering/windows"
	query := url.Values{}
	if afterSequence > 0 {
		query.Set("after_sequence", fmt.Sprintf("%d", afterSequence))
	}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	status, body, err := s.doJSONRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list metering windows failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[struct {
		Windows []*metering.Window `json:"windows"`
	}](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("list metering windows failed: %s", apiErr.Message)
	}
	return resp.Windows, status, nil
}

func (s *Session) ListAllMeteringEvents(ctx context.Context, batchSize int) ([]*metering.Event, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	var (
		afterSequence int64
		out           []*metering.Event
	)
	for {
		events, _, err := s.ListMeteringEvents(ctx, afterSequence, batchSize)
		if err != nil {
			return nil, err
		}
		if len(events) == 0 {
			return out, nil
		}
		out = append(out, events...)
		lastSequence := events[len(events)-1].Sequence
		if lastSequence <= afterSequence {
			return out, nil
		}
		afterSequence = lastSequence
		if len(events) < batchSize {
			return out, nil
		}
	}
}

func (s *Session) ListAllMeteringWindows(ctx context.Context, batchSize int) ([]*metering.Window, error) {
	if batchSize <= 0 {
		batchSize = 100
	}

	var (
		afterSequence int64
		out           []*metering.Window
	)
	for {
		windows, _, err := s.ListMeteringWindows(ctx, afterSequence, batchSize)
		if err != nil {
			return nil, err
		}
		if len(windows) == 0 {
			return out, nil
		}
		out = append(out, windows...)
		lastSequence := windows[len(windows)-1].Sequence
		if lastSequence <= afterSequence {
			return out, nil
		}
		afterSequence = lastSequence
		if len(windows) < batchSize {
			return out, nil
		}
	}
}
