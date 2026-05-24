package utils

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	gatewayspec "github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

type PutTeamQuotaRequest struct {
	LimitValue int64 `json:"limit_value"`
}

func (s *Session) PutTeamQuota(ctx context.Context, dimension quota.Dimension, limitValue int64) (*quota.Limit, int, error) {
	status, body, err := s.doJSONRequest(ctx, http.MethodPut, "/api/v1/quotas/"+string(dimension), PutTeamQuotaRequest{LimitValue: limitValue}, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("put team quota failed with status %d: %s", status, formatAPIError(body))
	}
	resp, apiErr, err := gatewayspec.DecodeResponse[quota.Limit](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("put team quota failed: %s", apiErr.Message)
	}
	return resp, status, nil
}

func (s *Session) DeleteTeamQuota(ctx context.Context, dimension quota.Dimension) (int, error) {
	status, body, err := s.doJSONRequest(ctx, http.MethodDelete, "/api/v1/quotas/"+string(dimension), nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("delete team quota failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}
