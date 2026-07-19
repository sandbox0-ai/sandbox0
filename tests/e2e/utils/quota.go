package utils

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	gatewayspec "github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// CapacityTeamQuotaPolicy builds a capacity policy write request in the
// canonical unit for the selected key.
func CapacityTeamQuotaPolicy(limit int64) apispec.TeamQuotaPolicyWriteRequest {
	var request apispec.TeamQuotaPolicyWriteRequest
	if err := request.FromTeamQuotaCapacityPolicyWriteRequest(
		apispec.TeamQuotaCapacityPolicyWriteRequest{
			Kind:  apispec.Capacity,
			Limit: limit,
		},
	); err != nil {
		panic(fmt.Sprintf("build capacity Team Quota policy: %v", err))
	}
	return request
}

// RateTeamQuotaPolicy builds a distributed token-bucket policy write request.
func RateTeamQuotaPolicy(tokens, intervalMillis, burst int64) apispec.TeamQuotaPolicyWriteRequest {
	var request apispec.TeamQuotaPolicyWriteRequest
	if err := request.FromTeamQuotaRatePolicyWriteRequest(
		apispec.TeamQuotaRatePolicyWriteRequest{
			Kind:       apispec.Rate,
			Tokens:     tokens,
			IntervalMs: intervalMillis,
			Burst:      burst,
		},
	); err != nil {
		panic(fmt.Sprintf("build rate Team Quota policy: %v", err))
	}
	return request
}

// ListTeamQuotas returns the complete effective policy and usage set for the
// selected team through the region-owner gateway admin API.
func (s *Session) ListTeamQuotas(ctx context.Context) (*apispec.TeamQuotaList, int, error) {
	path, err := s.teamQuotaAdminPath("")
	if err != nil {
		return nil, 0, err
	}
	status, body, err := s.doJSONRequest(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("list team quotas failed with status %d: %s", status, formatAPIError(body))
	}
	response, apiErr, err := gatewayspec.DecodeResponse[apispec.TeamQuotaList](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("list team quotas failed: %s", apiErr.Message)
	}
	return response, status, nil
}

// PutTeamQuota replaces one explicit team policy through the gateway admin API.
func (s *Session) PutTeamQuota(
	ctx context.Context,
	key apispec.TeamQuotaKey,
	request apispec.TeamQuotaPolicyWriteRequest,
) (*apispec.TeamQuotaPolicy, int, error) {
	path, err := s.teamQuotaAdminPath(string(key))
	if err != nil {
		return nil, 0, err
	}
	status, body, err := s.doJSONRequest(ctx, http.MethodPut, path, request, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("put team quota failed with status %d: %s", status, formatAPIError(body))
	}
	response, apiErr, err := gatewayspec.DecodeResponse[apispec.TeamQuotaPolicy](bytes.NewReader(body))
	if err != nil {
		return nil, status, err
	}
	if apiErr != nil {
		return nil, status, fmt.Errorf("put team quota failed: %s", apiErr.Message)
	}
	return response, status, nil
}

// DeleteTeamQuota deletes only the explicit team override. Region defaults are
// mandatory and cannot be deleted through this API.
func (s *Session) DeleteTeamQuota(ctx context.Context, key apispec.TeamQuotaKey) (int, error) {
	path, err := s.teamQuotaAdminPath(string(key))
	if err != nil {
		return 0, err
	}
	status, body, err := s.doJSONRequest(ctx, http.MethodDelete, path, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK {
		return status, fmt.Errorf("delete team quota failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

// OverrideTeamQuota snapshots the effective policy before writing a temporary
// override. The returned restore function deletes the temporary override when
// the original source was the region default, or restores the original explicit
// override otherwise.
func (s *Session) OverrideTeamQuota(
	ctx context.Context,
	key apispec.TeamQuotaKey,
	request apispec.TeamQuotaPolicyWriteRequest,
) (func(context.Context) error, int, error) {
	quotas, status, err := s.ListTeamQuotas(ctx)
	if err != nil {
		return nil, status, err
	}
	var original *apispec.TeamQuotaStatus
	for index := range quotas.Quotas {
		if quotas.Quotas[index].Key == key {
			quotaStatus := quotas.Quotas[index]
			original = &quotaStatus
			break
		}
	}
	if original == nil {
		return nil, http.StatusServiceUnavailable, fmt.Errorf("effective team quota %q is missing", key)
	}
	switch original.Source {
	case apispec.TeamQuotaPolicySourceDefault, apispec.TeamQuotaPolicySourceOverride:
	default:
		return nil, http.StatusServiceUnavailable, fmt.Errorf(
			"effective team quota %q has unknown policy source %q",
			key,
			original.Source,
		)
	}
	if _, status, err = s.PutTeamQuota(ctx, key, request); err != nil {
		return nil, status, err
	}
	restoreRequest, err := writeRequestFromTeamQuotaPolicy(original.Policy)
	if err != nil {
		return nil, http.StatusServiceUnavailable, err
	}
	restore := func(restoreCtx context.Context) error {
		if original.Source == apispec.TeamQuotaPolicySourceDefault {
			restoreStatus, restoreErr := s.DeleteTeamQuota(restoreCtx, key)
			if restoreErr != nil {
				return restoreErr
			}
			if restoreStatus != http.StatusOK {
				return fmt.Errorf("restore inherited team quota %q returned status %d", key, restoreStatus)
			}
			return nil
		}
		_, restoreStatus, restoreErr := s.PutTeamQuota(restoreCtx, key, restoreRequest)
		if restoreErr != nil {
			return restoreErr
		}
		if restoreStatus != http.StatusOK {
			return fmt.Errorf("restore team quota %q returned status %d", key, restoreStatus)
		}
		return nil
	}
	return restore, status, nil
}

func (s *Session) teamQuotaAdminPath(key string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("api session is nil")
	}
	teamID := strings.TrimSpace(s.teamID)
	if teamID == "" {
		return "", fmt.Errorf("team_id is required")
	}
	path := "/api/v1/teams/" + url.PathEscape(teamID) + "/quotas"
	if strings.TrimSpace(key) != "" {
		path += "/" + url.PathEscape(key)
	}
	return path, nil
}

func writeRequestFromTeamQuotaPolicy(
	policy apispec.TeamQuotaPolicy,
) (apispec.TeamQuotaPolicyWriteRequest, error) {
	var request apispec.TeamQuotaPolicyWriteRequest
	switch policy.Kind {
	case apispec.TeamQuotaKindCapacity:
		if policy.Limit == nil {
			return request, fmt.Errorf("capacity Team Quota policy is missing limit")
		}
		err := request.FromTeamQuotaCapacityPolicyWriteRequest(
			apispec.TeamQuotaCapacityPolicyWriteRequest{
				Kind:  apispec.Capacity,
				Limit: *policy.Limit,
			},
		)
		return request, err
	case apispec.TeamQuotaKindConcurrency:
		if policy.Limit == nil {
			return request, fmt.Errorf("concurrency Team Quota policy is missing limit")
		}
		err := request.FromTeamQuotaConcurrencyPolicyWriteRequest(
			apispec.TeamQuotaConcurrencyPolicyWriteRequest{
				Kind:  apispec.Concurrency,
				Limit: *policy.Limit,
			},
		)
		return request, err
	case apispec.TeamQuotaKindRate:
		if policy.Tokens == nil || policy.IntervalMs == nil || policy.Burst == nil {
			return request, fmt.Errorf("rate Team Quota policy is missing token-bucket fields")
		}
		err := request.FromTeamQuotaRatePolicyWriteRequest(
			apispec.TeamQuotaRatePolicyWriteRequest{
				Kind:       apispec.Rate,
				Tokens:     *policy.Tokens,
				IntervalMs: *policy.IntervalMs,
				Burst:      *policy.Burst,
			},
		)
		return request, err
	default:
		return request, fmt.Errorf("unknown Team Quota policy kind %q", policy.Kind)
	}
}
