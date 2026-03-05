package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	openapi_types "github.com/oapi-codegen/runtime/types"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) Login(ctx context.Context, t ContractT, email, password string) error {
	if s == nil {
		return fmt.Errorf("api session is nil")
	}
	req := apispec.LoginRequest{
		Email:    openapi_types.Email(email),
		Password: password,
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/auth/login", "/auth/login", req, false)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("login failed with status %d: %s", status, formatAPIError(body))
	}

	var resp apispec.SuccessLoginResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	if !resp.Success || resp.Data == nil {
		return fmt.Errorf("login response missing data")
	}
	if resp.Data.AccessToken == "" || resp.Data.User.Id == "" {
		return fmt.Errorf("login response missing user or token")
	}

	s.token = resp.Data.AccessToken
	s.userID = resp.Data.User.Id

	if resp.Data.User.DefaultTeamId != nil && *resp.Data.User.DefaultTeamId != "" {
		s.teamID = *resp.Data.User.DefaultTeamId
		return nil
	}

	teams, err := s.listTeams(ctx, t)
	if err != nil {
		return err
	}
	if len(teams) == 0 || teams[0].Id == "" {
		return fmt.Errorf("no team available for user %s", resp.Data.User.Id)
	}
	s.teamID = teams[0].Id
	return nil
}

func (s *Session) listTeams(ctx context.Context, t ContractT) ([]apispec.Team, error) {
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodGet, "/teams", "/teams", nil, true)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("list teams failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTeamListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.Teams == nil {
		return nil, nil
	}
	return *resp.Data.Teams, nil
}
