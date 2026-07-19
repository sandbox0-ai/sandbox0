package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func (s *Session) SelectedTeamID() string {
	if s == nil {
		return ""
	}
	return s.teamID
}

func (s *Session) UseTeam(teamID string) func() {
	if s == nil {
		return func() {}
	}
	previous := s.teamID
	s.SelectTeam(teamID)
	return func() {
		s.SelectTeam(previous)
	}
}

func (s *Session) SelectTeam(teamID string) {
	if s == nil {
		return
	}
	s.teamID = teamID
}

func (s *Session) CreateTeam(ctx context.Context, t ContractT, name, slug string, homeRegionID *string) (*apispec.Team, int, error) {
	if s == nil {
		return nil, 0, fmt.Errorf("api session is nil")
	}
	req := apispec.CreateTeamRequest{
		HomeRegionId: homeRegionID,
		Name:         name,
	}
	if slug != "" {
		req.Slug = &slug
	}
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodPost, "/teams", "/teams", req, true)
	if err != nil {
		return nil, status, err
	}
	if status != http.StatusCreated {
		return nil, status, fmt.Errorf("create team failed with status %d: %s", status, formatAPIError(body))
	}
	var resp apispec.SuccessTeamResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, status, err
	}
	if !resp.Success || resp.Data == nil || resp.Data.Id == "" {
		return nil, status, fmt.Errorf("create team response missing id")
	}
	return resp.Data, status, nil
}

func (s *Session) DeleteTeam(ctx context.Context, t ContractT, teamID string) (int, error) {
	if s == nil {
		return 0, fmt.Errorf("api session is nil")
	}
	if teamID == "" {
		return http.StatusOK, nil
	}
	specPath := "/teams/{id}"
	requestPath := "/teams/" + teamID
	status, body, err := s.doJSONSpecRequest(t, ctx, http.MethodDelete, specPath, requestPath, nil, true)
	if err != nil {
		return status, err
	}
	if status != http.StatusOK && status != http.StatusNotFound {
		return status, fmt.Errorf("delete team failed with status %d: %s", status, formatAPIError(body))
	}
	return status, nil
}

// DeleteTeamEventually retries the expected resource-inventory conflict while
// asynchronous deletion of team-owned resources finishes.
func (s *Session) DeleteTeamEventually(
	ctx context.Context,
	t ContractT,
	teamID string,
	timeout time.Duration,
) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		status, err := s.DeleteTeam(waitCtx, t, teamID)
		if err == nil {
			return nil
		}
		if status != http.StatusConflict {
			if status == 0 && waitCtx.Err() != nil && lastErr != nil {
				return fmt.Errorf("delete team did not become ready: %w: %v", waitCtx.Err(), lastErr)
			}
			return err
		}
		lastErr = err

		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf("delete team did not become ready: %w: %v", waitCtx.Err(), lastErr)
		case <-timer.C:
		}
	}
}
