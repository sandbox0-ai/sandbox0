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

// DeleteTeamEventually waits for asynchronous resource cleanup to stop
// blocking deletion. Only conflict responses are retried; other failures are
// returned immediately.
func (s *Session) DeleteTeamEventually(ctx context.Context, t ContractT, teamID string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	retry := time.NewTicker(500 * time.Millisecond)
	defer retry.Stop()

	var lastErr error
	for {
		status, err := s.DeleteTeam(ctx, t, teamID)
		if err == nil {
			return nil
		}
		if status != http.StatusConflict {
			return err
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("wait to delete team %q: %w", teamID, ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("delete team %q remained blocked after %s: %w", teamID, timeout, lastErr)
		case <-retry.C:
		}
	}
}
