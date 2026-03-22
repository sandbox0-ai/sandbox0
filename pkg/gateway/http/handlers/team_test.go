package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type stubTeamRepository struct {
	createdTeam     *identity.Team
	addedTeamMember *identity.TeamMember
}

func (s *stubTeamRepository) GetTeamsByUserID(context.Context, string) ([]*identity.Team, error) {
	return nil, nil
}

func (s *stubTeamRepository) CreateTeam(_ context.Context, team *identity.Team) error {
	copyTeam := *team
	copyTeam.ID = "team-1"
	s.createdTeam = &copyTeam
	team.ID = copyTeam.ID
	return nil
}

func (s *stubTeamRepository) GetTeamMember(context.Context, string, string) (*identity.TeamMember, error) {
	return nil, identity.ErrMemberNotFound
}

func (s *stubTeamRepository) GetTeamByID(context.Context, string) (*identity.Team, error) {
	return nil, identity.ErrTeamNotFound
}

func (s *stubTeamRepository) UpdateTeam(context.Context, *identity.Team) error {
	return nil
}

func (s *stubTeamRepository) DeleteTeam(context.Context, string) error {
	return nil
}

func (s *stubTeamRepository) GetTeamMembers(context.Context, string) ([]*identity.TeamMemberWithUser, error) {
	return nil, nil
}

func (s *stubTeamRepository) GetUserByEmail(context.Context, string) (*identity.User, error) {
	return nil, identity.ErrUserNotFound
}

func (s *stubTeamRepository) AddTeamMember(_ context.Context, member *identity.TeamMember) error {
	copyMember := *member
	s.addedTeamMember = &copyMember
	return nil
}

func (s *stubTeamRepository) UpdateTeamMemberRole(context.Context, string, string, string) error {
	return nil
}

func (s *stubTeamRepository) RemoveTeamMember(context.Context, string, string) error {
	return nil
}

type stubTeamRegionLookup struct {
	region *tenantdir.Region
	err    error
}

func (s *stubTeamRegionLookup) GetRegion(context.Context, string) (*tenantdir.Region, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.region, nil
}

func TestTeamHandlerCreateTeamRequiresHomeRegionInGlobalMode(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop(), WithCreateHomeRegionRequired(&stubTeamRegionLookup{
		region: &tenantdir.Region{
			ID:                 "aws/us-east-1",
			RegionalGatewayURL: "https://use1.example.com",
			Enabled:            true,
		},
	}))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name": "Example Team",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "home_region_id is required" {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if repo.createdTeam != nil {
		t.Fatal("team should not be created")
	}
}

func TestTeamHandlerCreateTeamRejectsUnknownHomeRegionInGlobalMode(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop(), WithCreateHomeRegionRequired(&stubTeamRegionLookup{
		err: tenantdir.ErrRegionNotFound,
	}))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name":           "Example Team",
		"home_region_id": "aws/us-east-1",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "home region not found" {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if repo.createdTeam != nil {
		t.Fatal("team should not be created")
	}
}

func TestTeamHandlerCreateTeamRejectsUnroutableHomeRegionInGlobalMode(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop(), WithCreateHomeRegionRequired(&stubTeamRegionLookup{
		region: &tenantdir.Region{
			ID:                 "aws/us-east-1",
			RegionalGatewayURL: "",
			Enabled:            true,
		},
	}))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name":           "Example Team",
		"home_region_id": "aws/us-east-1",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "home region is not routable" {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if repo.createdTeam != nil {
		t.Fatal("team should not be created")
	}
}

func TestTeamHandlerCreateTeamAllowsMissingHomeRegionWithoutGlobalRequirement(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop())

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name": "Example Team",
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if repo.createdTeam == nil {
		t.Fatal("expected team to be created")
	}
	if repo.createdTeam.HomeRegionID != nil {
		t.Fatalf("expected nil home region, got %#v", repo.createdTeam.HomeRegionID)
	}
	if repo.addedTeamMember == nil || repo.addedTeamMember.TeamID != "team-1" {
		t.Fatalf("expected creator to be added as team member, got %#v", repo.addedTeamMember)
	}
}

func performCreateTeamRequest(t *testing.T, handler *TeamHandler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT,
			UserID:     "user-1",
			TeamID:     "team-0",
			TeamRole:   "admin",
		})
		c.Next()
	})
	router.POST("/teams", handler.CreateTeam)

	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/teams", bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestTeamHandlerCreateTeamReturnsInternalErrorWhenRegionLookupFails(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop(), WithCreateHomeRegionRequired(&stubTeamRegionLookup{
		err: errors.New("db offline"),
	}))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name":           "Example Team",
		"home_region_id": "aws/us-east-1",
	})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}
