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
	teams           map[string]*identity.Team
	members         map[string]*identity.TeamMember
	memberLists     map[string][]*identity.TeamMemberWithUser
	searchMembers   []*identity.TeamMemberWithUser
	searchTeamID    string
	searchQuery     string
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

func (s *stubTeamRepository) GetTeamMember(_ context.Context, teamID, userID string) (*identity.TeamMember, error) {
	if s.members != nil {
		if member, ok := s.members[teamMemberKey(teamID, userID)]; ok {
			copyMember := *member
			return &copyMember, nil
		}
	}
	return nil, identity.ErrMemberNotFound
}

func (s *stubTeamRepository) GetTeamByID(_ context.Context, id string) (*identity.Team, error) {
	if s.teams != nil {
		if team, ok := s.teams[id]; ok {
			copyTeam := *team
			return &copyTeam, nil
		}
	}
	return nil, identity.ErrTeamNotFound
}

func (s *stubTeamRepository) UpdateTeam(context.Context, *identity.Team) error {
	return nil
}

func (s *stubTeamRepository) DeleteTeam(context.Context, string) error {
	return nil
}

func (s *stubTeamRepository) TransferTeamOwner(_ context.Context, teamID, userID string) (*identity.Team, error) {
	team, ok := s.teams[teamID]
	if !ok {
		return nil, identity.ErrTeamNotFound
	}
	member, ok := s.members[teamMemberKey(teamID, userID)]
	if !ok {
		return nil, identity.ErrMemberNotFound
	}
	member.Role = "admin"
	team.OwnerID = &member.UserID
	copyTeam := *team
	return &copyTeam, nil
}

func (s *stubTeamRepository) GetTeamMembers(_ context.Context, teamID string) ([]*identity.TeamMemberWithUser, error) {
	if s.memberLists != nil {
		if members, ok := s.memberLists[teamID]; ok {
			return members, nil
		}
	}
	members := make([]*identity.TeamMemberWithUser, 0)
	for _, member := range s.members {
		if member.TeamID == teamID {
			members = append(members, &identity.TeamMemberWithUser{
				ID:       member.ID,
				TeamID:   member.TeamID,
				UserID:   member.UserID,
				Role:     member.Role,
				JoinedAt: member.JoinedAt,
			})
		}
	}
	return members, nil
}

func (s *stubTeamRepository) SearchTeamMembers(_ context.Context, teamID, query string) ([]*identity.TeamMemberWithUser, error) {
	s.searchTeamID = teamID
	s.searchQuery = query
	return s.searchMembers, nil
}

func (s *stubTeamRepository) GetUserByEmail(context.Context, string) (*identity.User, error) {
	return nil, identity.ErrUserNotFound
}

func (s *stubTeamRepository) AddTeamMember(_ context.Context, member *identity.TeamMember) error {
	copyMember := *member
	s.addedTeamMember = &copyMember
	return nil
}

func (s *stubTeamRepository) UpdateTeamMemberRole(_ context.Context, teamID, userID, role string) error {
	member, ok := s.members[teamMemberKey(teamID, userID)]
	if !ok {
		return identity.ErrMemberNotFound
	}
	member.Role = role
	return nil
}

func (s *stubTeamRepository) RemoveTeamMember(_ context.Context, teamID, userID string) error {
	key := teamMemberKey(teamID, userID)
	if _, ok := s.members[key]; !ok {
		return identity.ErrMemberNotFound
	}
	delete(s.members, key)
	return nil
}

type stubTeamRegionLookup struct {
	region       *tenantdir.Region
	err          error
	requestedIDs []string
}

func (s *stubTeamRegionLookup) GetRegion(_ context.Context, regionID string) (*tenantdir.Region, error) {
	s.requestedIDs = append(s.requestedIDs, regionID)
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
			ID:                 "aws-us-east-1",
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
		"home_region_id": "aws-us-east-1",
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
			ID:                 "aws-us-east-1",
			RegionalGatewayURL: "",
			Enabled:            true,
		},
	}))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name":           "Example Team",
		"home_region_id": "aws-us-east-1",
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

func TestTeamHandlerCreateTeamRejectsInvalidHomeRegionIDFormat(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	lookup := &stubTeamRegionLookup{
		region: &tenantdir.Region{
			ID:                 "aws-us-east-1",
			RegionalGatewayURL: "https://use1.example.com",
			Enabled:            true,
		},
	}
	handler := NewTeamHandler(repo, zap.NewNop(), WithCreateHomeRegionRequired(lookup))

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name":           "Example Team",
		"home_region_id": "aws_us_east_1",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Message != "home_region_id must use provider-region format" {
		t.Fatalf("unexpected api error: %#v", apiErr)
	}
	if repo.createdTeam != nil {
		t.Fatalf("team should not be created: %#v", repo.createdTeam)
	}
	if len(lookup.requestedIDs) != 0 {
		t.Fatalf("lookup ids = %v", lookup.requestedIDs)
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
		"home_region_id": "aws-us-east-1",
	})

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
}

func TestTeamHandlerTransferTeamOwnerPromotesExistingMember(t *testing.T) {
	ownerID := "user-owner"
	nextOwnerID := "user-next"
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey("team-1", nextOwnerID)] = &identity.TeamMember{
		ID:     "member-next",
		TeamID: "team-1",
		UserID: nextOwnerID,
		Role:   "viewer",
	}

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodPut, "/teams/team-1/owner", map[string]any{
		"user_id": nextOwnerID,
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := *repo.teams["team-1"].OwnerID; got != nextOwnerID {
		t.Fatalf("owner = %q, want %q", got, nextOwnerID)
	}
	if got := repo.members[teamMemberKey("team-1", nextOwnerID)].Role; got != "admin" {
		t.Fatalf("new owner role = %q, want admin", got)
	}
}

func TestTeamHandlerTransferTeamOwnerRequiresCurrentOwner(t *testing.T) {
	ownerID := "user-owner"
	callerID := "user-admin"
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey("team-1", callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: "team-1",
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodPut, "/teams/team-1/owner", map[string]any{
		"user_id": callerID,
	})

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if got := *repo.teams["team-1"].OwnerID; got != ownerID {
		t.Fatalf("owner changed to %q", got)
	}
}

func TestTeamHandlerListTeamMembersUsesSearchQuery(t *testing.T) {
	ownerID := "user-owner"
	repo := newTeamManagementRepo(ownerID)
	repo.searchMembers = []*identity.TeamMemberWithUser{
		{ID: "member-owner", TeamID: "team-1", UserID: ownerID, Role: "admin", Email: "owner@example.com"},
	}

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodGet, "/teams/team-1/members?query=owner", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if repo.searchTeamID != "team-1" || repo.searchQuery != "owner" {
		t.Fatalf("search = (%q, %q), want (team-1, owner)", repo.searchTeamID, repo.searchQuery)
	}
}

func TestTeamHandlerUpdateTeamMemberRejectsOwnerDemotion(t *testing.T) {
	ownerID := "user-owner"
	callerID := "user-admin"
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey("team-1", callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: "team-1",
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodPut, "/teams/team-1/members/"+ownerID, map[string]any{
		"role": "viewer",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := repo.members[teamMemberKey("team-1", ownerID)].Role; got != "admin" {
		t.Fatalf("owner role = %q, want admin", got)
	}
}

func TestTeamHandlerRemoveTeamMemberRejectsOwnerRemoval(t *testing.T) {
	ownerID := "user-owner"
	callerID := "user-admin"
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey("team-1", callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: "team-1",
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodDelete, "/teams/team-1/members/"+ownerID, nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := repo.members[teamMemberKey("team-1", ownerID)]; !ok {
		t.Fatal("owner member was removed")
	}
}

func TestTeamHandlerUpdateTeamMemberRejectsLastAdminDemotion(t *testing.T) {
	adminID := "user-admin"
	repo := newTeamManagementRepo("")
	repo.members[teamMemberKey("team-1", adminID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: "team-1",
		UserID: adminID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, adminID, http.MethodPut, "/teams/team-1/members/"+adminID, map[string]any{
		"role": "viewer",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := repo.members[teamMemberKey("team-1", adminID)].Role; got != "admin" {
		t.Fatalf("admin role = %q, want admin", got)
	}
}

func TestTeamHandlerRemoveTeamMemberRejectsLastAdminRemoval(t *testing.T) {
	adminID := "user-admin"
	repo := newTeamManagementRepo("")
	repo.members[teamMemberKey("team-1", adminID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: "team-1",
		UserID: adminID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, adminID, http.MethodDelete, "/teams/team-1/members/"+adminID, nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := repo.members[teamMemberKey("team-1", adminID)]; !ok {
		t.Fatal("last admin was removed")
	}
}

func newTeamManagementRepo(ownerID string) *stubTeamRepository {
	var ownerPtr *string
	if ownerID != "" {
		ownerPtr = &ownerID
	}
	repo := &stubTeamRepository{
		teams: map[string]*identity.Team{
			"team-1": {
				ID:      "team-1",
				Name:    "Team One",
				Slug:    "team-one",
				OwnerID: ownerPtr,
			},
		},
		members: make(map[string]*identity.TeamMember),
	}
	if ownerID != "" {
		repo.members[teamMemberKey("team-1", ownerID)] = &identity.TeamMember{
			ID:     "member-owner",
			TeamID: "team-1",
			UserID: ownerID,
			Role:   "admin",
		}
	}
	return repo
}

func performTeamManagementRequest(t *testing.T, repo *stubTeamRepository, authUserID, method, path string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	handler := NewTeamHandler(repo, zap.NewNop())
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT,
			UserID:     authUserID,
			TeamID:     "team-1",
			TeamRole:   "admin",
		})
		c.Next()
	})
	router.GET("/teams/:id/members", handler.ListTeamMembers)
	router.PUT("/teams/:id/owner", handler.TransferTeamOwner)
	router.PUT("/teams/:id/members/:userId", handler.UpdateTeamMember)
	router.DELETE("/teams/:id/members/:userId", handler.RemoveTeamMember)

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		rawBody, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		reader = bytes.NewReader(rawBody)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func teamMemberKey(teamID, userID string) string {
	return teamID + ":" + userID
}
