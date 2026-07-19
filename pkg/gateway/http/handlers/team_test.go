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
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/teamresources"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"go.uber.org/zap"
)

const (
	testTeamID        = "11111111-1111-1111-1111-111111111111"
	testAuthTeamID    = "11111111-1111-1111-1111-111111111112"
	testOwnerUserID   = "22222222-2222-2222-2222-222222222222"
	testAdminUserID   = "33333333-3333-3333-3333-333333333333"
	testNextOwnerID   = "44444444-4444-4444-4444-444444444444"
	testCreatorUserID = "55555555-5555-5555-5555-555555555555"
)

type stubTeamRepository struct {
	createdTeam      *identity.Team
	updatedTeam      *identity.Team
	addedTeamMember  *identity.TeamMember
	teams            map[string]*identity.Team
	members          map[string]*identity.TeamMember
	memberLists      map[string][]*identity.TeamMemberWithUser
	searchMembers    []*identity.TeamMemberWithUser
	searchTeamID     string
	searchQuery      string
	deletedTeamID    string
	deletedOwnerID   string
	fencedTeamID     string
	fencedOwnerID    string
	unfencedTeamID   string
	unfencedOwnerID  string
	fenceErr         error
	unfenceErr       error
	transferOwnerErr error
	deleteErr        error
	deletionOrder    *[]string
}

func (s *stubTeamRepository) GetTeamsByUserID(context.Context, string) ([]*identity.Team, error) {
	return nil, nil
}

func (s *stubTeamRepository) CreateTeamWithOwner(_ context.Context, team *identity.Team) (*identity.TeamMember, error) {
	copyTeam := *team
	copyTeam.ID = testTeamID
	s.createdTeam = &copyTeam
	team.ID = copyTeam.ID
	member := &identity.TeamMember{
		TeamID: team.ID,
		UserID: *team.OwnerID,
		Role:   "admin",
	}
	s.addedTeamMember = member
	return member, nil
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

func (s *stubTeamRepository) UpdateTeam(_ context.Context, team *identity.Team) error {
	copyTeam := *team
	s.updatedTeam = &copyTeam
	if s.teams != nil {
		stored := copyTeam
		s.teams[team.ID] = &stored
	}
	return nil
}

func (s *stubTeamRepository) FenceTeamDeletionOwnedBy(
	_ context.Context,
	id string,
	expectedOwnerID string,
) error {
	s.fencedTeamID = id
	s.fencedOwnerID = expectedOwnerID
	if s.deletionOrder != nil {
		*s.deletionOrder = append(*s.deletionOrder, "fence-identity")
	}
	return s.fenceErr
}

func (s *stubTeamRepository) UnfenceTeamDeletionOwnedBy(
	_ context.Context,
	id string,
	expectedOwnerID string,
) error {
	s.unfencedTeamID = id
	s.unfencedOwnerID = expectedOwnerID
	if s.deletionOrder != nil {
		*s.deletionOrder = append(*s.deletionOrder, "unfence-identity")
	}
	return s.unfenceErr
}

func (s *stubTeamRepository) DeleteTeamOwnedBy(_ context.Context, id, expectedOwnerID string) error {
	s.deletedOwnerID = expectedOwnerID
	if s.deletionOrder != nil {
		*s.deletionOrder = append(*s.deletionOrder, "identity")
	}
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.deletedTeamID = id
	return nil
}

type stubTeamDeletePreflight struct {
	teamID      string
	inventory   *teamresources.Inventory
	inventories []*teamresources.Inventory
	calls       int
	err         error
}

func (s *stubTeamDeletePreflight) GetTeamResourceInventory(_ context.Context, teamID string) (*teamresources.Inventory, error) {
	s.teamID = teamID
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	if s.calls <= len(s.inventories) && s.inventories[s.calls-1] != nil {
		return s.inventories[s.calls-1], nil
	}
	if s.inventory != nil {
		return s.inventory, nil
	}
	return &teamresources.Inventory{TeamID: teamID}, nil
}

type stubTeamDeletionLifecycle struct {
	disableTeamID  string
	finalizeTeamID string
	disableErr     error
	finalizeErr    error
	order          *[]string
}

func (s *stubTeamDeletionLifecycle) DisableTeamAdmission(_ context.Context, teamID string) error {
	s.disableTeamID = teamID
	if s.order != nil {
		*s.order = append(*s.order, "disable-postgres")
	}
	return s.disableErr
}

func (s *stubTeamDeletionLifecycle) DisableTeamAdmissionWithFinalCheck(
	ctx context.Context,
	teamID string,
	finalCheck func(context.Context) error,
) error {
	if err := s.DisableTeamAdmission(ctx, teamID); err != nil {
		return err
	}
	if finalCheck != nil {
		return finalCheck(ctx)
	}
	return nil
}

func (s *stubTeamDeletionLifecycle) FinalizeTeamDeletion(_ context.Context, teamID string) error {
	s.finalizeTeamID = teamID
	if s.order != nil {
		*s.order = append(*s.order, "finalize-postgres")
	}
	return s.finalizeErr
}

type stubTeamDistributedAdmissionDisabler struct {
	teamID string
	err    error
	order  *[]string
}

func (s *stubTeamDistributedAdmissionDisabler) DisableTeamDistributedAdmission(_ context.Context, teamID string) error {
	s.teamID = teamID
	if s.order != nil {
		*s.order = append(*s.order, "disable-redis")
	}
	return s.err
}

func (s *stubTeamRepository) TransferTeamOwner(_ context.Context, teamID, expectedOwnerID, userID string) (*identity.Team, error) {
	if s.transferOwnerErr != nil {
		return nil, s.transferOwnerErr
	}
	team, ok := s.teams[teamID]
	if !ok {
		return nil, identity.ErrTeamNotFound
	}
	if team.OwnerID == nil || *team.OwnerID != expectedOwnerID {
		return nil, identity.ErrTeamOwnerChanged
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
	if repo.addedTeamMember == nil || repo.addedTeamMember.TeamID != testTeamID {
		t.Fatalf("expected creator to be added as team member, got %#v", repo.addedTeamMember)
	}
}

func TestTeamHandlerCreateTeamRejectsInvalidNameAndSlug(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	tests := []struct {
		name string
		body map[string]any
	}{
		{name: "whitespace name", body: map[string]any{"name": "   "}},
		{name: "whitespace slug", body: map[string]any{"name": "Example Team", "slug": "   "}},
		{name: "unsafe slug", body: map[string]any{"name": "Example Team", "slug": "Example Team"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &stubTeamRepository{}
			handler := NewTeamHandler(repo, zap.NewNop())

			rec := performCreateTeamRequest(t, handler, tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if repo.createdTeam != nil {
				t.Fatalf("team should not be created: %#v", repo.createdTeam)
			}
		})
	}
}

func TestTeamHandlerCreateTeamNormalizesNameAndSlug(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubTeamRepository{}
	handler := NewTeamHandler(repo, zap.NewNop())

	rec := performCreateTeamRequest(t, handler, map[string]any{
		"name": " Example Team ",
		"slug": " example-team ",
	})

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	if repo.createdTeam == nil {
		t.Fatal("team should be created")
	}
	if repo.createdTeam.Name != "Example Team" || repo.createdTeam.Slug != "example-team" {
		t.Fatalf("created team = %#v, want normalized name and slug", repo.createdTeam)
	}
}

func performCreateTeamRequest(t *testing.T, handler *TeamHandler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT,
			UserID:     testCreatorUserID,
			TeamID:     testAuthTeamID,
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
	ownerID := testOwnerUserID
	nextOwnerID := testNextOwnerID
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey(testTeamID, nextOwnerID)] = &identity.TeamMember{
		ID:     "member-next",
		TeamID: testTeamID,
		UserID: nextOwnerID,
		Role:   "viewer",
	}

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodPut, "/teams/"+testTeamID+"/owner", map[string]any{
		"user_id": nextOwnerID,
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := *repo.teams[testTeamID].OwnerID; got != nextOwnerID {
		t.Fatalf("owner = %q, want %q", got, nextOwnerID)
	}
	if got := repo.members[teamMemberKey(testTeamID, nextOwnerID)].Role; got != "admin" {
		t.Fatalf("new owner role = %q, want admin", got)
	}
}

func TestTeamHandlerTransferTeamOwnerRequiresCurrentOwner(t *testing.T) {
	ownerID := testOwnerUserID
	callerID := testAdminUserID
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey(testTeamID, callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: testTeamID,
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodPut, "/teams/"+testTeamID+"/owner", map[string]any{
		"user_id": callerID,
	})

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	if got := *repo.teams[testTeamID].OwnerID; got != ownerID {
		t.Fatalf("owner changed to %q", got)
	}
}

func TestTeamHandlerTransferTeamOwnerRejectsDeletionFence(t *testing.T) {
	ownerID := testOwnerUserID
	nextOwnerID := testNextOwnerID
	repo := newTeamManagementRepo(ownerID)
	repo.transferOwnerErr = identity.ErrTeamDeletionInProgress
	repo.members[teamMemberKey(testTeamID, nextOwnerID)] = &identity.TeamMember{
		ID:     "member-next",
		TeamID: testTeamID,
		UserID: nextOwnerID,
		Role:   "viewer",
	}

	rec := performTeamManagementRequest(
		t,
		repo,
		ownerID,
		http.MethodPut,
		"/teams/"+testTeamID+"/owner",
		map[string]any{"user_id": nextOwnerID},
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if got := *repo.teams[testTeamID].OwnerID; got != ownerID {
		t.Fatalf("owner changed to %q while deletion was fenced", got)
	}
}

func TestTeamHandlerListTeamMembersUsesSearchQuery(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	repo.searchMembers = []*identity.TeamMemberWithUser{
		{ID: "member-owner", TeamID: testTeamID, UserID: ownerID, Role: "admin", Email: "owner@example.com"},
	}

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodGet, "/teams/"+testTeamID+"/members?query=owner", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if repo.searchTeamID != testTeamID || repo.searchQuery != "owner" {
		t.Fatalf("search = (%q, %q), want (%s, owner)", repo.searchTeamID, repo.searchQuery, testTeamID)
	}
}

func TestTeamHandlerListTeamMembersSearchReturnsEmptyArray(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodGet, "/teams/"+testTeamID+"/members?query=missing", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	resp, apiErr, err := spec.DecodeResponse[struct {
		Members []*identity.TeamMemberWithUser `json:"members"`
	}](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.Members == nil {
		t.Fatal("members slice is nil, want empty array")
	}
	if len(resp.Members) != 0 {
		t.Fatalf("members = %d, want 0", len(resp.Members))
	}
	if repo.searchTeamID != testTeamID || repo.searchQuery != "missing" {
		t.Fatalf("search = (%q, %q), want (%s, missing)", repo.searchTeamID, repo.searchQuery, testTeamID)
	}
}

func TestTeamHandlerRejectsMalformedTeamIDs(t *testing.T) {
	ownerID := testOwnerUserID

	tests := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{name: "get team", method: http.MethodGet, path: "/teams/not-a-uuid"},
		{name: "list members", method: http.MethodGet, path: "/teams/not-a-uuid/members"},
		{name: "update team", method: http.MethodPut, path: "/teams/not-a-uuid", body: map[string]any{"name": "Nope"}},
		{name: "delete team", method: http.MethodDelete, path: "/teams/not-a-uuid"},
		{name: "transfer owner", method: http.MethodPut, path: "/teams/not-a-uuid/owner", body: map[string]any{"user_id": testNextOwnerID}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newTeamManagementRepo(ownerID)

			rec := performTeamManagementRequest(t, repo, ownerID, tt.method, tt.path, tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestTeamHandlerDeleteTeamReturnsConflictWhenResourcesExist(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	preflight := &stubTeamDeletePreflight{
		inventory: &teamresources.Inventory{
			TeamID: testTeamID,
			BlockingResources: []teamresources.ResourceCount{
				{Category: "sandboxes", Count: 2},
				{Category: "api_keys", Count: 1},
			},
			RetainedResources: []teamresources.ResourceCount{
				{Category: "usage_events", Count: 3},
			},
			RetentionPolicy: teamresources.MeteringRetentionPolicy,
		},
	}
	lifecycle := &stubTeamDeletionLifecycle{}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(preflight, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if preflight.teamID != testTeamID {
		t.Fatalf("preflight team id = %q, want %q", preflight.teamID, testTeamID)
	}
	if repo.deletedTeamID != "" {
		t.Fatalf("team was deleted despite blocking resources: %q", repo.deletedTeamID)
	}

	var payload spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Error == nil || payload.Error.Code != spec.CodeConflict {
		t.Fatalf("error = %#v, want conflict", payload.Error)
	}
	details, ok := payload.Error.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %#v, want object", payload.Error.Details)
	}
	resources, ok := details["blocking_resources"].([]any)
	if !ok || len(resources) != 2 {
		t.Fatalf("blocking_resources = %#v, want two entries", details["blocking_resources"])
	}
}

func TestTeamHandlerDeleteTeamReturnsInternalErrorWhenPreflightFails(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	preflight := &stubTeamDeletePreflight{err: errors.New("inventory failed")}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(preflight, &stubTeamDeletionLifecycle{}, &stubTeamDistributedAdmissionDisabler{})...,
	)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	if repo.deletedTeamID != "" {
		t.Fatalf("team was deleted after preflight error: %q", repo.deletedTeamID)
	}
}

func TestTeamHandlerDeleteTeamFailsClosedWhenRegionCoordinationIsUnavailable(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		WithTeamDeletionUnavailable("team deletion requires home-region coordination"),
	)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if repo.deletedTeamID != "" {
		t.Fatalf("team was deleted without region coordination: %q", repo.deletedTeamID)
	}
	var payload spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Error == nil || payload.Error.Code != spec.CodeUnavailable {
		t.Fatalf("error = %#v, want unavailable", payload.Error)
	}
}

func TestTeamHandlerDeleteTeamRunsAfterEmptyPreflight(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	preflight := &stubTeamDeletePreflight{}
	order := make([]string, 0, 4)
	repo.deletionOrder = &order
	lifecycle := &stubTeamDeletionLifecycle{order: &order}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{order: &order}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(preflight, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v; body=%q", err, rec.Body.String())
	}
	data, ok := response.Data.(map[string]any)
	if !response.Success || !ok || data["message"] != "team deleted" {
		t.Fatalf("response = %#v, want team deletion success", response)
	}
	if repo.deletedTeamID != testTeamID {
		t.Fatalf("deleted team id = %q, want %q", repo.deletedTeamID, testTeamID)
	}
	if repo.deletedOwnerID != ownerID {
		t.Fatalf("expected deletion owner = %q, want %q", repo.deletedOwnerID, ownerID)
	}
	if lifecycle.disableTeamID != testTeamID || lifecycle.finalizeTeamID != testTeamID {
		t.Fatalf("lifecycle team IDs = (%q, %q), want %q", lifecycle.disableTeamID, lifecycle.finalizeTeamID, testTeamID)
	}
	if distributedDisabler.teamID != testTeamID {
		t.Fatalf("rate tombstone team ID = %q, want %q", distributedDisabler.teamID, testTeamID)
	}
	wantOrder := []string{
		"fence-identity",
		"disable-postgres",
		"disable-redis",
		"finalize-postgres",
		"identity",
	}
	if !equalStrings(order, wantOrder) {
		t.Fatalf("deletion order = %#v, want %#v", order, wantOrder)
	}
}

func TestTeamHandlerDeleteTeamRejectsOwnerChangeAtIdentityDelete(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	repo.deleteErr = identity.ErrTeamOwnerChanged

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(
			&stubTeamDeletePreflight{},
			&stubTeamDeletionLifecycle{},
			&stubTeamDistributedAdmissionDisabler{},
		)...,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if repo.deletedTeamID != "" {
		t.Fatalf("team was deleted after owner changed: %q", repo.deletedTeamID)
	}
	if repo.deletedOwnerID != ownerID {
		t.Fatalf("expected deletion owner = %q, want %q", repo.deletedOwnerID, ownerID)
	}
}

func TestTeamHandlerDeleteTeamRejectsOwnerChangeBeforeQuotaTombstone(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	repo.fenceErr = identity.ErrTeamOwnerChanged
	lifecycle := &stubTeamDeletionLifecycle{}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(
			&stubTeamDeletePreflight{},
			lifecycle,
			distributedDisabler,
		)...,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if lifecycle.disableTeamID != "" ||
		distributedDisabler.teamID != "" ||
		repo.deletedTeamID != "" {
		t.Fatalf(
			"owner change reached quota deletion: disable=%q redis=%q identity=%q",
			lifecycle.disableTeamID,
			distributedDisabler.teamID,
			repo.deletedTeamID,
		)
	}
}

func TestTeamHandlerDeleteTeamRejectsResourceCreatedBeforeExclusiveFence(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	preflight := &stubTeamDeletePreflight{
		inventories: []*teamresources.Inventory{
			{TeamID: testTeamID},
			{
				TeamID: testTeamID,
				BlockingResources: []teamresources.ResourceCount{
					{Category: "api_keys", Count: 1},
				},
			},
		},
	}
	lifecycle := &stubTeamDeletionLifecycle{}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(preflight, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if preflight.calls != 2 {
		t.Fatalf("preflight calls = %d, want initial and fenced final checks", preflight.calls)
	}
	if lifecycle.finalizeTeamID != "" || distributedDisabler.teamID != "" || repo.deletedTeamID != "" {
		t.Fatalf(
			"deletion continued after final inventory: finalize=%q rate=%q identity=%q",
			lifecycle.finalizeTeamID,
			distributedDisabler.teamID,
			repo.deletedTeamID,
		)
	}
	if repo.unfencedTeamID != testTeamID || repo.unfencedOwnerID != ownerID {
		t.Fatalf(
			"released deletion fence = (%q, %q), want (%q, %q)",
			repo.unfencedTeamID,
			repo.unfencedOwnerID,
			testTeamID,
			ownerID,
		)
	}
}

func TestTeamHandlerDeleteTeamMapsQuotaDeletionConflictTo409(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	lifecycle := &stubTeamDeletionLifecycle{disableErr: &teamquota.TeamDeletionConflictError{
		TeamID:          testTeamID,
		LiveAllocations: 1,
	}}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(&stubTeamDeletePreflight{}, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	var payload struct {
		Error struct {
			Details teamresources.Inventory `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode conflict response: %v", err)
	}
	if len(payload.Error.Details.BlockingResources) != 1 ||
		payload.Error.Details.BlockingResources[0].Category != "team_quota_live_allocations" ||
		payload.Error.Details.BlockingResources[0].Count != 1 {
		t.Fatalf("blocking quota resources = %#v", payload.Error.Details.BlockingResources)
	}
	if repo.deletedTeamID != "" {
		t.Fatalf("team was deleted after quota conflict: %q", repo.deletedTeamID)
	}
	if distributedDisabler.teamID != "" || lifecycle.finalizeTeamID != "" {
		t.Fatalf("deletion continued after conflict: rate=%q finalize=%q", distributedDisabler.teamID, lifecycle.finalizeTeamID)
	}
	if repo.unfencedTeamID != testTeamID {
		t.Fatalf("quota conflict did not release identity deletion fence: %q", repo.unfencedTeamID)
	}
}

func TestTeamHandlerDeleteTeamStopsWhenDistributedMarkerFails(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	lifecycle := &stubTeamDeletionLifecycle{}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{err: errors.New("redis unavailable")}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(&stubTeamDeletePreflight{}, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("response = %d Retry-After=%q, want 503/1 body=%s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if lifecycle.disableTeamID != testTeamID || lifecycle.finalizeTeamID != "" || repo.deletedTeamID != "" {
		t.Fatalf(
			"state after marker failure = disable %q finalize %q identity %q",
			lifecycle.disableTeamID,
			lifecycle.finalizeTeamID,
			repo.deletedTeamID,
		)
	}
}

func TestTeamHandlerDeleteTeamStopsWhenFinalizationFails(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	lifecycle := &stubTeamDeletionLifecycle{finalizeErr: errors.New("postgres unavailable")}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}

	rec := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		coordinatedTeamDeletionOptions(&stubTeamDeletePreflight{}, lifecycle, distributedDisabler)...,
	)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if distributedDisabler.teamID != testTeamID || repo.deletedTeamID != "" {
		t.Fatalf("state after finalization failure = marker %q identity %q", distributedDisabler.teamID, repo.deletedTeamID)
	}
}

func TestTeamHandlerDeleteTeamIdentityFailureIsRetryable(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)
	repo.deleteErr = errors.New("identity transaction failed")
	lifecycle := &stubTeamDeletionLifecycle{}
	distributedDisabler := &stubTeamDistributedAdmissionDisabler{}
	options := coordinatedTeamDeletionOptions(&stubTeamDeletePreflight{}, lifecycle, distributedDisabler)

	first := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		options...,
	)
	if first.Code != http.StatusInternalServerError {
		t.Fatalf("first status = %d, want 500 body=%s", first.Code, first.Body.String())
	}
	if lifecycle.finalizeTeamID != testTeamID || distributedDisabler.teamID != testTeamID {
		t.Fatalf("durable deletion state was not completed before identity failure")
	}

	repo.deleteErr = nil
	second := performTeamManagementRequestWithOptions(
		t,
		repo,
		ownerID,
		http.MethodDelete,
		"/teams/"+testTeamID,
		nil,
		options...,
	)
	if second.Code != http.StatusOK || repo.deletedTeamID != testTeamID {
		t.Fatalf("retry = %d deleted=%q body=%s", second.Code, repo.deletedTeamID, second.Body.String())
	}
}

func TestTeamHandlerRejectsMalformedUserIDs(t *testing.T) {
	ownerID := testOwnerUserID

	tests := []struct {
		name   string
		method string
		path   string
		body   map[string]any
	}{
		{
			name:   "transfer owner body",
			method: http.MethodPut,
			path:   "/teams/" + testTeamID + "/owner",
			body:   map[string]any{"user_id": "not-a-uuid"},
		},
		{
			name:   "update member path",
			method: http.MethodPut,
			path:   "/teams/" + testTeamID + "/members/not-a-uuid",
			body:   map[string]any{"role": "viewer"},
		},
		{
			name:   "remove member path",
			method: http.MethodDelete,
			path:   "/teams/" + testTeamID + "/members/not-a-uuid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newTeamManagementRepo(ownerID)

			rec := performTeamManagementRequest(t, repo, ownerID, tt.method, tt.path, tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestTeamHandlerUpdateTeamRejectsInvalidNameAndSlug(t *testing.T) {
	ownerID := testOwnerUserID

	tests := []struct {
		name string
		body map[string]any
	}{
		{name: "whitespace name", body: map[string]any{"name": "   "}},
		{name: "whitespace slug", body: map[string]any{"slug": "   "}},
		{name: "unsafe slug", body: map[string]any{"slug": "Bad Slug"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newTeamManagementRepo(ownerID)

			rec := performTeamManagementRequest(t, repo, ownerID, http.MethodPut, "/teams/"+testTeamID, tt.body)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if repo.updatedTeam != nil {
				t.Fatalf("team should not be updated: %#v", repo.updatedTeam)
			}
		})
	}
}

func TestTeamHandlerUpdateTeamNormalizesNameAndSlug(t *testing.T) {
	ownerID := testOwnerUserID
	repo := newTeamManagementRepo(ownerID)

	rec := performTeamManagementRequest(t, repo, ownerID, http.MethodPut, "/teams/"+testTeamID, map[string]any{
		"name": " Team One Renamed ",
		"slug": " team-one-renamed ",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if repo.updatedTeam == nil {
		t.Fatal("team should be updated")
	}
	if repo.updatedTeam.Name != "Team One Renamed" || repo.updatedTeam.Slug != "team-one-renamed" {
		t.Fatalf("updated team = %#v, want normalized name and slug", repo.updatedTeam)
	}
}

func TestTeamHandlerUpdateTeamMemberRejectsOwnerDemotion(t *testing.T) {
	ownerID := testOwnerUserID
	callerID := testAdminUserID
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey(testTeamID, callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: testTeamID,
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodPut, "/teams/"+testTeamID+"/members/"+ownerID, map[string]any{
		"role": "viewer",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := repo.members[teamMemberKey(testTeamID, ownerID)].Role; got != "admin" {
		t.Fatalf("owner role = %q, want admin", got)
	}
}

func TestTeamHandlerRemoveTeamMemberRejectsOwnerRemoval(t *testing.T) {
	ownerID := testOwnerUserID
	callerID := testAdminUserID
	repo := newTeamManagementRepo(ownerID)
	repo.members[teamMemberKey(testTeamID, callerID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: testTeamID,
		UserID: callerID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, callerID, http.MethodDelete, "/teams/"+testTeamID+"/members/"+ownerID, nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := repo.members[teamMemberKey(testTeamID, ownerID)]; !ok {
		t.Fatal("owner member was removed")
	}
}

func TestTeamHandlerUpdateTeamMemberRejectsLastAdminDemotion(t *testing.T) {
	adminID := testAdminUserID
	repo := newTeamManagementRepo("")
	repo.members[teamMemberKey(testTeamID, adminID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: testTeamID,
		UserID: adminID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, adminID, http.MethodPut, "/teams/"+testTeamID+"/members/"+adminID, map[string]any{
		"role": "viewer",
	})

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if got := repo.members[teamMemberKey(testTeamID, adminID)].Role; got != "admin" {
		t.Fatalf("admin role = %q, want admin", got)
	}
}

func TestTeamHandlerRemoveTeamMemberRejectsLastAdminRemoval(t *testing.T) {
	adminID := testAdminUserID
	repo := newTeamManagementRepo("")
	repo.members[teamMemberKey(testTeamID, adminID)] = &identity.TeamMember{
		ID:     "member-admin",
		TeamID: testTeamID,
		UserID: adminID,
		Role:   "admin",
	}

	rec := performTeamManagementRequest(t, repo, adminID, http.MethodDelete, "/teams/"+testTeamID+"/members/"+adminID, nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if _, ok := repo.members[teamMemberKey(testTeamID, adminID)]; !ok {
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
			testTeamID: {
				ID:      testTeamID,
				Name:    "Team One",
				Slug:    "team-one",
				OwnerID: ownerPtr,
			},
		},
		members: make(map[string]*identity.TeamMember),
	}
	if ownerID != "" {
		repo.members[teamMemberKey(testTeamID, ownerID)] = &identity.TeamMember{
			ID:     "member-owner",
			TeamID: testTeamID,
			UserID: ownerID,
			Role:   "admin",
		}
	}
	return repo
}

func performTeamManagementRequest(t *testing.T, repo *stubTeamRepository, authUserID, method, path string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	return performTeamManagementRequestWithOptions(t, repo, authUserID, method, path, body)
}

func performTeamManagementRequestWithOptions(t *testing.T, repo *stubTeamRepository, authUserID, method, path string, body map[string]any, opts ...TeamHandlerOption) *httptest.ResponseRecorder {
	t.Helper()

	handler := NewTeamHandler(repo, zap.NewNop(), opts...)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT,
			UserID:     authUserID,
			TeamID:     testTeamID,
			TeamRole:   "admin",
		})
		c.Next()
	})
	router.GET("/teams/:id", handler.GetTeam)
	router.GET("/teams/:id/members", handler.ListTeamMembers)
	router.POST("/teams/:id/members", handler.AddTeamMember)
	router.PUT("/teams/:id", handler.UpdateTeam)
	router.PUT("/teams/:id/owner", handler.TransferTeamOwner)
	router.DELETE("/teams/:id", handler.DeleteTeam)
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

func coordinatedTeamDeletionOptions(
	preflight TeamDeletePreflight,
	lifecycle TeamDeletionLifecycle,
	distributedDisabler TeamDistributedAdmissionDisabler,
) []TeamHandlerOption {
	return []TeamHandlerOption{
		WithTeamDeletePreflight(preflight),
		WithTeamDeletionLifecycle(lifecycle),
		WithTeamDistributedAdmissionDisabler(distributedDisabler),
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func teamMemberKey(teamID, userID string) string {
	return teamID + ":" + userID
}
