package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type stubRegionRepository struct {
	regions map[string]*tenantdir.Region
}

func (s *stubRegionRepository) ListRegions(_ context.Context) ([]*tenantdir.Region, error) {
	var regions []*tenantdir.Region
	for _, region := range s.regions {
		regions = append(regions, region)
	}
	return regions, nil
}

func (s *stubRegionRepository) GetRegion(_ context.Context, regionID string) (*tenantdir.Region, error) {
	region, ok := s.regions[regionID]
	if !ok {
		return nil, tenantdir.ErrRegionNotFound
	}
	return region, nil
}

func (s *stubRegionRepository) CreateRegion(_ context.Context, region *tenantdir.Region) error {
	if _, exists := s.regions[region.ID]; exists {
		return tenantdir.ErrRegionAlreadyExists
	}
	copied := *region
	s.regions[region.ID] = &copied
	return nil
}

func (s *stubRegionRepository) UpdateRegion(_ context.Context, region *tenantdir.Region) error {
	if _, exists := s.regions[region.ID]; !exists {
		return tenantdir.ErrRegionNotFound
	}
	copied := *region
	s.regions[region.ID] = &copied
	return nil
}

func (s *stubRegionRepository) DeleteRegion(_ context.Context, regionID string) error {
	if _, exists := s.regions[regionID]; !exists {
		return tenantdir.ErrRegionNotFound
	}
	delete(s.regions, regionID)
	return nil
}

func TestRegionHandlerCreateRegion(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubRegionRepository{regions: map[string]*tenantdir.Region{}}
	handler := NewRegionHandler(repo, zap.NewNop())

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod:    authn.AuthMethodJWT,
			UserID:        "admin-1",
			IsSystemAdmin: true,
		})
		c.Next()
	})
	router.POST("/regions", handler.CreateRegion)

	req := httptest.NewRequest(http.MethodPost, "/regions", strings.NewReader(`{"id":"aws/us-east-1","display_name":"US East 1","edge_gateway_url":"https://use1.example.com","metering_export_url":"https://metering.use1.example.com"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Data tenantdir.Region `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.ID != "aws/us-east-1" {
		t.Fatalf("expected region id, got %q", response.Data.ID)
	}
	if response.Data.EdgeGatewayURL != "https://use1.example.com" {
		t.Fatalf("expected edge url, got %q", response.Data.EdgeGatewayURL)
	}
	if response.Data.MeteringExportURL != "https://metering.use1.example.com" {
		t.Fatalf("expected metering export url, got %q", response.Data.MeteringExportURL)
	}
}

func TestRegionHandlerUpdateRegionCanClearMeteringExportURL(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := &stubRegionRepository{regions: map[string]*tenantdir.Region{
		"aws-us-east-1": {
			ID:                "aws-us-east-1",
			DisplayName:       "US East 1",
			EdgeGatewayURL:    "https://use1.example.com",
			MeteringExportURL: "https://metering.use1.example.com",
			Enabled:           true,
		},
	}}
	handler := NewRegionHandler(repo, zap.NewNop())

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod:    authn.AuthMethodJWT,
			UserID:        "admin-1",
			IsSystemAdmin: true,
		})
		c.Next()
	})
	router.PUT("/regions/:id", handler.UpdateRegion)

	req := httptest.NewRequest(http.MethodPut, "/regions/aws-us-east-1", strings.NewReader(`{"metering_export_url":""}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Data tenantdir.Region `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.MeteringExportURL != "" {
		t.Fatalf("expected metering export url to be cleared, got %q", response.Data.MeteringExportURL)
	}
}
