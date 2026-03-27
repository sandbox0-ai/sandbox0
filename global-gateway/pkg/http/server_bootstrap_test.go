package http

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
)

type fakeBootstrapRegionRepo struct {
	regions map[string]*tenantdir.Region
}

func (f *fakeBootstrapRegionRepo) GetRegion(_ context.Context, regionID string) (*tenantdir.Region, error) {
	if region, ok := f.regions[regionID]; ok {
		copy := *region
		return &copy, nil
	}
	return nil, tenantdir.ErrRegionNotFound
}

func (f *fakeBootstrapRegionRepo) CreateRegion(_ context.Context, region *tenantdir.Region) error {
	copy := *region
	f.regions[region.ID] = &copy
	return nil
}

func (f *fakeBootstrapRegionRepo) UpdateRegion(_ context.Context, region *tenantdir.Region) error {
	copy := *region
	f.regions[region.ID] = &copy
	return nil
}

func TestEnsureBootstrapRegionCreatesMissingRegion(t *testing.T) {
	repo := &fakeBootstrapRegionRepo{regions: map[string]*tenantdir.Region{}}

	err := ensureBootstrapRegion(context.Background(), repo, &config.BootstrapRegionConfig{
		ID:                 "aws/us-east-1",
		DisplayName:        "US East 1",
		RegionalGatewayURL: "http://regional-gateway:8080",
		Enabled:            true,
	})
	if err != nil {
		t.Fatalf("ensureBootstrapRegion returned error: %v", err)
	}

	region, ok := repo.regions["aws/us-east-1"]
	if !ok {
		t.Fatal("expected bootstrap region to be created")
	}
	if region.RegionalGatewayURL != "http://regional-gateway:8080" {
		t.Fatalf("unexpected regional gateway url: %q", region.RegionalGatewayURL)
	}
}

func TestEnsureBootstrapRegionUpdatesExistingRegion(t *testing.T) {
	repo := &fakeBootstrapRegionRepo{
		regions: map[string]*tenantdir.Region{
			"aws/us-east-1": {
				ID:                 "aws/us-east-1",
				DisplayName:        "old",
				RegionalGatewayURL: "http://old:8080",
				Enabled:            false,
			},
		},
	}

	err := ensureBootstrapRegion(context.Background(), repo, &config.BootstrapRegionConfig{
		ID:                 "aws/us-east-1",
		DisplayName:        "US East 1",
		RegionalGatewayURL: "http://regional-gateway:8080",
		Enabled:            true,
	})
	if err != nil {
		t.Fatalf("ensureBootstrapRegion returned error: %v", err)
	}

	region := repo.regions["aws/us-east-1"]
	if region.DisplayName != "US East 1" {
		t.Fatalf("unexpected display name: %q", region.DisplayName)
	}
	if region.RegionalGatewayURL != "http://regional-gateway:8080" {
		t.Fatalf("unexpected regional gateway url: %q", region.RegionalGatewayURL)
	}
	if !region.Enabled {
		t.Fatal("expected region to be enabled")
	}
}
