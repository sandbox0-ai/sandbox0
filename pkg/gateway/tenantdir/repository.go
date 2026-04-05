package tenantdir

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrRegionAlreadyExists = errors.New("region already exists")

// Repository persists region directory state.
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a region directory repository.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// CreateRegion creates a region directory entry.
func (r *Repository) CreateRegion(ctx context.Context, region *Region) error {
	regionID := CanonicalRegionID(region.ID)
	if regionID == "" {
		regionID = strings.TrimSpace(region.ID)
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO regions (id, display_name, regional_gateway_url, metering_export_url, enabled)
		VALUES ($1, $2, $3, $4, $5)
	`, regionID, region.DisplayName, region.RegionalGatewayURL, nullableString(region.MeteringExportURL), region.Enabled)
	if err != nil {
		if isDuplicateKeyError(err) {
			return ErrRegionAlreadyExists
		}
		return fmt.Errorf("insert region: %w", err)
	}
	return nil
}

// GetRegion retrieves a region by ID.
func (r *Repository) GetRegion(ctx context.Context, regionID string) (*Region, error) {
	region, err := r.getRegionExact(ctx, CanonicalRegionID(regionID))
	if err != nil {
		return nil, err
	}
	region.ID = CanonicalRegionID(region.ID)
	return region, nil
}

func (r *Repository) getRegionExact(ctx context.Context, regionID string) (*Region, error) {
	var region Region
	err := r.pool.QueryRow(ctx, `
		SELECT id, display_name, regional_gateway_url, COALESCE(metering_export_url, ''), enabled
		FROM regions
		WHERE id = $1
	`, regionID).Scan(&region.ID, &region.DisplayName, &region.RegionalGatewayURL, &region.MeteringExportURL, &region.Enabled)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrRegionNotFound
		}
		return nil, fmt.Errorf("query region: %w", err)
	}
	return &region, nil
}

// ListRegions lists all configured regions.
func (r *Repository) ListRegions(ctx context.Context) ([]*Region, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, display_name, regional_gateway_url, COALESCE(metering_export_url, ''), enabled
		FROM regions
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("query regions: %w", err)
	}
	defer rows.Close()

	var regions []*Region
	for rows.Next() {
		var region Region
		if err := rows.Scan(&region.ID, &region.DisplayName, &region.RegionalGatewayURL, &region.MeteringExportURL, &region.Enabled); err != nil {
			return nil, fmt.Errorf("scan region: %w", err)
		}
		region.ID = CanonicalRegionID(region.ID)
		regions = append(regions, &region)
	}
	return regions, nil
}

// UpdateRegion updates a region directory entry.
func (r *Repository) UpdateRegion(ctx context.Context, region *Region) error {
	storedID, err := r.resolveStoredRegionID(ctx, region.ID)
	if err != nil {
		return err
	}
	result, err := r.pool.Exec(ctx, `
		UPDATE regions
		SET display_name = $2, regional_gateway_url = $3, metering_export_url = $4, enabled = $5
		WHERE id = $1
	`, storedID, region.DisplayName, region.RegionalGatewayURL, nullableString(region.MeteringExportURL), region.Enabled)
	if err != nil {
		return fmt.Errorf("update region: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrRegionNotFound
	}
	return nil
}

// DeleteRegion deletes a region directory entry.
func (r *Repository) DeleteRegion(ctx context.Context, regionID string) error {
	storedID, err := r.resolveStoredRegionID(ctx, regionID)
	if err != nil {
		return err
	}
	result, err := r.pool.Exec(ctx, `DELETE FROM regions WHERE id = $1`, storedID)
	if err != nil {
		return fmt.Errorf("delete region: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrRegionNotFound
	}
	return nil
}

func (r *Repository) resolveStoredRegionID(ctx context.Context, regionID string) (string, error) {
	region, err := r.getRegionExact(ctx, CanonicalRegionID(regionID))
	if err != nil {
		return "", err
	}
	return region.ID, nil
}

func isDuplicateKeyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate key")
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
