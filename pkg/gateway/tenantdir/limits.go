package tenantdir

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

const (
	MaxRegionIDBytes          = 128
	MaxRegionDisplayNameBytes = 256
	MaxRegionURLBytes         = 8 * 1024
)

func validateRegion(region *Region) error {
	if region == nil {
		return fmt.Errorf("region is required")
	}
	for _, field := range []struct {
		name     string
		value    string
		maxBytes int64
	}{
		{name: "region id", value: region.ID, maxBytes: MaxRegionIDBytes},
		{name: "region display name", value: region.DisplayName, maxBytes: MaxRegionDisplayNameBytes},
		{name: "regional gateway URL", value: region.RegionalGatewayURL, maxBytes: MaxRegionURLBytes},
		{name: "metering export URL", value: region.MeteringExportURL, maxBytes: MaxRegionURLBytes},
	} {
		if err := resourceguard.String(field.name, field.value, field.maxBytes); err != nil {
			return err
		}
	}
	return nil
}
