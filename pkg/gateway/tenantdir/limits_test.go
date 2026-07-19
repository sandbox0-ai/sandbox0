package tenantdir

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

func TestValidateRegionBoundsPersistedText(t *testing.T) {
	tests := []Region{
		{ID: strings.Repeat("r", MaxRegionIDBytes+1)},
		{DisplayName: strings.Repeat("n", MaxRegionDisplayNameBytes+1)},
		{RegionalGatewayURL: strings.Repeat("u", MaxRegionURLBytes+1)},
		{MeteringExportURL: strings.Repeat("u", MaxRegionURLBytes+1)},
	}
	for i := range tests {
		if err := validateRegion(&tests[i]); !resourceguard.IsTooLarge(err) {
			t.Fatalf("case %d error = %v, want too large", i, err)
		}
	}
}
