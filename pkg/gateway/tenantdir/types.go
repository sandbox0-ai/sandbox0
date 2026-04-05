package tenantdir

import "errors"

var ErrRegionNotFound = errors.New("region not found")

// Region describes a routable region entry.
type Region struct {
	ID                 string `json:"id"`
	DisplayName        string `json:"display_name,omitempty"`
	RegionalGatewayURL string `json:"regional_gateway_url,omitempty"`
	MeteringExportURL  string `json:"metering_export_url,omitempty"`
	Enabled            bool   `json:"enabled"`
}
