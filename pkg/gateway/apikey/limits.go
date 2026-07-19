package apikey

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
)

const (
	// MaxCreateRequestBytes is the hard JSON body limit for API key creation.
	MaxCreateRequestBytes int64 = 8 << 10
	// MaxNameBytes is the maximum UTF-8 byte length of an API key name.
	MaxNameBytes int64 = 128
)

// ValidateCreateInput protects the repository boundary from oversized values,
// including calls that do not originate from the public HTTP handler.
func ValidateCreateInput(name string, roles []string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("api key name is required")
	}
	if err := resourceguard.String("api key name", name, MaxNameBytes); err != nil {
		return err
	}
	if _, err := resourceguard.CanonicalJSON(
		"api key roles",
		roles,
		MaxCreateRequestBytes,
	); err != nil {
		return err
	}
	return nil
}
