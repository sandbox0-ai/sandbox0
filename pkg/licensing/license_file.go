package licensing

import (
	"errors"
	"strings"
)

var ErrLicenseFileRequired = errors.New("license_file is required")

func RequireLicenseFile(path string) error {
	if strings.TrimSpace(path) == "" {
		return ErrLicenseFileRequired
	}
	return nil
}
