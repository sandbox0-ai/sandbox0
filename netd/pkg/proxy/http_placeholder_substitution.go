package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

const maxHTTPPlaceholderSubstitutionBodyBytes int64 = 1 << 20

func applyResolvedHTTPPlaceholderSubstitutions(ctx *egressAuthContext, protocol string, req *http.Request) error {
	if ctx == nil || len(ctx.ResolvedPlaceholderSubstitutions) == 0 {
		return nil
	}
	if err := applyHTTPPlaceholderSubstitutions(req, ctx.ResolvedPlaceholderSubstitutions); err != nil {
		applyEgressAuthFailurePolicy(ctx, protocol, "placeholder_substitution_failed")
		if ctx.ShouldBypass() {
			return nil
		}
		return err
	}
	return nil
}

func applyHTTPPlaceholderSubstitutions(req *http.Request, replacements []resolvedPlaceholderSubstitution) error {
	if req == nil || len(replacements) == 0 {
		return nil
	}
	if err := validateResolvedPlaceholderSubstitutions(replacements); err != nil {
		return err
	}

	if hasPlaceholderSubstitutionLocation(replacements, egressauth.PlaceholderSubstitutionLocationBody) {
		if err := replaceHTTPBodyPlaceholders(req, replacements); err != nil {
			return err
		}
	}
	if hasPlaceholderSubstitutionLocation(replacements, egressauth.PlaceholderSubstitutionLocationHeader) {
		replaceHTTPHeaderPlaceholders(req.Header, replacements)
	}
	if hasPlaceholderSubstitutionLocation(replacements, egressauth.PlaceholderSubstitutionLocationQuery) {
		replaceHTTPQueryPlaceholders(req.URL, replacements)
	}
	return nil
}

func validateResolvedPlaceholderSubstitutions(replacements []resolvedPlaceholderSubstitution) error {
	for _, replacement := range replacements {
		if strings.TrimSpace(replacement.Placeholder) == "" {
			return fmt.Errorf("placeholder substitution placeholder is required")
		}
		if len(replacement.Locations) == 0 {
			return fmt.Errorf("placeholder substitution locations are required")
		}
		for _, location := range replacement.Locations {
			switch location {
			case egressauth.PlaceholderSubstitutionLocationHeader,
				egressauth.PlaceholderSubstitutionLocationQuery,
				egressauth.PlaceholderSubstitutionLocationBody:
			default:
				return fmt.Errorf("placeholder substitution location %q is not supported", location)
			}
		}
	}
	return nil
}

func hasPlaceholderSubstitutionLocation(replacements []resolvedPlaceholderSubstitution, location egressauth.PlaceholderSubstitutionLocation) bool {
	for _, replacement := range replacements {
		for _, candidate := range replacement.Locations {
			if candidate == location {
				return true
			}
		}
	}
	return false
}

func replaceHTTPHeaderPlaceholders(headers http.Header, replacements []resolvedPlaceholderSubstitution) {
	for key, values := range headers {
		for index, value := range values {
			values[index] = replacePlaceholdersInString(value, replacements, egressauth.PlaceholderSubstitutionLocationHeader)
		}
		headers[key] = values
	}
}

func replaceHTTPQueryPlaceholders(target *url.URL, replacements []resolvedPlaceholderSubstitution) {
	if target == nil || target.RawQuery == "" {
		return
	}
	values := target.Query()
	if len(values) == 0 {
		return
	}

	replaced := make(url.Values, len(values))
	for key, rawValues := range values {
		replacedKey := replacePlaceholdersInString(key, replacements, egressauth.PlaceholderSubstitutionLocationQuery)
		for _, value := range rawValues {
			replaced.Add(replacedKey, replacePlaceholdersInString(value, replacements, egressauth.PlaceholderSubstitutionLocationQuery))
		}
	}
	target.RawQuery = replaced.Encode()
}

func replaceHTTPBodyPlaceholders(req *http.Request, replacements []resolvedPlaceholderSubstitution) error {
	if req == nil || req.Body == nil || req.Body == http.NoBody {
		return nil
	}
	if encoding := strings.TrimSpace(req.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return fmt.Errorf("placeholder substitution does not support content-encoding %q", encoding)
	}
	if req.ContentLength < 0 {
		return fmt.Errorf("placeholder substitution body requires a known content length")
	}
	if req.ContentLength > maxHTTPPlaceholderSubstitutionBodyBytes {
		return fmt.Errorf("placeholder substitution body exceeds %d bytes", maxHTTPPlaceholderSubstitutionBodyBytes)
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxHTTPPlaceholderSubstitutionBodyBytes+1))
	if closeErr := req.Body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("read placeholder substitution body: %w", err)
	}
	if int64(len(body)) > maxHTTPPlaceholderSubstitutionBodyBytes {
		return fmt.Errorf("placeholder substitution body exceeds %d bytes", maxHTTPPlaceholderSubstitutionBodyBytes)
	}

	replaced := replacePlaceholdersInString(string(body), replacements, egressauth.PlaceholderSubstitutionLocationBody)
	setHTTPRequestBody(req, []byte(replaced))
	return nil
}

func replacePlaceholdersInString(value string, replacements []resolvedPlaceholderSubstitution, location egressauth.PlaceholderSubstitutionLocation) string {
	for _, replacement := range replacements {
		if !placeholderSubstitutionAppliesToLocation(replacement, location) {
			continue
		}
		value = strings.ReplaceAll(value, replacement.Placeholder, replacement.Value)
	}
	return value
}

func placeholderSubstitutionAppliesToLocation(replacement resolvedPlaceholderSubstitution, location egressauth.PlaceholderSubstitutionLocation) bool {
	for _, candidate := range replacement.Locations {
		if candidate == location {
			return true
		}
	}
	return false
}

func setHTTPRequestBody(req *http.Request, body []byte) {
	if len(body) == 0 {
		req.Body = http.NoBody
		req.GetBody = func() (io.ReadCloser, error) {
			return http.NoBody, nil
		}
		req.ContentLength = 0
		req.TransferEncoding = nil
		req.Header.Set("Content-Length", "0")
		return
	}

	bodyCopy := append([]byte(nil), body...)
	req.Body = io.NopCloser(bytes.NewReader(bodyCopy))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bodyCopy)), nil
	}
	req.ContentLength = int64(len(bodyCopy))
	req.TransferEncoding = nil
	req.Header.Set("Content-Length", strconv.Itoa(len(bodyCopy)))
}
