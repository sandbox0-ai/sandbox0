package policy

import (
	"net/http"
	"strings"
)

func MatchHTTPRequest(match *CompiledHTTPMatch, req *http.Request) bool {
	if match == nil {
		return true
	}
	if req == nil {
		return false
	}
	if len(match.Methods) > 0 && !matchString(strings.ToUpper(strings.TrimSpace(req.Method)), match.Methods) {
		return false
	}
	path := "/"
	if req.URL != nil && req.URL.Path != "" {
		path = req.URL.Path
	}
	if len(match.Paths) > 0 && !matchString(path, match.Paths) {
		return false
	}
	if len(match.PathPrefixes) > 0 && !matchPathPrefix(path, match.PathPrefixes) {
		return false
	}
	if len(match.Query) > 0 {
		if req.URL == nil {
			return false
		}
		values := req.URL.Query()
		for _, matcher := range match.Query {
			if !matchHTTPValues(values[matcher.Name], matcher) {
				return false
			}
		}
	}
	if len(match.Headers) > 0 {
		for _, matcher := range match.Headers {
			if !matchHTTPValues(req.Header.Values(matcher.Name), matcher) {
				return false
			}
		}
	}
	return true
}

func matchPathPrefix(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func matchHTTPValues(actual []string, matcher CompiledHTTPValueMatch) bool {
	if len(actual) == 0 {
		return false
	}
	if len(matcher.Values) == 0 {
		return matcher.Present
	}
	for _, got := range actual {
		for _, want := range matcher.Values {
			if got == want {
				return true
			}
		}
	}
	return false
}
