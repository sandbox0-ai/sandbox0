package middleware

import "testing"

func TestRequestPathAllowedWithoutUpstreamTimeout(t *testing.T) {
	testCases := []struct {
		path string
		want bool
	}{
		{path: "/api/v1/sandboxes", want: true},
		{path: "/api/v1/sandboxes/sb-1", want: true},
		{path: "/api/v1/sandboxes/sb-1/contexts/ctx-1/exec", want: true},
		{path: "/api/v1/sandboxvolumes", want: true},
		{path: "/api/v1/sandboxvolumes/vol-1/snapshots/snap-1/restore", want: true},
		{path: "/api/v1/templates", want: false},
		{path: "/readyz", want: false},
		{path: "/api/v1/sandboxes-extra", want: false},
	}

	for _, tc := range testCases {
		if got := RequestPathAllowedWithoutUpstreamTimeout(tc.path); got != tc.want {
			t.Fatalf("RequestPathAllowedWithoutUpstreamTimeout(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}
