package cases

import (
	"net/http"
	"testing"
)

func TestIsRetrySafeClaimStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{name: "ambiguous transport error", status: 0, want: false},
		{name: "template projection not found", status: http.StatusNotFound, want: true},
		{name: "conflict may follow resource allocation", status: http.StatusConflict, want: false},
		{name: "quota exceeded", status: http.StatusTooManyRequests, want: false},
		{name: "internal error may be ambiguous", status: http.StatusInternalServerError, want: false},
		{name: "data plane not ready", status: http.StatusServiceUnavailable, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRetrySafeClaimStatus(tt.status); got != tt.want {
				t.Fatalf("isRetrySafeClaimStatus(%d) = %t, want %t", tt.status, got, tt.want)
			}
		})
	}
}
