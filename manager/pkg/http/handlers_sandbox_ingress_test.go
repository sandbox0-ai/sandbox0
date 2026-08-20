package http

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestSandboxClaimIngressStartedAtUsesSignedClaimsOnly(t *testing.T) {
	want := time.Date(2026, time.August, 20, 8, 9, 10, 123456789, time.FixedZone("test", 8*60*60))
	claims := &internalauth.Claims{Audit: &internalauth.AuditContext{IngressStartedAt: &want}}
	got := sandboxClaimIngressStartedAt(claims)
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("started at = %s (%s), want %s in UTC", got, got.Location(), want)
	}
	if got := sandboxClaimIngressStartedAt(nil); !got.IsZero() {
		t.Fatalf("nil claims start = %s, want zero", got)
	}
}
