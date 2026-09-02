package operationid

import "testing"

func TestFromIdempotencyKeyIsStableAndScoped(t *testing.T) {
	first := FromIdempotencyKey("sandbox.fork", "team-1", "user-1", "sb-1", "request-1")
	retry := FromIdempotencyKey("sandbox.fork", "team-1", "user-1", "sb-1", "request-1")
	other := FromIdempotencyKey("sandbox.fork", "team-1", "user-1", "sb-2", "request-1")
	if first == "" || first != retry || first == other {
		t.Fatalf("operation ids = first %q retry %q other %q", first, retry, other)
	}
}

func TestFromIdempotencyKeyRejectsMissingAndOversizedKeys(t *testing.T) {
	if got := FromIdempotencyKey("sandbox.fork", "team", "user", "sb", "   "); got != "" {
		t.Fatalf("missing key operation id = %q", got)
	}
	oversized := make([]byte, 256)
	for index := range oversized {
		oversized[index] = 'a'
	}
	if got := FromIdempotencyKey("sandbox.fork", "team", "user", "sb", string(oversized)); got != "" {
		t.Fatalf("oversized key operation id = %q", got)
	}
}
