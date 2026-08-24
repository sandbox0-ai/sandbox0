package rediscache

import "testing"

func TestKeyHelpers(t *testing.T) {
	if got := JoinKeyPrefix(" sandbox0:", ":cluster-gateway ", "", "get-sandbox-internal"); got != "sandbox0:cluster-gateway:get-sandbox-internal" {
		t.Fatalf("JoinKeyPrefix() = %q", got)
	}
	if got := HashedKey("sandbox0:test", "sb-1"); got != "sandbox0:test:a7ff505d82505710543b84805429e58e652bee6b1b0cf7dbff606a87df3516be" {
		t.Fatalf("HashedKey() = %q", got)
	}
}
