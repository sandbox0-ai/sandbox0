package rediscache

import "testing"

func TestSpecEnabled(t *testing.T) {
	disabled := false
	enabled := true
	tests := []struct {
		name           string
		specPresent    bool
		backendType    string
		builtinEnabled *bool
		want           bool
	}{
		{name: "missing spec", specPresent: false, want: false},
		{name: "builtin default enabled", specPresent: true, backendType: "builtin", want: true},
		{name: "builtin explicit enabled", specPresent: true, backendType: "builtin", builtinEnabled: &enabled, want: true},
		{name: "builtin explicit disabled", specPresent: true, backendType: "builtin", builtinEnabled: &disabled, want: false},
		{name: "external enabled", specPresent: true, backendType: "external", want: true},
		{name: "unknown enabled for validation layer", specPresent: true, backendType: "custom", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SpecEnabled(tt.specPresent, tt.backendType, tt.builtinEnabled); got != tt.want {
				t.Fatalf("SpecEnabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestKeyHelpers(t *testing.T) {
	if got := JoinKeyPrefix(" sandbox0:", ":cluster-gateway ", "", "get-sandbox-internal"); got != "sandbox0:cluster-gateway:get-sandbox-internal" {
		t.Fatalf("JoinKeyPrefix() = %q", got)
	}
	if got := HashedKey("sandbox0:test", "sb-1"); got != "sandbox0:test:a7ff505d82505710543b84805429e58e652bee6b1b0cf7dbff606a87df3516be" {
		t.Fatalf("HashedKey() = %q", got)
	}
}
