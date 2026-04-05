package tenantdir

import "testing"

func TestCanonicalRegionID(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "canonical unchanged", raw: "aws/us-east-1", want: "aws/us-east-1"},
		{name: "public label canonicalized", raw: "aws-us-east-1", want: "aws/us-east-1"},
		{name: "trimmed", raw: "  aws-us-east-1  ", want: "aws/us-east-1"},
		{name: "empty", raw: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanonicalRegionID(tt.raw); got != tt.want {
				t.Fatalf("CanonicalRegionID(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestSameRegionID(t *testing.T) {
	if !SameRegionID("aws/us-east-1", "aws-us-east-1") {
		t.Fatal("expected canonical and public ids to match")
	}
	if SameRegionID("aws/us-east-1", "aws/us-west-2") {
		t.Fatal("expected different regions to not match")
	}
}
