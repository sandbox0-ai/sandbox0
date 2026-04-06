package tenantdir

import "testing"

func TestIsNormalizedRegionID(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "valid", value: "aws-us-east-1", want: true},
		{name: "single segment", value: "use1", want: false},
		{name: "underscore", value: "aws_us_east_1", want: false},
		{name: "uppercase", value: "AWS-us-east-1", want: false},
		{name: "empty", value: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNormalizedRegionID(tt.value); got != tt.want {
				t.Fatalf("IsNormalizedRegionID(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
