package quantity

import "testing"

func TestParseAndCompare(t *testing.T) {
	tests := []struct {
		left, right string
	}{
		{"1", "1000m"},
		{"1Gi", "1024Mi"},
		{"1.5G", "1500M"},
		{"1e3", "1k"},
	}
	for _, test := range tests {
		if got := MustParse(test.left).Cmp(MustParse(test.right)); got != 0 {
			t.Errorf("Cmp(%q, %q) = %d, want 0", test.left, test.right, got)
		}
	}
}

func TestRoundedValues(t *testing.T) {
	if got := MustParse("1.1").Value(); got != 2 {
		t.Fatalf("Value() = %d, want 2", got)
	}
	if got := MustParse("-1.1").Value(); got != -2 {
		t.Fatalf("negative Value() = %d, want -2", got)
	}
	if got := MustParse("1500m").MilliValue(); got != 1500 {
		t.Fatalf("MilliValue() = %d, want 1500", got)
	}
	if got := MustParse("1Gi").Value(); got != 1<<30 {
		t.Fatalf("binary Value() = %d, want %d", got, 1<<30)
	}
}

func TestNewMilliCanonicalRepresentation(t *testing.T) {
	if got := NewMilli(1500).String(); got != "1500m" {
		t.Fatalf("NewMilli(1500).String() = %q", got)
	}
	if got := NewMilli(2000).String(); got != "2" {
		t.Fatalf("NewMilli(2000).String() = %q", got)
	}
}
