package rootfs

import "testing"

func TestRewriteOverlayUpperWorkOptionsReplacesExistingValues(t *testing.T) {
	got, err := RewriteOverlayUpperWorkOptions([]string{
		"index=off",
		"lowerdir=/lower",
		"upperdir=/old-upper",
		"workdir=/old-work",
	}, "/s0fs/upper", "/s0fs/work")
	if err != nil {
		t.Fatalf("RewriteOverlayUpperWorkOptions() error = %v", err)
	}
	want := []string{"index=off", "lowerdir=/lower", "upperdir=/s0fs/upper", "workdir=/s0fs/work"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("options = %#v, want %#v", got, want)
	}
}

func TestRewriteOverlayUpperWorkOptionsAppendsMissingValues(t *testing.T) {
	got, err := RewriteOverlayUpperWorkOptions([]string{
		"lowerdir=/lower",
	}, "/s0fs/upper", "/s0fs/work")
	if err != nil {
		t.Fatalf("RewriteOverlayUpperWorkOptions() error = %v", err)
	}
	want := []string{"lowerdir=/lower", "upperdir=/s0fs/upper", "workdir=/s0fs/work"}
	if !stringSlicesEqual(got, want) {
		t.Fatalf("options = %#v, want %#v", got, want)
	}
}

func TestRewriteOverlayUpperWorkOptionsRequiresUpperAndWork(t *testing.T) {
	if _, err := RewriteOverlayUpperWorkOptions(nil, "", "/s0fs/work"); err == nil {
		t.Fatal("RewriteOverlayUpperWorkOptions() error = nil, want error")
	}
	if _, err := RewriteOverlayUpperWorkOptions(nil, "/s0fs/upper", ""); err == nil {
		t.Fatal("RewriteOverlayUpperWorkOptions() error = nil, want error")
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
