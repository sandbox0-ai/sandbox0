package template

import "testing"

func TestBuildSnapshotIDUsesReservedDeterministicPrefix(t *testing.T) {
	got := BuildSnapshotID(" 123e4567-e89b-12d3-a456-426614174000 ")
	want := "template-build-123e4567e89b12d3a456426614174000"
	if got != want {
		t.Fatalf("BuildSnapshotID() = %q, want %q", got, want)
	}
	if !IsBuildSnapshotID(got) {
		t.Fatalf("IsBuildSnapshotID(%q) = false, want true", got)
	}
	if IsBuildSnapshotID("snapshot-user-visible") {
		t.Fatal("IsBuildSnapshotID() classified a public snapshot as internal")
	}
}
