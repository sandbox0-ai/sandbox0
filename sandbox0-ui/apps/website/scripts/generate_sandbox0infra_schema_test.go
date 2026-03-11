package main

import "testing"

func TestSourcePathForOutputUsesRepoRelativePath(t *testing.T) {
	repoRoot := "/tmp/workspace/sandbox0"
	sourcePath := "/tmp/workspace/sandbox0/infra-operator/chart/crds/infra.sandbox0.ai_sandbox0infras.yaml"

	got := sourcePathForOutput(repoRoot, sourcePath)
	want := "infra-operator/chart/crds/infra.sandbox0.ai_sandbox0infras.yaml"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestSourcePathForOutputFallsBackOutsideRepo(t *testing.T) {
	repoRoot := "/tmp/workspace/sandbox0"
	sourcePath := "/Users/example/private/file.yaml"

	got := sourcePathForOutput(repoRoot, sourcePath)
	want := "file.yaml"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
