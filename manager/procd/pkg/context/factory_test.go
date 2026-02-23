package context

import (
	"testing"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/repl"
)

func TestCreateREPLProcess_CustomConfigNameRequired(t *testing.T) {
	_, err := createREPLProcess("ctx", process.ProcessConfig{Alias: ""}, &repl.REPLConfig{
		Candidates: []repl.ExecCandidate{
			{Name: "sh", Args: []string{"-i"}},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing repl_config.name")
	}
}

func TestCreateREPLProcess_CustomConfigAliasMismatch(t *testing.T) {
	_, err := createREPLProcess("ctx", process.ProcessConfig{Alias: "python"}, &repl.REPLConfig{
		Name: "psql",
		Candidates: []repl.ExecCandidate{
			{Name: "sh", Args: []string{"-i"}},
		},
	})
	if err == nil {
		t.Fatal("expected error for alias mismatch")
	}
}

func TestCreateREPLProcess_CustomConfigUsesName(t *testing.T) {
	proc, err := createREPLProcess("ctx", process.ProcessConfig{}, &repl.REPLConfig{
		Name: "custom",
		Candidates: []repl.ExecCandidate{
			{Name: "sh", Args: []string{"-i"}},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	replProc, ok := proc.(*repl.REPL)
	if !ok {
		t.Fatalf("expected repl.REPL, got %T", proc)
	}
	if replProc.Alias() != "custom" {
		t.Fatalf("unexpected alias: %s", replProc.Alias())
	}
}
