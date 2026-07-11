package reaper

import (
	"os/exec"
	"testing"
)

func TestParseProcStatHandlesSpacesAndParenthesesInCommand(t *testing.T) {
	pid, ppid, state, ok := parseProcStat("123 (worker (copy) job) Z 42 1 1 0")
	if !ok || pid != 123 || ppid != 42 || state != "Z" {
		t.Fatalf("parseProcStat() = (%d, %d, %q, %v)", pid, ppid, state, ok)
	}
}

func TestStartManagedRegistersBeforeReturning(t *testing.T) {
	cmd := exec.Command("/bin/true")
	if err := StartManaged(cmd.Start, func() int { return cmd.Process.Pid }); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	if !isManaged(pid) {
		t.Fatal("started child was not registered")
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	Untrack(pid)
	if isManaged(pid) {
		t.Fatal("waited child remained registered")
	}
}
