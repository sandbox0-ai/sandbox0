package reaper

import (
	"testing"
)

func TestParseProcStatHandlesSpacesAndParenthesesInCommand(t *testing.T) {
	pid, ppid, state, ok := parseProcStat("123 (worker (copy) job) Z 42 1 1 0")
	if !ok || pid != 123 || ppid != 42 || state != "Z" {
		t.Fatalf("parseProcStat() = (%d, %d, %q, %v)", pid, ppid, state, ok)
	}
}
