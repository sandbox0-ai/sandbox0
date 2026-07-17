package session

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestValidateExecutionScopeSpec(t *testing.T) {
	spec := normalizeSpec(SessionSpec{
		Command: []string{"agent"},
		ExecutionScope: &ExecutionScopeSpec{
			Namespace:             " codex ",
			Kind:                  " native_session ",
			IDEnvironmentVariable: " CODEX_THREAD_ID ",
		},
	})
	if err := validateSpec(spec); err != nil {
		t.Fatalf("validateSpec() error = %v", err)
	}
	if spec.ExecutionScope.Namespace != "codex" ||
		spec.ExecutionScope.Kind != "native_session" ||
		spec.ExecutionScope.IDEnvironmentVariable != "CODEX_THREAD_ID" {
		t.Fatalf("normalizeSpec() execution scope = %+v", spec.ExecutionScope)
	}

	spec.ExecutionScope.IDEnvironmentVariable = "invalid-name"
	if err := validateSpec(spec); err == nil || !strings.Contains(err.Error(), "uppercase environment variable") {
		t.Fatalf("validateSpec() error = %v", err)
	}
}

func TestExecutionScopeRootsOnlyReturnsRunningAttributedSessions(t *testing.T) {
	supervisor := &Supervisor{
		sessions: map[string]*managedSession{
			"attributed": {
				record: Session{
					Spec: SessionSpec{ExecutionScope: &ExecutionScopeSpec{
						Namespace:             "codex",
						Kind:                  "native_session",
						IDEnvironmentVariable: "CODEX_THREAD_ID",
					}},
					Attempt: &Attempt{ID: "att-1", PID: 123},
				},
				runtime: &attemptRuntime{attemptID: "att-1", processStartTimeTicks: 999},
			},
			"unattributed": {
				record: Session{Attempt: &Attempt{PID: 456}},
			},
			"stale-attempt": {
				record: Session{
					Spec: SessionSpec{ExecutionScope: &ExecutionScopeSpec{
						Namespace:             "codex",
						Kind:                  "native_session",
						IDEnvironmentVariable: "CODEX_THREAD_ID",
					}},
					Attempt: &Attempt{ID: "att-current", PID: 789},
				},
				runtime: &attemptRuntime{attemptID: "att-old", processStartTimeTicks: 1000},
			},
		},
	}
	roots := supervisor.ExecutionScopeRoots()
	if len(roots) != 1 ||
		roots[0].PID != 123 ||
		roots[0].ProcessStartTimeTicks != 999 ||
		roots[0].Spec.Namespace != "codex" {
		t.Fatalf("ExecutionScopeRoots() = %+v", roots)
	}
}

func TestExecutionScopeUpdateDoesNotRestartAttempt(t *testing.T) {
	base := SessionSpec{
		Command: []string{"agent"},
		CWD:     "/workspace",
		Env:     map[string]string{"HOME": "/workspace"},
	}
	withScope := base
	withScope.ExecutionScope = &ExecutionScopeSpec{
		Namespace:             "codex",
		Kind:                  "native_session",
		IDEnvironmentVariable: "CODEX_THREAD_ID",
	}
	if !reflect.DeepEqual(executionSpec(base), executionSpec(withScope)) {
		t.Fatal("execution scope metadata unexpectedly changes the process execution spec")
	}
}

func TestReadProcessStartTimeTicksIsStableForProcessLifetime(t *testing.T) {
	first, err := readProcessStartTimeTicks("/proc", os.Getpid())
	if err != nil {
		t.Fatalf("readProcessStartTimeTicks() error = %v", err)
	}
	second, err := readProcessStartTimeTicks("/proc", os.Getpid())
	if err != nil {
		t.Fatalf("readProcessStartTimeTicks() second error = %v", err)
	}
	if first == 0 || second != first {
		t.Fatalf("process starttime = %d then %d", first, second)
	}
}
