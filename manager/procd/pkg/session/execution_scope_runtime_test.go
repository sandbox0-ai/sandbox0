package session_test

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/execution"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"go.uber.org/zap"
)

func TestExecutionScopeMetadataUpdateKeepsRunningAttemptAndUpdatesResolver(t *testing.T) {
	const (
		helperEnv     = "SANDBOX0_EXECUTION_SCOPE_RUNTIME_HELPER"
		helperAddrEnv = "SANDBOX0_EXECUTION_SCOPE_RUNTIME_ADDR"
		beforeIDEnv   = "SCOPE_BEFORE"
		afterIDEnv    = "SCOPE_AFTER"
	)
	if os.Getenv(helperEnv) == "1" {
		connection, err := net.Dial("tcp4", os.Getenv(helperAddrEnv))
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if _, err := connection.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, connection)
		for {
			time.Sleep(time.Hour)
		}
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	store, err := session.NewFileStore(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	supervisor, err := session.NewSupervisor(store, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Initialize("sandbox-1", 1, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, value := range supervisor.List() {
			if err := supervisor.Delete(value.ID); err != nil && !errors.Is(err, session.ErrSessionNotFound) {
				t.Errorf("delete session %s: %v", value.ID, err)
			}
		}
		if err := supervisor.Close(); err != nil {
			t.Errorf("close supervisor: %v", err)
		}
	})

	spec := session.SessionSpec{
		Command: []string{
			"/bin/sh",
			"-c",
			`"$HELPER_BINARY" -test.run=^TestExecutionScopeMetadataUpdateKeepsRunningAttemptAndUpdatesResolver$ & wait`,
		},
		Env: map[string]string{
			"HELPER_BINARY": os.Args[0],
			helperEnv:       "1",
			helperAddrEnv:   listener.Addr().String(),
			beforeIDEnv:     "thread-before",
			afterIDEnv:      "thread-after",
		},
		Lifecycle: session.LifecycleSpec{
			DesiredState:           session.DesiredStateRunning,
			StopGracePeriodSeconds: 1,
		},
		ExecutionScope: &session.ExecutionScopeSpec{
			Namespace:             "codex",
			Kind:                  "native_session",
			IDEnvironmentVariable: beforeIDEnv,
		},
	}
	created, _, err := supervisor.Create(spec, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForRunningSession(t, supervisor, created.ID)

	tcpListener := listener.(*net.TCPListener)
	if err := tcpListener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, 1)
	if _, err := io.ReadFull(connection, ready); err != nil {
		t.Fatal(err)
	}

	before, err := supervisor.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Attempt == nil || before.Attempt.PID <= 0 {
		t.Fatalf("running session attempt = %#v", before.Attempt)
	}
	beforeAttemptID := before.Attempt.ID
	beforePID := before.Attempt.PID
	beforeRoots := supervisor.ExecutionScopeRoots()
	if len(beforeRoots) != 1 ||
		beforeRoots[0].PID != beforePID ||
		beforeRoots[0].ProcessStartTimeTicks == 0 ||
		beforeRoots[0].Spec.IDEnvironmentVariable != beforeIDEnv {
		t.Fatalf("ExecutionScopeRoots() before update = %+v", beforeRoots)
	}

	resolver := execution.NewResolver("/proc", supervisor.ExecutionScopeRoots)
	query := socketQueryForAcceptedConnection(t, connection)
	beforeScope, ok, err := resolver.ResolveSocket(query)
	if err != nil {
		t.Fatalf("ResolveSocket() before update error = %v", err)
	}
	if !ok ||
		beforeScope.Namespace != "codex" ||
		beforeScope.Kind != "native_session" ||
		beforeScope.ID != "thread-before" {
		t.Fatalf("ResolveSocket() before update = %+v, ok = %v", beforeScope, ok)
	}

	updatedSpec := before.Spec
	updatedSpec.ExecutionScope = &session.ExecutionScopeSpec{
		Namespace:             "codex-updated",
		Kind:                  "native_turn",
		IDEnvironmentVariable: afterIDEnv,
	}
	updated, err := supervisor.Update(created.ID, updatedSpec)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Phase != session.PhaseRunning ||
		updated.Attempt == nil ||
		updated.Attempt.ID != beforeAttemptID ||
		updated.Attempt.PID != beforePID {
		t.Fatalf(
			"execution scope update changed running attempt: before=(%s, %d), after=%#v",
			beforeAttemptID,
			beforePID,
			updated,
		)
	}

	afterRoots := supervisor.ExecutionScopeRoots()
	if len(afterRoots) != 1 ||
		afterRoots[0].PID != beforePID ||
		afterRoots[0].ProcessStartTimeTicks != beforeRoots[0].ProcessStartTimeTicks ||
		afterRoots[0].Spec.Namespace != "codex-updated" ||
		afterRoots[0].Spec.Kind != "native_turn" ||
		afterRoots[0].Spec.IDEnvironmentVariable != afterIDEnv {
		t.Fatalf("ExecutionScopeRoots() after update = %+v", afterRoots)
	}

	afterScope, ok, err := resolver.ResolveSocket(query)
	if err != nil {
		t.Fatalf("ResolveSocket() after update error = %v", err)
	}
	if !ok ||
		afterScope.Namespace != "codex-updated" ||
		afterScope.Kind != "native_turn" ||
		afterScope.ID != "thread-after" {
		t.Fatalf("ResolveSocket() after update = %+v, ok = %v", afterScope, ok)
	}
}

func waitForRunningSession(t *testing.T, supervisor *session.Supervisor, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		value, err := supervisor.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if value.Phase == session.PhaseRunning && value.Attempt != nil && value.Attempt.PID > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	value, _ := supervisor.Get(id)
	t.Fatalf("session did not reach running phase: %#v", value)
}

func socketQueryForAcceptedConnection(t *testing.T, connection net.Conn) execution.SocketQuery {
	t.Helper()
	local, ok := connection.LocalAddr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("local address = %T, want *net.TCPAddr", connection.LocalAddr())
	}
	remote, ok := connection.RemoteAddr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("remote address = %T, want *net.TCPAddr", connection.RemoteAddr())
	}
	return execution.SocketQuery{
		Transport:  "tcp",
		LocalIP:    remote.IP,
		LocalPort:  remote.Port,
		RemoteIP:   local.IP,
		RemotePort: local.Port,
	}
}
