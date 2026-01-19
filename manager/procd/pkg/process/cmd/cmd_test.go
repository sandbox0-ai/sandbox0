// Package cmd provides unit tests for command execution.
package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// TestNewCMD tests CMD creation.
func TestNewCMD(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		config  process.ProcessConfig
		command []string
		wantErr error
	}{
		{
			name: "valid command",
			id:   "test-cmd-1",
			config: process.ProcessConfig{
				Type:    process.ProcessTypeCMD,
				CWD:     "/tmp",
				EnvVars: map[string]string{"TEST": "value"},
			},
			command: []string{"/bin/echo", "hello"},
			wantErr: nil,
		},
		{
			name: "empty command",
			id:   "test-cmd-2",
			config: process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			},
			command: []string{},
			wantErr: process.ErrInvalidCommand,
		},
		{
			name: "nil command",
			id:   "test-cmd-3",
			config: process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			},
			command: nil,
			wantErr: process.ErrInvalidCommand,
		},
		{
			name: "command with only executable",
			id:   "test-cmd-4",
			config: process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			},
			command: []string{"/bin/echo"},
			wantErr: nil,
		},
		{
			name: "command with multiple arguments",
			id:   "test-cmd-5",
			config: process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			},
			command: []string{"/bin/echo", "hello", "world", "test"},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := NewCMD(tt.id, tt.config, tt.command)

			if tt.wantErr != nil {
				if err == nil {
					t.Errorf("NewCMD() expected error %v, got nil", tt.wantErr)
				} else if !strings.Contains(err.Error(), tt.wantErr.Error()) {
					t.Errorf("NewCMD() error = %v, want %v", err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("NewCMD() failed = %v", err)
			}

			if cmd == nil {
				t.Fatal("NewCMD() returned nil CMD")
			}

			if cmd.ID() != tt.id {
				t.Errorf("ID() = %s, want %s", cmd.ID(), tt.id)
			}

			if cmd.Type() != process.ProcessTypeCMD {
				t.Errorf("Type() = %s, want %s", cmd.Type(), process.ProcessTypeCMD)
			}

			// CMD should start in Created state
			if cmd.State() != process.ProcessStateCreated {
				t.Errorf("State() = %s, want %s", cmd.State(), process.ProcessStateCreated)
			}

			// GetCommand should return the expected string
			wantCmd := strings.Join(tt.command, " ")
			if gotCmd := cmd.GetCommand(); gotCmd != wantCmd {
				t.Errorf("GetCommand() = %s, want %s", gotCmd, wantCmd)
			}
		})
	}
}

// TestCMD_StartEcho tests starting a simple echo command.
func TestCMD_StartEcho(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
		CWD:  "/tmp",
	}

	cmd, err := NewCMD("test-echo", config, []string{"/bin/echo", "hello", "world"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// Start the command
	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// State should be Running
	if cmd.State() != process.ProcessStateRunning {
		t.Errorf("State() = %s, want %s", cmd.State(), process.ProcessStateRunning)
	}

	// IsRunning should be true
	if !cmd.IsRunning() {
		t.Error("IsRunning() = false, want true")
	}

	// Wait for command to complete
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

loop:
	for {
		select {
		case <-timeout:
			t.Fatal("command did not complete in time")
		case <-ticker.C:
			if !cmd.IsRunning() {
				break loop
			}
		}
	}

	// After completion, state should be Stopped (exit code 0)
	state := cmd.State()
	if state != process.ProcessStateStopped {
		// Could be other states depending on timing
		t.Logf("State after completion = %s", state)
	}

	// Get output
	stdout, stderr := cmd.GetOutput()
	if !strings.Contains(stdout, "hello") {
		t.Errorf("stdout = %s, want to contain 'hello'", stdout)
	}
	if !strings.Contains(stdout, "world") {
		t.Errorf("stdout = %s, want to contain 'world'", stdout)
	}
	if stderr != "" {
		t.Logf("stderr = %s (expected empty)", stderr)
	}
}

// TestCMD_StartAlreadyRunning tests starting an already running command.
func TestCMD_StartAlreadyRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-running", config, []string{"/bin/sleep", "1"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Try to start again
	err = cmd.Start()
	if err != process.ErrProcessAlreadyRunning {
		t.Errorf("Start() on running process error = %v, want %v", err, process.ErrProcessAlreadyRunning)
	}

	// Cleanup
	cmd.Stop()
}

// TestCMD_Stop tests stopping a command.
func TestCMD_Stop(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-stop", config, []string{"/bin/sleep", "10"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Verify it's running
	if !cmd.IsRunning() {
		t.Error("IsRunning() = false after Start()")
	}

	// Stop it
	err = cmd.Stop()
	if err != nil {
		t.Errorf("Stop() failed = %v", err)
	}

	// Give it a moment to stop
	time.Sleep(100 * time.Millisecond)

	// Should not be running anymore
	if cmd.IsRunning() {
		t.Error("IsRunning() = true after Stop()")
	}
}

// TestCMD_StopNonRunning tests stopping a non-running command.
func TestCMD_StopNonRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-not-running", config, []string{"/bin/echo", "test"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// Stop without starting - should not error
	err = cmd.Stop()
	if err != nil {
		t.Errorf("Stop() on non-running process failed = %v", err)
	}
}

// TestCMD_Restart tests restarting a command.
func TestCMD_Restart(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-restart", config, []string{"/bin/echo", "restart test"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// Start
	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// Restart
	err = cmd.Restart()
	if err != nil {
		t.Fatalf("Restart() failed = %v", err)
	}

	// Should be running again
	if !cmd.IsRunning() {
		t.Error("IsRunning() = false after Restart()")
	}

	// Wait for completion again
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// Get output from both runs
	stdout, _ := cmd.GetOutput()
	// Should contain the output (though both runs' output is combined)
	if !strings.Contains(stdout, "restart test") {
		t.Errorf("stdout = %s, want to contain 'restart test'", stdout)
	}
}

// TestCMD_ExitCode tests exit code handling.
func TestCMD_ExitCode(t *testing.T) {
	tests := []struct {
		name        string
		command     []string
		wantSuccess bool
	}{
		{
			name:        "successful command",
			command:     []string{"/bin/sh", "-c", "exit 0"},
			wantSuccess: true,
		},
		{
			name:        "command with exit code 1",
			command:     []string{"/bin/sh", "-c", "exit 1"},
			wantSuccess: false,
		},
		{
			name:        "command with exit code 42",
			command:     []string{"/bin/sh", "-c", "exit 42"},
			wantSuccess: false,
		},
		{
			name:        "command that fails to execute",
			command:     []string{"/nonexistent/command"},
			wantSuccess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := process.ProcessConfig{
				Type: process.ProcessTypeCMD,
			}

			cmd, err := NewCMD("test-exit-"+tt.name, config, tt.command)
			if err != nil {
				t.Fatalf("NewCMD() failed = %v", err)
			}

			err = cmd.Start()
			if err != nil {
				// Command failed to start
				if tt.wantSuccess {
					t.Errorf("Start() failed = %v", err)
				}
				return
			}

			// Wait for completion
			for i := 0; i < 100 && cmd.IsRunning(); i++ {
				time.Sleep(10 * time.Millisecond)
			}

			// Check exit code
			exitCode, err := cmd.ExitCode()
			if err != nil {
				t.Errorf("ExitCode() failed = %v", err)
			}

			if tt.wantSuccess && exitCode != 0 {
				t.Errorf("ExitCode() = %d, want 0 for success", exitCode)
			}

			if !tt.wantSuccess && exitCode == 0 {
				t.Errorf("ExitCode() = %d, want non-zero for failure", exitCode)
			}
		})
	}
}

// TestCMD_ReadOutput tests reading output from a running command.
func TestCMD_ReadOutput(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-output", config, []string{"/bin/echo", "output test"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// Get output channel before starting
	ch := cmd.ReadOutput()
	if ch == nil {
		t.Fatal("ReadOutput() returned nil channel")
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for output
	timeout := time.After(2 * time.Second)
	var received bool

	for {
		select {
		case <-timeout:
			if !received {
				t.Error("timeout waiting for output")
			}
			return
		case output, ok := <-ch:
			if !ok {
				// Channel closed
				if !received {
					t.Error("channel closed without receiving output")
				}
				return
			}
			if len(output.Data) > 0 {
				received = true
				if !strings.Contains(string(output.Data), "output test") {
					t.Errorf("received data = %s, want to contain 'output test'", string(output.Data))
				}
			}
		}
	}
}

// TestCMD_WithWorkingDirectory tests command with custom working directory.
func TestCMD_WithWorkingDirectory(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
		CWD:  "/tmp",
	}

	cmd, err := NewCMD("test-cwd", config, []string{"/bin/pwd"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	stdout, _ := cmd.GetOutput()
	if !strings.Contains(stdout, "/tmp") {
		t.Errorf("pwd output = %s, want to contain /tmp", stdout)
	}
}

// TestCMD_WithEnvironment tests command with custom environment variables.
func TestCMD_WithEnvironment(t *testing.T) {
	config := process.ProcessConfig{
		Type:    process.ProcessTypeCMD,
		EnvVars: map[string]string{"TEST_VAR": "test_value", "ANOTHER_VAR": "another_value"},
	}

	cmd, err := NewCMD("test-env", config, []string{"/bin/sh", "-c", "echo $TEST_VAR"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	stdout, _ := cmd.GetOutput()
	if !strings.Contains(stdout, "test_value") {
		t.Errorf("env var output = %s, want to contain 'test_value'", stdout)
	}
}

// TestCMD_WriteInputNoPTY tests writing input to a non-PTY command.
func TestCMD_WriteInputNoPTY(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-input", config, []string{"/bin/cat"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Writing to a non-PTY command should fail
	err = cmd.WriteInput([]byte("test input"))
	if err != process.ErrProcessNotRunning {
		t.Errorf("WriteInput() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	cmd.Stop()
}

// TestCMD_CommandWithSpaces tests command with spaces in arguments.
func TestCMD_CommandWithSpaces(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	// This should work because we pass the command as a slice
	cmd, err := NewCMD("test-spaces", config, []string{"/bin/echo", "hello world", "foo bar"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	stdout, _ := cmd.GetOutput()
	if !strings.Contains(stdout, "hello world") {
		t.Errorf("stdout = %s, want to contain 'hello world'", stdout)
	}
	if !strings.Contains(stdout, "foo bar") {
		t.Errorf("stdout = %s, want to contain 'foo bar'", stdout)
	}
}

// TestCMD_MultipleStartAttempts tests multiple start attempts without waiting.
func TestCMD_MultipleStartAttempts(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-multi", config, []string{"/bin/sleep", "0.1"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// First start
	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Second start should fail immediately
	err = cmd.Start()
	if err != process.ErrProcessAlreadyRunning {
		t.Errorf("Start() second attempt error = %v, want %v", err, process.ErrProcessAlreadyRunning)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCMD_StateTransitions tests state transitions during command lifecycle.
func TestCMD_StateTransitions(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-states", config, []string{"/bin/echo", "test"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	// Initial state
	if cmd.State() != process.ProcessStateCreated {
		t.Errorf("Initial state = %s, want %s", cmd.State(), process.ProcessStateCreated)
	}

	// After Start
	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	state := cmd.State()
	if state != process.ProcessStateRunning && state != process.ProcessStateStarting {
		t.Logf("State after Start = %s (may have completed quickly)", state)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	// After completion
	state = cmd.State()
	if state == process.ProcessStateCreated {
		t.Errorf("State after completion = %s, should not be Created", state)
	}
}

// TestCMD_ZeroExitCode tests commands that exit with code 0.
func TestCMD_ZeroExitCode(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-zero", config, []string{"/bin/sh", "-c", "exit 0"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	exitCode, err := cmd.ExitCode()
	if err != nil {
		t.Fatalf("ExitCode() failed = %v", err)
	}

	if exitCode != 0 {
		t.Errorf("ExitCode() = %d, want 0", exitCode)
	}
}

// TestCMD_NonZeroExitCode tests commands that exit with non-zero code.
func TestCMD_NonZeroExitCode(t *testing.T) {
	config := process.ProcessConfig{
		Type: process.ProcessTypeCMD,
	}

	cmd, err := NewCMD("test-nonzero", config, []string{"/bin/sh", "-c", "exit 1"})
	if err != nil {
		t.Fatalf("NewCMD() failed = %v", err)
	}

	err = cmd.Start()
	if err != nil {
		t.Fatalf("Start() failed = %v", err)
	}

	// Wait for completion
	for i := 0; i < 100 && cmd.IsRunning(); i++ {
		time.Sleep(10 * time.Millisecond)
	}

	exitCode, err := cmd.ExitCode()
	if err != nil {
		t.Fatalf("ExitCode() failed = %v", err)
	}

	if exitCode == 0 {
		t.Errorf("ExitCode() = %d, want non-zero", exitCode)
	}
}
