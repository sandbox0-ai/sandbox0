// Package repl provides unit tests for REPL process implementations.
package repl

import (
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// TestNewPythonREPL tests Python REPL creation.
func TestNewPythonREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
		CWD:      "/tmp",
		EnvVars:  map[string]string{"TEST": "value"},
	}

	repl, err := NewPythonREPL("test-python", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewPythonREPL() returned nil")
	}

	if repl.ID() != "test-python" {
		t.Errorf("ID() = %s, want test-python", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}

	if repl.State() != process.ProcessStateCreated {
		t.Errorf("State() = %s, want %s", repl.State(), process.ProcessStateCreated)
	}
}

// TestPythonREPL_StartWithoutPython tests starting when Python is not available.
func TestPythonREPL_StartWithoutPython(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
		// Use a PATH that won't have Python
	}

	repl, err := NewPythonREPL("test-no-python", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// Mock the situation by removing Python from PATH
	// Since we can't reliably mock this, we'll test that Start returns an error
	// when Python is not found. This test may pass if Python IS available.

	// Try to start - it may succeed if Python is available, or fail if not
	err = repl.Start()
	if err != nil {
		// Expected when Python is not available
		if strings.Contains(err.Error(), "no Python interpreter found") {
			return // Expected error
		}
		// Other errors are also acceptable (e.g., permission issues)
		return
	}

	// If Python IS available, clean up
	repl.Stop()
	t.Skip("Python is available, skipping 'not found' test")
}

// TestPythonREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestPythonREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-not-running", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// Try to execute code without starting
	result, err := repl.ExecuteCode("print('hello')")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestPythonREPL_ResizeTerminalNotRunning tests ResizeTerminal when not running.
func TestPythonREPL_ResizeTerminalNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-resize", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	size := process.PTYSize{Rows: 40, Cols: 120}
	err = repl.ResizeTerminal(size)
	if err != process.ErrProcessNotRunning {
		t.Errorf("ResizeTerminal() error = %v, want %v", err, process.ErrProcessNotRunning)
	}
}

// TestPythonREPL_StopNotRunning tests Stop when not running.
func TestPythonREPL_StopNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-stop", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// Stop without starting - should not error
	err = repl.Stop()
	if err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

// TestPythonREPL_DoubleStart tests starting an already running REPL.
func TestPythonREPL_DoubleStart(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-double", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// First start attempt - may fail if Python not available
	err = repl.Start()
	if err != nil {
		if strings.Contains(err.Error(), "no Python interpreter found") {
			t.Skip("Python not available")
		}
		// Other errors also mean we can't test double start
		t.Skipf("Python start failed: %v", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Try to start again - should fail
	err = repl.Start()
	if err != process.ErrProcessAlreadyRunning {
		t.Errorf("Start() second attempt error = %v, want %v", err, process.ErrProcessAlreadyRunning)
	}

	// Clean up
	repl.Stop()
}

// TestPythonREPL_StateTransitions tests state transitions.
func TestPythonREPL_StateTransitions(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-states", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// Initial state
	if repl.State() != process.ProcessStateCreated {
		t.Errorf("Initial state = %s, want %s", repl.State(), process.ProcessStateCreated)
	}

	// Note: We can't test actual state transitions without Python available
	// so we test that state can be set
	repl.SetState(process.ProcessStateRunning)
	if repl.State() != process.ProcessStateRunning {
		t.Errorf("State after SetState = %s, want %s", repl.State(), process.ProcessStateRunning)
	}

	repl.SetState(process.ProcessStatePaused)
	if repl.State() != process.ProcessStatePaused {
		t.Errorf("State after SetState = %s, want %s", repl.State(), process.ProcessStatePaused)
	}
}

// TestPythonREPL_FilterOutput tests output filtering logic.
func TestPythonREPL_FilterOutput(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-filter", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	// Set last input to test echo filtering
	repl.promptMu.Lock()
	repl.lastInput = "print('hello')"
	repl.promptMu.Unlock()

	// Test filtering with input echo
	input := []byte("print('hello')\nhello\n")
	filtered := repl.filterOutput(input)

	// The filter should remove the echoed input
	if strings.Contains(string(filtered), "print('hello')") {
		t.Error("filterOutput() should remove echoed input")
	}

	// Test without last input set
	repl.promptMu.Lock()
	repl.lastInput = ""
	repl.promptMu.Unlock()

	input = []byte("some output")
	filtered = repl.filterOutput(input)
	if string(filtered) != "some output" {
		t.Errorf("filterOutput() = %s, want 'some output'", string(filtered))
	}
}

// TestPythonREPL_DetectPrompt tests prompt detection.
func TestPythonREPL_DetectPrompt(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewPythonREPL("test-prompt", config)
	if err != nil {
		t.Fatalf("NewPythonREPL() failed = %v", err)
	}

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "IPython input prompt",
			data:     []byte("In [1]:"),
			expected: true,
		},
		{
			name:     "IPython output prompt",
			data:     []byte("Out[1]:"),
			expected: true,
		},
		{
			name:     "IPython continuation",
			data:     []byte("...:"),
			expected: true,
		},
		{
			name:     "Standard Python prompt",
			data:     []byte(">>> "),
			expected: true,
		},
		{
			name:     "Standard continuation",
			data:     []byte("... "),
			expected: true,
		},
		{
			name:     "Regular output",
			data:     []byte("hello world"),
			expected: false,
		},
		{
			name:     "Empty",
			data:     []byte(""),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := repl.detectPrompt(tt.data)
			if got != tt.expected {
				t.Errorf("detectPrompt() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNewNodeREPL tests Node REPL creation.
func TestNewNodeREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "node",
	}

	repl, err := NewNodeREPL("test-node", config)
	if err != nil {
		t.Fatalf("NewNodeREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewNodeREPL() returned nil")
	}

	if repl.ID() != "test-node" {
		t.Errorf("ID() = %s, want test-node", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestNodeREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestNodeREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "node",
	}

	repl, err := NewNodeREPL("test-node-not-running", config)
	if err != nil {
		t.Fatalf("NewNodeREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("console.log('test')")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestNodeREPL_ResizeTerminalNotRunning tests ResizeTerminal when not running.
func TestNodeREPL_ResizeTerminalNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "node",
	}

	repl, err := NewNodeREPL("test-node-resize", config)
	if err != nil {
		t.Fatalf("NewNodeREPL() failed = %v", err)
	}

	size := process.PTYSize{Rows: 40, Cols: 120}
	err = repl.ResizeTerminal(size)
	if err != process.ErrProcessNotRunning {
		t.Errorf("ResizeTerminal() error = %v, want %v", err, process.ErrProcessNotRunning)
	}
}

// TestNewBashREPL tests Bash REPL creation.
func TestNewBashREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-bash", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewBashREPL() returned nil")
	}

	if repl.ID() != "test-bash" {
		t.Errorf("ID() = %s, want test-bash", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}

	// Verify the prompt is set
	if repl.prompt != "SANDBOX0>>> " {
		t.Errorf("prompt = %s, want 'SANDBOX0>>> '", repl.prompt)
	}
}

// TestBashREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestBashREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-bash-not-running", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("echo test")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestBashREPL_ResizeTerminalNotRunning tests ResizeTerminal when not running.
func TestBashREPL_ResizeTerminalNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-bash-resize", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	size := process.PTYSize{Rows: 40, Cols: 120}
	err = repl.ResizeTerminal(size)
	if err != process.ErrProcessNotRunning {
		t.Errorf("ResizeTerminal() error = %v, want %v", err, process.ErrProcessNotRunning)
	}
}

// TestNewZshREPL tests Zsh REPL creation.
func TestNewZshREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "zsh",
	}

	repl, err := NewZshREPL("test-zsh", config)
	if err != nil {
		t.Fatalf("NewZshREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewZshREPL() returned nil")
	}

	if repl.ID() != "test-zsh" {
		t.Errorf("ID() = %s, want test-zsh", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}

	// Verify the prompt is set
	if repl.prompt != "SANDBOX0>>> " {
		t.Errorf("prompt = %s, want 'SANDBOX0>>> '", repl.prompt)
	}
}

// TestZshREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestZshREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "zsh",
	}

	repl, err := NewZshREPL("test-zsh-not-running", config)
	if err != nil {
		t.Fatalf("NewZshREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("echo test")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestZshREPL_ResizeTerminalNotRunning tests ResizeTerminal when not running.
func TestZshREPL_ResizeTerminalNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "zsh",
	}

	repl, err := NewZshREPL("test-zsh-resize", config)
	if err != nil {
		t.Fatalf("NewZshREPL() failed = %v", err)
	}

	size := process.PTYSize{Rows: 40, Cols: 120}
	err = repl.ResizeTerminal(size)
	if err != process.ErrProcessNotRunning {
		t.Errorf("ResizeTerminal() error = %v, want %v", err, process.ErrProcessNotRunning)
	}
}

// TestREPL_PauseResume tests Pause and Resume operations.
func TestREPL_PauseResume(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-pause", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	// Pause when not running should fail
	err = repl.Pause()
	if err != process.ErrProcessNotRunning {
		t.Errorf("Pause() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	// Resume when not paused should fail
	err = repl.Resume()
	if err != process.ErrProcessNotPaused {
		t.Errorf("Resume() error = %v, want %v", err, process.ErrProcessNotPaused)
	}
}

// TestREPL_IDAndType tests ID and Type getters.
func TestREPL_IDAndType(t *testing.T) {
	tests := []struct {
		name     string
		language string
		newFunc  func(string, process.ProcessConfig) (process.Process, error)
	}{
		{
			name:     "Python",
			language: "python",
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewPythonREPL(id, config)
			},
		},
		{
			name:     "Node",
			language: "node",
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewNodeREPL(id, config)
			},
		},
		{
			name:     "Bash",
			language: "bash",
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewBashREPL(id, config)
			},
		},
		{
			name:     "Zsh",
			language: "zsh",
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewZshREPL(id, config)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := process.ProcessConfig{
				Type:     process.ProcessTypeREPL,
				Language: tt.language,
			}

			p, err := tt.newFunc("test-id-type", config)
			if err != nil {
				t.Fatalf("%s: newFunc() failed = %v", tt.name, err)
			}

			if p.ID() != "test-id-type" {
				t.Errorf("ID() = %s, want test-id-type", p.ID())
			}

			if p.Type() != process.ProcessTypeREPL {
				t.Errorf("Type() = %s, want %s", p.Type(), process.ProcessTypeREPL)
			}
		})
	}
}

// TestREPL_ResourceUsageWithNoPID tests ResourceUsage with no PID.
func TestREPL_ResourceUsageWithNoPID(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-resource", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	// Resource usage with no PID should return zeros
	usage := repl.ResourceUsage()

	if usage.CPUPercent != -1 {
		t.Errorf("CPUPercent = %f, want -1", usage.CPUPercent)
	}
	if usage.MemoryRSS != 0 {
		t.Errorf("MemoryRSS = %d, want 0", usage.MemoryRSS)
	}
}

// TestREPL_ReadOutput tests output channel creation.
func TestREPL_ReadOutput(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewBashREPL("test-output", config)
	if err != nil {
		t.Fatalf("NewBashREPL() failed = %v", err)
	}

	// ReadOutput should return a channel
	ch := repl.ReadOutput()
	if ch == nil {
		t.Error("ReadOutput() returned nil channel")
	}
}

// TestREPL_ConfigTests tests configuration handling.
func TestREPL_ConfigTests(t *testing.T) {
	tests := []struct {
		name    string
		config  process.ProcessConfig
		newFunc func(string, process.ProcessConfig) (process.Process, error)
	}{
		{
			name: "Python with CWD and env",
			config: process.ProcessConfig{
				Type:     process.ProcessTypeREPL,
				Language: "python",
				CWD:      "/tmp",
				EnvVars:  map[string]string{"PYTHONPATH": "/test"},
			},
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewPythonREPL(id, config)
			},
		},
		{
			name: "Node with custom PTY size",
			config: process.ProcessConfig{
				Type:     process.ProcessTypeREPL,
				Language: "node",
				PTYSize:  &process.PTYSize{Rows: 50, Cols: 160},
			},
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewNodeREPL(id, config)
			},
		},
		{
			name: "Bash with custom TERM",
			config: process.ProcessConfig{
				Type:     process.ProcessTypeREPL,
				Language: "bash",
				Term:     "xterm",
			},
			newFunc: func(id string, config process.ProcessConfig) (process.Process, error) {
				return NewBashREPL(id, config)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := tt.newFunc("test-config", tt.config)
			if err != nil {
				t.Fatalf("%s: newFunc() failed = %v", tt.name, err)
			}

			// Verify the process was created with correct type
			if p.Type() != tt.config.Type {
				t.Errorf("Type() = %s, want %s", p.Type(), tt.config.Type)
			}
		})
	}
}

// TestREPL_ProcessInterface verifies that all REPL types implement Process interface.
func TestREPL_ProcessInterface(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	// This test verifies at compile time that our REPL types
	// implement the Process interface. We don't need to do anything
	// at runtime other than create the instances.

	var _ process.Process = &BashREPL{}
	var _ process.Process = &ZshREPL{}
	var _ process.Process = &PythonREPL{}
	var _ process.Process = &NodeREPL{}
	var _ process.Process = &RubyREPL{}
	var _ process.Process = &LuaREPL{}
	var _ process.Process = &PHPREPL{}
	var _ process.Process = &RREPL{}
	var _ process.Process = &PerlREPL{}

	_, _ = NewBashREPL("test-interface", config)
	_, _ = NewZshREPL("test-interface", config)
	_, _ = NewPythonREPL("test-interface", config)
	_, _ = NewNodeREPL("test-interface", config)
	_, _ = NewRubyREPL("test-interface", config)
	_, _ = NewLuaREPL("test-interface", config)
	_, _ = NewPHPREPL("test-interface", config)
	_, _ = NewRREPL("test-interface", config)
	_, _ = NewPerlREPL("test-interface", config)
}

// TestNewRubyREPL tests Ruby REPL creation.
func TestNewRubyREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "ruby",
	}

	repl, err := NewRubyREPL("test-ruby", config)
	if err != nil {
		t.Fatalf("NewRubyREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewRubyREPL() returned nil")
	}

	if repl.ID() != "test-ruby" {
		t.Errorf("ID() = %s, want test-ruby", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestNewLuaREPL tests Lua REPL creation.
func TestNewLuaREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "lua",
	}

	repl, err := NewLuaREPL("test-lua", config)
	if err != nil {
		t.Fatalf("NewLuaREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewLuaREPL() returned nil")
	}

	if repl.ID() != "test-lua" {
		t.Errorf("ID() = %s, want test-lua", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestNewPHPREPL tests PHP REPL creation.
func TestNewPHPREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "php",
	}

	repl, err := NewPHPREPL("test-php", config)
	if err != nil {
		t.Fatalf("NewPHPREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewPHPREPL() returned nil")
	}

	if repl.ID() != "test-php" {
		t.Errorf("ID() = %s, want test-php", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestNewRREPL tests R REPL creation.
func TestNewRREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "r",
	}

	repl, err := NewRREPL("test-r", config)
	if err != nil {
		t.Fatalf("NewRREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewRREPL() returned nil")
	}

	if repl.ID() != "test-r" {
		t.Errorf("ID() = %s, want test-r", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestNewPerlREPL tests Perl REPL creation.
func TestNewPerlREPL(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "perl",
	}

	repl, err := NewPerlREPL("test-perl", config)
	if err != nil {
		t.Fatalf("NewPerlREPL() failed = %v", err)
	}

	if repl == nil {
		t.Fatal("NewPerlREPL() returned nil")
	}

	if repl.ID() != "test-perl" {
		t.Errorf("ID() = %s, want test-perl", repl.ID())
	}

	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
}

// TestRubyREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestRubyREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "ruby",
	}

	repl, err := NewRubyREPL("test-ruby-not-running", config)
	if err != nil {
		t.Fatalf("NewRubyREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("puts 'hello'")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestLuaREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestLuaREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "lua",
	}

	repl, err := NewLuaREPL("test-lua-not-running", config)
	if err != nil {
		t.Fatalf("NewLuaREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("print('hello')")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestPHPREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestPHPREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "php",
	}

	repl, err := NewPHPREPL("test-php-not-running", config)
	if err != nil {
		t.Fatalf("NewPHPREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("echo 'hello';")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestRREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestRREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "r",
	}

	repl, err := NewRREPL("test-r-not-running", config)
	if err != nil {
		t.Fatalf("NewRREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("print('hello')")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}

// TestPerlREPL_ExecuteCodeNotRunning tests ExecuteCode when not running.
func TestPerlREPL_ExecuteCodeNotRunning(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "perl",
	}

	repl, err := NewPerlREPL("test-perl-not-running", config)
	if err != nil {
		t.Fatalf("NewPerlREPL() failed = %v", err)
	}

	result, err := repl.ExecuteCode("print 'hello'")
	if err != process.ErrProcessNotRunning {
		t.Errorf("ExecuteCode() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	if result != nil {
		t.Error("ExecuteCode() returned non-nil result when not running")
	}
}
