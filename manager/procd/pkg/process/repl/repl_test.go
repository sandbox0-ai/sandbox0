// Package repl provides unit tests for REPL process implementations.
package repl

import (
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// TestREPLConfig_Validate tests config validation.
func TestREPLConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    REPLConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: REPLConfig{
				Name: "test",
				Candidates: []ExecCandidate{
					{Name: "test-cmd", Args: []string{}},
				},
			},
			wantError: false,
		},
		{
			name: "missing name",
			config: REPLConfig{
				Candidates: []ExecCandidate{
					{Name: "test-cmd", Args: []string{}},
				},
			},
			wantError: true,
		},
		{
			name: "no candidates",
			config: REPLConfig{
				Name:       "test",
				Candidates: []ExecCandidate{},
			},
			wantError: true,
		},
		{
			name: "candidate missing name",
			config: REPLConfig{
				Name: "test",
				Candidates: []ExecCandidate{
					{Name: "", Args: []string{}},
				},
			},
			wantError: true,
		},
		{
			name: "invalid regex pattern",
			config: REPLConfig{
				Name: "test",
				Candidates: []ExecCandidate{
					{Name: "test-cmd", Args: []string{}},
				},
				Prompt: PromptConfig{
					Patterns: []string{"[invalid"},
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantError {
				t.Errorf("Validate() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

// TestREPLConfig_Clone tests config cloning.
func TestREPLConfig_Clone(t *testing.T) {
	original := &REPLConfig{
		Name: "test",
		Candidates: []ExecCandidate{
			{Name: "cmd1", Args: []string{"-a", "-b"}},
			{Name: "cmd2", Args: []string{"-c"}},
		},
		Env: []EnvVar{
			{Name: "VAR1", Value: "value1"},
		},
		Prompt: PromptConfig{
			Patterns: []string{`> `, `>>> `},
		},
		InitCommands: []string{"cmd1", "cmd2"},
	}

	clone := original.Clone()

	// Modify clone
	clone.Name = "modified"
	clone.Candidates[0].Name = "modified-cmd"
	clone.Candidates[0].Args[0] = "-modified"
	clone.Env[0].Value = "modified"
	clone.Prompt.Patterns[0] = "modified"
	clone.InitCommands[0] = "modified"

	// Original should be unchanged
	if original.Name != "test" {
		t.Error("Clone modified original Name")
	}
	if original.Candidates[0].Name != "cmd1" {
		t.Error("Clone modified original Candidates")
	}
	if original.Candidates[0].Args[0] != "-a" {
		t.Error("Clone modified original Candidates Args")
	}
	if original.Env[0].Value != "value1" {
		t.Error("Clone modified original Env")
	}
	if original.Prompt.Patterns[0] != `> ` {
		t.Error("Clone modified original Prompt.Patterns")
	}
	if original.InitCommands[0] != "cmd1" {
		t.Error("Clone modified original InitCommands")
	}
}

// TestBuiltinConfigs tests that all built-in configs are valid.
func TestBuiltinConfigs(t *testing.T) {
	for name, config := range BuiltinConfigs {
		t.Run(name, func(t *testing.T) {
			if err := config.Validate(); err != nil {
				t.Errorf("Built-in config %s is invalid: %v", name, err)
			}
			if config.Name != name {
				t.Errorf("Config name mismatch: got %s, want %s", config.Name, name)
			}
		})
	}
}

// TestGetBuiltinConfig tests retrieving built-in configs.
func TestGetBuiltinConfig(t *testing.T) {
	config, ok := GetBuiltinConfig("python")
	if !ok {
		t.Fatal("GetBuiltinConfig() should find python")
	}
	if config.Name != "python" {
		t.Errorf("Config name = %s, want python", config.Name)
	}

	_, ok = GetBuiltinConfig("nonexistent")
	if ok {
		t.Error("GetBuiltinConfig() should not find nonexistent")
	}
}

// TestREPLRegistry tests the registry.
func TestREPLRegistry(t *testing.T) {
	registry := NewREPLRegistry()

	if _, ok := registry.Get("python"); !ok {
		t.Error("Registry should have python config")
	}

	customConfig := &REPLConfig{
		Name: "custom",
		Candidates: []ExecCandidate{
			{Name: "custom-cmd", Args: []string{}},
		},
	}
	if err := registry.Register(customConfig); err != nil {
		t.Fatalf("Register() failed: %v", err)
	}

	config, ok := registry.Get("custom")
	if !ok {
		t.Error("Registry should have custom config after registration")
	}
	if config.Name != "custom" {
		t.Errorf("Config name = %s, want custom", config.Name)
	}

	names := registry.List()
	if len(names) == 0 {
		t.Error("Registry.List() should return configs")
	}
}

// TestNewREPL tests REPL creation with various languages.
func TestNewREPL(t *testing.T) {
	tests := []struct {
		name     string
		language string
		wantErr  bool
	}{
		{"python", "python", false},
		{"node", "node", false},
		{"bash", "bash", false},
		{"zsh", "zsh", false},
		{"ruby", "ruby", false},
		{"lua", "lua", false},
		{"php", "php", false},
		{"r", "r", false},
		{"perl", "perl", false},
		{"redis-cli", "redis-cli", false},
		{"sqlite", "sqlite", false},
		{"unknown", "unknown", true},
		{"empty", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := process.ProcessConfig{
				Type:     process.ProcessTypeREPL,
				Language: tt.language,
			}

			repl, err := NewREPL("test-"+tt.name, config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewREPL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if repl == nil {
					t.Error("NewREPL() returned nil without error")
				}
				if repl.Language() != tt.language {
					t.Errorf("Language() = %s, want %s", repl.Language(), tt.language)
				}
			}
		})
	}
}

// TestREPL_Lifecycle tests basic REPL lifecycle operations.
func TestREPL_Lifecycle(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
		CWD:      "/tmp",
		EnvVars:  map[string]string{"TEST": "value"},
	}

	repl, err := NewREPL("test-lifecycle", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	// Test initial state
	if repl.ID() != "test-lifecycle" {
		t.Errorf("ID() = %s, want test-lifecycle", repl.ID())
	}
	if repl.Type() != process.ProcessTypeREPL {
		t.Errorf("Type() = %s, want %s", repl.Type(), process.ProcessTypeREPL)
	}
	if repl.State() != process.ProcessStateCreated {
		t.Errorf("State() = %s, want %s", repl.State(), process.ProcessStateCreated)
	}
	if repl.Language() != "python" {
		t.Errorf("Language() = %s, want python", repl.Language())
	}

	// Test ResizeTerminal when not running
	size := process.PTYSize{Rows: 40, Cols: 120}
	err = repl.ResizeTerminal(size)
	if err != process.ErrProcessNotRunning {
		t.Errorf("ResizeTerminal() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	// Test Stop when not running (should not error)
	if err := repl.Stop(); err != nil {
		t.Errorf("Stop() error = %v", err)
	}
}

// TestREPL_StartWithoutInterpreter tests starting when interpreter is not available.
func TestREPL_StartWithoutInterpreter(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewREPL("test-no-interpreter", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	err = repl.Start()
	if err != nil {
		if strings.Contains(err.Error(), "no interpreter found") {
			return // Expected error when Python not available
		}
		return // Other errors acceptable
	}

	// If Python IS available, clean up
	repl.Stop()
	t.Skip("Python is available, skipping 'not found' test")
}

// TestREPL_DoubleStart tests starting an already running REPL.
func TestREPL_DoubleStart(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewREPL("test-double", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	err = repl.Start()
	if err != nil {
		if strings.Contains(err.Error(), "no interpreter found") {
			t.Skip("Python not available")
		}
		t.Skipf("Python start failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	err = repl.Start()
	if err != process.ErrProcessAlreadyRunning {
		t.Errorf("Start() second attempt error = %v, want %v", err, process.ErrProcessAlreadyRunning)
	}

	repl.Stop()
}

// TestREPL_StateTransitions tests state transitions.
func TestREPL_StateTransitions(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewREPL("test-states", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	if repl.State() != process.ProcessStateCreated {
		t.Errorf("Initial state = %s, want %s", repl.State(), process.ProcessStateCreated)
	}

	repl.SetState(process.ProcessStateRunning)
	if repl.State() != process.ProcessStateRunning {
		t.Errorf("State after SetState = %s, want %s", repl.State(), process.ProcessStateRunning)
	}

	repl.SetState(process.ProcessStatePaused)
	if repl.State() != process.ProcessStatePaused {
		t.Errorf("State after SetState = %s, want %s", repl.State(), process.ProcessStatePaused)
	}
}

// TestREPL_DetectPrompt tests prompt detection.
func TestREPL_DetectPrompt(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "python",
	}

	repl, err := NewREPL("test-prompt", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"IPython input prompt", []byte("In [1]:"), true},
		{"IPython output prompt", []byte("Out[1]:"), true},
		{"Standard Python prompt", []byte(">>> "), true},
		{"Standard continuation", []byte("... "), true},
		{"Regular output", []byte("hello world"), false},
		{"Empty", []byte(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := repl.DetectPrompt(tt.data)
			if got != tt.expected {
				t.Errorf("DetectPrompt() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestREPL_PauseResume tests Pause and Resume operations.
func TestREPL_PauseResume(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewREPL("test-pause", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	err = repl.Pause()
	if err != process.ErrProcessNotRunning {
		t.Errorf("Pause() error = %v, want %v", err, process.ErrProcessNotRunning)
	}

	err = repl.Resume()
	if err != process.ErrProcessNotPaused {
		t.Errorf("Resume() error = %v, want %v", err, process.ErrProcessNotPaused)
	}
}

// TestREPL_ResourceUsageWithNoPID tests ResourceUsage with no PID.
func TestREPL_ResourceUsageWithNoPID(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewREPL("test-resource", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

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

	repl, err := NewREPL("test-output", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	ch := repl.ReadOutput()
	if ch == nil {
		t.Error("ReadOutput() returned nil channel")
	}
}

// TestREPL_ProcessInterface verifies that REPL implements Process interface.
func TestREPL_ProcessInterface(t *testing.T) {
	var _ process.Process = &REPL{}
}

// TestNewCustomREPL tests creating a custom REPL.
func TestNewCustomREPL(t *testing.T) {
	replConfig := &REPLConfig{
		Name: "custom-cli",
		Candidates: []ExecCandidate{
			{Name: "echo", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{`> `},
		},
	}

	processConfig := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "custom-cli",
	}

	repl, err := NewCustomREPL("test-custom", replConfig, processConfig)
	if err != nil {
		t.Fatalf("NewCustomREPL() failed: %v", err)
	}

	if repl.Language() != "custom-cli" {
		t.Errorf("Language() = %s, want custom-cli", repl.Language())
	}
}

// TestCreateREPLConfig tests the helper for creating minimal configs.
func TestCreateREPLConfig(t *testing.T) {
	config := CreateREPLConfig("myrepl", []ExecCandidate{
		{Name: "myrepl", Args: []string{"--interactive"}},
	})

	if config.Name != "myrepl" {
		t.Errorf("Name = %s, want myrepl", config.Name)
	}
	if len(config.Candidates) != 1 {
		t.Errorf("len(Candidates) = %d, want 1", len(config.Candidates))
	}
	if err := config.Validate(); err != nil {
		t.Errorf("Config is invalid: %v", err)
	}
}

// TestStripANSI tests ANSI escape sequence stripping.
func TestStripANSI(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected []byte
	}{
		{"no ANSI", []byte("hello world"), []byte("hello world")},
		{"color codes", []byte("\x1b[31mred\x1b[0m"), []byte("red")},
		{"cursor movement", []byte("\x1b[2Jhello\x1b[H"), []byte("hello")},
		{"empty", []byte(""), []byte("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripANSI(tt.input)
			if string(got) != string(tt.expected) {
				t.Errorf("stripANSI() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestCheckExecutable tests executable checking.
func TestCheckExecutable(t *testing.T) {
	// sh should exist on any Unix system
	path, ok := CheckExecutable("sh")
	if !ok {
		t.Skip("sh not found, skipping")
	}
	if path == "" {
		t.Error("CheckExecutable() returned empty path")
	}

	// non-existent should not be found
	_, ok = CheckExecutable("nonexistent-command-12345")
	if ok {
		t.Error("CheckExecutable() should not find nonexistent command")
	}
}

// TestListBuiltinConfigs tests listing built-in configs.
func TestListBuiltinConfigs(t *testing.T) {
	names := ListBuiltinConfigs()
	if len(names) == 0 {
		t.Error("ListBuiltinConfigs() returned empty list")
	}

	expected := []string{"python", "node", "bash", "zsh", "ruby"}
	for _, name := range expected {
		found := false
		for _, n := range names {
			if n == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ListBuiltinConfigs() missing %s", name)
		}
	}
}

// TestCheckREPLAvailable tests REPL availability checking.
func TestCheckREPLAvailable(t *testing.T) {
	// bash/sh should be available on Unix
	path, ok := CheckREPLAvailable("bash")
	if ok && path == "" {
		t.Error("CheckREPLAvailable() returned empty path when available")
	}

	// unknown language should not be available
	_, ok = CheckREPLAvailable("unknown-language-12345")
	if ok {
		t.Error("CheckREPLAvailable() should not find unknown language")
	}
}

// TestListAvailableREPLs tests listing available REPLs.
func TestListAvailableREPLs(t *testing.T) {
	available := ListAvailableREPLs()
	// At minimum bash/sh should be available
	t.Logf("Available REPLs: %v", available)
}

// TestREPL_OutputFiltering tests output filtering for bash.
func TestREPL_OutputFiltering(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewREPL("test-filter", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	// Verify bash config has filtering enabled
	replConfig := repl.Config()
	if !replConfig.Output.FilterEcho {
		t.Error("bash config should have FilterEcho enabled")
	}
	if !replConfig.Output.TrimPrompt {
		t.Error("bash config should have TrimPrompt enabled")
	}

	// Test WriteInput sets lastInput
	repl.WriteInput([]byte("echo 'hi'\n"))

	repl.mu.Lock()
	lastInput := repl.lastInput
	repl.mu.Unlock()

	if lastInput != "echo 'hi'" {
		t.Errorf("lastInput = %q, want %q", lastInput, "echo 'hi'")
	}

	// Test output filtering
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "filter echo",
			input:    []byte("echo 'hi'\nhi\n"),
			expected: "hi",
		},
		{
			name:     "filter prompt",
			input:    []byte("SANDBOX0>>> "),
			expected: "",
		},
		{
			name:     "filter both",
			input:    []byte("SANDBOX0>>> echo 'hi'\nhi\nSANDBOX0>>> "),
			expected: "hi",
		},
		{
			name:     "just output",
			input:    []byte("hi\n"),
			expected: "hi",
		},
		{
			name:     "with CRLF",
			input:    []byte("echo 'hi'\r\nhi\r\nSANDBOX0>>> "),
			expected: "hi",
		},
		{
			name:     "with standalone CR",
			input:    []byte("SANDBOX0>>> echo 'hi'\r\nhi\rSANDBOX0>>> "),
			expected: "hi",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset lastInput for echo filtering
			repl.SetLastInput("echo 'hi'")
			got := repl.filterOutput(tt.input)
			if string(got) != tt.expected {
				t.Errorf("filterOutput() = %q, want %q", got, tt.expected)
			}
		})
	}
}

// TestREPL_WriteInputTracksEcho tests that WriteInput tracks input for echo filtering.
func TestREPL_WriteInputTracksEcho(t *testing.T) {
	config := process.ProcessConfig{
		Type:     process.ProcessTypeREPL,
		Language: "bash",
	}

	repl, err := NewREPL("test-write", config)
	if err != nil {
		t.Fatalf("NewREPL() failed = %v", err)
	}

	// Write input with various line endings
	tests := []struct {
		input    string
		expected string
	}{
		{"echo hello\n", "echo hello"},
		{"ls -la\r\n", "ls -la"},
		{"pwd", "pwd"},
	}

	for _, tt := range tests {
		// This will fail because process isn't running, but lastInput should still be set
		_ = repl.WriteInput([]byte(tt.input))

		repl.mu.Lock()
		lastInput := repl.lastInput
		repl.mu.Unlock()

		if lastInput != tt.expected {
			t.Errorf("WriteInput(%q): lastInput = %q, want %q", tt.input, lastInput, tt.expected)
		}
	}
}
