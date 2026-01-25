// Package repl provides REPL process implementations.
package repl

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// PythonREPL implements a Python REPL using IPython.
type PythonREPL struct {
	*process.BaseProcess
	runner    *process.PTYRunner
	promptMu  sync.Mutex
	lastInput string
}

// NewPythonREPL creates a new Python REPL process.
func NewPythonREPL(id string, config process.ProcessConfig) (*PythonREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	repl := &PythonREPL{
		BaseProcess: bp,
	}
	repl.runner = process.NewPTYRunner(bp, repl.filterOutput, nil)
	return repl, nil
}

// Start starts the Python REPL process.
func (p *PythonREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := p.GetConfig()

	// Try Python interpreters in order of preference:
	// 1. ipython - best interactive experience
	// 2. python3 - modern Python 3.x
	// 3. python - usually points to default Python
	// 4. python2 - legacy Python 2.x (for compatibility)
	var cmd *exec.Cmd
	pythonCandidates := []struct {
		name string
		args []string
	}{
		{"ipython3", []string{"--simple-prompt", "-i", "--no-banner", "--colors=NoColor"}},
		{"python3", []string{"-i", "-u"}},
		{"python", []string{"-i", "-u"}},
		{"python2", []string{"-i", "-u"}},
	}

	for _, candidate := range pythonCandidates {
		if path, err := exec.LookPath(candidate.name); err == nil {
			cmd = exec.Command(path, candidate.args...)
			break
		}
	}

	if cmd == nil {
		p.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: no Python interpreter found (tried: ipython3, python3, python, python2)", process.ErrProcessStartFailed)
	}

	// Set working directory
	if config.CWD != "" {
		cmd.Dir = config.CWD
	}

	// Set environment variables
	env := os.Environ()
	for k, v := range config.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	// Force unbuffered output
	env = append(env, "PYTHONUNBUFFERED=1")
	cmd.Env = env

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return p.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Python REPL process.
func (p *PythonREPL) Stop() error {
	return p.runner.Stop()
}

// Restart restarts the process.
func (p *PythonREPL) Restart() error {
	if err := p.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return p.Start()
}

// ExecuteCode executes Python code in the REPL.
func (p *PythonREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !p.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := p.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	p.promptMu.Lock()
	defer p.promptMu.Unlock()

	p.lastInput = code

	// Write code to PTY
	_, err := fmt.Fprintln(ptyFile, code)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (p *PythonREPL) ResizeTerminal(size process.PTYSize) error {
	if !p.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return p.BaseProcess.ResizePTY(size)
}

func (p *PythonREPL) filterOutput(data []byte) []byte {
	p.promptMu.Lock()
	lastInput := p.lastInput
	p.promptMu.Unlock()

	// Remove echo of the last input command
	if lastInput != "" && bytes.Contains(data, []byte(lastInput)) {
		data = bytes.Replace(data, []byte(lastInput+"\n"), []byte{}, 1)
		data = bytes.Replace(data, []byte(lastInput+"\r\n"), []byte{}, 1)
	}

	return data
}

// detectPrompt checks if the output contains a Python prompt.
func (p *PythonREPL) detectPrompt(data []byte) bool {
	patterns := []string{
		"In [", // IPython input prompt
		"Out[", // IPython output prompt
		"...:", // Continuation prompt
		">>> ", // Standard Python prompt
		"... ", // Standard continuation
	}

	str := string(data)
	for _, pattern := range patterns {
		if strings.Contains(str, pattern) {
			return true
		}
	}
	return false
}
