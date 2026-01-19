// Package repl provides REPL process implementations.
package repl

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// PythonREPL implements a Python REPL using IPython.
type PythonREPL struct {
	*process.BaseProcess
	cmd       *exec.Cmd
	promptMu  sync.Mutex
	lastInput string
}

// NewPythonREPL creates a new Python REPL process.
func NewPythonREPL(id string, config process.ProcessConfig) (*PythonREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &PythonREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the Python REPL process.
func (p *PythonREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	p.SetState(process.ProcessStateStarting)

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

	// Get PTY size
	ptySize := config.PTYSize
	if ptySize == nil {
		ptySize = &process.PTYSize{Rows: 24, Cols: 80}
	}

	// Start with PTY
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		p.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	p.cmd = cmd
	p.SetPTY(ptmx)
	p.SetPID(cmd.Process.Pid)
	p.SetState(process.ProcessStateRunning)

	// Start output reader
	go p.readOutput(ptmx)

	// Start process monitor
	go p.monitorProcess()

	return nil
}

// Stop stops the Python REPL process.
func (p *PythonREPL) Stop() error {
	if !p.IsRunning() {
		return nil
	}

	if p.cmd != nil && p.cmd.Process != nil {
		// Send SIGTERM first
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			// If SIGTERM fails, use SIGKILL
			p.cmd.Process.Kill()
		}
	}

	// Close PTY
	if ptyFile := p.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	p.SetState(process.ProcessStateStopped)
	p.CloseOutput()

	return nil
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
	ptyFile := p.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (p *PythonREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			// Filter out echo of input if needed
			data = p.filterOutput(data)

			if len(data) > 0 {
				p.PublishOutput(process.ProcessOutput{
					Source: process.OutputSourcePTY,
					Data:   data,
				})
			}
		}
		if err != nil {
			if err != io.EOF {
				// Log error if needed
			}
			break
		}
	}
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

func (p *PythonREPL) monitorProcess() {
	if p.cmd == nil {
		return
	}

	err := p.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	p.SetExitCode(exitCode)

	if exitCode == 0 {
		p.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		p.SetState(process.ProcessStateKilled)
	} else {
		p.SetState(process.ProcessStateCrashed)
	}

	p.CloseOutput()
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
