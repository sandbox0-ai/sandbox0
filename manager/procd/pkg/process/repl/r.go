package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// RREPL implements an R language REPL.
type RREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewRREPL creates a new R REPL process.
func NewRREPL(id string, config process.ProcessConfig) (*RREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &RREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the R REPL process.
func (r *RREPL) Start() error {
	if r.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := r.GetConfig()

	// Try R interpreters
	var cmd *exec.Cmd
	rCandidates := []struct {
		name string
		args []string
	}{
		{"R", []string{"--interactive", "--quiet", "--no-save", "--no-restore"}},
		{"Rscript", []string{"--vanilla"}},
	}

	for _, candidate := range rCandidates {
		if path, err := exec.LookPath(candidate.name); err == nil {
			cmd = exec.Command(path, candidate.args...)
			break
		}
	}

	if cmd == nil {
		r.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: no R interpreter found (tried: R, Rscript)", process.ErrProcessStartFailed)
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
	cmd.Env = env

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return r.runner.Start(cmd, config.PTYSize)
}

// Stop stops the R REPL process.
func (r *RREPL) Stop() error {
	return r.runner.Stop()
}

// Restart restarts the process.
func (r *RREPL) Restart() error {
	if err := r.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return r.Start()
}

// ExecuteCode executes R code in the REPL.
func (r *RREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !r.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := r.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

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
func (r *RREPL) ResizeTerminal(size process.PTYSize) error {
	if !r.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return r.BaseProcess.ResizePTY(size)
}
