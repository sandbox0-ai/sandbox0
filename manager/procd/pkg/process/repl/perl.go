package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// PerlREPL implements a Perl REPL.
type PerlREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewPerlREPL creates a new Perl REPL process.
func NewPerlREPL(id string, config process.ProcessConfig) (*PerlREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &PerlREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the Perl REPL process.
func (p *PerlREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	config := p.GetConfig()

	// Try Perl interpreters in order of preference
	var cmd *exec.Cmd
	perlCandidates := []struct {
		name string
		args []string
	}{
		{"re.pl", []string{}},          // Perl REPL if installed via cpanm Devel::REPL
		{"perl", []string{"-de", "0"}}, // Perl debugger as REPL
	}

	for _, candidate := range perlCandidates {
		if path, err := exec.LookPath(candidate.name); err == nil {
			cmd = exec.Command(path, candidate.args...)
			break
		}
	}

	if cmd == nil {
		p.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: no Perl interpreter found (tried: re.pl, perl)", process.ErrProcessStartFailed)
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

	return p.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Perl REPL process.
func (p *PerlREPL) Stop() error {
	return p.runner.Stop()
}

// Restart restarts the process.
func (p *PerlREPL) Restart() error {
	if err := p.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return p.Start()
}

// ExecuteCode executes Perl code in the REPL.
func (p *PerlREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !p.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := p.GetPTY()
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
func (p *PerlREPL) ResizeTerminal(size process.PTYSize) error {
	if !p.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return p.BaseProcess.ResizePTY(size)
}
