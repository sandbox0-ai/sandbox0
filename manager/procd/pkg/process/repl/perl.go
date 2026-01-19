package repl

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// PerlREPL implements a Perl REPL.
type PerlREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewPerlREPL creates a new Perl REPL process.
func NewPerlREPL(id string, config process.ProcessConfig) (*PerlREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &PerlREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the Perl REPL process.
func (p *PerlREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	p.SetState(process.ProcessStateStarting)

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

// Stop stops the Perl REPL process.
func (p *PerlREPL) Stop() error {
	if !p.IsRunning() {
		return nil
	}

	if p.cmd != nil && p.cmd.Process != nil {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			p.cmd.Process.Kill()
		}
	}

	if ptyFile := p.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	p.SetState(process.ProcessStateStopped)
	p.CloseOutput()

	return nil
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
	ptyFile := p.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (p *PerlREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		nr, err := ptmx.Read(buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])

			p.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourcePTY,
				Data:   data,
			})
		}
		if err != nil {
			if err != io.EOF {
				// Log error if needed
			}
			break
		}
	}
}

func (p *PerlREPL) monitorProcess() {
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
