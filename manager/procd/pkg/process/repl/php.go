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

// PHPREPL implements a PHP REPL.
type PHPREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewPHPREPL creates a new PHP REPL process.
func NewPHPREPL(id string, config process.ProcessConfig) (*PHPREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &PHPREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the PHP REPL process.
func (p *PHPREPL) Start() error {
	if p.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	p.SetState(process.ProcessStateStarting)

	config := p.GetConfig()

	// Check for PHP
	phpPath, err := exec.LookPath("php")
	if err != nil {
		p.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: php not found", process.ErrProcessStartFailed)
	}

	// Use PHP interactive mode
	cmd := exec.Command(phpPath, "-a")

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

// Stop stops the PHP REPL process.
func (p *PHPREPL) Stop() error {
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
func (p *PHPREPL) Restart() error {
	if err := p.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return p.Start()
}

// ExecuteCode executes PHP code in the REPL.
func (p *PHPREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
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
func (p *PHPREPL) ResizeTerminal(size process.PTYSize) error {
	ptyFile := p.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (p *PHPREPL) readOutput(ptmx *os.File) {
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

func (p *PHPREPL) monitorProcess() {
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
