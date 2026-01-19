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

// BashREPL implements a Bash shell REPL.
type BashREPL struct {
	*process.BaseProcess
	cmd    *exec.Cmd
	prompt string
}

// NewBashREPL creates a new Bash REPL process.
func NewBashREPL(id string, config process.ProcessConfig) (*BashREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &BashREPL{
		BaseProcess: bp,
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Bash REPL process.
func (b *BashREPL) Start() error {
	if b.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	b.SetState(process.ProcessStateStarting)

	config := b.GetConfig()

	// Start interactive bash
	cmd := exec.Command("bash", "--norc", "--noprofile", "-i")

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
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

	// Set TERM
	term := config.Term
	if term == "" {
		term = "xterm-256color"
	}
	env = append(env, fmt.Sprintf("TERM=%s", term))

	// Set custom prompt
	env = append(env, fmt.Sprintf("PS1=%s", b.prompt))

	cmd.Env = env

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
		b.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	b.cmd = cmd
	b.SetPTY(ptmx)
	b.SetPID(cmd.Process.Pid)
	b.SetState(process.ProcessStateRunning)

	// Start output reader
	go b.readOutput(ptmx)

	// Start process monitor
	go b.monitorProcess()

	return nil
}

// Stop stops the Bash REPL process.
func (b *BashREPL) Stop() error {
	if !b.IsRunning() {
		return nil
	}

	if b.cmd != nil && b.cmd.Process != nil {
		if err := b.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			b.cmd.Process.Kill()
		}
	}

	if ptyFile := b.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	b.SetState(process.ProcessStateStopped)
	b.CloseOutput()

	return nil
}

// Restart restarts the process.
func (b *BashREPL) Restart() error {
	if err := b.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return b.Start()
}

// ExecuteCode executes a command in the Bash REPL.
func (b *BashREPL) ExecuteCode(cmd string) (*process.ExecutionResult, error) {
	if !b.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := b.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	// Write command to PTY
	_, err := fmt.Fprintln(ptyFile, cmd)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (b *BashREPL) ResizeTerminal(size process.PTYSize) error {
	ptyFile := b.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (b *BashREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			b.PublishOutput(process.ProcessOutput{
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

func (b *BashREPL) monitorProcess() {
	if b.cmd == nil {
		return
	}

	err := b.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	b.SetExitCode(exitCode)

	if exitCode == 0 {
		b.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		b.SetState(process.ProcessStateKilled)
	} else {
		b.SetState(process.ProcessStateCrashed)
	}

	b.CloseOutput()
}
