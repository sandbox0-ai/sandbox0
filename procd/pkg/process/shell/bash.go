// Package shell provides shell process implementations.
package shell

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/sandbox0-ai/infra/procd/pkg/process"
)

// BashShell implements a Bash shell process.
type BashShell struct {
	*process.BaseProcess
	cmd    *exec.Cmd
	prompt string
}

// NewBashShell creates a new Bash shell process.
func NewBashShell(id string, config process.ProcessConfig) (*BashShell, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeShell, config)

	return &BashShell{
		BaseProcess: bp,
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Bash shell process.
func (b *BashShell) Start() error {
	if b.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	b.SetState(process.ProcessStateStarting)

	config := b.GetConfig()

	// Start interactive bash
	cmd := exec.Command("bash", "--norc", "--noprofile", "-i")

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

// Stop stops the Bash shell process.
func (b *BashShell) Stop() error {
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
func (b *BashShell) Restart() error {
	if err := b.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return b.Start()
}

// ExecuteCommand executes a shell command.
func (b *BashShell) ExecuteCommand(cmd string) (*process.ExecutionResult, error) {
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
func (b *BashShell) ResizeTerminal(size process.PTYSize) error {
	ptyFile := b.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (b *BashShell) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			b.PublishOutput(process.ProcessOutput{
				Timestamp: time.Now(),
				Source:    process.OutputSourcePTY,
				Data:      data,
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

func (b *BashShell) monitorProcess() {
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

// ZshShell implements a Zsh shell process.
type ZshShell struct {
	*process.BaseProcess
	cmd    *exec.Cmd
	prompt string
}

// NewZshShell creates a new Zsh shell process.
func NewZshShell(id string, config process.ProcessConfig) (*ZshShell, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeShell, config)

	return &ZshShell{
		BaseProcess: bp,
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Zsh shell process.
func (z *ZshShell) Start() error {
	if z.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	z.SetState(process.ProcessStateStarting)

	config := z.GetConfig()

	// Check if zsh is available
	zshPath, err := exec.LookPath("zsh")
	if err != nil {
		// Fall back to bash
		zshPath = "bash"
	}

	cmd := exec.Command(zshPath, "--no-rcs", "-i")

	if config.CWD != "" {
		cmd.Dir = config.CWD
	}

	env := os.Environ()
	for k, v := range config.EnvVars {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}

	term := config.Term
	if term == "" {
		term = "xterm-256color"
	}
	env = append(env, fmt.Sprintf("TERM=%s", term))
	env = append(env, fmt.Sprintf("PS1=%s", z.prompt))

	cmd.Env = env

	ptySize := config.PTYSize
	if ptySize == nil {
		ptySize = &process.PTYSize{Rows: 24, Cols: 80}
	}

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: ptySize.Rows,
		Cols: ptySize.Cols,
	})
	if err != nil {
		z.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	z.cmd = cmd
	z.SetPTY(ptmx)
	z.SetPID(cmd.Process.Pid)
	z.SetState(process.ProcessStateRunning)

	go z.readOutput(ptmx)
	go z.monitorProcess()

	return nil
}

// Stop stops the Zsh shell process.
func (z *ZshShell) Stop() error {
	if !z.IsRunning() {
		return nil
	}

	if z.cmd != nil && z.cmd.Process != nil {
		if err := z.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			z.cmd.Process.Kill()
		}
	}

	if ptyFile := z.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	z.SetState(process.ProcessStateStopped)
	z.CloseOutput()

	return nil
}

// Restart restarts the process.
func (z *ZshShell) Restart() error {
	if err := z.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return z.Start()
}

// ExecuteCommand executes a shell command.
func (z *ZshShell) ExecuteCommand(cmd string) (*process.ExecutionResult, error) {
	if !z.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := z.GetPTY()
	if ptyFile == nil {
		return nil, process.ErrProcessNotRunning
	}

	_, err := fmt.Fprintln(ptyFile, cmd)
	if err != nil {
		return nil, err
	}

	return &process.ExecutionResult{
		Output: []byte{},
	}, nil
}

// ResizeTerminal resizes the PTY.
func (z *ZshShell) ResizeTerminal(size process.PTYSize) error {
	ptyFile := z.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (z *ZshShell) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			z.PublishOutput(process.ProcessOutput{
				Timestamp: time.Now(),
				Source:    process.OutputSourcePTY,
				Data:      data,
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

func (z *ZshShell) monitorProcess() {
	if z.cmd == nil {
		return
	}

	err := z.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	z.SetExitCode(exitCode)

	if exitCode == 0 {
		z.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		z.SetState(process.ProcessStateKilled)
	} else {
		z.SetState(process.ProcessStateCrashed)
	}

	z.CloseOutput()
}
