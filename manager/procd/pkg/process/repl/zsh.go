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

// ZshREPL implements a Zsh shell REPL.
type ZshREPL struct {
	*process.BaseProcess
	cmd    *exec.Cmd
	prompt string
}

// NewZshREPL creates a new Zsh REPL process.
func NewZshREPL(id string, config process.ProcessConfig) (*ZshREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &ZshREPL{
		BaseProcess: bp,
		prompt:      "SANDBOX0>>> ",
	}, nil
}

// Start starts the Zsh REPL process.
func (z *ZshREPL) Start() error {
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

	// Create a new process group so we can send signals to all child processes
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

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

// Stop stops the Zsh REPL process.
func (z *ZshREPL) Stop() error {
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
func (z *ZshREPL) Restart() error {
	if err := z.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return z.Start()
}

// ExecuteCode executes a command in the Zsh REPL.
func (z *ZshREPL) ExecuteCode(cmd string) (*process.ExecutionResult, error) {
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
func (z *ZshREPL) ResizeTerminal(size process.PTYSize) error {
	ptyFile := z.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (z *ZshREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		n, err := ptmx.Read(buf)
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])

			z.PublishOutput(process.ProcessOutput{
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

func (z *ZshREPL) monitorProcess() {
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
