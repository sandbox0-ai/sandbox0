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

// LuaREPL implements a Lua REPL.
type LuaREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewLuaREPL creates a new Lua REPL process.
func NewLuaREPL(id string, config process.ProcessConfig) (*LuaREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &LuaREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the Lua REPL process.
func (l *LuaREPL) Start() error {
	if l.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	l.SetState(process.ProcessStateStarting)

	config := l.GetConfig()

	// Try Lua interpreters in order of preference
	var cmd *exec.Cmd
	luaCandidates := []string{"lua", "lua5.4", "lua5.3", "lua5.2", "lua5.1", "luajit"}

	for _, candidate := range luaCandidates {
		if path, err := exec.LookPath(candidate); err == nil {
			cmd = exec.Command(path, "-i")
			break
		}
	}

	if cmd == nil {
		l.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: no Lua interpreter found (tried: lua, lua5.4, lua5.3, lua5.2, lua5.1, luajit)", process.ErrProcessStartFailed)
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
		l.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	l.cmd = cmd
	l.SetPTY(ptmx)
	l.SetPID(cmd.Process.Pid)
	l.SetState(process.ProcessStateRunning)

	// Start output reader
	go l.readOutput(ptmx)

	// Start process monitor
	go l.monitorProcess()

	return nil
}

// Stop stops the Lua REPL process.
func (l *LuaREPL) Stop() error {
	if !l.IsRunning() {
		return nil
	}

	if l.cmd != nil && l.cmd.Process != nil {
		if err := l.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			l.cmd.Process.Kill()
		}
	}

	if ptyFile := l.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	l.SetState(process.ProcessStateStopped)
	l.CloseOutput()

	return nil
}

// Restart restarts the process.
func (l *LuaREPL) Restart() error {
	if err := l.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return l.Start()
}

// ExecuteCode executes Lua code in the REPL.
func (l *LuaREPL) ExecuteCode(code string) (*process.ExecutionResult, error) {
	if !l.IsRunning() {
		return nil, process.ErrProcessNotRunning
	}

	ptyFile := l.GetPTY()
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
func (l *LuaREPL) ResizeTerminal(size process.PTYSize) error {
	ptyFile := l.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (l *LuaREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		nr, err := ptmx.Read(buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])

			l.PublishOutput(process.ProcessOutput{
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

func (l *LuaREPL) monitorProcess() {
	if l.cmd == nil {
		return
	}

	err := l.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	l.SetExitCode(exitCode)

	if exitCode == 0 {
		l.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		l.SetState(process.ProcessStateKilled)
	} else {
		l.SetState(process.ProcessStateCrashed)
	}

	l.CloseOutput()
}
