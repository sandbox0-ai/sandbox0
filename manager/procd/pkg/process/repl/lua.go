package repl

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// LuaREPL implements a Lua REPL.
type LuaREPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
}

// NewLuaREPL creates a new Lua REPL process.
func NewLuaREPL(id string, config process.ProcessConfig) (*LuaREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &LuaREPL{
		BaseProcess: bp,
		runner:      process.NewPTYRunner(bp, nil, nil),
	}, nil
}

// Start starts the Lua REPL process.
func (l *LuaREPL) Start() error {
	if l.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

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

	return l.runner.Start(cmd, config.PTYSize)
}

// Stop stops the Lua REPL process.
func (l *LuaREPL) Stop() error {
	return l.runner.Stop()
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
	if !l.IsRunning() {
		return process.ErrProcessNotRunning
	}

	return l.BaseProcess.ResizePTY(size)
}
