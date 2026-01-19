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

// RREPL implements an R language REPL.
type RREPL struct {
	*process.BaseProcess
	cmd *exec.Cmd
}

// NewRREPL creates a new R REPL process.
func NewRREPL(id string, config process.ProcessConfig) (*RREPL, error) {
	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, config)

	return &RREPL{
		BaseProcess: bp,
	}, nil
}

// Start starts the R REPL process.
func (r *RREPL) Start() error {
	if r.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	r.SetState(process.ProcessStateStarting)

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
		r.SetState(process.ProcessStateCrashed)
		return fmt.Errorf("%w: %v", process.ErrProcessStartFailed, err)
	}

	r.cmd = cmd
	r.SetPTY(ptmx)
	r.SetPID(cmd.Process.Pid)
	r.SetState(process.ProcessStateRunning)

	// Start output reader
	go r.readOutput(ptmx)

	// Start process monitor
	go r.monitorProcess()

	return nil
}

// Stop stops the R REPL process.
func (r *RREPL) Stop() error {
	if !r.IsRunning() {
		return nil
	}

	if r.cmd != nil && r.cmd.Process != nil {
		if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			r.cmd.Process.Kill()
		}
	}

	if ptyFile := r.GetPTY(); ptyFile != nil {
		ptyFile.Close()
	}

	r.SetState(process.ProcessStateStopped)
	r.CloseOutput()

	return nil
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
	ptyFile := r.GetPTY()
	if ptyFile == nil {
		return process.ErrProcessNotRunning
	}

	return pty.Setsize(ptyFile, &pty.Winsize{
		Rows: size.Rows,
		Cols: size.Cols,
	})
}

func (r *RREPL) readOutput(ptmx *os.File) {
	buf := make([]byte, 4096)
	for {
		nr, err := ptmx.Read(buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])

			r.PublishOutput(process.ProcessOutput{
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

func (r *RREPL) monitorProcess() {
	if r.cmd == nil {
		return
	}

	err := r.cmd.Wait()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}
	}

	r.SetExitCode(exitCode)

	if exitCode == 0 {
		r.SetState(process.ProcessStateStopped)
	} else if exitCode == -1 || exitCode == 137 {
		r.SetState(process.ProcessStateKilled)
	} else {
		r.SetState(process.ProcessStateCrashed)
	}

	r.CloseOutput()
}
