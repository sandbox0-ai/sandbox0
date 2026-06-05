package repl

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

// execCandidate represents an executable candidate with arguments.
type execCandidate struct {
	name string
	args []string
}

// startWithCandidates tries to start a process with the given candidates in order.
func startWithCandidates(base *process.BaseProcess, runner *process.PTYRunner, config process.ProcessConfig, candidates []execCandidate, extraEnv map[string]string) error {
	if len(candidates) == 0 {
		if base != nil {
			base.SetState(process.ProcessStateCrashed)
		}
		return fmt.Errorf("%w: no interpreter candidates configured", process.ErrProcessStartFailed)
	}

	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, candidate.name)
	}

	var errs []string
	foundAny := false

	for _, candidate := range candidates {
		env := process.MergeEnvironment(os.Environ(), config.EnvVars, extraEnv)
		path, err := process.LookPath(candidate.name, env)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: not found", candidate.name))
			continue
		}
		foundAny = true

		cmd, err := process.NewCommandContext(context.Background(), path, candidate.args, process.LaunchOptions{
			CWD: config.CWD,
			Env: env,
		})
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", path, err))
			continue
		}

		// Note: Do NOT set Setpgid for PTY processes.
		// PTY automatically creates a new session and handles terminal control.
		// Setting Setpgid would conflict with PTY's session management.

		err = runner.Start(cmd, config.PTYSize)
		if err == nil {
			return nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", path, err))
	}

	if base != nil {
		base.SetState(process.ProcessStateCrashed)
	}

	if !foundAny {
		return fmt.Errorf("%w: no interpreter found (tried: %s)", process.ErrProcessStartFailed, strings.Join(names, ", "))
	}
	return fmt.Errorf("%w: no usable interpreter found (%s)", process.ErrProcessStartFailed, strings.Join(errs, "; "))
}

// CheckExecutable checks if an executable is available in PATH.
func CheckExecutable(name string) (string, bool) {
	path, err := process.LookPath(name, os.Environ())
	return path, err == nil
}

// CheckREPLAvailable checks if a REPL type is available on the system.
func CheckREPLAvailable(language string) (string, bool) {
	config, ok := DefaultRegistry.Get(language)
	if !ok {
		return "", false
	}

	for _, candidate := range config.Candidates {
		if path, ok := CheckExecutable(candidate.Name); ok {
			return path, true
		}
	}
	return "", false
}

// ListAvailableREPLs returns the list of REPL types available on the system.
func ListAvailableREPLs() []string {
	var available []string
	for _, name := range DefaultRegistry.List() {
		if _, ok := CheckREPLAvailable(name); ok {
			available = append(available, name)
		}
	}
	return available
}
