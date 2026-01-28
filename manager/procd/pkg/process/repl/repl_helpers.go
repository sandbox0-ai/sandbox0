package repl

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// execCandidate represents an executable candidate with arguments.
type execCandidate struct {
	name string
	args []string
}

// startWithCandidates tries to start a process with the given candidates in order.
func startWithCandidates(base *process.BaseProcess, runner *process.PTYRunner, config process.ProcessConfig, candidates []execCandidate, extraEnv []string) error {
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
		path, err := exec.LookPath(candidate.name)
		if err != nil {
			errs = append(errs, fmt.Sprintf("%s: not found", candidate.name))
			continue
		}
		foundAny = true

		cmd := exec.Command(path, candidate.args...)
		if config.CWD != "" {
			cmd.Dir = config.CWD
		}

		env := os.Environ()
		for k, v := range config.EnvVars {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		env = append(env, extraEnv...)
		cmd.Env = env

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
	path, err := exec.LookPath(name)
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
