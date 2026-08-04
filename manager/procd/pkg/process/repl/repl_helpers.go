package repl

import (
	"fmt"
	"os"
	"os/exec"
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

		// Note: Do NOT set Setpgid for PTY processes.
		// PTY automatically creates a new session and handles terminal control.
		// Setting Setpgid would conflict with PTY's session management.

		cmd.Env = process.MergeEnvironment(os.Environ(), config.EnvVars, extraEnv)

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
