package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

// Workloads are ordered executable invocations, never interpolated shell text.
// Only the first step measures the first user executable in a fresh sandbox.
type workload struct {
	Name  string        `json:"name"`
	Steps []commandStep `json:"steps"`
}

type commandStep struct {
	Name           string            `json:"name"`
	Argv           []string          `json:"argv"`
	ExpectStdout   string            `json:"expect_stdout,omitempty"`
	StdoutContains string            `json:"stdout_contains,omitempty"`
	EnvVars        map[string]string `json:"env_vars,omitempty"`
}

type commandSample struct {
	Name              string            `json:"name"`
	ContextID         string            `json:"context_id,omitempty"`
	HTTPStatus        int               `json:"http_status"`
	StartedAt         time.Time         `json:"started_at"`
	CompletedAt       time.Time         `json:"completed_at"`
	Duration          time.Duration     `json:"duration_ns"`
	ClaimToCompletion time.Duration     `json:"claim_to_completion_ns"`
	ExitCode          *int              `json:"exit_code"`
	Stdout            *string           `json:"stdout"`
	Stderr            string            `json:"stderr,omitempty"`
	State             string            `json:"state"`
	Running           *bool             `json:"running"`
	StdoutMatched     bool              `json:"stdout_matched"`
	Passed            bool              `json:"passed"`
	Error             string            `json:"error,omitempty"`
	EnvVars           map[string]string `json:"env_vars,omitempty"`
}

func shellWorkload() workload {
	return workload{Name: "shell", Steps: []commandStep{{
		Name: "shell", Argv: []string{"/bin/sh", "-c", `printf '%s\n' "$1"`, "runtime-slot-slo", "sandbox0-first-command"},
		ExpectStdout: "sandbox0-first-command\n",
	}}}
}

func loadWorkload(name, path string) (workload, error) {
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return workload{}, err
		}
		defer file.Close()
		payload, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
		if err != nil {
			return workload{}, err
		}
		if len(payload) > 64<<10 {
			return workload{}, errors.New("workload exceeds 64 KiB")
		}
		var work workload
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&work); err != nil {
			return workload{}, fmt.Errorf("decode workload: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return workload{}, errors.New("workload must contain exactly one JSON object")
		}
		return work, work.validate()
	}
	switch name {
	case "none":
		return workload{Name: "none"}, nil
	case "shell":
		return shellWorkload(), nil
	case "coding-agent":
		return workload{Name: name, Steps: []commandStep{
			{Name: "node", Argv: []string{"node", "-e", "process.stdout.write(process.argv[1])", "sandbox0-node-first-command\n"}, ExpectStdout: "sandbox0-node-first-command\n"},
			{Name: "codex-version", Argv: []string{"codex", "--version"}, StdoutContains: "codex"},
		}}, nil
	default:
		return workload{}, fmt.Errorf("unknown workload %q", name)
	}
}

func (w workload) validate() error {
	if w.Name == "none" {
		if len(w.Steps) != 0 {
			return errors.New("the none workload must not contain executable steps")
		}
		return nil
	}
	if strings.TrimSpace(w.Name) == "" || len(w.Name) > 128 || len(w.Steps) == 0 || len(w.Steps) > 16 {
		return errors.New("workload needs a name and 1..16 command steps")
	}
	payload, err := json.Marshal(w)
	if err != nil || len(payload) > 64<<10 {
		return errors.New("workload exceeds 64 KiB")
	}
	names := make(map[string]bool)
	for _, step := range w.Steps {
		if strings.TrimSpace(step.Name) == "" || len(step.Name) > 128 || names[step.Name] {
			return errors.New("command step names must be non-empty and unique, at most 128 bytes")
		}
		names[step.Name] = true
		if len(step.Argv) == 0 || len(step.Argv) > 128 || strings.TrimSpace(step.Argv[0]) == "" {
			return fmt.Errorf("step %s needs executable argv (1..128 arguments)", step.Name)
		}
		for _, arg := range step.Argv {
			if strings.ContainsRune(arg, 0) {
				return fmt.Errorf("step %s argv contains NUL", step.Name)
			}
		}
		for key, value := range step.EnvVars {
			if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, 0) {
				return fmt.Errorf("step %s has an invalid command environment entry", step.Name)
			}
		}
		if (step.ExpectStdout == "") == (step.StdoutContains == "") {
			return fmt.Errorf("step %s needs exactly one non-empty expect_stdout or stdout_contains", step.Name)
		}
	}
	return nil
}

func (c config) validateWorkload() error {
	if err := c.validateCases(); err != nil {
		return err
	}
	if c.contextTTL < time.Second || c.contextTTL > time.Minute || c.contextTTL%time.Second != 0 {
		return errors.New("context TTL must be whole seconds in [1s, 1m]")
	}
	if c.firstCommandHardLimit < 0 {
		return errors.New("first-command hard limit must be non-negative")
	}
	if len(c.cases) > 0 {
		return nil // Every case was independently validated above.
	}
	if len(c.workload.Steps) == 0 && c.firstCommandHardLimit > 0 {
		return errors.New("a first-command limit requires an executable workload")
	}
	return c.workload.validate()
}

func (s commandStep) matches(stdout *string) bool {
	return stdout != nil && ((s.ExpectStdout != "" && *stdout == s.ExpectStdout) ||
		(s.StdoutContains != "" && strings.Contains(*stdout, s.StdoutContains)))
}

// Check only explicitly requested entries. Never retain the sandbox's entire
// inherited environment in a diagnostic report; it may contain credentials.
func (s commandStep) matchesEnvironment(observed map[string]string) bool {
	for key, value := range s.EnvVars {
		if got, present := observed[key]; !present || got != value {
			return false
		}
	}
	return true
}

func runCommands(ctx context.Context, cfg config, claimStarted time.Time, result *sample) {
	// All steps together share one unchanged request-timeout budget. A timeout
	// may leave an unknown context running; never retry its POST. Sandbox DELETE
	// and terminal absence reclaim known and unknown contexts after every batch.
	commandCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	seen := make(map[string]bool)
	for _, step := range cfg.workload.Steps {
		current := executeCommand(commandCtx, cfg, result.SandboxID, step)
		if current.Passed && seen[current.ContextID] {
			current.Passed = false
			current.Error = "context ID duplicates an earlier command step"
		}
		seen[current.ContextID] = true
		current.ClaimToCompletion = time.Since(claimStarted)
		result.Steps = append(result.Steps, current)
		if len(result.Steps) == 1 {
			result.FirstCommandDuration = current.ClaimToCompletion
		}
		result.WorkloadDuration = current.ClaimToCompletion
		if !current.Passed {
			result.Error = fmt.Sprintf("command step %s: %s", step.Name, current.Error)
			return
		}
	}
}

func executeCommand(ctx context.Context, cfg config, sandboxID string, step commandStep) (result commandSample) {
	result.Name = step.Name
	started := time.Now()
	result.StartedAt = started.UTC()
	defer func() {
		result.Duration = time.Since(started)
		result.CompletedAt = time.Now().UTC()
	}()
	endpoint, err := url.JoinPath(cfg.endpoint, sandboxID, "contexts")
	if err != nil {
		result.Error = err.Error()
		return
	}
	// Marshal argv directly into cmd.command. Even shell metacharacters in argv
	// stay data; the harness never joins them into a script or expands variables.
	body, err := json.Marshal(struct {
		Type string `json:"type"`
		Cmd  struct {
			Command []string `json:"command"`
		} `json:"cmd"`
		EnvVars       map[string]string `json:"env_vars,omitempty"`
		WaitUntilDone bool              `json:"wait_until_done"`
		TTLSec        int               `json:"ttl_sec"`
	}{Type: "cmd", Cmd: struct {
		Command []string `json:"command"`
	}{step.Argv}, EnvVars: step.EnvVars, WaitUntilDone: true, TTLSec: int(cfg.contextTTL / time.Second)})
	if err != nil {
		result.Error = err.Error()
		return
	}
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		result.Error = err.Error()
		return
	}
	request.Header.Set("Authorization", "Bearer "+cfg.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := cfg.client.Do(request)
	if err != nil {
		result.Error = err.Error()
		return
	}
	defer response.Body.Close()
	result.HTTPStatus = response.StatusCode
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		result.Error = err.Error()
		return
	}
	if len(payload) > 1<<20 || !json.Valid(payload) {
		result.Error = "context response must be valid JSON no larger than 1 MiB"
		return
	}
	if response.StatusCode != http.StatusCreated {
		result.Error = fmt.Sprintf("context status %d: %s", response.StatusCode, truncate(string(payload), 512))
		return
	}
	decoded, apiErr, err := spec.DecodeResponse[struct {
		ID       string            `json:"id"`
		Type     string            `json:"type"`
		Running  *bool             `json:"running"`
		ExitCode *int              `json:"exit_code"`
		Stdout   *string           `json:"stdout"`
		Stderr   string            `json:"stderr"`
		State    string            `json:"state"`
		EnvVars  map[string]string `json:"env_vars"`
	}](bytes.NewReader(payload))
	if err != nil || apiErr != nil || decoded == nil {
		result.Error = fmt.Sprintf("invalid context response: decode=%v api=%v", err, apiErr)
		return
	}
	result.ContextID, result.ExitCode, result.Stdout = decoded.ID, decoded.ExitCode, decoded.Stdout
	result.Running, result.State, result.Stderr = decoded.Running, decoded.State, decoded.Stderr
	result.StdoutMatched = step.matches(result.Stdout)
	if len(step.EnvVars) > 0 {
		result.EnvVars = make(map[string]string, len(step.EnvVars))
		for key := range step.EnvVars {
			if value, present := decoded.EnvVars[key]; present {
				result.EnvVars[key] = value
			}
		}
	}
	switch {
	case !validClaimSandboxID(decoded.ID) || decoded.Type != "cmd":
		result.Error = "context response lacks a valid CMD identity"
	case result.Running == nil || *result.Running || result.State != "stopped" || result.ExitCode == nil:
		result.Error = "context response lacks successful terminal completion evidence"
	case *result.ExitCode != 0:
		result.Error = fmt.Sprintf("command exit code %d", *result.ExitCode)
	case !result.StdoutMatched:
		result.Error = "command stdout did not match expectation"
	case !step.matchesEnvironment(result.EnvVars):
		result.Error = "context response did not confirm the requested command environment"
	default:
		result.Passed = true
	}
	return
}

// Recheck evidence during report accounting: missing steps, absent exit codes,
// and readiness-only responses must fail even if an error string was omitted.
func successfulCommands(s sample, work workload) (first, all bool) {
	if !s.ClaimSucceeded || len(s.Steps) == 0 || len(s.Steps) > len(work.Steps) {
		return false, false
	}
	seen := make(map[string]bool)
	previous := s.WallDuration
	for i, current := range s.Steps {
		step := work.Steps[i]
		if current.Name != step.Name || !current.Passed || current.Error != "" ||
			current.HTTPStatus != http.StatusCreated || !validClaimSandboxID(current.ContextID) || seen[current.ContextID] ||
			current.ExitCode == nil || *current.ExitCode != 0 || current.State != "stopped" ||
			current.Running == nil || *current.Running || !current.StdoutMatched || !step.matches(current.Stdout) || !step.matchesEnvironment(current.EnvVars) ||
			current.Duration <= 0 || current.ClaimToCompletion < previous {
			return first, false
		}
		seen[current.ContextID] = true
		previous = current.ClaimToCompletion
		if i == 0 {
			first = s.FirstCommandDuration == current.ClaimToCompletion && s.FirstCommandDuration > 0
		}
	}
	return first, first && len(s.Steps) == len(work.Steps) && s.WorkloadDuration == previous
}
