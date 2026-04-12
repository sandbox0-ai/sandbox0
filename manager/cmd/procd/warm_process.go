package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

const warmProcessesEnvVar = "SANDBOX0_WARM_PROCESSES"

var warmProcessExit = os.Exit

type warmProcessSpec struct {
	Type    string            `json:"type"`
	Alias   string            `json:"alias,omitempty"`
	Command []string          `json:"command,omitempty"`
	CWD     string            `json:"cwd,omitempty"`
	EnvVars map[string]string `json:"envVars,omitempty"`
}

type warmProcessChecker struct {
	manager    *ctxpkg.Manager
	contextIDs []string
}

func parseWarmProcesses(raw string) ([]warmProcessSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var specs []warmProcessSpec
	if err := json.Unmarshal([]byte(raw), &specs); err != nil {
		return nil, err
	}
	for i, spec := range specs {
		if err := validateWarmProcessSpec(i, spec); err != nil {
			return nil, err
		}
	}
	return specs, nil
}

func validateWarmProcessSpec(index int, spec warmProcessSpec) error {
	field := fmt.Sprintf("warmProcesses[%d]", index)
	switch process.ProcessType(spec.Type) {
	case process.ProcessTypeREPL:
		if len(spec.Command) > 0 {
			return fmt.Errorf("%s.command is only valid for cmd warm processes", field)
		}
	case process.ProcessTypeCMD:
		if len(spec.Command) == 0 || strings.TrimSpace(spec.Command[0]) == "" {
			return fmt.Errorf("%s.command[0] is required for cmd warm processes", field)
		}
	default:
		return fmt.Errorf("%s.type must be one of: repl, cmd", field)
	}
	return nil
}

func startWarmProcesses(manager *ctxpkg.Manager, logger *zap.Logger) ([]string, error) {
	if manager == nil {
		return nil, nil
	}
	specs, err := parseWarmProcesses(os.Getenv(warmProcessesEnvVar))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", warmProcessesEnvVar, err)
	}
	if len(specs) == 0 {
		return nil, nil
	}

	contextIDs := make([]string, 0, len(specs))
	for i, spec := range specs {
		ctx, err := manager.CreateContextWithPolicyAndREPLConfig(process.ProcessConfig{
			Type:    process.ProcessType(spec.Type),
			Alias:   spec.Alias,
			Command: append([]string(nil), spec.Command...),
			CWD:     spec.CWD,
			EnvVars: cloneStringMap(spec.EnvVars),
		}, nil, ctxpkg.CleanupPolicy{})
		if err != nil {
			return nil, fmt.Errorf("start warm process %d: %w", i, err)
		}
		ctx.SetCleanupPolicy(ctxpkg.CleanupPolicy{})
		ctx.AddExitHandler(func(event process.ExitEvent) {
			if logger != nil {
				logger.Error("Warm process exited; terminating procd for Kubernetes restart",
					zap.String("context_id", ctx.ID),
					zap.Int("exit_code", event.ExitCode),
					zap.String("state", string(event.State)),
				)
			}
			warmProcessExit(1)
		})
		contextIDs = append(contextIDs, ctx.ID)
		if logger != nil {
			logger.Info("Started warm process",
				zap.String("context_id", ctx.ID),
				zap.String("type", spec.Type),
				zap.String("alias", spec.Alias),
				zap.Strings("command", spec.Command),
			)
		}
	}
	return contextIDs, nil
}

func (r warmProcessChecker) Check() error {
	for _, contextID := range r.contextIDs {
		ctx, err := r.manager.GetContext(contextID)
		if err != nil {
			return fmt.Errorf("warm process context %s is missing", contextID)
		}
		if !ctx.IsRunning() {
			return fmt.Errorf("warm process context %s is not running", contextID)
		}
	}
	return nil
}

func (r warmProcessChecker) CheckHealth() error {
	for _, contextID := range r.contextIDs {
		ctx, err := r.manager.GetContext(contextID)
		if err != nil {
			return fmt.Errorf("warm process context %s is missing", contextID)
		}
		if !ctx.IsRunning() && !ctx.IsPaused() {
			return fmt.Errorf("warm process context %s is not running", contextID)
		}
	}
	return nil
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}
