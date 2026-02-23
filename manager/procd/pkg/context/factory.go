package context

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/cmd"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/repl"
)

// Alias aliases map alternative names to canonical REPL names.
var aliasAliases = map[string]string{
	"python3":    "python",
	"javascript": "node",
	"nodejs":     "node",
	"rb":         "ruby",
	"R":          "r",
	"pl":         "perl",
}

// createREPLProcess creates a REPL process based on alias or a custom config.
// Supports built-in aliases registered in the REPL registry and custom per-context configs.
func createREPLProcess(ctxID string, config process.ProcessConfig, replConfig *repl.REPLConfig) (process.Process, error) {
	procID := ctxID + "-proc"
	alias := config.Alias
	if alias == "" {
		alias = "python"
	}

	if replConfig != nil {
		if replConfig.Name == "" {
			return nil, fmt.Errorf("repl_config.name is required")
		}
		if config.Alias == "" {
			config.Alias = replConfig.Name
		} else if config.Alias != replConfig.Name {
			return nil, fmt.Errorf("alias must match repl_config.name")
		}
		return repl.NewCustomREPL(procID, replConfig, config)
	}

	// Resolve alias aliases
	if canonical, ok := aliasAliases[alias]; ok {
		alias = canonical
	}

	// Update config with resolved alias
	config.Alias = alias

	// Use the unified REPL factory
	return repl.NewREPL(procID, config)
}

// createCMDProcess creates a CMD process for one-time command execution.
// The command parameter should be provided in config.
func createCMDProcess(ctxID string, config process.ProcessConfig, command []string) (process.Process, error) {
	procID := ctxID + "-proc"
	return cmd.NewCMD(procID, config, command)
}
