package context

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/cmd"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/repl"
)

// Language aliases map alternative names to canonical REPL names.
var languageAliases = map[string]string{
	"python3":    "python",
	"javascript": "node",
	"nodejs":     "node",
	"rb":         "ruby",
	"R":          "r",
	"pl":         "perl",
}

// createREPLProcess creates a REPL process based on language or a custom config.
// Supports built-in languages registered in the REPL registry and custom per-context configs.
func createREPLProcess(ctxID string, config process.ProcessConfig, replConfig *repl.REPLConfig) (process.Process, error) {
	procID := ctxID + "-proc"
	lang := config.Language
	if lang == "" {
		lang = "python"
	}

	if replConfig != nil {
		if replConfig.Name == "" {
			return nil, fmt.Errorf("repl_config.name is required")
		}
		if config.Language == "" {
			config.Language = replConfig.Name
		} else if config.Language != replConfig.Name {
			return nil, fmt.Errorf("language must match repl_config.name")
		}
		return repl.NewCustomREPL(procID, replConfig, config)
	}

	// Resolve language aliases
	if canonical, ok := languageAliases[lang]; ok {
		lang = canonical
	}

	// Update config with resolved language
	config.Language = lang

	// Use the unified REPL factory
	return repl.NewREPL(procID, config)
}

// createCMDProcess creates a CMD process for one-time command execution.
// The command parameter should be provided in config.
func createCMDProcess(ctxID string, config process.ProcessConfig, command []string) (process.Process, error) {
	procID := ctxID + "-proc"
	return cmd.NewCMD(procID, config, command)
}
