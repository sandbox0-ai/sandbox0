package context

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/cmd"
	"github.com/sandbox0-ai/infra/manager/procd/pkg/process/repl"
)

// createREPLProcess creates a REPL process based on language.
// Now supports Python, Node.js, Bash, Zsh, Ruby, Lua, PHP, R, and Perl.
func createREPLProcess(ctxID string, config process.ProcessConfig) (process.Process, error) {
	procID := ctxID + "-proc"
	lang := config.Language
	if lang == "" {
		lang = "python"
	}

	switch lang {
	case "python", "python3":
		return repl.NewPythonREPL(procID, config)
	case "javascript", "node", "nodejs":
		return repl.NewNodeREPL(procID, config)
	case "bash":
		return repl.NewBashREPL(procID, config)
	case "zsh":
		return repl.NewZshREPL(procID, config)
	case "ruby", "rb":
		return repl.NewRubyREPL(procID, config)
	case "lua":
		return repl.NewLuaREPL(procID, config)
	case "php":
		return repl.NewPHPREPL(procID, config)
	case "r", "R":
		return repl.NewRREPL(procID, config)
	case "perl", "pl":
		return repl.NewPerlREPL(procID, config)
	default:
		return nil, fmt.Errorf("%w: %s", process.ErrUnsupportedLanguage, lang)
	}
}

// createCMDProcess creates a CMD process for one-time command execution.
// The command parameter should be provided in config.
func createCMDProcess(ctxID string, config process.ProcessConfig, command []string) (process.Process, error) {
	procID := ctxID + "-proc"
	return cmd.NewCMD(procID, config, command)
}
