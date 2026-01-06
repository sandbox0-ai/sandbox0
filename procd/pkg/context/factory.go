package context

import (
	"fmt"

	"github.com/sandbox0-ai/infra/procd/pkg/process"
	"github.com/sandbox0-ai/infra/procd/pkg/process/repl"
	"github.com/sandbox0-ai/infra/procd/pkg/process/shell"
)

// createREPLProcess creates a REPL process based on language.
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
	default:
		return nil, fmt.Errorf("%w: %s", process.ErrUnsupportedLanguage, lang)
	}
}

// createShellProcess creates a Shell process based on type.
func createShellProcess(ctxID string, config process.ProcessConfig) (process.Process, error) {
	procID := ctxID + "-proc"
	shellType := config.Language
	if shellType == "" {
		shellType = "bash"
	}

	switch shellType {
	case "bash":
		return shell.NewBashShell(procID, config)
	case "zsh":
		return shell.NewZshShell(procID, config)
	default:
		return nil, fmt.Errorf("%w: %s", process.ErrUnsupportedLanguage, shellType)
	}
}
