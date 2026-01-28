package context

import (
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

// createREPLProcess creates a REPL process based on language.
// Supports all languages registered in the REPL registry, including:
// Python, Node.js, Bash, Zsh, Ruby, Lua, PHP, R, Perl, Redis, SQLite, MySQL, PostgreSQL,
// Elixir, Erlang, Scala, Clojure, Haskell, OCaml, Julia, Swift, Kotlin, Groovy, and more.
func createREPLProcess(ctxID string, config process.ProcessConfig) (process.Process, error) {
	procID := ctxID + "-proc"
	lang := config.Language
	if lang == "" {
		lang = "python"
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
