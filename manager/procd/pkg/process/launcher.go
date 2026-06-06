package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// LauncherConfig controls how procd launches user processes.
type LauncherConfig struct {
	RootPath       string
	LauncherPath   string
	ExternalMounts []string
}

// LaunchOptions are applied to one exec.Cmd before it starts.
type LaunchOptions struct {
	CWD string
	Env []string
}

var (
	launcherMu      sync.RWMutex
	defaultLauncher LauncherConfig
)

// ConfigureLauncher sets the process launcher used by new CMD, REPL and
// function executions. An empty RootPath preserves the legacy direct execution.
func ConfigureLauncher(cfg LauncherConfig) {
	cfg.RootPath = filepath.Clean(strings.TrimSpace(cfg.RootPath))
	if cfg.RootPath == "." || cfg.RootPath == string(filepath.Separator) {
		cfg.RootPath = ""
	}
	cfg.ExternalMounts = normalizeLauncherExternalMounts(cfg.ExternalMounts)
	launcherMu.Lock()
	defaultLauncher = cfg
	launcherMu.Unlock()
}

// ConfigureExternalMounts updates the pod-local mount paths that the launcher
// should bind into the sandbox rootfs before pivoting.
func ConfigureExternalMounts(paths []string) {
	launcherMu.Lock()
	defaultLauncher.ExternalMounts = normalizeLauncherExternalMounts(paths)
	launcherMu.Unlock()
}

// CurrentLauncher returns the configured launcher snapshot.
func CurrentLauncher() LauncherConfig {
	launcherMu.RLock()
	cfg := defaultLauncher
	launcherMu.RUnlock()
	return cfg
}

// NewCommandContext creates an exec.Cmd and applies the configured launcher.
func NewCommandContext(ctx context.Context, name string, args []string, opts LaunchOptions) (*exec.Cmd, error) {
	cfg := CurrentLauncher()
	if !cfg.enabled() {
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Dir = opts.CWD
		cmd.Env = opts.Env
		return cmd, nil
	}
	cmdPath, err := cfg.resolveCommand(name, opts.Env)
	if err != nil {
		return nil, err
	}
	return cfg.newPivotCommand(ctx, cmdPath, args, opts), nil
}

// LookPath resolves an executable using the configured rootfs when enabled.
func LookPath(name string, env []string) (string, error) {
	cfg := CurrentLauncher()
	if !cfg.enabled() {
		return exec.LookPath(name)
	}
	return cfg.resolveCommand(name, env)
}

// SetProcessGroup preserves existing SysProcAttr settings while enabling a new
// process group for signal fan-out.
func SetProcessGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

func (cfg LauncherConfig) enabled() bool {
	return strings.TrimSpace(cfg.RootPath) != ""
}

func (cfg LauncherConfig) newPivotCommand(ctx context.Context, cmdPath string, args []string, opts LaunchOptions) *exec.Cmd {
	launcherPath := strings.TrimSpace(cfg.LauncherPath)
	if launcherPath == "" {
		launcherPath = "/proc/self/exe"
	}
	launcherArgs := []string{
		rootFSLauncherArg,
		"--root", cfg.RootPath,
		"--cwd", cfg.sandboxPath(opts.CWD),
	}
	for _, mount := range cfg.ExternalMounts {
		launcherArgs = append(launcherArgs, "--external-mount", mount)
	}
	launcherArgs = append(launcherArgs, "--", cmdPath)
	launcherArgs = append(launcherArgs, args...)
	cmd := exec.CommandContext(ctx, launcherPath, launcherArgs...)
	cmd.Dir = string(filepath.Separator)
	cmd.Env = opts.Env
	return cmd
}

func (cfg LauncherConfig) resolveCommand(name string, env []string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: command cannot be empty", ErrInvalidCommand)
	}
	if strings.ContainsRune(name, filepath.Separator) {
		sandboxPath := cfg.sandboxPath(name)
		hostPath := cfg.hostPath(sandboxPath)
		if err := isExecutable(hostPath); err != nil {
			return "", err
		}
		return sandboxPath, nil
	}

	pathEnv := envValue(env, "PATH")
	if pathEnv == "" {
		pathEnv = os.Getenv("PATH")
	}
	for _, dir := range filepath.SplitList(pathEnv) {
		if strings.TrimSpace(dir) == "" {
			continue
		}
		sandboxPath := filepath.Join(cfg.sandboxPath(dir), name)
		if err := isExecutable(cfg.hostPath(sandboxPath)); err == nil {
			return sandboxPath, nil
		}
	}
	return "", fmt.Errorf("%w: executable %q not found in sandbox rootfs", ErrProcessStartFailed, name)
}

func (cfg LauncherConfig) sandboxPath(path string) string {
	clean := filepath.Clean(string(filepath.Separator) + path)
	if clean == "." {
		return string(filepath.Separator)
	}
	return clean
}

func (cfg LauncherConfig) hostPath(sandboxPath string) string {
	rel := strings.TrimPrefix(cfg.sandboxPath(sandboxPath), string(filepath.Separator))
	return filepath.Clean(filepath.Join(cfg.RootPath, rel))
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for i := len(env) - 1; i >= 0; i-- {
		if strings.HasPrefix(env[i], prefix) {
			return strings.TrimPrefix(env[i], prefix)
		}
	}
	return ""
}

func isExecutable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.IsDir() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%w: %s is not executable", ErrProcessStartFailed, path)
	}
	return nil
}

func normalizeLauncherExternalMounts(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		clean := filepath.Clean(strings.TrimSpace(path))
		if clean == "." || clean == "" || clean == string(filepath.Separator) || !filepath.IsAbs(clean) {
			continue
		}
		if _, ok := seen[clean]; ok {
			continue
		}
		seen[clean] = struct{}{}
		out = append(out, clean)
	}
	return out
}
