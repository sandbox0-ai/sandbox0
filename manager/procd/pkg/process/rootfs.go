package process

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

// ApplyRootPath configures cmd so the child process starts inside config.RootPath.
func ApplyRootPath(cmd *exec.Cmd, config ProcessConfig) {
	if cmd == nil {
		return
	}
	rootPath := cleanRootPath(config.RootPath)
	if rootPath == "" {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Chroot = rootPath
	if cmd.Dir == "" {
		cmd.Dir = "/"
	}
}

// ResolveCommandInRoot resolves command using PATH inside rootPath when a
// sandbox filesystem root is configured. Absolute commands are kept as paths
// inside the chroot.
func ResolveCommandInRoot(rootPath, command string, env []string) string {
	command = strings.TrimSpace(command)
	rootPath = cleanRootPath(rootPath)
	if rootPath == "" || command == "" {
		return command
	}
	if strings.Contains(command, "/") {
		return command
	}
	for _, dir := range rootSearchPath(env) {
		candidate := filepath.Join(rootPath, strings.TrimPrefix(dir, "/"), command)
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() && info.Mode()&0111 != 0 {
			return filepath.Join(dir, command)
		}
	}
	return command
}

func cleanRootPath(rootPath string) string {
	rootPath = filepath.Clean(strings.TrimSpace(rootPath))
	if rootPath == "" || rootPath == "." || rootPath == "/" {
		return ""
	}
	return rootPath
}

func rootSearchPath(env []string) []string {
	for _, item := range env {
		if !strings.HasPrefix(item, "PATH=") {
			continue
		}
		value := strings.TrimPrefix(item, "PATH=")
		if value == "" {
			break
		}
		parts := strings.Split(value, ":")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		if len(out) > 0 {
			return out
		}
		break
	}
	return []string{"/usr/local/sbin", "/usr/local/bin", "/usr/sbin", "/usr/bin", "/sbin", "/bin"}
}
