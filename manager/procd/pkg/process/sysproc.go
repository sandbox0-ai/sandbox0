package process

import (
	"os/exec"
	"strings"
	"syscall"
)

// ApplySysProcAttr applies process isolation settings shared by context and
// direct function runner executions.
func ApplySysProcAttr(cmd *exec.Cmd, config ProcessConfig, setpgid bool) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.Setpgid = attr.Setpgid || setpgid
	if rootFS := strings.TrimSpace(config.RootFS); rootFS != "" {
		attr.Chroot = rootFS
	}
	cmd.SysProcAttr = attr
}
