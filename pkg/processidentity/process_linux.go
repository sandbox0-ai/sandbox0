//go:build linux

// Package processidentity identifies a host process without confusing PID reuse.
package processidentity

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func Current() (string, error) { return identity(os.Getpid()) }

func identity(pid int) (string, error) {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return "", fmt.Errorf("invalid process stat")
	}
	fields := strings.Fields(string(stat[end+1:]))
	if len(fields) < 20 {
		return "", fmt.Errorf("incomplete process stat")
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return "", os.ErrNotExist
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", err
	}
	return fmt.Sprintf("process-v1:%s:%d:%s", strings.TrimSpace(string(boot)), pid, fields[19]), nil
}

func Alive(value string) (bool, error) {
	fields := strings.Split(value, ":")
	if len(fields) != 4 || fields[0] != "process-v1" {
		return false, fmt.Errorf("invalid process identity")
	}
	pid, err := strconv.Atoi(fields[2])
	if err != nil || pid <= 0 {
		return false, fmt.Errorf("invalid process identity PID")
	}
	current, err := identity(pid)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == current, nil
}
