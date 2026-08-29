package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func loadSourceDSN(path, environmentValue string) (string, error) {
	return loadDSN("source", path, environmentValue)
}

func loadTargetDSN(path, environmentValue string) (string, error) {
	return loadDSN("target", path, environmentValue)
}

func loadDSN(role, path, environmentValue string) (string, error) {
	if path == "" {
		if environmentValue == "" {
			return "", fmt.Errorf("%s database DSN is required", role)
		}
		return environmentValue, nil
	}
	payload, err := readOwnerOnlyFile(path, maxDSNFileBytes, role+" DSN")
	if err != nil {
		return "", err
	}
	dsn := strings.TrimSpace(string(payload))
	if dsn == "" || strings.ContainsAny(dsn, "\r\n") {
		return "", fmt.Errorf("%s DSN file must contain exactly one non-empty line", role)
	}
	return dsn, nil
}

func readOwnerOnlyFile(path string, maxBytes int64, label string) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("%s size bound is invalid", label)
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) || clean != path || clean == string(filepath.Separator) {
		return nil, fmt.Errorf("%s file path must be canonical and absolute", label)
	}
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s file: %w", label, err)
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("wrap %s file descriptor", label)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %s file: %w", label, err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 ||
		info.Size() <= 0 || info.Size() > maxBytes || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("%s must be an owner-only, expected-owner, single-link regular file within 1..%d bytes", label, maxBytes)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", label, err)
	}
	if int64(len(payload)) != info.Size() {
		return nil, fmt.Errorf("%s file changed while being read", label)
	}
	return payload, nil
}

func writeAtomicOwnerOnly(path string, payload []byte) error {
	clean := filepath.Clean(strings.TrimSpace(path))
	if clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("output path must name a file")
	}
	directory := filepath.Dir(clean)
	temporary, err := os.CreateTemp(directory, ".legacy-ack-migration-*.tmp")
	if err != nil {
		return fmt.Errorf("create migration report: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect migration report: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write migration report: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync migration report: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close migration report: %w", err)
	}
	if err := os.Rename(temporaryPath, clean); err != nil {
		return fmt.Errorf("publish migration report: %w", err)
	}
	return nil
}
