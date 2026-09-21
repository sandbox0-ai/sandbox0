//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// The qualified stock release executes these companions. Main-binary identity
// alone cannot bind the sentry that actually ran the guest. Keep ordering stable:
// the digest is shared by independently installed source and destination bundles.
var cpuBundleNames = [...]string{
	"runsc", "gvisor-bin/checkpointgofer", "gvisor-bin/gvisor-sentry-prewarmer",
	"gvisor-bin/gvisor_sentry", "gvisor-bin/runsc-fd-parking", "gvisor-bin/runsc-metric-server",
}

type cpuBundleFile struct {
	path string
	info os.FileInfo
}

func (r *Command) cpuBundleFiles() ([]cpuBundleFile, error) {
	// Upstream supports test overrides and symlink-dependent companion lookup.
	// Migration qualifies only the canonical installed layout. Ordinary claims
	// remain usable when their installation cannot provide this evidence.
	for _, name := range []string{"GVISOR_SIDECAR_BINARIES_DIR", "GVISOR_ENFORCE_RELEASE"} {
		if os.Getenv(name) != "" {
			return nil, fmt.Errorf("migration runtime bundle disallows %s", name)
		}
	}
	configured, err := exec.LookPath(r.config.Path)
	if err != nil {
		return nil, err
	}
	configured, err = filepath.Abs(configured)
	if err != nil {
		return nil, err
	}
	main, _, err := r.cpuExecutable()
	if err != nil {
		return nil, err
	}
	if main != configured {
		return nil, fmt.Errorf("migration runtime bundle requires a canonical runsc path")
	}
	files := make([]cpuBundleFile, 0, len(cpuBundleNames))
	for i, name := range cpuBundleNames {
		path := filepath.Join(filepath.Dir(main), name)
		if i == 0 {
			path = main
		}
		member := *r
		member.config.Path = path
		resolved, info, err := member.cpuExecutable()
		if err != nil {
			return nil, fmt.Errorf("inspect migration runtime member %s: %w", name, err)
		}
		if resolved != path {
			return nil, fmt.Errorf("migration runtime member %s is not canonical", name)
		}
		files = append(files, cpuBundleFile{path: path, info: info})
	}
	return files, nil
}

type cpuBundleMonitor struct {
	command *Command
	files   []cpuBundleFile
	guards  []cpuExecutableGuard
}

func watchCPUBundle(r *Command) (*cpuBundleMonitor, error) {
	files, err := r.cpuBundleFiles()
	if err != nil {
		return nil, err
	}
	monitor := &cpuBundleMonitor{command: r, files: files}
	for _, file := range files {
		guard, err := watchCPUExecutable(file.path, file.info)
		if err != nil {
			_ = monitor.Close()
			return nil, err
		}
		monitor.guards = append(monitor.guards, guard)
	}
	if err := monitor.Unchanged(); err != nil {
		_ = monitor.Close()
		return nil, err
	}
	return monitor, nil
}

func (m *cpuBundleMonitor) Unchanged() error {
	for _, guard := range m.guards {
		if err := guard.Unchanged(); err != nil {
			return err
		}
	}
	files, err := m.command.cpuBundleFiles()
	if err != nil {
		return err
	}
	if len(files) != len(m.files) {
		return fmt.Errorf("migration runtime bundle changed")
	}
	for i, file := range files {
		if file.path != m.files[i].path || !sameCPUExecutable(file.info, m.files[i].info) {
			return fmt.Errorf("migration runtime bundle member changed")
		}
	}
	return nil
}

func (m *cpuBundleMonitor) Close() error {
	var result error
	for _, guard := range m.guards {
		result = errors.Join(result, guard.Close())
	}
	return result
}

// ExecutableDigest binds the complete qualified runtime bundle. Absolute paths
// are excluded, so identical installations on different nodes compare equally.
// Expensive hashing occurs only at warm-up and migration preflight; warm claims
// validate the retained inode monitors instead.
func (r *Command) ExecutableDigest(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", fmt.Errorf("executable observation requires context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	monitor, err := watchCPUBundle(r)
	if err != nil {
		return "", err
	}
	defer monitor.Close()
	hash := sha256.New()
	_, _ = hash.Write([]byte("sandbox0-runsc-bundle-v1\x00"))
	for i, file := range monitor.files {
		member := *r
		member.config.Path = file.path
		digest, err := member.executableFileDigest(ctx)
		if err != nil {
			return "", err
		}
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00", cpuBundleNames[i], digest)
	}
	if err := monitor.Unchanged(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
