package nodebootstrap

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const maxRuntimeConfigBytes = 8 << 20

type stagedRuntimeConfig struct {
	root  string
	files []stagedRuntimeFile
}

type stagedRuntimeFile struct {
	relative string
	mode     fs.FileMode
}

func stageRuntimeConfig(payload []byte) (*stagedRuntimeConfig, error) {
	return stageRuntimeConfigAt(payload, "/run")
}

func stageRuntimeConfigAt(payload []byte, temporaryDirectory string) (*stagedRuntimeConfig, error) {
	if len(payload) == 0 || len(payload) > maxRuntimeConfigBytes {
		return nil, errors.New("rendered node runtime archive size is invalid")
	}
	root, err := os.MkdirTemp(temporaryDirectory, "sandbox0-node-runtime.")
	if err != nil {
		return nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()
	gzipReader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	archive := tar.NewReader(io.LimitReader(gzipReader, maxRuntimeConfigBytes+1))
	result := &stagedRuntimeConfig{root: root}
	seen := make(map[string]struct{})
	var total int64
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimPrefix(header.Name, "./")
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(name, "node-runtime/") ||
			filepath.ToSlash(filepath.Clean(name)) != name || strings.Contains(name, "..") ||
			header.Size <= 0 || header.Size > 2<<20 || header.Mode&0o022 != 0 ||
			header.Uid != 0 || header.Gid != 0 {
			return nil, fmt.Errorf("unsafe rendered node runtime member %q", header.Name)
		}
		relative := strings.TrimPrefix(name, "node-runtime/")
		if !allowedRuntimeConfigPath(relative) {
			return nil, fmt.Errorf("rendered node runtime path %q is outside the installation contract", relative)
		}
		if _, exists := seen[relative]; exists {
			return nil, fmt.Errorf("duplicate rendered node runtime path %q", relative)
		}
		seen[relative] = struct{}{}
		total += header.Size
		if total > maxRuntimeConfigBytes {
			return nil, errors.New("rendered node runtime contents are too large")
		}
		destination := filepath.Join(root, filepath.FromSlash(relative))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return nil, err
		}
		contents, err := io.ReadAll(io.LimitReader(archive, header.Size+1))
		if err != nil || int64(len(contents)) != header.Size {
			return nil, errors.New("rendered node runtime member is truncated")
		}
		mode := fs.FileMode(header.Mode & 0o777)
		if mode != 0o600 && mode != 0o644 {
			return nil, fmt.Errorf("rendered node runtime path %q has an unsafe mode", relative)
		}
		if err := os.WriteFile(destination, contents, mode); err != nil {
			return nil, err
		}
		if err := os.Chmod(destination, mode); err != nil {
			return nil, err
		}
		result.files = append(result.files, stagedRuntimeFile{relative: relative, mode: mode})
	}
	for _, required := range requiredRuntimeConfigPaths() {
		if _, exists := seen[required]; !exists {
			return nil, fmt.Errorf("rendered node runtime is missing %s", required)
		}
	}
	cleanup = false
	return result, nil
}

func allowedRuntimeConfigPath(relative string) bool {
	if strings.HasPrefix(relative, "etc/sandbox0/") && len(relative) > len("etc/sandbox0/") {
		return true
	}
	return relative == "etc/nomad.d/30-sandbox0-gvisor.hcl" ||
		relative == "opt/cni/config/10-sandbox0.conflist"
}

func requiredRuntimeConfigPaths() []string {
	return []string{
		"etc/sandbox0/ctld.yaml",
		"etc/sandbox0/ctld-networking.yaml",
		"etc/sandbox0/ctld.env",
		"etc/sandbox0/ctld-a.env",
		"etc/sandbox0/ctld-b.env",
		"etc/sandbox0/internal-auth/data-public.pem",
		"etc/sandbox0/pki/manager-ca.pem",
		"etc/sandbox0/tokens/nomad.token",
		"etc/nomad.d/30-sandbox0-gvisor.hcl",
		"opt/cni/config/10-sandbox0.conflist",
	}
}

func (s *stagedRuntimeConfig) close() {
	if s != nil && s.root != "" {
		_ = os.RemoveAll(s.root)
	}
}

func (s *stagedRuntimeConfig) path(relative string) string {
	return filepath.Join(s.root, filepath.FromSlash(relative))
}

func (s *stagedRuntimeConfig) install() error {
	for _, file := range s.files {
		destination := "/" + filepath.FromSlash(file.relative)
		payload, err := os.ReadFile(s.path(file.relative))
		if err != nil {
			return err
		}
		if err := atomicWriteFile(destination, payload, file.mode); err != nil {
			return err
		}
	}
	return nil
}

// InstallRenderedRuntimeConfig validates and installs a control-plane-rendered
// node archive for an exact fixed worker identity. Elastic workers use the same
// staging and installation path as part of Bootstrapper.Initial.
func InstallRenderedRuntimeConfig(
	payload []byte,
	nodeName, nodeID, nodeUID, regionID, clusterID, allocationCIDR string,
) error {
	if os.Geteuid() != 0 {
		return errors.New("rendered node runtime config installation requires root")
	}
	staged, err := stageRuntimeConfig(payload)
	if err != nil {
		return err
	}
	defer staged.close()
	if err := validateRuntimeConfigIdentity(staged, nodeName, nodeID, nodeUID,
		regionID, clusterID, allocationCIDR); err != nil {
		return err
	}
	return staged.install()
}

func validateRuntimeConfigIdentity(
	staged *stagedRuntimeConfig,
	nodeName, nodeID, nodeUID, regionID, clusterID, allocationCIDR string,
) error {
	values, err := parseEnvironmentFile(staged.path("etc/sandbox0/ctld.env"))
	if err != nil {
		return err
	}
	expected := map[string]string{
		"SANDBOX0_NODE_NAME":     nodeName,
		"SANDBOX0_NOMAD_NODE_ID": nodeID,
		"SANDBOX0_NODE_UID":      nodeUID,
		"SANDBOX0_REGION_ID":     regionID,
		"SANDBOX0_CLUSTER_ID":    clusterID,
	}
	for key, value := range expected {
		if values[key] != value {
			return fmt.Errorf("rendered ctld environment does not bind exact %s", key)
		}
	}
	conflist, err := os.ReadFile(staged.path("opt/cni/config/10-sandbox0.conflist"))
	if err != nil {
		return err
	}
	if !bytes.Contains(conflist, []byte(allocationCIDR)) {
		return errors.New("rendered CNI configuration does not bind the allocated node CIDR")
	}
	return nil
}

func parseEnvironmentFile(file string) (map[string]string, error) {
	handle, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer handle.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(io.LimitReader(handle, 1<<20))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || strings.TrimSpace(key) != key || key == "" {
			return nil, errors.New("rendered ctld environment contains an invalid assignment")
		}
		values[key] = value
	}
	return values, scanner.Err()
}

func atomicWriteFile(destination string, payload []byte, mode fs.FileMode) error {
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination || mode&0o022 != 0 {
		return fmt.Errorf("unsafe node bootstrap destination %q", destination)
	}
	if err := ensureCanonicalDirectory(filepath.Dir(destination), 0o755); err != nil {
		return err
	}
	if info, err := os.Lstat(destination); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing to replace non-regular node bootstrap path %s", destination)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".sandbox0-node-bootstrap.")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryName, destination)
}

func ensureCanonicalDirectory(directory string, mode fs.FileMode) error {
	directory = filepath.Clean(directory)
	if !filepath.IsAbs(directory) {
		return errors.New("node bootstrap directory must be absolute")
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(directory, "/"), "/") {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, mode); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("node bootstrap parent %s is not a canonical directory", current)
		}
	}
	return nil
}
