package volume

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
)

const defaultVolumeCacheRoot = "/var/lib/sandbox0/juicefs-cache"

type JuiceFSStageProvider struct {
	Config      *apiconfig.StorageProxyConfig
	JuiceFSBin  string
	StagingRoot string
	CacheRoot   string
	Runner      CommandRunner

	mu      sync.Mutex
	mounted map[string]struct{}
}

func NewJuiceFSStageProvider(cfg *apiconfig.StorageProxyConfig, juicefsBin, stagingRoot, cacheRoot string, runner CommandRunner) *JuiceFSStageProvider {
	if strings.TrimSpace(juicefsBin) == "" {
		juicefsBin = "juicefs"
	}
	if strings.TrimSpace(stagingRoot) == "" {
		stagingRoot = defaultStagingRoot
	}
	if strings.TrimSpace(cacheRoot) == "" {
		cacheRoot = defaultVolumeCacheRoot
	}
	return &JuiceFSStageProvider{
		Config:      cfg,
		JuiceFSBin:  juicefsBin,
		StagingRoot: filepath.Clean(stagingRoot),
		CacheRoot:   filepath.Clean(cacheRoot),
		Runner:      runner,
		mounted:     make(map[string]struct{}),
	}
}

func (p *JuiceFSStageProvider) EnsureStaged(ctx context.Context, req ctldapi.VolumeAttachRequest) (string, error) {
	if p == nil || p.Config == nil {
		return "", fmt.Errorf("ctld JuiceFS staging is not configured")
	}
	volumeID, err := cleanVolumeID(req.SandboxVolumeID)
	if err != nil {
		return "", err
	}
	teamID := strings.TrimSpace(req.TeamID)
	if teamID == "" {
		return "", fmt.Errorf("team_id is required for ctld JuiceFS staging")
	}

	stageDir := filepath.Join(p.StagingRoot, volumeID)
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return "", fmt.Errorf("create volume staging dir: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.mounted[volumeID]; ok {
		return stageDir, nil
	}
	if mounted, err := isMountPoint(stageDir); err != nil {
		return "", err
	} else if mounted {
		p.mounted[volumeID] = struct{}{}
		return stageDir, nil
	}

	args, err := p.mountArgs(req, stageDir)
	if err != nil {
		return "", err
	}
	if p.Runner != nil {
		if err := p.Runner.Run(ctx, p.JuiceFSBin, args...); err != nil {
			return "", err
		}
	} else if err := startBackgroundCommand(ctx, p.JuiceFSBin, args...); err != nil {
		return "", err
	}
	if err := waitForMountPoint(ctx, stageDir, 10*time.Second); err != nil {
		return "", err
	}
	p.mounted[volumeID] = struct{}{}
	return stageDir, nil
}

func (p *JuiceFSStageProvider) Release(_ context.Context, _ ctldapi.VolumeDetachRequest) error {
	// Keep node-local JuiceFS mounts warm across dynamic sandbox attaches. A
	// later idle reaper can reclaim these without putting storage-proxy back in
	// the per-file-operation path.
	return nil
}

func (p *JuiceFSStageProvider) mountArgs(req ctldapi.VolumeAttachRequest, stageDir string) ([]string, error) {
	volumeID, err := cleanVolumeID(req.SandboxVolumeID)
	if err != nil {
		return nil, err
	}
	prefix, err := naming.S3VolumePrefix(req.TeamID, volumeID)
	if err != nil {
		return nil, err
	}
	volumePath, err := naming.JuiceFSVolumePath(volumeID)
	if err != nil {
		return nil, err
	}
	cacheDir := filepath.Join(p.CacheRoot, volumeID)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create JuiceFS cache dir: %w", err)
	}

	args := []string{
		"mount",
		"--subdir", volumePath,
		"--storage", normalizeObjectStorageType(p.Config.ObjectStorageType),
		"--bucket", p.Config.S3Bucket,
		"--cache-dir", cacheDir,
	}
	if p.Config.S3Region != "" {
		args = append(args, "--region", p.Config.S3Region)
	}
	if p.Config.S3Endpoint != "" {
		args = append(args, "--endpoint", p.Config.S3Endpoint)
	}
	if p.Config.S3AccessKey != "" {
		args = append(args, "--access-key", p.Config.S3AccessKey)
	}
	if p.Config.S3SecretKey != "" {
		args = append(args, "--secret-key", p.Config.S3SecretKey)
	}
	if p.Config.S3SessionToken != "" {
		args = append(args, "--session-token", p.Config.S3SessionToken)
	}
	if prefix != "" {
		args = append(args, "--object-prefix", prefix)
	}
	if p.Config.JuiceFSEncryptionKeyPath != "" {
		args = append(args, "--encryption-key-path", p.Config.JuiceFSEncryptionKeyPath)
	}
	if p.Config.JuiceFSEncryptionPassphrase != "" {
		args = append(args, "--encryption-passphrase", p.Config.JuiceFSEncryptionPassphrase)
	}
	if p.Config.JuiceFSEncryptionAlgo != "" {
		args = append(args, "--encryption-algo", p.Config.JuiceFSEncryptionAlgo)
	}
	if req.CacheSize != "" {
		args = append(args, "--cache-size", req.CacheSize)
	}
	if req.Prefetch > 0 {
		args = append(args, "--prefetch", strconv.Itoa(int(req.Prefetch)))
	}
	if req.BufferSize != "" {
		args = append(args, "--buffer-size", req.BufferSize)
	}
	if req.Writeback {
		args = append(args, "--writeback")
	}
	if p.Config.JuiceFSAttrTimeout != "" {
		args = append(args, "--attr-cache", p.Config.JuiceFSAttrTimeout)
	}
	if p.Config.JuiceFSEntryTimeout != "" {
		args = append(args, "--entry-cache", p.Config.JuiceFSEntryTimeout)
	}
	if p.Config.JuiceFSDirEntryTimeout != "" {
		args = append(args, "--dir-entry-cache", p.Config.JuiceFSDirEntryTimeout)
	}
	if strings.EqualFold(strings.TrimSpace(req.AccessMode), "ROX") {
		args = append(args, "--read-only")
	}
	args = append(args, p.Config.MetaURL, stageDir)
	return args, nil
}

func startBackgroundCommand(ctx context.Context, name string, args ...string) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return nil
}

func normalizeObjectStorageType(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	switch value {
	case "", "builtin":
		return "s3"
	default:
		return value
	}
}

func isMountPoint(path string) (bool, error) {
	cleaned, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("read mountinfo: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && fields[4] == cleaned {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func waitForMountPoint(ctx context.Context, path string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		mounted, err := isMountPoint(path)
		if err != nil {
			return err
		}
		if mounted {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for JuiceFS mount at %s", path)
		case <-ticker.C:
		}
	}
}
