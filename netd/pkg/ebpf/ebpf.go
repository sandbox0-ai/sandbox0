// Package ebpf provides eBPF-based network traffic control for netd.
// It uses tc-bpf for bandwidth limiting and traffic shaping.
package ebpf

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Manager manages eBPF programs for traffic control
type Manager struct {
	logger     *zap.Logger
	bpfFSPath  string
	pinPath    string
	useEDT     bool // Use Earliest Departure Time for pacing
	edtHorizon time.Duration
	mu         sync.RWMutex
	programs   map[string]*Program
	queueDiscs map[string]*QDisc
}

// Program represents a loaded eBPF program
type Program struct {
	Name    string
	Path    string
	PinPath string
	Iface   string
	Type    ProgramType
	Loaded  bool
}

// ProgramType represents the type of eBPF program
type ProgramType string

const (
	ProgramTypeTC       ProgramType = "tc"
	ProgramTypeXDP      ProgramType = "xdp"
	ProgramTypeCgroup   ProgramType = "cgroup"
	ProgramTypeSchedCLS ProgramType = "sched_cls"
)

// QDisc represents a tc queueing discipline
type QDisc struct {
	Iface   string
	Handle  string
	Type    QDiscType
	Options map[string]string
}

// QDiscType represents the type of qdisc
type QDiscType string

const (
	QDiscTypeHTB   QDiscType = "htb"
	QDiscTypeFQ    QDiscType = "fq"
	QDiscTypeFQCOD QDiscType = "fq_codel"
	QDiscTypeBPF   QDiscType = "clsact"
)

// RateLimitConfig configures rate limiting for an interface
type RateLimitConfig struct {
	SandboxID      string
	Iface          string
	EgressRateBps  int64
	IngressRateBps int64
	BurstBytes     int64
	UseBPF         bool
	EDTHorizon     time.Duration
}

// NewManager creates a new eBPF Manager
func NewManager(logger *zap.Logger, bpfFSPath string, pinPathName string, useEDT bool, edtHorizon time.Duration) (*Manager, error) {
	if bpfFSPath == "" {
		bpfFSPath = "/sys/fs/bpf"
	}

	if pinPathName == "" {
		pinPathName = "netd"
	}

	pinPath := filepath.Join(bpfFSPath, pinPathName)

	// Check if bpffs is mounted
	if err := checkBPFFS(bpfFSPath); err != nil {
		logger.Warn("BPF filesystem not available, falling back to non-eBPF mode",
			zap.String("path", bpfFSPath),
			zap.Error(err),
		)
	}

	// Create pin path directory
	if err := os.MkdirAll(pinPath, 0700); err != nil {
		logger.Warn("Failed to create BPF pin path",
			zap.String("path", pinPath),
			zap.Error(err),
		)
	}

	return &Manager{
		logger:     logger,
		bpfFSPath:  bpfFSPath,
		pinPath:    pinPath,
		useEDT:     useEDT,
		edtHorizon: edtHorizon,
		programs:   make(map[string]*Program),
		queueDiscs: make(map[string]*QDisc),
	}, nil
}

// Initialize sets up the eBPF environment
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info("Initializing eBPF manager",
		zap.String("pinPath", m.pinPath),
		zap.Bool("useEDT", m.useEDT),
	)

	// Check eBPF support
	if err := m.checkEBPFSupport(); err != nil {
		m.logger.Warn("eBPF support limited", zap.Error(err))
	}

	return nil
}

// IsAvailable checks if eBPF is available on this system
func (m *Manager) IsAvailable() bool {
	return checkBPFFS(m.bpfFSPath) == nil
}

// ApplyRateLimit applies rate limiting using tc with optional eBPF
func (m *Manager) ApplyRateLimit(ctx context.Context, cfg *RateLimitConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info("Applying rate limit",
		zap.String("sandboxID", cfg.SandboxID),
		zap.String("iface", cfg.Iface),
		zap.Int64("egressRateBps", cfg.EgressRateBps),
		zap.Int64("ingressRateBps", cfg.IngressRateBps),
		zap.Bool("useBPF", cfg.UseBPF),
	)

	if cfg.UseBPF && m.IsAvailable() {
		return m.applyBPFRateLimit(ctx, cfg)
	}

	return m.applyTCRateLimit(ctx, cfg)
}

// applyBPFRateLimit applies rate limiting using tc-bpf with fq qdisc
func (m *Manager) applyBPFRateLimit(ctx context.Context, cfg *RateLimitConfig) error {
	// Use fq (Fair Queue) qdisc with EDT (Earliest Departure Time) for precise pacing
	// This is more efficient than HTB for rate limiting

	// First, set up clsact qdisc for BPF attachment
	if err := m.setupClsact(ctx, cfg.Iface); err != nil {
		m.logger.Warn("Failed to setup clsact, falling back to TC", zap.Error(err))
		return m.applyTCRateLimit(ctx, cfg)
	}

	// Apply fq qdisc for egress with rate limit
	if cfg.EgressRateBps > 0 {
		if err := m.applyFQQdisc(ctx, cfg.Iface, cfg.EgressRateBps, cfg.BurstBytes); err != nil {
			m.logger.Warn("Failed to apply fq qdisc", zap.Error(err))
			return m.applyTCRateLimit(ctx, cfg)
		}
	}

	// For ingress rate limiting, we use policing
	if cfg.IngressRateBps > 0 {
		if err := m.applyIngressPolicing(ctx, cfg.Iface, cfg.IngressRateBps, cfg.BurstBytes); err != nil {
			m.logger.Warn("Failed to apply ingress policing", zap.Error(err))
		}
	}

	return nil
}

// setupClsact sets up clsact qdisc for BPF programs
func (m *Manager) setupClsact(ctx context.Context, iface string) error {
	// Check if clsact already exists
	output, err := exec.CommandContext(ctx, "tc", "qdisc", "show", "dev", iface).Output()
	if err == nil && strings.Contains(string(output), "clsact") {
		return nil
	}

	// Add clsact qdisc
	cmd := exec.CommandContext(ctx, "tc", "qdisc", "add", "dev", iface, "clsact")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add clsact: %w (%s)", err, string(output))
	}

	m.queueDiscs[iface+":clsact"] = &QDisc{
		Iface:  iface,
		Handle: "ffff:fff0",
		Type:   QDiscTypeBPF,
	}

	return nil
}

// applyFQQdisc applies fq (Fair Queue) qdisc with rate limiting
func (m *Manager) applyFQQdisc(ctx context.Context, iface string, rateBps int64, burstBytes int64) error {
	// Delete existing qdisc (ignore errors)
	exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", iface, "root").Run()

	// Calculate flow limit (rate / MTU * 2)
	flowLimit := rateBps / 8 / 1500 * 2
	if flowLimit < 100 {
		flowLimit = 100
	}

	// Add fq qdisc with rate limiting
	// fq with maxrate provides EDT-based pacing
	args := []string{
		"qdisc", "add", "dev", iface, "root", "handle", "1:", "fq",
		"maxrate", fmt.Sprintf("%dbit", rateBps),
		"flow_limit", fmt.Sprintf("%d", flowLimit),
	}

	if m.useEDT {
		args = append(args, "horizon", m.edtHorizon.String())
	}

	cmd := exec.CommandContext(ctx, "tc", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add fq qdisc: %w (%s)", err, string(output))
	}

	m.queueDiscs[iface+":fq"] = &QDisc{
		Iface:  iface,
		Handle: "1:",
		Type:   QDiscTypeFQ,
		Options: map[string]string{
			"maxrate": fmt.Sprintf("%dbit", rateBps),
		},
	}

	return nil
}

// applyIngressPolicing applies ingress rate limiting using policing
func (m *Manager) applyIngressPolicing(ctx context.Context, iface string, rateBps int64, burstBytes int64) error {
	// Delete existing ingress qdisc (ignore errors)
	exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", iface, "ingress").Run()

	// Add ingress qdisc
	cmd := exec.CommandContext(ctx, "tc", "qdisc", "add", "dev", iface, "handle", "ffff:", "ingress")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add ingress qdisc: %w (%s)", err, string(output))
	}

	// Add policing filter
	burst := burstBytes
	if burst == 0 {
		burst = rateBps / 8 // 1 second worth of data
	}

	filterArgs := []string{
		"filter", "add", "dev", iface, "parent", "ffff:",
		"protocol", "all", "prio", "1",
		"basic", "police",
		"rate", fmt.Sprintf("%dbit", rateBps),
		"burst", fmt.Sprintf("%d", burst),
		"drop",
	}

	cmd = exec.CommandContext(ctx, "tc", filterArgs...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add policing filter: %w (%s)", err, string(output))
	}

	return nil
}

// applyTCRateLimit applies rate limiting using traditional tc htb
func (m *Manager) applyTCRateLimit(ctx context.Context, cfg *RateLimitConfig) error {
	// Delete existing qdisc (ignore errors)
	exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", cfg.Iface, "root").Run()

	// Add htb qdisc
	cmd := exec.CommandContext(ctx, "tc", "qdisc", "add", "dev", cfg.Iface, "root", "handle", "1:", "htb", "default", "10")
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add htb qdisc: %w (%s)", err, string(output))
	}

	// Add class for rate limiting
	burst := cfg.BurstBytes
	if burst == 0 {
		burst = cfg.EgressRateBps / 8
	}

	classArgs := []string{
		"class", "add", "dev", cfg.Iface,
		"parent", "1:", "classid", "1:10",
		"htb", "rate", fmt.Sprintf("%dbit", cfg.EgressRateBps),
		"burst", fmt.Sprintf("%d", burst),
	}

	cmd = exec.CommandContext(ctx, "tc", classArgs...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("add htb class: %w (%s)", err, string(output))
	}

	m.queueDiscs[cfg.Iface+":htb"] = &QDisc{
		Iface:  cfg.Iface,
		Handle: "1:",
		Type:   QDiscTypeHTB,
		Options: map[string]string{
			"rate": fmt.Sprintf("%dbit", cfg.EgressRateBps),
		},
	}

	// Apply ingress policing if needed
	if cfg.IngressRateBps > 0 {
		if err := m.applyIngressPolicing(ctx, cfg.Iface, cfg.IngressRateBps, cfg.BurstBytes); err != nil {
			m.logger.Warn("Failed to apply ingress policing", zap.Error(err))
		}
	}

	return nil
}

// RemoveRateLimit removes rate limiting from an interface
func (m *Manager) RemoveRateLimit(ctx context.Context, iface string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove root qdisc
	exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", iface, "root").Run()

	// Remove ingress qdisc
	exec.CommandContext(ctx, "tc", "qdisc", "del", "dev", iface, "ingress").Run()

	// Clean up tracking
	delete(m.queueDiscs, iface+":htb")
	delete(m.queueDiscs, iface+":fq")
	delete(m.queueDiscs, iface+":clsact")

	return nil
}

// AttachBPFProgram attaches a BPF program to tc
func (m *Manager) AttachBPFProgram(ctx context.Context, iface, progPath, section string, ingress bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Ensure clsact qdisc exists
	if err := m.setupClsact(ctx, iface); err != nil {
		return fmt.Errorf("setup clsact: %w", err)
	}

	direction := "egress"
	if ingress {
		direction = "ingress"
	}

	// Attach BPF program
	args := []string{
		"filter", "add", "dev", iface, direction,
		"bpf", "direct-action", "obj", progPath,
	}
	if section != "" {
		args = append(args, "sec", section)
	}

	cmd := exec.CommandContext(ctx, "tc", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("attach bpf: %w (%s)", err, string(output))
	}

	m.programs[fmt.Sprintf("%s:%s:%s", iface, direction, progPath)] = &Program{
		Name:   filepath.Base(progPath),
		Path:   progPath,
		Iface:  iface,
		Type:   ProgramTypeSchedCLS,
		Loaded: true,
	}

	return nil
}

// DetachBPFProgram detaches a BPF program from tc
func (m *Manager) DetachBPFProgram(ctx context.Context, iface string, ingress bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	direction := "egress"
	if ingress {
		direction = "ingress"
	}

	cmd := exec.CommandContext(ctx, "tc", "filter", "del", "dev", iface, direction)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("detach bpf: %w (%s)", err, string(output))
	}

	// Clean up tracking
	for key := range m.programs {
		if strings.HasPrefix(key, fmt.Sprintf("%s:%s:", iface, direction)) {
			delete(m.programs, key)
		}
	}

	return nil
}

// Cleanup removes all eBPF resources
func (m *Manager) Cleanup(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Info("Cleaning up eBPF manager")

	// Note: We don't remove qdiscs here as they are per-sandbox
	// and should be cleaned up individually

	m.programs = make(map[string]*Program)
	m.queueDiscs = make(map[string]*QDisc)

	return nil
}

// checkBPFFS checks if bpffs is mounted
func checkBPFFS(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", path)
	}

	// Try to read bpf maps directory
	mapsPath := filepath.Join(path, "tc")
	if _, err := os.Stat(mapsPath); os.IsNotExist(err) {
		// Try to create directory to verify write access
		if err := os.MkdirAll(mapsPath, 0700); err != nil {
			return fmt.Errorf("cannot write to bpffs: %w", err)
		}
	}

	return nil
}

// checkEBPFSupport checks eBPF support level
func (m *Manager) checkEBPFSupport() error {
	// Check for bpf syscall support
	cmd := exec.Command("bpftool", "feature")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("bpftool not available: %w", err)
	}

	// Log available features
	m.logger.Debug("eBPF features", zap.String("output", string(output)))

	return nil
}

// GetStats returns statistics for the managed qdiscs
func (m *Manager) GetStats(ctx context.Context, iface string) (map[string]any, error) {
	cmd := exec.CommandContext(ctx, "tc", "-s", "qdisc", "show", "dev", iface)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("get tc stats: %w", err)
	}

	// Parse output (simplified)
	stats := map[string]any{
		"raw": string(output),
	}

	return stats, nil
}
