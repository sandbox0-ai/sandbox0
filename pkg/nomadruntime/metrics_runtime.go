package nomadruntime

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	runtimeMetricBundleMaxBytes     = 1 << 20
	runtimeMetricCgroupStatMaxBytes = 64 << 10
	runtimeMetricCgroupStatMaxLines = 1024
)

type runtimeMetricBinding struct {
	Target           RuntimeMetricTarget
	Parent           string
	MemoryCgroupPath string
}

type runtimeMetricSessionReader interface {
	RecoverySession(string) (rootfssession.RecoverySession, error)
}

var _ runtimeMetricProvider = (*nodeRuntime)(nil)
var _ runtimeMetricSessionReader = (*rootfsRuntime)(nil)

// ListRuntimeMetricTargets derives a bounded projection from the existing
// writer/slot journals and the exact immutable OCI assignment. It stores no
// additional lifecycle authority and never lists guest-controlled paths.
func (d *nodeRuntime) ListRuntimeMetricTargets(ctx context.Context) ([]RuntimeMetricTarget, error) {
	if d.runtime == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return nil, err
	}
	targets := make([]RuntimeMetricTarget, 0)
	bindings := make(map[string]runtimeMetricBinding)
	for _, session := range sessions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !metricSessionLive(session, time.Now()) {
			continue
		}
		binding, err := d.runtimeMetricBinding(session)
		if errors.Is(err, errdefs.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("validate live runtime metric binding: %w", err)
		}
		targets = append(targets, binding.Target)
		if len(targets) > RuntimeMetricMaxTargets {
			return nil, errdefs.ErrResourceExhausted
		}
		bindings[binding.Target.BindingDigest] = binding
	}
	targets, err = NormalizeRuntimeMetricTargets(targets)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.metricTargets = bindings
	d.mu.Unlock()
	return targets, nil
}

// RuntimeMetricStats uses the caller's target only as an exact lookup key.
// The durable binding is checked before and after stock runsc sampling, so
// deletion, pause or a new runtime generation cannot relabel an old sample.
func (d *nodeRuntime) RuntimeMetricStats(ctx context.Context, target RuntimeMetricTarget) (RuntimeMetricSample, error) {
	if err := target.Validate(); err != nil {
		return RuntimeMetricSample{}, errdefs.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return RuntimeMetricSample{}, err
	}
	d.mu.Lock()
	binding, present := d.metricTargets[target.BindingDigest]
	d.mu.Unlock()
	if !present || binding.Target != target {
		return RuntimeMetricSample{}, errdefs.ErrNotFound
	}
	reader, ok := d.runtime.(runtimeMetricSessionReader)
	if !ok || d.runner == nil {
		return RuntimeMetricSample{}, errdefs.ErrUnavailable
	}
	verify := func() (runtimeMetricBinding, error) {
		if err := ctx.Err(); err != nil {
			return runtimeMetricBinding{}, err
		}
		session, err := reader.RecoverySession(binding.Parent)
		if err != nil {
			return runtimeMetricBinding{}, err
		}
		current, err := d.runtimeMetricBinding(session)
		if err != nil {
			return runtimeMetricBinding{}, err
		}
		if current != binding {
			return runtimeMetricBinding{}, errdefs.ErrNotFound
		}
		return current, nil
	}
	if _, err := verify(); err != nil {
		return RuntimeMetricSample{}, err
	}
	stats, err := d.runner.Stats(ctx, target.RunscContainerID)
	if err != nil {
		return RuntimeMetricSample{}, err
	}
	memory, err := readRuntimeMetricMemoryCgroup(binding.MemoryCgroupPath)
	if err != nil {
		return RuntimeMetricSample{}, err
	}
	if _, err := verify(); err != nil {
		return RuntimeMetricSample{}, err
	}
	result := RuntimeMetricSample{
		Version: RuntimeMetricSampleVersion, ObservedAt: time.Now().UTC(),
		Stats: stats, MemoryCgroup: memory,
	}
	if err := result.Validate(target); err != nil {
		return RuntimeMetricSample{}, err
	}
	return result, nil
}

func metricSessionLive(session rootfssession.RecoverySession, now time.Time) bool {
	if !session.Live || session.State != "ready" || session.Consumer == nil || session.RetireOperationID != "" ||
		session.CrashOperationID != "" || session.ExternalCrash || session.BranchRemoved || session.PressureOperationID != "" {
		return false
	}
	expires, err := session.Consumer.Validate()
	return err == nil && expires.After(now)
}

func (d *nodeRuntime) runtimeMetricBinding(session rootfssession.RecoverySession) (runtimeMetricBinding, error) {
	if !metricSessionLive(session, time.Now()) {
		return runtimeMetricBinding{}, errdefs.ErrNotFound
	}
	stage := session.Stage
	if err := stage.ValidateDurableBinding(); err != nil {
		return runtimeMetricBinding{}, errdefs.ErrFailedPrecondition
	}
	boot, err := os.ReadFile(d.config.RuntimeSlotNodeBootIDFile)
	if err != nil {
		return runtimeMetricBinding{}, err
	}
	if stage.Identity.NodeUID != d.nodeUID || stage.Identity.BootID != strings.TrimSpace(string(boot)) {
		return runtimeMetricBinding{}, errdefs.ErrNotFound
	}
	record, err := d.journal.Get(stage.Identity.SlotNonce)
	if err != nil {
		return runtimeMetricBinding{}, err
	}
	registration, consumer := record.Registration, session.Consumer
	if record.Cleanup != nil || record.Proof != nil || record.CompletedAt != "" {
		return runtimeMetricBinding{}, errdefs.ErrNotFound
	}
	if registration.ClusterID != d.clusterID || registration.NodeID != d.nodeID ||
		registration.NodeBootID != stage.Identity.BootID || registration.AllocationID != stage.Identity.AllocationID ||
		registration.RunscContainerID != consumer.ContainerID || registration.StableMount != consumer.StableMount ||
		registration.MountNamespaceID != consumer.HostMountNamespace || consumer.ActiveKey != stage.Identity.SlotNonce {
		return runtimeMetricBinding{}, errdefs.ErrFailedPrecondition
	}
	payload, err := readRuntimeMetricBundle(consumer.StableMount, d.config.RootFSConsumerMountRoot, uint32(os.Geteuid()))
	if err != nil {
		return runtimeMetricBinding{}, err
	}
	return runtimeMetricBindingFromBundle(payload, session, d.config.RuntimeResourceCgroupRoot)
}

func runtimeMetricBindingFromBundle(
	payload []byte,
	session rootfssession.RecoverySession,
	resourceCgroupRoot string,
) (runtimeMetricBinding, error) {
	var spec specs.Spec
	if len(payload) > runtimeMetricBundleMaxBytes || json.Unmarshal(payload, &spec) != nil || spec.Process == nil ||
		spec.Linux == nil || spec.Linux.Resources == nil || spec.Root == nil || spec.Root.Path != "rootfs" {
		return runtimeMetricBinding{}, errors.New("invalid runtime metric OCI bundle")
	}
	stage := session.Stage
	if spec.Annotations["com.sandbox0.alloc-id"] != stage.Identity.AllocationID ||
		spec.Annotations["com.sandbox0.task-id"] != stage.Identity.SlotNonce {
		return runtimeMetricBinding{}, errdefs.ErrFailedPrecondition
	}
	var assignment runtimecontrol.Assignment
	assignments := 0
	for _, env := range spec.Process.Env {
		if raw, found := strings.CutPrefix(env, runtimecontrol.EnvStaticAssignment+"="); found {
			assignments++
			if len(raw) > protocol.MaxRuntimeAssignmentBytes || json.Unmarshal([]byte(raw), &assignment) != nil {
				return runtimeMetricBinding{}, errors.New("invalid static runtime metric assignment")
			}
		}
	}
	revision, err := assignment.Revision()
	if assignments != 1 || err != nil || revision != stage.Labels[protocol.RuntimeAssignmentRevisionLabel] {
		return runtimeMetricBinding{}, errdefs.ErrFailedPrecondition
	}
	cpu, memory := spec.Linux.Resources.CPU, spec.Linux.Resources.Memory
	if cpu == nil || cpu.Period == nil || cpu.Quota == nil || *cpu.Period == 0 || *cpu.Period > 1_000_000 ||
		*cpu.Quota <= 0 || *cpu.Quota > maxRuntimeMetricCPUMilli*1000 ||
		(*cpu.Quota*1000)%int64(*cpu.Period) != 0 || memory == nil || memory.Limit == nil ||
		*memory.Limit <= 0 || *memory.Limit%(1<<20) != 0 {
		return runtimeMetricBinding{}, errors.New("invalid runtime metric resource limits")
	}
	memoryCgroupPath, err := runtimeMetricMemoryCgroupHostPath(spec.Linux.CgroupsPath, resourceCgroupRoot)
	if err != nil {
		return runtimeMetricBinding{}, err
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		return runtimeMetricBinding{}, err
	}
	target := RuntimeMetricTarget{Version: RuntimeMetricTargetVersion, TeamID: assignment.TeamID,
		SandboxID: assignment.SandboxID, RuntimeGeneration: assignment.RuntimeGeneration,
		CPUMillicpu: *cpu.Quota * 1000 / int64(*cpu.Period), MemoryMiB: *memory.Limit / (1 << 20),
		AllocationID: stage.Identity.AllocationID, NodeBootID: stage.Identity.BootID,
		LaunchAttempt: stage.Identity.LaunchAttempt, RunscContainerID: session.Consumer.ContainerID,
		BindingDigest: hex.EncodeToString(digest[:]),
		SeriesEpoch:   RuntimeMetricSeriesEpoch(stage.Identity.AllocationID, stage.Identity.BootID, stage.Identity.LaunchAttempt, session.Consumer.ContainerID)}
	if err := target.Validate(); err != nil {
		return runtimeMetricBinding{}, err
	}
	return runtimeMetricBinding{
		Target: target, Parent: session.Stage.Parent, MemoryCgroupPath: memoryCgroupPath,
	}, nil
}

func runtimeMetricMemoryCgroupHostPath(cgroupsPath, resourceCgroupRoot string) (string, error) {
	if cgroupsPath == "" {
		return "", nil
	}
	resourceCgroupRoot = strings.TrimSpace(resourceCgroupRoot)
	if resourceCgroupRoot == "" {
		resourceCgroupRoot = protocol.RuntimeResourceCgroupRoot
	}
	const cgroupV2Mount = "/sys/fs/cgroup"
	if !filepath.IsAbs(resourceCgroupRoot) || filepath.Clean(resourceCgroupRoot) != resourceCgroupRoot ||
		resourceCgroupRoot == string(filepath.Separator) {
		return "", fmt.Errorf("runtime resource cgroup root must be canonical: %w", errdefs.ErrFailedPrecondition)
	}
	relativeRoot, err := filepath.Rel(cgroupV2Mount, resourceCgroupRoot)
	if err != nil || relativeRoot == "." || relativeRoot == ".." || filepath.IsAbs(relativeRoot) ||
		strings.HasPrefix(relativeRoot, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("runtime resource cgroup root must be below %s: %w", cgroupV2Mount, errdefs.ErrFailedPrecondition)
	}
	if !filepath.IsAbs(cgroupsPath) || filepath.Clean(cgroupsPath) != cgroupsPath || cgroupsPath == string(filepath.Separator) {
		return "", fmt.Errorf("runtime metric OCI cgroups path must be canonical: %w", errdefs.ErrFailedPrecondition)
	}
	hostPath := filepath.Join(cgroupV2Mount, cgroupsPath)
	if filepath.Dir(hostPath) != filepath.Clean(resourceCgroupRoot) {
		return "", fmt.Errorf("runtime metric OCI cgroups path escapes the resource cgroup root: %w", errdefs.ErrPermissionDenied)
	}
	return hostPath, nil
}

func readRuntimeMetricMemoryCgroup(path string) (*RuntimeMetricMemoryCgroup, error) {
	if path == "" {
		return nil, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect runtime memory cgroup: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || stat.Uid != uint32(os.Geteuid()) ||
		info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("runtime memory cgroup is not runtime-owned canonical directory: %w", errdefs.ErrPermissionDenied)
	}
	current, err := readRuntimeMetricCgroupNumber(filepath.Join(path, "memory.current"))
	if err != nil {
		return nil, err
	}
	inactiveFile, err := readRuntimeMetricCgroupStat(filepath.Join(path, "memory.stat"), "inactive_file")
	if err != nil {
		return nil, err
	}
	return &RuntimeMetricMemoryCgroup{CurrentBytes: current, InactiveFileBytes: inactiveFile}, nil
}

func readRuntimeMetricCgroupNumber(path string) (uint64, error) {
	payload, err := readRuntimeMetricCgroupFile(path, 32)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(payload, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", filepath.Base(path), errdefs.ErrFailedPrecondition)
	}
	return value, nil
}

func readRuntimeMetricCgroupStat(path, want string) (uint64, error) {
	payload, err := readRuntimeMetricCgroupFile(path, runtimeMetricCgroupStatMaxBytes)
	if err != nil {
		return 0, err
	}
	found := false
	var value uint64
	for lineIndex, line := range strings.Split(payload, "\n") {
		if lineIndex >= runtimeMetricCgroupStatMaxLines {
			return 0, fmt.Errorf("%s exceeds %d lines: %w", filepath.Base(path), runtimeMetricCgroupStatMaxLines, errdefs.ErrFailedPrecondition)
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 {
			return 0, fmt.Errorf("parse %s line %d: %w", filepath.Base(path), lineIndex+1, errdefs.ErrFailedPrecondition)
		}
		if fields[0] != want {
			continue
		}
		if found {
			return 0, fmt.Errorf("%s duplicates %s: %w", filepath.Base(path), want, errdefs.ErrFailedPrecondition)
		}
		parsed, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s %s: %w", filepath.Base(path), want, errdefs.ErrFailedPrecondition)
		}
		value, found = parsed, true
	}
	if !found {
		return 0, fmt.Errorf("%s lacks %s: %w", filepath.Base(path), want, errdefs.ErrFailedPrecondition)
	}
	return value, nil
}

func readRuntimeMetricCgroupFile(path string, maxBytes int) (string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("read runtime memory cgroup %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil {
		return "", fmt.Errorf("read runtime memory cgroup %s: %w", filepath.Base(path), err)
	}
	if len(payload) > maxBytes {
		return "", fmt.Errorf("runtime memory cgroup %s exceeds %d bytes: %w", filepath.Base(path), maxBytes, errdefs.ErrFailedPrecondition)
	}
	return strings.TrimSpace(string(payload)), nil
}

// Only read the sibling config.json of an already registered host mount.
// The root, bundle and file must be protected runtime-owned paths. Nomad's
// intermediate task directory is deliberately writable by nobody; it is not
// identity evidence. Reject every symlink, confine opens with os.Root, and
// authenticate the bundle assignment against the durable writer digest.
func readRuntimeMetricBundle(stableMount, root string, ownerUID uint32) ([]byte, error) {
	if err := validateConsumerMountPath(stableMount, root); err != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	if filepath.Base(stableMount) != "rootfs" {
		return nil, errdefs.ErrFailedPrecondition
	}
	bundle := filepath.Dir(stableMount)
	relative, err := filepath.Rel(root, bundle)
	if err != nil {
		return nil, err
	}
	opened, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	defer opened.Close()
	for part := relative; ; part = filepath.Dir(part) {
		info, err := opened.Lstat(part)
		if err != nil {
			return nil, err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		protectedBoundary := part == relative || part == "."
		if !ok || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
			(protectedBoundary && (stat.Uid != ownerUID || info.Mode().Perm()&0o022 != 0)) {
			return nil, errdefs.ErrPermissionDenied
		}
		if part == "." {
			break
		}
	}
	file, err := opened.OpenFile(filepath.Join(relative, "config.json"), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Uid != ownerUID || info.Mode().Perm()&0o022 != 0 || info.Size() > runtimeMetricBundleMaxBytes {
		return nil, errdefs.ErrPermissionDenied
	}
	payload, err := io.ReadAll(io.LimitReader(file, runtimeMetricBundleMaxBytes+1))
	if err != nil || len(payload) > runtimeMetricBundleMaxBytes {
		return nil, errors.New("cannot read bounded runtime metric bundle")
	}
	return payload, nil
}
