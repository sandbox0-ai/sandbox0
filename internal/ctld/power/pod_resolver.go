package power

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const defaultCgroupRoot = "/sys/fs/cgroup"
const defaultProcRoot = "/proc"

type PodResolver struct {
	K8sClient  kubernetes.Interface
	NodeName   string
	CgroupRoot string
	ProcRoot   string
	Adapters   []RuntimeAdapter
}

type RuntimeAdapter interface {
	Name() string
	Matches(pod *corev1.Pod) bool
	ResolveTarget(resolver *PodResolver, pod *corev1.Pod, podCgroupDir string, base Target) (Target, error)
}

func NewPodResolver(k8sClient kubernetes.Interface, nodeName, cgroupRoot string) *PodResolver {
	if strings.TrimSpace(cgroupRoot) == "" {
		cgroupRoot = defaultCgroupRoot
	}
	return &PodResolver{
		K8sClient:  k8sClient,
		NodeName:   strings.TrimSpace(nodeName),
		CgroupRoot: filepath.Clean(cgroupRoot),
		ProcRoot:   defaultProcRoot,
		Adapters: []RuntimeAdapter{
			KataRuntimeAdapter{},
			GVisorRuntimeAdapter{},
			RuncRuntimeAdapter{},
		},
	}
}

func (r *PodResolver) Resolve(req *http.Request, sandboxID string) (Target, error) {
	if r == nil || r.K8sClient == nil {
		return Target{}, ErrNotImplemented
	}
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	pod, err := r.lookupSandboxPod(ctx, sandboxID)
	if err != nil {
		return Target{}, err
	}
	if r.NodeName != "" && pod.Spec.NodeName != r.NodeName {
		return Target{}, fmt.Errorf("sandbox %s is scheduled on node %s, not %s", sandboxID, pod.Spec.NodeName, r.NodeName)
	}
	base := Target{
		SandboxID:    sandboxID,
		PodNamespace: pod.Namespace,
		PodName:      pod.Name,
		PodUID:       string(pod.UID),
	}
	var adapter RuntimeAdapter
	for _, candidate := range r.runtimeAdapters() {
		if !candidate.Matches(pod) {
			continue
		}
		adapter = candidate
		break
	}
	if adapter == nil {
		return Target{}, ErrNotImplemented
	}
	podCgroupDir, err := r.resolvePodCgroupDir(pod)
	if err != nil {
		return Target{}, err
	}
	return adapter.ResolveTarget(r, pod, podCgroupDir, base)
}

func isKataRuntimeClassName(runtimeClassName *string) bool {
	if runtimeClassName == nil {
		return false
	}
	raw := strings.ToLower(strings.TrimSpace(*runtimeClassName))
	return strings.Contains(raw, "kata")
}

func isGVisorRuntimeClassName(runtimeClassName *string) bool {
	if runtimeClassName == nil {
		return false
	}
	raw := strings.ToLower(strings.TrimSpace(*runtimeClassName))
	return strings.Contains(raw, "gvisor") || strings.Contains(raw, "runsc")
}

func isRuncRuntimeClassName(runtimeClassName *string) bool {
	if runtimeClassName == nil {
		return true
	}
	raw := strings.ToLower(strings.TrimSpace(*runtimeClassName))
	if raw == "" {
		return true
	}
	return strings.Contains(raw, "runc")
}

func (r *PodResolver) runtimeAdapters() []RuntimeAdapter {
	if r == nil || len(r.Adapters) == 0 {
		return nil
	}
	return r.Adapters
}

type KataRuntimeAdapter struct{}

func (KataRuntimeAdapter) Name() string { return "kata" }

func (KataRuntimeAdapter) Matches(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return isKataRuntimeClassName(pod.Spec.RuntimeClassName)
}

func (KataRuntimeAdapter) ResolveTarget(resolver *PodResolver, pod *corev1.Pod, podCgroupDir string, base Target) (Target, error) {
	cgroupDir, err := resolver.resolveKataSandboxCgroupDir(pod, podCgroupDir)
	if err != nil {
		return Target{}, err
	}
	base.Runtime = "kata"
	base.CgroupDir = cgroupDir
	return base, nil
}

type GVisorRuntimeAdapter struct{}

func (GVisorRuntimeAdapter) Name() string { return "gvisor" }

func (GVisorRuntimeAdapter) Matches(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return isGVisorRuntimeClassName(pod.Spec.RuntimeClassName)
}

func (GVisorRuntimeAdapter) ResolveTarget(resolver *PodResolver, pod *corev1.Pod, podCgroupDir string, base Target) (Target, error) {
	if err := resolver.validateGVisorSandboxCgroup(pod, podCgroupDir); err != nil {
		return Target{}, err
	}
	base.Runtime = "gvisor"
	base.CgroupDir = podCgroupDir
	return base, nil
}

type RuncRuntimeAdapter struct{}

func (RuncRuntimeAdapter) Name() string { return "runc" }

func (RuncRuntimeAdapter) Matches(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	return isRuncRuntimeClassName(pod.Spec.RuntimeClassName)
}

func (RuncRuntimeAdapter) ResolveTarget(_ *PodResolver, _ *corev1.Pod, podCgroupDir string, base Target) (Target, error) {
	base.Runtime = "runc"
	base.CgroupDir = podCgroupDir
	return base, nil
}

func (r *PodResolver) resolveKataSandboxCgroupDir(pod *corev1.Pod, podCgroupDir string) (string, error) {
	path, err := r.findKataSandboxCgroupDir(podCgroupDir)
	if err != nil {
		return "", fmt.Errorf("resolve kata sandbox cgroup for sandbox pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return path, nil
}

func (r *PodResolver) findKataSandboxCgroupDir(root string) (string, error) {
	procRoot := strings.TrimSpace(r.ProcRoot)
	if procRoot == "" {
		procRoot = defaultProcRoot
	}
	root = filepath.Clean(root)
	rootDepth := pathDepth(root)
	maxDepth := 6
	var found string
	stopWalk := errors.New("stop walk")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		if path == root {
			return nil
		}
		if pathDepth(path)-rootDepth > maxDepth {
			return filepath.SkipDir
		}
		if !hasCgroupControls(path) || !fileExists(filepath.Join(path, "cgroup.procs")) {
			return nil
		}
		if !hasKataSandboxProcesses(procRoot, path) {
			return nil
		}
		found = path
		return stopWalk
	})
	if err != nil && err != stopWalk {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("kata sandbox cgroup not found under %s; ensure sandbox_cgroup_only=true", root)
	}
	return found, nil
}

func hasKataSandboxProcesses(procRoot, cgroupPath string) bool {
	data, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return false
	}
	for _, field := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || pid <= 0 {
			continue
		}
		if isKataProcess(procRoot, pid) {
			return true
		}
	}
	return false
}

func isKataProcess(procRoot string, pid int) bool {
	markers := []string{"containerd-shim-kata-v2", "kata", "qemu-system", "cloud-hypervisor", "firecracker", "dragonball"}
	return processMatchesMarkers(procRoot, pid, markers)
}

func isGVisorProcess(procRoot string, pid int) bool {
	markers := []string{"containerd-shim-runsc-v1", "runsc", "gvisor"}
	return processMatchesMarkers(procRoot, pid, markers)
}

func processMatchesMarkers(procRoot string, pid int, markers []string) bool {
	for _, candidate := range []string{filepath.Join(procRoot, strconv.Itoa(pid), "cmdline"), filepath.Join(procRoot, strconv.Itoa(pid), "comm")} {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		raw := strings.ToLower(strings.ReplaceAll(string(data), "\x00", " "))
		for _, marker := range markers {
			if strings.Contains(raw, marker) {
				return true
			}
		}
	}
	return false
}

func (r *PodResolver) validateGVisorSandboxCgroup(pod *corev1.Pod, podCgroupDir string) error {
	procRoot := strings.TrimSpace(r.ProcRoot)
	if procRoot == "" {
		procRoot = defaultProcRoot
	}
	if hasRuntimeProcessesInCgroupTree(procRoot, podCgroupDir, isGVisorProcess) {
		return nil
	}
	return fmt.Errorf("resolve gvisor sandbox cgroup for sandbox pod %s/%s: gvisor sandbox processes not found under %s", pod.Namespace, pod.Name, podCgroupDir)
}

func hasRuntimeProcessesInCgroupTree(procRoot, root string, match func(string, int) bool) bool {
	root = filepath.Clean(root)
	rootDepth := pathDepth(root)
	maxDepth := 6
	matched := false
	stopWalk := errors.New("stop walk")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		if pathDepth(path)-rootDepth > maxDepth {
			return filepath.SkipDir
		}
		if !fileExists(filepath.Join(path, "cgroup.procs")) {
			return nil
		}
		if hasMatchingProcesses(procRoot, path, match) {
			matched = true
			return stopWalk
		}
		return nil
	})
	return err == nil && matched || err == stopWalk
}

func hasMatchingProcesses(procRoot, cgroupPath string, match func(string, int) bool) bool {
	data, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.procs"))
	if err != nil {
		return false
	}
	for _, field := range strings.Fields(string(data)) {
		pid, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || pid <= 0 {
			continue
		}
		if match(procRoot, pid) {
			return true
		}
	}
	return false
}

func (r *PodResolver) lookupSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	selector := labels.SelectorFromSet(map[string]string{controller.LabelSandboxID: sandboxID}).String()
	pods, err := r.K8sClient.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list sandbox pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return nil, ErrSandboxNotFound
	}
	return pods.Items[0].DeepCopy(), nil
}

func (r *PodResolver) resolvePodCgroupDir(pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	uid := strings.TrimSpace(string(pod.UID))
	if uid == "" {
		return "", fmt.Errorf("sandbox pod %s/%s has no uid", pod.Namespace, pod.Name)
	}

	for _, candidate := range candidatePodCgroupDirs(r.CgroupRoot, uid, pod.Status.QOSClass) {
		if hasCgroupControls(candidate) {
			return candidate, nil
		}
	}

	path, err := findPodCgroupDir(r.CgroupRoot, uid)
	if err != nil {
		return "", fmt.Errorf("resolve cgroup for sandbox pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return path, nil
}

func candidatePodCgroupDirs(root, uid string, qosClass corev1.PodQOSClass) []string {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return nil
	}
	root = filepath.Clean(root)
	uidSystemd := strings.ReplaceAll(uid, "-", "_")

	type candidate struct {
		cgroupfs string
		systemd  string
	}

	ordered := []candidate{}
	switch qosClass {
	case corev1.PodQOSBurstable:
		ordered = append(ordered, candidate{
			cgroupfs: filepath.Join(root, "kubepods", "burstable", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", fmt.Sprintf("kubepods-burstable-pod%s.slice", uidSystemd)),
		})
	case corev1.PodQOSBestEffort:
		ordered = append(ordered, candidate{
			cgroupfs: filepath.Join(root, "kubepods", "besteffort", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", "kubepods-besteffort.slice", fmt.Sprintf("kubepods-besteffort-pod%s.slice", uidSystemd)),
		})
	default:
		ordered = append(ordered, candidate{
			cgroupfs: filepath.Join(root, "kubepods", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", fmt.Sprintf("kubepods-pod%s.slice", uidSystemd)),
		})
	}

	ordered = append(ordered,
		candidate{
			cgroupfs: filepath.Join(root, "kubepods", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", fmt.Sprintf("kubepods-pod%s.slice", uidSystemd)),
		},
		candidate{
			cgroupfs: filepath.Join(root, "kubepods", "burstable", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", fmt.Sprintf("kubepods-burstable-pod%s.slice", uidSystemd)),
		},
		candidate{
			cgroupfs: filepath.Join(root, "kubepods", "besteffort", "pod"+uid),
			systemd:  filepath.Join(root, "kubepods.slice", "kubepods-besteffort.slice", fmt.Sprintf("kubepods-besteffort-pod%s.slice", uidSystemd)),
		},
	)

	seen := make(map[string]struct{}, len(ordered)*2)
	paths := make([]string, 0, len(ordered)*2)
	for _, item := range ordered {
		for _, candidate := range []string{item.cgroupfs, item.systemd} {
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			paths = append(paths, candidate)
		}
	}
	return paths
}

func findPodCgroupDir(root, uid string) (string, error) {
	uid = strings.TrimSpace(uid)
	if uid == "" {
		return "", fmt.Errorf("pod uid is empty")
	}
	root = filepath.Clean(root)
	uidSystemd := strings.ReplaceAll(uid, "-", "_")
	matchFragments := []string{"pod" + uid, "pod" + uidSystemd}
	maxDepth := 8
	rootDepth := pathDepth(root)

	var found string
	stopWalk := errors.New("stop walk")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.IsDir() {
			return nil
		}
		if pathDepth(path)-rootDepth > maxDepth {
			return filepath.SkipDir
		}
		base := filepath.Base(path)
		for _, fragment := range matchFragments {
			if strings.Contains(base, fragment) && hasCgroupControls(path) {
				found = path
				return stopWalk
			}
		}
		return nil
	})
	if err != nil && err != stopWalk {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("pod cgroup not found under %s", root)
	}
	return found, nil
}

func hasCgroupControls(path string) bool {
	if path == "" {
		return false
	}
	if !fileExists(filepath.Join(path, "cgroup.freeze")) && !fileExists(filepath.Join(path, "freezer.state")) {
		return false
	}
	if !fileExists(filepath.Join(path, "memory.current")) && !fileExists(filepath.Join(path, "memory.usage_in_bytes")) {
		return false
	}
	return true
}

func pathDepth(path string) int {
	clean := filepath.Clean(path)
	if clean == string(os.PathSeparator) {
		return 0
	}
	return strings.Count(clean, string(os.PathSeparator))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
