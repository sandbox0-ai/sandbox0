package power

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(value string) *string {
	return &value
}

func newTestResolverPodCache(t *testing.T, pods ...*corev1.Pod) (corelisters.PodLister, cache.Indexer) {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
		podSandboxIDIndex:    podSandboxIDIndexFunc,
	})
	for _, pod := range pods {
		require.NoError(t, indexer.Add(pod))
	}
	return corelisters.NewPodLister(indexer), indexer
}

func createKataSandboxCgroup(t *testing.T, procRoot, dir, pid, process string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(procRoot, pid), 0o755))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(procRoot, pid, "cmdline"), []byte(process+"\x00--sandbox"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("64\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(pid+"\n"), 0o644))
}

func createRuntimeProcessCgroup(t *testing.T, procRoot, dir, pid, process string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(procRoot, pid), 0o755))
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(procRoot, pid, "cmdline"), []byte(process+"\x00--sandbox"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("64\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.procs"), []byte(pid+"\n"), 0o644))
}

func TestPodResolverResolveUsesQoSCandidate(t *testing.T) {
	root := t.TempDir()
	procRoot := t.TempDir()
	uid := types.UID("12345678-1234-1234-1234-1234567890ab")
	podDir := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", "kubepods-burstable-pod12345678_1234_1234_1234_1234567890ab.slice")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))
	dir := filepath.Join(podDir, "kata_sandbox")
	createKataSandboxCgroup(t, procRoot, dir, "4321", "containerd-shim-kata-v2")

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-1"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("kata-shared")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSBurstable},
	})

	resolver := NewPodResolver(client, "node-a", root)
	resolver.ProcRoot = procRoot
	target, err := resolver.Resolve(&http.Request{}, "sandbox-1")
	require.NoError(t, err)
	assert.Equal(t, "sandbox-1", target.SandboxID)
	assert.Equal(t, "kata", target.Runtime)
	assert.Equal(t, dir, target.CgroupDir)
	assert.Equal(t, "default", target.PodNamespace)
	assert.Equal(t, "sandbox-1", target.PodName)
	assert.Equal(t, string(uid), target.PodUID)
}

func TestPodResolverResolveFallsBackToWalk(t *testing.T) {
	root := t.TempDir()
	procRoot := t.TempDir()
	uid := types.UID("11111111-2222-3333-4444-555555555555")
	podDir := filepath.Join(root, "custom.slice", "nested", "pod11111111_2222_3333_4444_555555555555.slice")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("128\n"), 0o644))
	dir := filepath.Join(podDir, "kata", "sandbox")
	createKataSandboxCgroup(t, procRoot, dir, "9876", "qemu-system-x86_64")

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-2",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-2"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("kata")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	})

	resolver := NewPodResolver(client, "node-a", root)
	resolver.ProcRoot = procRoot
	target, err := resolver.Resolve(&http.Request{}, "sandbox-2")
	require.NoError(t, err)
	assert.Equal(t, "kata", target.Runtime)
	assert.Equal(t, dir, target.CgroupDir)
}

func TestPodResolverResolveRejectsWrongNode(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-3",
			Namespace: "default",
			UID:       types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-3"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-b", RuntimeClassName: strPtr("kata")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	})

	resolver := NewPodResolver(client, "node-a", t.TempDir())
	_, err := resolver.Resolve(&http.Request{}, "sandbox-3")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scheduled on node node-b")
}

func TestPodResolverResolveUsesPodCgroupForDefaultRunc(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
	podDir := filepath.Join(root, "kubepods", "podbbbbbbbb-cccc-dddd-eeee-ffffffffffff")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-4",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-4"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	})

	resolver := NewPodResolver(client, "node-a", root)
	target, err := resolver.Resolve(&http.Request{}, "sandbox-4")
	require.NoError(t, err)
	assert.Equal(t, "runc", target.Runtime)
	assert.Equal(t, podDir, target.CgroupDir)
}

func TestPodResolverResolvePodDoesNotRequireSandboxLabel(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("abababab-cccc-dddd-eeee-ffffffffffff")
	podDir := filepath.Join(root, "kubepods", "podabababab-cccc-dddd-eeee-ffffffffffff")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "idle-pod-1",
			Namespace: "tpl-default",
			UID:       uid,
		},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Containers: []corev1.Container{{
				Name:  "procd",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 49983}},
			}},
		},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed, PodIP: "10.0.0.10"},
	})

	resolver := NewPodResolver(client, "node-a", root)
	target, err := resolver.ResolvePod(&http.Request{}, "tpl-default", "idle-pod-1")
	require.NoError(t, err)
	assert.Empty(t, target.SandboxID)
	assert.Equal(t, "runc", target.Runtime)
	assert.Equal(t, podDir, target.CgroupDir)
	assert.Equal(t, "tpl-default", target.PodNamespace)
	assert.Equal(t, "idle-pod-1", target.PodName)
	assert.Equal(t, "10.0.0.10", target.PodIP)
	assert.Equal(t, int32(49983), target.ProcdPort)
}

func TestPodResolverResolvePodUsesCacheBeforeLiveGet(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("acacacac-bbbb-cccc-dddd-eeeeeeeeeeee")
	podDir := filepath.Join(root, "kubepods", "podacacacac-bbbb-cccc-dddd-eeeeeeeeeeee")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cached-pod", Namespace: "default", UID: uid},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	}
	lister, indexer := newTestResolverPodCache(t, pod)
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("live get should not be called")
	})

	resolver := NewPodResolver(client, "node-a", root)
	resolver.SetPodCache(lister, indexer)
	target, err := resolver.ResolvePod(&http.Request{}, "default", "cached-pod")
	require.NoError(t, err)
	assert.Equal(t, "cached-pod", target.PodName)
	assert.Equal(t, podDir, target.CgroupDir)
}

func TestPodResolverResolvePodFallsBackToLiveGetOnCacheMiss(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("adadadad-bbbb-cccc-dddd-eeeeeeeeeeee")
	podDir := filepath.Join(root, "kubepods", "podadadadad-bbbb-cccc-dddd-eeeeeeeeeeee")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "live-pod", Namespace: "default", UID: uid},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
		Status:     corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	}
	lister, indexer := newTestResolverPodCache(t)
	resolver := NewPodResolver(fake.NewSimpleClientset(pod), "node-a", root)
	resolver.SetPodCache(lister, indexer)

	target, err := resolver.ResolvePod(&http.Request{}, "default", "live-pod")
	require.NoError(t, err)
	assert.Equal(t, "live-pod", target.PodName)
	assert.Equal(t, podDir, target.CgroupDir)
}

func TestPodResolverResolveUsesSandboxIDCacheIndexBeforeLiveList(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("aeaeaeae-bbbb-cccc-dddd-eeeeeeeeeeee")
	podDir := filepath.Join(root, "kubepods", "podaeaeaeae-bbbb-cccc-dddd-eeeeeeeeeeee")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-cache-pod",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-cache"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a"},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	}
	lister, indexer := newTestResolverPodCache(t, pod)
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("live list should not be called")
	})

	resolver := NewPodResolver(client, "node-a", root)
	resolver.SetPodCache(lister, indexer)
	target, err := resolver.Resolve(&http.Request{}, "sandbox-cache")
	require.NoError(t, err)
	assert.Equal(t, "sandbox-cache", target.SandboxID)
	assert.Equal(t, "sandbox-cache-pod", target.PodName)
	assert.Equal(t, podDir, target.CgroupDir)
}

func TestPodResolverResolveUsesPodCgroupForGVisor(t *testing.T) {
	root := t.TempDir()
	procRoot := t.TempDir()
	uid := types.UID("dddddddd-eeee-ffff-1111-222222222222")
	podDir := filepath.Join(root, "kubepods", "burstable", "poddddddddd-eeee-ffff-1111-222222222222")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))
	createRuntimeProcessCgroup(t, procRoot, filepath.Join(podDir, "sandbox"), "3456", "containerd-shim-runsc-v1")

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-6",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-6"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("gvisor")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSBurstable},
	})

	resolver := NewPodResolver(client, "node-a", root)
	resolver.ProcRoot = procRoot
	target, err := resolver.Resolve(&http.Request{}, "sandbox-6")
	require.NoError(t, err)
	assert.Equal(t, "gvisor", target.Runtime)
	assert.Equal(t, podDir, target.CgroupDir)
}

func TestPodResolverResolveRequiresGVisorSandboxProcesses(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("99999999-eeee-ffff-1111-222222222222")
	podDir := filepath.Join(root, "kubepods", "pod99999999-eeee-ffff-1111-222222222222")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-8",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-8"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("gvisor")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	})

	resolver := NewPodResolver(client, "node-a", root)
	_, err := resolver.Resolve(&http.Request{}, "sandbox-8")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gvisor sandbox processes not found")
}

func TestPodResolverResolveRejectsUnknownRuntime(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-7",
			Namespace: "default",
			UID:       types.UID("eeeeeeee-ffff-1111-2222-333333333333"),
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-7"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("wasmtime")},
	})

	resolver := NewPodResolver(client, "node-a", t.TempDir())
	_, err := resolver.Resolve(&http.Request{}, "sandbox-7")
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestPodResolverResolveRequiresDedicatedKataSandboxCgroup(t *testing.T) {
	root := t.TempDir()
	uid := types.UID("cccccccc-1111-2222-3333-444444444444")
	podDir := filepath.Join(root, "kubepods", "podcccccccc-1111-2222-3333-444444444444")
	require.NoError(t, os.MkdirAll(podDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "cgroup.freeze"), []byte("0\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(podDir, "memory.current"), []byte("64\n"), 0o644))

	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-5",
			Namespace: "default",
			UID:       uid,
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-5"},
		},
		Spec:   corev1.PodSpec{NodeName: "node-a", RuntimeClassName: strPtr("kata")},
		Status: corev1.PodStatus{QOSClass: corev1.PodQOSGuaranteed},
	})

	resolver := NewPodResolver(client, "node-a", root)
	_, err := resolver.Resolve(&http.Request{}, "sandbox-5")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sandbox_cgroup_only=true")
}
