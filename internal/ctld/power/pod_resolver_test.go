package power

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func strPtr(value string) *string {
	return &value
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

func TestPodResolverResolveRejectsNonKataRuntime(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-4",
			Namespace: "default",
			UID:       types.UID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
			Labels:    map[string]string{controller.LabelSandboxID: "sandbox-4"},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	})

	resolver := NewPodResolver(client, "node-a", t.TempDir())
	_, err := resolver.Resolve(&http.Request{}, "sandbox-4")
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
