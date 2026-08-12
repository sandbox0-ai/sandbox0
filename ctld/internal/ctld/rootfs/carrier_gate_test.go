package rootfs

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReleaseCarrierGateValidatesPodIdentityAndIsIdempotent(t *testing.T) {
	const (
		podUID = "2de3d1d4-2c3e-4d58-8f75-f1571efda490"
		slot   = "s0-0123456789abcdef"
	)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "carrier", Namespace: "sandbox0", UID: types.UID(podUID), Annotations: map[string]string{carrier.AnnotationSlot: slot}},
		Spec: corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{
			Name: carrier.GateVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		}}},
	}
	kubeletRoot := t.TempDir()
	gateDir := filepath.Join(kubeletRoot, podUID, "volumes", "kubernetes.io~empty-dir", carrier.GateVolumeName)
	require.NoError(t, os.MkdirAll(gateDir, 0o755))
	controller := NewController(Config{KubernetesClient: fake.NewSimpleClientset(pod), NodeName: "node-a", KubeletPodsRoot: kubeletRoot})
	req := ctldapi.ReleaseCarrierGateRequest{Namespace: "sandbox0", PodName: "carrier", PodUID: podUID, Slot: slot}

	for range 2 {
		response, status := controller.ReleaseCarrierGate(httptest.NewRequest(http.MethodPost, "/", nil), req)
		require.Equal(t, http.StatusOK, status, response.Error)
		assert.True(t, response.Released)
	}
	payload, err := os.ReadFile(filepath.Join(gateDir, carrier.GateReleaseFile))
	require.NoError(t, err)
	assert.Equal(t, slot+"\n", string(payload))

	wrong := req
	wrong.PodUID = "a57ed706-4462-41ba-9e6b-300c88d190da"
	response, status := controller.ReleaseCarrierGate(httptest.NewRequest(http.MethodPost, "/", nil), wrong)
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, response.Error, "identity")
}

func TestReleaseCarrierGateRejectsPreexistingSymlink(t *testing.T) {
	const (
		podUID = "2de3d1d4-2c3e-4d58-8f75-f1571efda490"
		slot   = "s0-0123456789abcdef"
	)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "carrier", Namespace: "sandbox0", UID: types.UID(podUID), Annotations: map[string]string{carrier.AnnotationSlot: slot}},
		Spec:       corev1.PodSpec{NodeName: "node-a", Volumes: []corev1.Volume{{Name: carrier.GateVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}},
	}
	root := t.TempDir()
	dir := filepath.Join(root, podUID, "volumes", "kubernetes.io~empty-dir", carrier.GateVolumeName)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.Symlink("elsewhere", filepath.Join(dir, carrier.GateReleaseFile)))
	controller := NewController(Config{KubernetesClient: fake.NewSimpleClientset(pod), NodeName: "node-a", KubeletPodsRoot: root})
	response, status := controller.ReleaseCarrierGate(httptest.NewRequest(http.MethodPost, "/", nil), ctldapi.ReleaseCarrierGateRequest{
		Namespace: "sandbox0", PodName: "carrier", PodUID: podUID, Slot: slot,
	})
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, response.Error, "regular file")
}
