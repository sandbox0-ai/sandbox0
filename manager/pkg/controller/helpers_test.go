package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestIsPodReady(t *testing.T) {
	t.Run("returns false for nil pods", func(t *testing.T) {
		if IsPodReady(nil) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns true only for running pods with ready condition", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if !IsPodReady(pod) {
			t.Fatal("IsPodReady() = false, want true")
		}
	})

	t.Run("returns false for running but not-ready pods", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionFalse,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false for pending pods", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false when ready condition is missing", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.ContainersReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})

	t.Run("returns false for unknown ready condition", func(t *testing.T) {
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{
						Type:   corev1.PodReady,
						Status: corev1.ConditionUnknown,
					},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false")
		}
	})
}

func TestEnsureNetdMITMCASecretCopiesCertIntoTemplateNamespace(t *testing.T) {
	configPath := writeHelpersManagerConfig(t, `
netd_mitm_ca_secret_name: fullmode-netd-mitm-ca
netd_mitm_ca_secret_namespace: sandbox0-system
`)
	t.Setenv("CONFIG_PATH", configPath)

	client := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "fullmode-netd-mitm-ca",
			Namespace: "sandbox0-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"ca.crt": []byte("test-ca"),
			"ca.key": []byte("should-not-copy"),
		},
	})

	err := EnsureNetdMITMCASecret(context.Background(), client, "tpl-default")
	require.NoError(t, err)

	copied, err := client.CoreV1().Secrets("tpl-default").Get(context.Background(), "fullmode-netd-mitm-ca", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, corev1.SecretTypeOpaque, copied.Type)
	require.Equal(t, map[string][]byte{
		"ca.crt": []byte("test-ca"),
	}, copied.Data)
	require.Equal(t, "sandbox0-manager", copied.Labels["app.kubernetes.io/managed-by"])
}

func TestEnsureNetdMITMCASecretNoopsWithoutConfiguredSecret(t *testing.T) {
	configPath := writeHelpersManagerConfig(t, "{}\n")
	t.Setenv("CONFIG_PATH", configPath)

	client := fake.NewSimpleClientset()
	err := EnsureNetdMITMCASecret(context.Background(), client, "tpl-default")
	require.NoError(t, err)

	secrets, err := client.CoreV1().Secrets("tpl-default").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Empty(t, secrets.Items)
}

func writeHelpersManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	require.NoError(t, err)
	return path
}
