package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
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

	t.Run("requires sandbox readiness gate to be true when present", func(t *testing.T) {
		pod := &corev1.Pod{
			Spec: corev1.PodSpec{
				ReadinessGates: []corev1.PodReadinessGate{{
					ConditionType: v1alpha1.SandboxPodReadinessConditionType,
				}},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					{Type: v1alpha1.SandboxPodReadinessConditionType, Status: corev1.ConditionFalse},
				},
			},
		}
		if IsPodReady(pod) {
			t.Fatal("IsPodReady() = true, want false when sandbox readiness gate is false")
		}
		pod.Status.Conditions[1].Status = corev1.ConditionTrue
		if !IsPodReady(pod) {
			t.Fatal("IsPodReady() = false, want true when sandbox readiness gate is true")
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

func TestDesiredSandboxPodReadiness(t *testing.T) {
	t.Run("running active sandbox is ready", func(t *testing.T) {
		status, reason, _ := DesiredSandboxPodReadiness(&corev1.Pod{
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
		require.Equal(t, corev1.ConditionTrue, status)
		require.Equal(t, "SandboxActive", reason)
	})

	t.Run("paused sandbox is not ready", func(t *testing.T) {
		status, reason, _ := DesiredSandboxPodReadiness(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{AnnotationPaused: "true"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
		require.Equal(t, corev1.ConditionFalse, status)
		require.Equal(t, "PowerStatePaused", reason)
	})

	t.Run("resuming sandbox is not ready", func(t *testing.T) {
		status, reason, _ := DesiredSandboxPodReadiness(&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{
					AnnotationPowerStateDesired:  "active",
					AnnotationPowerStateObserved: "paused",
					AnnotationPowerStatePhase:    "resuming",
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		})
		require.Equal(t, corev1.ConditionFalse, status)
		require.Equal(t, "PowerStateTransitioning", reason)
	})
}

func TestEnsureSandboxPodReadinessConditionUpdatesGateStatus(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-a",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{
				ConditionType: v1alpha1.SandboxPodReadinessConditionType,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	client := fake.NewSimpleClientset(pod.DeepCopy())
	updated, err := EnsureSandboxPodReadinessCondition(context.Background(), client, pod)
	require.NoError(t, err)
	require.NotNil(t, updated)

	var condition *corev1.PodCondition
	for i := range updated.Status.Conditions {
		if updated.Status.Conditions[i].Type == v1alpha1.SandboxPodReadinessConditionType {
			condition = &updated.Status.Conditions[i]
			break
		}
	}
	require.NotNil(t, condition)
	require.Equal(t, corev1.ConditionTrue, condition.Status)
	require.Equal(t, "SandboxActive", condition.Reason)
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

func TestEnsureProcdConfigSecretHandlesAlreadyExistsWhenListerIsStale(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "internal_jwt_public.key")
	require.NoError(t, os.WriteFile(keyPath, []byte("test-public-key"), 0o600))
	previousPath := internalauth.DefaultInternalJWTPublicKeyPath
	internalauth.DefaultInternalJWTPublicKeyPath = keyPath
	t.Cleanup(func() {
		internalauth.DefaultInternalJWTPublicKeyPath = previousPath
	})

	template := &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "template-a",
			Namespace: "tpl-a",
		},
	}

	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "procd-secret-mrswmylvnr2a-template-a",
			Namespace: "tpl-a",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			procdInternalJWTPublicKey: []byte("stale"),
		},
	}

	client := fake.NewSimpleClientset(existing.DeepCopy())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})

	err := EnsureProcdConfigSecret(context.Background(), client, corelisters.NewSecretLister(indexer), template)
	require.NoError(t, err)

	updated, err := client.CoreV1().Secrets("tpl-a").Get(context.Background(), existing.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, []byte("test-public-key"), updated.Data[procdInternalJWTPublicKey])
	require.Equal(t, "template-a", updated.Labels[LabelTemplateID])
	require.Len(t, updated.OwnerReferences, 1)
	require.Equal(t, "SandboxTemplate", updated.OwnerReferences[0].Kind)
	require.Equal(t, "template-a", updated.OwnerReferences[0].Name)
}

func writeHelpersManagerConfig(t *testing.T, contents string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	err := os.WriteFile(path, []byte(contents), 0o600)
	require.NoError(t, err)
	return path
}
