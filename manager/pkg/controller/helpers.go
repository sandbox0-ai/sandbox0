package controller

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	managernaming "github.com/sandbox0-ai/sandbox0/manager/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/podmeta"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxpod"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/retry"
)

const (
	procdInternalJWTPublicKey = "internal_jwt_public.key"
	netdMITMCACertKey         = "ca.crt"
	serviceAccountNamespace   = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

// EnsureProcdConfigSecret creates or updates the procd config Secret for a template.
func EnsureProcdConfigSecret(
	ctx context.Context,
	client kubernetes.Interface,
	secretLister corelisters.SecretLister,
	template *v1alpha1.SandboxTemplate,
) error {
	clusterID := naming.ClusterIDOrDefault(template.Spec.ClusterId)
	name, err := managernaming.ProcdConfigSecretName(clusterID, template.Name)
	if err != nil {
		return fmt.Errorf("generate procd config secret name: %w", err)
	}

	publicKey, err := os.ReadFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		return fmt.Errorf("read internal auth public key: %w", err)
	}

	labels := map[string]string{
		LabelTemplateID: template.Name,
	}
	var ownerRefs []metav1.OwnerReference
	// Resume can reconstruct a template after its CR has been deleted. Such a
	// synthetic object has no UID and cannot be a Kubernetes owner.
	if template.UID != "" {
		ownerRefs = []metav1.OwnerReference{
			*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
		}
	}
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       template.Namespace,
			Labels:          labels,
			OwnerReferences: ownerRefs,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			procdInternalJWTPublicKey: publicKey,
		},
	}

	current, err := secretLister.Secrets(template.Namespace).Get(name)
	if err == nil && secretMatches(current, desired) {
		return nil
	}
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get procd config secret: %w", err)
		}
		if _, err := client.CoreV1().Secrets(template.Namespace).Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create procd config secret: %w", err)
			}
		} else {
			return nil
		}
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.CoreV1().Secrets(template.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated := current.DeepCopy()
		updated.Labels = labels
		updated.OwnerReferences = ownerRefs
		updated.Data = desired.Data
		updated.Type = desired.Type

		if reflect.DeepEqual(current.Labels, updated.Labels) &&
			reflect.DeepEqual(current.OwnerReferences, updated.OwnerReferences) &&
			reflect.DeepEqual(current.Data, updated.Data) &&
			reflect.DeepEqual(current.Type, updated.Type) {
			return nil
		}

		_, err = client.CoreV1().Secrets(template.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update procd config secret: %w", err)
	}
	return nil
}

// IsPodReady returns true only when the pod is running and reports the
// Kubernetes PodReady condition as true.
func IsPodReady(pod *corev1.Pod) bool {
	return podmeta.IsReady(pod)
}

func HasSandboxPodReadinessGate(pod *corev1.Pod) bool {
	return sandboxpod.HasReadinessGate(pod)
}

// EnsureNetdMITMCASecret copies the manager-local network-runtime MITM CA certificate into the template namespace.
func EnsureNetdMITMCASecret(
	ctx context.Context,
	client kubernetes.Interface,
	secretLister corelisters.SecretLister,
	templateNamespace string,
) error {
	cfg := config.LoadManagerConfig()
	if cfg == nil || cfg.NetdMITMCASecretName == "" {
		return nil
	}
	if templateNamespace == "" {
		return fmt.Errorf("template namespace is required to ensure network-runtime MITM CA secret")
	}

	sourceNamespace, err := resolveNetdMITMCASecretNamespace(cfg)
	if err != nil {
		return err
	}

	source, err := getSecret(ctx, client, secretLister, sourceNamespace, cfg.NetdMITMCASecretName)
	if err != nil {
		return fmt.Errorf("get network-runtime MITM CA secret %s/%s: %w", sourceNamespace, cfg.NetdMITMCASecretName, err)
	}

	certPEM := source.Data[netdMITMCACertKey]
	if len(certPEM) == 0 {
		return fmt.Errorf("network-runtime MITM CA secret %s/%s missing %q", sourceNamespace, cfg.NetdMITMCASecretName, netdMITMCACertKey)
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.NetdMITMCASecretName,
			Namespace: templateNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "sandbox0-manager",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			netdMITMCACertKey: append([]byte(nil), certPEM...),
		},
	}

	current, err := getSecret(ctx, client, secretLister, templateNamespace, cfg.NetdMITMCASecretName)
	if err == nil && secretMatches(current, desired) {
		return nil
	}
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get target network-runtime MITM CA secret: %w", err)
		}
		if _, err := client.CoreV1().Secrets(templateNamespace).Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create target network-runtime MITM CA secret: %w", err)
			}
		} else {
			return nil
		}
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.CoreV1().Secrets(templateNamespace).Get(ctx, cfg.NetdMITMCASecretName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated := current.DeepCopy()
		updated.Type = desired.Type
		updated.Data = desired.Data
		updated.Labels = desired.Labels
		if reflect.DeepEqual(current.Type, updated.Type) &&
			reflect.DeepEqual(current.Data, updated.Data) &&
			reflect.DeepEqual(current.Labels, updated.Labels) {
			return nil
		}

		_, err = client.CoreV1().Secrets(templateNamespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update target network-runtime MITM CA secret: %w", err)
	}
	return nil
}

func getSecret(ctx context.Context, client kubernetes.Interface, secretLister corelisters.SecretLister, namespace, name string) (*corev1.Secret, error) {
	if secretLister != nil {
		return secretLister.Secrets(namespace).Get(name)
	}
	return client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
}

func secretMatches(current, desired *corev1.Secret) bool {
	if current == nil || desired == nil {
		return false
	}
	return reflect.DeepEqual(current.Labels, desired.Labels) &&
		reflect.DeepEqual(current.OwnerReferences, desired.OwnerReferences) &&
		reflect.DeepEqual(current.Data, desired.Data) &&
		reflect.DeepEqual(current.Type, desired.Type)
}

func resolveNetdMITMCASecretNamespace(cfg *config.ManagerConfig) (string, error) {
	if cfg != nil {
		if namespace := strings.TrimSpace(cfg.NetdMITMCASecretNamespace); namespace != "" {
			return namespace, nil
		}
	}

	if namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); namespace != "" {
		return namespace, nil
	}

	data, err := os.ReadFile(serviceAccountNamespace)
	if err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace, nil
		}
	}

	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}
	return "", fmt.Errorf("resolve network-runtime MITM CA source namespace")
}
