package controller

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
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
	name, err := naming.ProcdConfigSecretName(clusterID, template.Name)
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
	ownerRefs := []metav1.OwnerReference{
		*metav1.NewControllerRef(template, v1alpha1.SchemeGroupVersion.WithKind("SandboxTemplate")),
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

	existing, err := secretLister.Secrets(template.Namespace).Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get procd config secret: %w", err)
		}
		if _, err := client.CoreV1().Secrets(template.Namespace).Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create procd config secret: %w", err)
			}
			existing, err = client.CoreV1().Secrets(template.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get procd config secret after already exists: %w", err)
			}
		} else {
			return nil
		}
	}

	updated := existing.DeepCopy()
	updated.Labels = labels
	updated.OwnerReferences = ownerRefs
	updated.Data = desired.Data
	updated.Type = desired.Type

	if reflect.DeepEqual(existing.Labels, updated.Labels) &&
		reflect.DeepEqual(existing.OwnerReferences, updated.OwnerReferences) &&
		reflect.DeepEqual(existing.Data, updated.Data) &&
		reflect.DeepEqual(existing.Type, updated.Type) {
		return nil
	}

	if _, err := client.CoreV1().Secrets(template.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update procd config secret: %w", err)
	}
	return nil
}

// IsPodReady returns true only when the pod is running and reports the
// Kubernetes PodReady condition as true.
func IsPodReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// EnsureNetdMITMCASecret copies the manager-local netd MITM CA cert into the template namespace.
func EnsureNetdMITMCASecret(
	ctx context.Context,
	client kubernetes.Interface,
	templateNamespace string,
) error {
	cfg := config.LoadManagerConfig()
	if cfg == nil || cfg.NetdMITMCASecretName == "" {
		return nil
	}
	if templateNamespace == "" {
		return fmt.Errorf("template namespace is required to ensure netd MITM CA secret")
	}

	sourceNamespace, err := resolveNetdMITMCASecretNamespace(cfg)
	if err != nil {
		return err
	}

	source, err := client.CoreV1().Secrets(sourceNamespace).Get(ctx, cfg.NetdMITMCASecretName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get netd MITM CA secret %s/%s: %w", sourceNamespace, cfg.NetdMITMCASecretName, err)
	}

	certPEM := source.Data[netdMITMCACertKey]
	if len(certPEM) == 0 {
		return fmt.Errorf("netd MITM CA secret %s/%s missing %q", sourceNamespace, cfg.NetdMITMCASecretName, netdMITMCACertKey)
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

	existing, err := client.CoreV1().Secrets(templateNamespace).Get(ctx, cfg.NetdMITMCASecretName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get target netd MITM CA secret: %w", err)
		}
		if _, err := client.CoreV1().Secrets(templateNamespace).Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create target netd MITM CA secret: %w", err)
		}
		return nil
	}

	updated := existing.DeepCopy()
	updated.Type = desired.Type
	updated.Data = desired.Data
	updated.Labels = desired.Labels
	if reflect.DeepEqual(existing.Type, updated.Type) &&
		reflect.DeepEqual(existing.Data, updated.Data) &&
		reflect.DeepEqual(existing.Labels, updated.Labels) {
		return nil
	}

	if _, err := client.CoreV1().Secrets(templateNamespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update target netd MITM CA secret: %w", err)
	}
	return nil
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
	return "", fmt.Errorf("resolve netd MITM CA source namespace")
}
