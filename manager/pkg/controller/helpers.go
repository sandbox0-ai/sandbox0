package controller

import (
	"context"
	"fmt"
	"os"
	"reflect"

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
			return fmt.Errorf("create procd config secret: %w", err)
		}
		return nil
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
