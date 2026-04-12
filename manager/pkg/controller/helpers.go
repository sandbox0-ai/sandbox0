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
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
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
	if !podConditionTrue(pod.Status.Conditions, corev1.PodReady) {
		return false
	}
	if HasSandboxPodReadinessGate(pod) {
		if !podConditionTrue(pod.Status.Conditions, v1alpha1.SandboxPodReadinessConditionType) {
			return false
		}
		live := findPodCondition(pod.Status.Conditions, v1alpha1.SandboxPodLivenessConditionType)
		return live == nil || live.Status != corev1.ConditionFalse
	}
	return true
}

func HasSandboxPodReadinessGate(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, gate := range pod.Spec.ReadinessGates {
		if gate.ConditionType == v1alpha1.SandboxPodReadinessConditionType {
			return true
		}
	}
	return false
}

func EnsureSandboxPodProbeConditions(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, startup, readiness, liveness *sandboxprobe.Response) (*corev1.Pod, error) {
	if client == nil || pod == nil || !HasSandboxPodReadinessGate(pod) {
		return pod, nil
	}

	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !HasSandboxPodReadinessGate(current) {
			updated = current
			return nil
		}

		current = current.DeepCopy()
		changed := false
		apply := func(condition corev1.PodCondition) {
			existing := findPodCondition(current.Status.Conditions, condition.Type)
			if existing != nil && existing.Status == condition.Status && existing.Reason == condition.Reason && existing.Message == condition.Message {
				return
			}
			setPodCondition(&current.Status.Conditions, condition)
			changed = true
		}

		if startup != nil {
			apply(podConditionFromProbe(v1alpha1.SandboxPodStartupConditionType, *startup, corev1.ConditionFalse))
		}
		if liveness != nil {
			apply(podConditionFromProbe(v1alpha1.SandboxPodLivenessConditionType, *liveness, corev1.ConditionUnknown))
		}
		if readiness != nil {
			readyCondition := podConditionFromProbe(v1alpha1.SandboxPodReadinessConditionType, *readiness, corev1.ConditionFalse)
			if startup != nil && startup.Status != sandboxprobe.StatusPassed {
				readyCondition.Status = corev1.ConditionFalse
				readyCondition.Reason = "SandboxStartupProbeFailed"
				readyCondition.Message = startup.Message
			}
			if liveness != nil && liveness.Status == sandboxprobe.StatusFailed {
				readyCondition.Status = corev1.ConditionFalse
				readyCondition.Reason = "SandboxLivenessProbeFailed"
				readyCondition.Message = liveness.Message
			}
			apply(readyCondition)
		}

		if !changed {
			updated = current
			return nil
		}
		updated, err = client.CoreV1().Pods(current.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func podConditionFromProbe(conditionType corev1.PodConditionType, result sandboxprobe.Response, suspendedStatus corev1.ConditionStatus) corev1.PodCondition {
	status := corev1.ConditionFalse
	switch result.Status {
	case sandboxprobe.StatusPassed:
		status = corev1.ConditionTrue
	case sandboxprobe.StatusSuspended:
		status = suspendedStatus
	}
	reason := result.Reason
	if reason == "" {
		reason = "SandboxProbe" + string(result.Status)
	}
	return corev1.PodCondition{
		Type:               conditionType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            result.Message,
	}
}

func DesiredSandboxPodReadiness(pod *corev1.Pod) (corev1.ConditionStatus, string, string) {
	if pod == nil {
		return corev1.ConditionFalse, "PodMissing", "pod is missing"
	}
	if pod.Status.Phase != corev1.PodRunning {
		return corev1.ConditionFalse, "PodNotRunning", fmt.Sprintf("pod phase is %s", pod.Status.Phase)
	}

	desired, observed, phase := sandboxPowerStateFromAnnotations(pod.Annotations)
	if desired != "active" {
		return corev1.ConditionFalse, "PowerStatePaused", "sandbox desired power state is paused"
	}
	if observed != "active" || phase != "stable" {
		return corev1.ConditionFalse, "PowerStateTransitioning", "sandbox power state is not yet active and stable"
	}
	return corev1.ConditionTrue, "SandboxActive", "sandbox is active and ready"
}

func EnsureSandboxPodReadinessCondition(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) (*corev1.Pod, error) {
	if client == nil || pod == nil || !HasSandboxPodReadinessGate(pod) {
		return pod, nil
	}

	var updated *corev1.Pod
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !HasSandboxPodReadinessGate(current) {
			updated = current
			return nil
		}

		status, reason, message := DesiredSandboxPodReadiness(current)
		existing := findPodCondition(current.Status.Conditions, v1alpha1.SandboxPodReadinessConditionType)
		if existing != nil && existing.Status == status && existing.Reason == reason && existing.Message == message {
			updated = current
			return nil
		}

		current = current.DeepCopy()
		setPodCondition(&current.Status.Conditions, corev1.PodCondition{
			Type:               v1alpha1.SandboxPodReadinessConditionType,
			Status:             status,
			LastTransitionTime: metav1.Now(),
			Reason:             reason,
			Message:            message,
		})

		updated, err = client.CoreV1().Pods(current.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

func sandboxPowerStateFromAnnotations(annotations map[string]string) (desired, observed, phase string) {
	if annotations == nil {
		return "active", "active", "stable"
	}
	legacyPaused := annotations[AnnotationPaused] == "true"
	desired = strings.TrimSpace(annotations[AnnotationPowerStateDesired])
	if desired == "" {
		if legacyPaused {
			desired = "paused"
		} else {
			desired = "active"
		}
	}
	observed = strings.TrimSpace(annotations[AnnotationPowerStateObserved])
	if observed == "" {
		if legacyPaused {
			observed = "paused"
		} else {
			observed = "active"
		}
	}
	phase = strings.TrimSpace(annotations[AnnotationPowerStatePhase])
	if phase == "" {
		switch desired {
		case observed:
			phase = "stable"
		case "paused":
			phase = "pausing"
		default:
			phase = "resuming"
		}
	}
	return desired, observed, phase
}

func podConditionTrue(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func findPodCondition(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) *corev1.PodCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func setPodCondition(conditions *[]corev1.PodCondition, condition corev1.PodCondition) {
	if conditions == nil {
		return
	}
	for i := range *conditions {
		if (*conditions)[i].Type != condition.Type {
			continue
		}
		if (*conditions)[i].Status == condition.Status {
			condition.LastTransitionTime = (*conditions)[i].LastTransitionTime
		}
		(*conditions)[i] = condition
		return
	}
	*conditions = append(*conditions, condition)
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
