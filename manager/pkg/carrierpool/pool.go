// Package carrierpool owns the single cluster-wide pool of pre-scheduled Pods.
package carrierpool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	api "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	managernaming "github.com/sandbox0-ai/sandbox0/manager/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/carrier"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	sharedTemplateName                 = "shared-carrier"
	procdSecretKey                     = "internal_jwt_public.key"
	carrierBaseVolumeName              = "carrier-base"
	carrierBaseMountPath               = "/run/sandbox0/carrier-base"
	carrierGatePollInterval            = 200 * time.Millisecond
	sharedCarrierImmutableShapeVersion = "v2-ephemeral-8Gi"
)

type Config struct {
	Namespace         string
	ClusterID         string
	MinIdle           int
	MaxIdle           int
	CarrierImageRef   string
	WaiterImageRef    string
	ReconcileInterval time.Duration
	ActivationTimeout time.Duration
	Generation        string
}

type Pool struct {
	k8s    kubernetes.Interface
	config Config
	logger *zap.Logger
}

func New(k8sClient kubernetes.Interface, cfg Config, logger *zap.Logger) (*Pool, error) {
	if k8sClient == nil {
		return nil, fmt.Errorf("carrier pool Kubernetes client is required")
	}
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.CarrierImageRef = strings.TrimSpace(cfg.CarrierImageRef)
	cfg.WaiterImageRef = strings.TrimSpace(cfg.WaiterImageRef)
	if cfg.Namespace == "" || cfg.CarrierImageRef == "" {
		return nil, fmt.Errorf("carrier pool namespace and image are required")
	}
	if cfg.WaiterImageRef == "" {
		cfg.WaiterImageRef = cfg.CarrierImageRef
	}
	if cfg.MinIdle < 0 || cfg.MaxIdle < cfg.MinIdle {
		return nil, fmt.Errorf("invalid carrier pool bounds %d..%d", cfg.MinIdle, cfg.MaxIdle)
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 5 * time.Second
	}
	if cfg.ActivationTimeout <= 0 {
		cfg.ActivationTimeout = 15 * time.Second
	}
	if strings.TrimSpace(cfg.Generation) == "" {
		cfg.Generation = "v1"
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Pool{k8s: k8sClient, config: cfg, logger: logger}, nil
}

func (p *Pool) Run(ctx context.Context) error {
	if err := p.ensureProcdSecret(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(p.config.ReconcileInterval)
	defer ticker.Stop()
	for {
		if err := p.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Warn("Shared carrier pool reconcile failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Pool) Reconcile(ctx context.Context) error {
	pods, err := p.list(ctx)
	if err != nil {
		return err
	}
	current := make([]corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Annotations[carrier.AnnotationState] == carrier.StateReserved {
			continue
		}
		if pod.Labels[carrier.LabelGeneration] != p.config.Generation || carrierUnusable(&pod, p.config.Generation, p.config.ActivationTimeout, time.Now()) {
			grace := int64(0)
			if err := p.k8s.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
			continue
		}
		current = append(current, pod)
	}
	for len(current) < p.config.MinIdle && len(current) < p.config.MaxIdle {
		pod, err := p.create(ctx, false, nil)
		if err != nil {
			return err
		}
		current = append(current, *pod)
	}
	if len(current) > p.config.MaxIdle {
		sort.Slice(current, func(i, j int) bool {
			return current[i].CreationTimestamp.Time.Before(current[j].CreationTimestamp.Time)
		})
		for _, pod := range current[p.config.MaxIdle:] {
			grace := int64(0)
			if err := p.k8s.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace}); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func carrierUnusable(pod *corev1.Pod, generation string, activationTimeout time.Duration, now time.Time) bool {
	if pod == nil || pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
		return true
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == "carrier-wait" && status.State.Terminated != nil {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "procd" && (status.State.Running != nil || status.State.Terminated != nil || status.RestartCount > 0) {
			return true
		}
	}
	if GateReady(pod, generation) || pod.CreationTimestamp.IsZero() {
		return false
	}
	return activationTimeout > 0 && now.Sub(pod.CreationTimestamp.Time) >= activationTimeout
}

// Reserve atomically transfers one CarrierReady Pod out of pool ownership.
func (p *Pool) Reserve(ctx context.Context) (*corev1.Pod, error) {
	pods, err := p.list(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].CreationTimestamp.Time.Before(pods[j].CreationTimestamp.Time) })
	for _, candidate := range pods {
		if !CarrierReady(&candidate, p.config.Generation) {
			continue
		}
		current, err := p.k8s.CoreV1().Pods(candidate.Namespace).Get(ctx, candidate.Name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		if !CarrierReady(current, p.config.Generation) {
			continue
		}
		updated := current.DeepCopy()
		updated.Annotations[carrier.AnnotationState] = carrier.StateReserved
		updated, err = p.k8s.CoreV1().Pods(updated.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return updated, nil
	}
	return nil, nil
}

// CreateCold creates a reserved carrier without adding another warm pool.
func (p *Pool) CreateCold(ctx context.Context, template *api.SandboxTemplate) (*corev1.Pod, error) {
	if template == nil {
		return nil, fmt.Errorf("cold carrier template is required")
	}
	return p.create(ctx, true, template)
}

func (p *Pool) Delete(ctx context.Context, pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	grace := int64(0)
	err := p.k8s.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &grace})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (p *Pool) create(ctx context.Context, reserved bool, coldTemplate *api.SandboxTemplate) (*corev1.Pod, error) {
	pod, err := p.newCarrierPod(reserved, coldTemplate)
	if err != nil {
		return nil, err
	}
	created, err := p.k8s.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("create carrier Pod: %w", err)
	}
	return created, nil
}

func (p *Pool) newCarrierPod(reserved bool, coldTemplate *api.SandboxTemplate) (*corev1.Pod, error) {
	slot, err := newSlot()
	if err != nil {
		return nil, err
	}
	marker, err := carrier.MarkerImage(slot)
	if err != nil {
		return nil, err
	}
	template := &api.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: sharedTemplateName, Namespace: p.config.Namespace},
		Spec: api.SandboxTemplateSpec{
			ClusterId: stringPointer(p.config.ClusterID),
			MainContainer: api.ContainerSpec{
				Image: marker,
				Resources: api.ResourceQuota{
					CPU: resource.MustParse("150m"), Memory: resource.MustParse("128Mi"), EphemeralStorage: resource.MustParse(api.DefaultSandboxEphemeralStorage),
				},
			},
		},
	}
	namespace := p.config.Namespace
	poolLabel := "shared"
	if coldTemplate != nil {
		template = coldTemplate.DeepCopy()
		namespace = template.Namespace
		poolLabel = "cold"
	}
	spec := api.BuildPodSpec(template)
	if coldTemplate == nil {
		spec.TopologySpreadConstraints = append(spec.TopologySpreadConstraints, corev1.TopologySpreadConstraint{
			MaxSkew:           1,
			TopologyKey:       corev1.LabelHostname,
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				carrier.LabelPool:       poolLabel,
				carrier.LabelGeneration: p.config.Generation,
			}},
		})
	}
	if len(spec.Containers) != 1 {
		return nil, fmt.Errorf("carrier Pod requires exactly one main container")
	}
	spec.Containers[0].Image = marker
	spec.Containers[0].ImagePullPolicy = corev1.PullNever
	spec.RestartPolicy = corev1.RestartPolicyNever
	spec.Volumes = append(spec.Volumes,
		corev1.Volume{
			Name: carrierBaseVolumeName,
			VolumeSource: corev1.VolumeSource{Image: &corev1.ImageVolumeSource{
				Reference:  p.config.CarrierImageRef,
				PullPolicy: corev1.PullIfNotPresent,
			}},
		},
		corev1.Volume{Name: carrier.GateVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	)
	spec.InitContainers = []corev1.Container{{
		Name: "carrier-wait", Image: p.config.WaiterImageRef, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/bin/sh", "-ec", fmt.Sprintf("while [ ! -f %s/%s ]; do sleep %.2f; done", carrier.GateMountPath, carrier.GateReleaseFile, carrierGatePollInterval.Seconds())},
		VolumeMounts: []corev1.VolumeMount{
			{Name: carrierBaseVolumeName, MountPath: carrierBaseMountPath, ReadOnly: true},
			{Name: carrier.GateVolumeName, MountPath: carrier.GateMountPath},
		},
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m"), corev1.ResourceMemory: resource.MustParse("4Mi")}},
	}}
	state := carrier.StateReady
	if reserved {
		state = carrier.StateReserved
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "s0-carrier-", Namespace: namespace,
		Labels:      map[string]string{carrier.LabelPool: poolLabel, carrier.LabelGeneration: p.config.Generation},
		Annotations: map[string]string{carrier.AnnotationSlot: slot, carrier.AnnotationState: state},
	}, Spec: spec}
	return pod, nil
}

func (p *Pool) list(ctx context.Context) ([]corev1.Pod, error) {
	selector := labels.Set{carrier.LabelPool: "shared"}.AsSelector().String()
	list, err := p.k8s.CoreV1().Pods(p.config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (p *Pool) ensureProcdSecret(ctx context.Context) error {
	name, err := managernaming.ProcdConfigSecretName(naming.ClusterIDOrDefault(stringPointer(p.config.ClusterID)), sharedTemplateName)
	if err != nil {
		return err
	}
	publicKey, err := os.ReadFile(internalauth.DefaultInternalJWTPublicKeyPath)
	if err != nil {
		return fmt.Errorf("read carrier procd public key: %w", err)
	}
	desired := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: p.config.Namespace}, Type: corev1.SecretTypeOpaque, Data: map[string][]byte{procdSecretKey: publicKey}}
	current, err := p.k8s.CoreV1().Secrets(p.config.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = p.k8s.CoreV1().Secrets(p.config.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if string(current.Data[procdSecretKey]) == string(publicKey) {
		return nil
	}
	updated := current.DeepCopy()
	updated.Data = desired.Data
	_, err = p.k8s.CoreV1().Secrets(p.config.Namespace).Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

// CarrierReady is deliberately independent from Kubernetes Pod Ready: the
// init gate must still be running and the main container must never have run.
func CarrierReady(pod *corev1.Pod, generation string) bool {
	return pod != nil && pod.Annotations[carrier.AnnotationState] == carrier.StateReady && GateReady(pod, generation)
}

// GateReady reports that kubelet has created the Pod sandbox and is blocked in
// the init waiter. It applies to both pooled and cold reserved carriers.
func GateReady(pod *corev1.Pod, generation string) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Labels[carrier.LabelGeneration] != generation || strings.TrimSpace(pod.Spec.NodeName) == "" {
		return false
	}
	readyToStart := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReadyToStartContainers && condition.Status == corev1.ConditionTrue {
			readyToStart = true
		}
	}
	if !readyToStart {
		return false
	}
	initRunning := false
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name == "carrier-wait" && status.State.Running != nil {
			initRunning = true
		}
	}
	if !initRunning {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "procd" && (status.RestartCount > 0 || status.State.Running != nil || status.State.Terminated != nil || status.LastTerminationState.Terminated != nil) {
			return false
		}
	}
	return true
}

// Compatible reports whether a template fits the platform-standard hot shape.
func Compatible(tpl *api.SandboxTemplate) (bool, string) {
	if tpl == nil {
		return false, "template_missing"
	}
	if len(tpl.Spec.VolumeMounts) != 0 || len(tpl.Spec.EnvVars) != 0 || len(tpl.Spec.MainContainer.Env) != 0 || tpl.Spec.MainContainer.SecurityContext != nil {
		return false, "dynamic_container_shape"
	}
	if tpl.Spec.Pod != nil && (len(tpl.Spec.Pod.NodeSelector) != 0 || len(tpl.Spec.Pod.Tolerations) != 0 || tpl.Spec.Pod.ServiceAccountName != "" || len(tpl.Spec.Pod.EmptyDirMounts) != 0) {
		return false, "dynamic_pod_shape"
	}
	ephemeralStorage := tpl.Spec.MainContainer.Resources.EphemeralStorage.DeepCopy()
	if ephemeralStorage.Sign() <= 0 {
		ephemeralStorage = resource.MustParse(api.DefaultSandboxEphemeralStorage)
	}
	if ephemeralStorage.Cmp(resource.MustParse(api.DefaultSandboxEphemeralStorage)) != 0 {
		return false, "immutable_ephemeral_storage_shape"
	}
	return true, ""
}

func newSlot() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "s0-" + hex.EncodeToString(value), nil
}

// GenerationForConfig returns a stable rollout identity for immutable carrier shape inputs.
func GenerationForConfig(values ...string) string {
	values = append(values, sharedCarrierImmutableShapeVersion)
	hash := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(hash[:8])
}

func stringPointer(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
