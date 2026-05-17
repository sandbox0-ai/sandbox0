package redis

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
)

const (
	componentName = "redis"
	defaultImage  = "redis:7-alpine"
	defaultPort   = int32(6379)
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type RateLimitConfig struct {
	URL       string
	KeyPrefix string
	Timeout   metav1.Duration
	FailOpen  bool
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// CleanupBuiltinResources removes builtin Redis resources.
func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinRedisResources(ctx, infra)
}

// Reconcile reconciles the region-level Redis component.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	if infra.Spec.Redis == nil {
		return r.cleanupBuiltinRedisResources(ctx, infra)
	}

	switch infra.Spec.Redis.Type {
	case infrav1alpha1.RedisTypeBuiltin, "":
		logger.Info("Reconciling builtin Redis")
		if !resolveBuiltinRedisConfig(infra).Enabled {
			return r.cleanupBuiltinRedisResources(ctx, infra)
		}
		return r.reconcileBuiltinRedis(ctx, infra)
	case infrav1alpha1.RedisTypeExternal:
		logger.Info("Using external Redis")
		if err := r.cleanupBuiltinRedisResources(ctx, infra); err != nil {
			return err
		}
		return ValidateExternalRedis(ctx, r.Resources.Client, infra)
	default:
		return fmt.Errorf("unsupported redis type: %s", infra.Spec.Redis.Type)
	}
}

func ValidateExternalRedis(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra.Spec.Redis == nil || infra.Spec.Redis.External == nil {
		return fmt.Errorf("external redis configuration is required")
	}
	if strings.TrimSpace(infra.Spec.Redis.External.URLSecret.Name) == "" {
		return fmt.Errorf("redis external urlSecret.name is required")
	}
	_, err := externalRedisURL(ctx, c, infra)
	return err
}

func (r *Reconciler) reconcileBuiltinRedis(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if err := r.reconcileRedisDeployment(ctx, infra); err != nil {
		return err
	}
	if err := r.reconcileRedisService(ctx, infra); err != nil {
		return err
	}
	return r.ensureRedisReady(ctx, infra)
}

func (r *Reconciler) reconcileRedisDeployment(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinRedisName(infra)
	cfg := resolveBuiltinRedisConfig(infra)
	labels := redisLabels(infra)
	replicas := int32(1)

	deploy := &appsv1.Deployment{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desired := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    common.EnsureManagedLabels(labels, name),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      common.EnsureManagedLabels(labels, name),
					Annotations: common.EnsurePodTemplateAnnotations(nil),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  componentName,
							Image: cfg.Image,
							Args:  []string{"--save", "", "--appendonly", "no"},
							Ports: []corev1.ContainerPort{
								{Name: componentName, ContainerPort: cfg.Port},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("50m"),
									corev1.ResourceMemory: resource.MustParse("64Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
							LivenessProbe:  redisProbe(cfg.Port),
							ReadinessProbe: redisProbe(cfg.Port),
						},
					},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}

	deploy.Labels = desired.Labels
	deploy.Spec = desired.Spec
	return r.Resources.Client.Update(ctx, deploy)
}

func (r *Reconciler) reconcileRedisService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinRedisName(infra)
	cfg := resolveBuiltinRedisConfig(infra)
	labels := redisLabels(infra)
	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{
			Name:       componentName,
			Port:       cfg.Port,
			TargetPort: intstr.FromInt(int(cfg.Port)),
			Protocol:   corev1.ProtocolTCP,
		},
	})
}

func (r *Reconciler) ensureRedisReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinRedisName(infra)
	deploy := &appsv1.Deployment{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy); err != nil {
		return err
	}
	if deploy.Status.ReadyReplicas < 1 {
		return fmt.Errorf("redis deployment %q not ready: %d/1 ready", name, deploy.Status.ReadyReplicas)
	}

	host := fmt.Sprintf("%s.%s.svc", name, infra.Namespace)
	port := resolveBuiltinRedisConfig(infra).Port
	return waitForTCP(ctx, host, port, 3*time.Second)
}

func (r *Reconciler) cleanupBuiltinRedisResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinRedisName(infra)
	deploy := &appsv1.Deployment{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy); err == nil {
		if err := r.Resources.Client.Delete(ctx, deploy); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}

	svc := &corev1.Service{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, svc); err == nil {
		if err := r.Resources.Client.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
			return err
		}
	} else if !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// ApplyGatewayRateLimitConfig injects region-level Redis settings into gateway
// process config. Without spec.redis, gateways keep memory rate limiting.
func ApplyGatewayRateLimitConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.GatewayConfig) error {
	if cfg == nil {
		return nil
	}
	rl, ok, err := GetRateLimitConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok {
		cfg.RateLimitBackend = ratelimit.BackendMemory
		cfg.RateLimitRedisURL = ""
		return nil
	}
	cfg.RateLimitBackend = ratelimit.BackendRedis
	cfg.RateLimitRedisURL = rl.URL
	cfg.RateLimitRedisKeyPrefix = rl.KeyPrefix
	cfg.RateLimitRedisTimeout = rl.Timeout
	cfg.RateLimitFailOpen = rl.FailOpen
	return nil
}

func GetRateLimitConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (RateLimitConfig, bool, error) {
	if infra == nil || !infrav1alpha1.IsRedisEnabled(infra) {
		return RateLimitConfig{}, false, nil
	}

	var redisURL string
	switch infra.Spec.Redis.Type {
	case infrav1alpha1.RedisTypeBuiltin, "":
		redisURL = builtinRedisURL(infra)
	case infrav1alpha1.RedisTypeExternal:
		var err error
		redisURL, err = externalRedisURL(ctx, c, infra)
		if err != nil {
			return RateLimitConfig{}, false, err
		}
	default:
		return RateLimitConfig{}, false, fmt.Errorf("unsupported redis type: %s", infra.Spec.Redis.Type)
	}

	keyPrefix := strings.TrimSpace(infra.Spec.Redis.KeyPrefix)
	if keyPrefix == "" {
		keyPrefix = ratelimit.DefaultRedisKeyPrefix
	}
	timeout := infra.Spec.Redis.OperationTimeout
	if timeout.Duration == 0 {
		timeout.Duration = ratelimit.DefaultRedisTimeout
	}
	failOpen := true
	if infra.Spec.Redis.FailOpen != nil {
		failOpen = *infra.Spec.Redis.FailOpen
	}
	return RateLimitConfig{
		URL:       redisURL,
		KeyPrefix: keyPrefix,
		Timeout:   timeout,
		FailOpen:  failOpen,
	}, true, nil
}

func externalRedisURL(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	ref := infra.Spec.Redis.External.URLSecret
	if ref.Key == "" {
		ref.Key = "url"
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: infra.Namespace}, secret); err != nil {
		return "", fmt.Errorf("redis url secret not found: %w", err)
	}
	value := strings.TrimSpace(string(secret.Data[ref.Key]))
	if value == "" {
		return "", fmt.Errorf("key %s not found in redis url secret %q", ref.Key, ref.Name)
	}
	return value, nil
}

func builtinRedisURL(infra *infrav1alpha1.Sandbox0Infra) string {
	cfg := resolveBuiltinRedisConfig(infra)
	return fmt.Sprintf("redis://%s:%d/0", builtinRedisHost(infra), cfg.Port)
}

func builtinRedisHost(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra.Namespace == "" {
		return builtinRedisName(infra)
	}
	return fmt.Sprintf("%s.%s.svc", builtinRedisName(infra), infra.Namespace)
}

func builtinRedisName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-redis", infra.Name)
}

func redisLabels(infra *infrav1alpha1.Sandbox0Infra) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       componentName,
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  componentName,
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}
}

func redisProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(port))},
		},
		InitialDelaySeconds: 3,
		PeriodSeconds:       5,
	}
}

func resolveBuiltinRedisConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinRedisConfig {
	cfg := infrav1alpha1.BuiltinRedisConfig{
		Enabled: true,
		Image:   defaultImage,
		Port:    defaultPort,
	}
	if infra == nil || infra.Spec.Redis == nil || infra.Spec.Redis.Builtin == nil {
		return cfg
	}
	builtin := infra.Spec.Redis.Builtin
	cfg.Enabled = builtin.Enabled
	if strings.TrimSpace(builtin.Image) != "" {
		cfg.Image = builtin.Image
	}
	if builtin.Port != 0 {
		cfg.Port = builtin.Port
	}
	return cfg
}

func waitForTCP(ctx context.Context, host string, port int32, timeout time.Duration) error {
	dialer := net.Dialer{}
	dialCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	conn, err := dialer.DialContext(dialCtx, "tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return fmt.Errorf("redis service %q not reachable: %w", net.JoinHostPort(host, fmt.Sprintf("%d", port)), err)
	}
	_ = conn.Close()
	return nil
}
