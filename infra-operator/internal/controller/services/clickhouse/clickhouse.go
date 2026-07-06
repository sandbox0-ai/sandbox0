package clickhouse

import (
	"context"
	"fmt"
	"net/url"
	"strings"

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

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	obsclickhouse "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/clickhouse"
)

const (
	componentName          = "clickhouse"
	legacyComponentName    = "sandbox-observability-clickhouse"
	defaultImage           = "clickhouse/clickhouse-server:24.8"
	defaultNativePort      = int32(9000)
	defaultHTTPPort        = int32(8123)
	defaultSecretKey       = "dsn"
	defaultUsername        = "sandbox0"
	defaultDataVolume      = "data"
	defaultPersistenceSize = "50Gi"
	DefaultMeteringDB      = "sandbox0_metering"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type RuntimeConfig struct {
	DSN                    string
	NativePort             int32
	HTTPPort               int32
	ConnectTimeout         metav1.Duration
	SchemaMigrationEnabled bool
	Databases              infrav1alpha1.ClickHouseDatabaseConfig
}

type resolvedConfig struct {
	Type                   infrav1alpha1.ClickHouseType
	Builtin                infrav1alpha1.BuiltinClickHouseConfig
	External               *infrav1alpha1.ExternalClickHouseConfig
	Databases              infrav1alpha1.ClickHouseDatabaseConfig
	SchemaMigrationEnabled bool
	Legacy                 bool
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinResources(ctx, infra)
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	cfg, ok, err := resolve(infra)
	if err != nil {
		return err
	}
	if !ok {
		return r.cleanupBuiltinResources(ctx, infra)
	}
	switch cfg.Type {
	case infrav1alpha1.ClickHouseTypeBuiltin:
		logger.Info("Reconciling builtin ClickHouse")
		if !cfg.Builtin.Enabled {
			return r.cleanupBuiltinResources(ctx, infra)
		}
		if err := r.reconcileBuiltinSecret(ctx, infra, cfg); err != nil {
			return err
		}
		if err := r.reconcileBuiltinPVC(ctx, infra, cfg); err != nil {
			return err
		}
		if err := r.reconcileBuiltinStatefulSet(ctx, infra, cfg); err != nil {
			return err
		}
		if err := r.reconcileBuiltinService(ctx, infra, cfg); err != nil {
			return err
		}
		return r.ensureBuiltinReady(ctx, infra)
	case infrav1alpha1.ClickHouseTypeExternal:
		logger.Info("Using external ClickHouse")
		if err := r.cleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
		_, _, err := GetRuntimeConfig(ctx, r.Resources.Client, infra)
		return err
	default:
		return r.cleanupBuiltinResources(ctx, infra)
	}
}

func GetRuntimeConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (RuntimeConfig, bool, error) {
	cfg, ok, err := resolve(infra)
	if err != nil || !ok {
		return RuntimeConfig{}, ok, err
	}
	runtime := RuntimeConfig{
		NativePort:             firstNonZero(cfg.Builtin.NativePort, defaultNativePort),
		HTTPPort:               firstNonZero(cfg.Builtin.HTTPPort, defaultHTTPPort),
		SchemaMigrationEnabled: cfg.SchemaMigrationEnabled,
		Databases:              cfg.Databases,
	}
	switch cfg.Type {
	case infrav1alpha1.ClickHouseTypeBuiltin:
		dsn, err := builtinDSN(ctx, c, infra)
		if err != nil {
			return RuntimeConfig{}, false, err
		}
		runtime.DSN = dsn
	case infrav1alpha1.ClickHouseTypeExternal:
		dsn, err := externalDSN(ctx, c, infra, cfg.External)
		if err != nil {
			return RuntimeConfig{}, false, err
		}
		runtime.DSN = dsn
		if cfg.External != nil {
			runtime.ConnectTimeout = cfg.External.ConnectTimeout
		}
	default:
		return RuntimeConfig{}, false, nil
	}
	return runtime, true, nil
}

func (r *Reconciler) reconcileBuiltinSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg resolvedConfig) error {
	name := BuiltinSecretName(infra)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		password := common.GenerateRandomString(32)
		observabilityDB := firstNonEmpty(cfg.Databases.Observability, obsclickhouse.DefaultDatabase)
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"username":               defaultUsername,
				"password":               password,
				"database":               observabilityDB,
				"observability_database": observabilityDB,
				"metering_database":      firstNonEmpty(cfg.Databases.Metering, DefaultMeteringDB),
				"host":                   builtinHost(infra),
				"port":                   fmt.Sprintf("%d", firstNonZero(cfg.Builtin.NativePort, defaultNativePort)),
				"dsn":                    buildClickHouseDSN(defaultUsername, password, builtinHost(infra), firstNonZero(cfg.Builtin.NativePort, defaultNativePort), observabilityDB),
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}
	return nil
}

func (r *Reconciler) reconcileBuiltinPVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg resolvedConfig) error {
	pvcName := BuiltinPVCName(infra)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if !errors.IsNotFound(err) {
		return nil
	}
	size := resource.MustParse(defaultPersistenceSize)
	if cfg.Builtin.Persistence != nil {
		size = cfg.Builtin.Persistence.Size
	}
	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: infra.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if cfg.Builtin.Persistence != nil && strings.TrimSpace(cfg.Builtin.Persistence.StorageClass) != "" {
		pvc.Spec.StorageClassName = &cfg.Builtin.Persistence.StorageClass
	}
	if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
		return err
	}
	return r.Resources.Client.Create(ctx, pvc)
}

func (r *Reconciler) reconcileBuiltinStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg resolvedConfig) error {
	name := BuiltinName(infra)
	labels := Labels(infra)
	replicas := int32(1)
	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace, Labels: common.EnsureManagedLabels(labels, name)},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      common.EnsureManagedLabels(labels, name),
					Annotations: common.EnsurePodTemplateAnnotations(nil),
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  componentName,
							Image: firstNonEmpty(cfg.Builtin.Image, defaultImage),
							Ports: []corev1.ContainerPort{
								{Name: "native", ContainerPort: firstNonZero(cfg.Builtin.NativePort, defaultNativePort)},
								{Name: "http", ContainerPort: firstNonZero(cfg.Builtin.HTTPPort, defaultHTTPPort)},
							},
							Env: []corev1.EnvVar{
								secretEnv("CLICKHOUSE_USER", BuiltinSecretName(infra), "username"),
								secretEnv("CLICKHOUSE_PASSWORD", BuiltinSecretName(infra), "password"),
								secretEnv("CLICKHOUSE_DB", BuiltinSecretName(infra), "database"),
								{Name: "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", Value: "1"},
							},
							VolumeMounts: []corev1.VolumeMount{{
								Name:      defaultDataVolume,
								MountPath: "/var/lib/clickhouse",
							}},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("250m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("2"),
									corev1.ResourceMemory: resource.MustParse("4Gi"),
								},
							},
							LivenessProbe:  tcpProbe(firstNonZero(cfg.Builtin.NativePort, defaultNativePort), 30),
							ReadinessProbe: tcpProbe(firstNonZero(cfg.Builtin.NativePort, defaultNativePort), 5),
						},
					},
					Volumes: []corev1.Volume{{
						Name: defaultDataVolume,
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: BuiltinPVCName(infra)},
						},
					}},
				},
			},
		},
	}
	return r.Resources.ApplyStatefulSet(ctx, infra, desired)
}

func (r *Reconciler) reconcileBuiltinService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, cfg resolvedConfig) error {
	return r.Resources.ReconcileServicePorts(ctx, infra, BuiltinName(infra), Labels(infra), corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{Name: "native", Port: firstNonZero(cfg.Builtin.NativePort, defaultNativePort), TargetPort: intstr.FromString("native")},
		{Name: "http", Port: firstNonZero(cfg.Builtin.HTTPPort, defaultHTTPPort), TargetPort: intstr.FromString("http")},
	})
}

func (r *Reconciler) ensureBuiltinReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := BuiltinName(infra)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < replicas {
		return fmt.Errorf("clickhouse statefulset %q not ready: %d/%d ready", name, sts.Status.ReadyReplicas, replicas)
	}
	return nil
}

func (r *Reconciler) cleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return nil
	}
	for _, name := range []string{BuiltinName(infra), legacyBuiltinName(infra)} {
		for _, obj := range []client.Object{&appsv1.StatefulSet{}, &corev1.Service{}} {
			if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, obj); err == nil {
				if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
					return err
				}
			} else if !errors.IsNotFound(err) {
				return err
			}
		}
	}
	cfg, _, _ := resolve(infra)
	if cfg.Builtin.StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}
	for _, nameSet := range []struct {
		secret string
		pvc    string
	}{
		{secret: BuiltinSecretName(infra), pvc: BuiltinPVCName(infra)},
		{secret: legacyBuiltinSecretName(infra), pvc: legacyBuiltinPVCName(infra)},
	} {
		for _, obj := range []client.Object{&corev1.PersistentVolumeClaim{}, &corev1.Secret{}} {
			name := nameSet.pvc
			if _, ok := obj.(*corev1.Secret); ok {
				name = nameSet.secret
			}
			if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, obj); err == nil {
				if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
					return err
				}
			} else if !errors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func resolve(infra *infrav1alpha1.Sandbox0Infra) (resolvedConfig, bool, error) {
	cfg := defaultResolvedConfig()
	if infra == nil {
		return cfg, false, nil
	}
	if infra.Spec.ClickHouse != nil {
		cfg.Type = infra.Spec.ClickHouse.Type
		if cfg.Type == "" {
			cfg.Type = infrav1alpha1.ClickHouseTypeDisabled
		}
		if infra.Spec.ClickHouse.Builtin != nil {
			cfg.Builtin = mergeBuiltinConfig(cfg.Builtin, *infra.Spec.ClickHouse.Builtin)
		}
		if infra.Spec.ClickHouse.External != nil {
			external := *infra.Spec.ClickHouse.External
			cfg.External = &external
		}
		cfg.Databases = mergeDatabases(cfg.Databases, infra.Spec.ClickHouse.Databases)
		if infra.Spec.ClickHouse.SchemaMigration.Enabled != nil {
			cfg.SchemaMigrationEnabled = *infra.Spec.ClickHouse.SchemaMigration.Enabled
		}
		return cfg, cfg.Type == infrav1alpha1.ClickHouseTypeBuiltin || cfg.Type == infrav1alpha1.ClickHouseTypeExternal, nil
	}
	if infra.Spec.SandboxObservability == nil {
		return cfg, false, nil
	}
	switch infra.Spec.SandboxObservability.Type {
	case infrav1alpha1.SandboxObservabilityTypeBuiltin:
		cfg.Type = infrav1alpha1.ClickHouseTypeBuiltin
		cfg.Legacy = true
		if infra.Spec.SandboxObservability.Builtin != nil {
			cfg.Builtin.Enabled = infra.Spec.SandboxObservability.Builtin.Enabled
			cfg.Builtin.Image = firstNonEmpty(infra.Spec.SandboxObservability.Builtin.ClickHouse.Image, cfg.Builtin.Image)
			cfg.Builtin.NativePort = firstNonZero(infra.Spec.SandboxObservability.Builtin.ClickHouse.NativePort, cfg.Builtin.NativePort)
			cfg.Builtin.HTTPPort = firstNonZero(infra.Spec.SandboxObservability.Builtin.ClickHouse.HTTPPort, cfg.Builtin.HTTPPort)
			cfg.Builtin.Persistence = infra.Spec.SandboxObservability.Builtin.ClickHouse.Persistence
			if infra.Spec.SandboxObservability.Builtin.ClickHouse.StatefulResourcePolicy != "" {
				cfg.Builtin.StatefulResourcePolicy = infra.Spec.SandboxObservability.Builtin.ClickHouse.StatefulResourcePolicy
			}
			cfg.Databases.Observability = firstNonEmpty(infra.Spec.SandboxObservability.Builtin.ClickHouse.Database, cfg.Databases.Observability)
		}
		return cfg, cfg.Builtin.Enabled, nil
	case infrav1alpha1.SandboxObservabilityTypeExternal:
		cfg.Type = infrav1alpha1.ClickHouseTypeExternal
		cfg.Legacy = true
		legacy := infra.Spec.SandboxObservability.External
		if legacy == nil {
			return cfg, false, fmt.Errorf("external sandbox observability configuration is required")
		}
		cfg.External = &infrav1alpha1.ExternalClickHouseConfig{
			DSNSecret: infrav1alpha1.ClickHouseDSNSecretRef{
				Name: legacy.ClickHouse.DSNSecret.Name,
				Key:  legacy.ClickHouse.DSNSecret.Key,
			},
			ConnectTimeout: legacy.ClickHouse.ConnectTimeout,
		}
		cfg.Databases.Observability = firstNonEmpty(legacy.ClickHouse.Database, cfg.Databases.Observability)
		cfg.SchemaMigrationEnabled = !legacy.ClickHouse.SkipSchemaMigration
		return cfg, true, nil
	default:
		return cfg, false, nil
	}
}

func defaultResolvedConfig() resolvedConfig {
	return resolvedConfig{
		Type: infrav1alpha1.ClickHouseTypeDisabled,
		Builtin: infrav1alpha1.BuiltinClickHouseConfig{
			Enabled:                true,
			Image:                  defaultImage,
			NativePort:             defaultNativePort,
			HTTPPort:               defaultHTTPPort,
			StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
		},
		Databases: infrav1alpha1.ClickHouseDatabaseConfig{
			Observability: obsclickhouse.DefaultDatabase,
			Metering:      DefaultMeteringDB,
		},
		SchemaMigrationEnabled: true,
	}
}

func mergeBuiltinConfig(base, override infrav1alpha1.BuiltinClickHouseConfig) infrav1alpha1.BuiltinClickHouseConfig {
	base.Enabled = override.Enabled
	base.Image = firstNonEmpty(override.Image, base.Image)
	base.NativePort = firstNonZero(override.NativePort, base.NativePort)
	base.HTTPPort = firstNonZero(override.HTTPPort, base.HTTPPort)
	if override.Persistence != nil {
		base.Persistence = override.Persistence
	}
	if override.StatefulResourcePolicy != "" {
		base.StatefulResourcePolicy = override.StatefulResourcePolicy
	}
	return base
}

func mergeDatabases(base, override infrav1alpha1.ClickHouseDatabaseConfig) infrav1alpha1.ClickHouseDatabaseConfig {
	base.Observability = firstNonEmpty(override.Observability, base.Observability)
	base.Metering = firstNonEmpty(override.Metering, base.Metering)
	return base
}

func externalDSN(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, external *infrav1alpha1.ExternalClickHouseConfig) (string, error) {
	if external == nil {
		return "", fmt.Errorf("external clickhouse configuration is required")
	}
	ref := external.DSNSecret
	if ref.Key == "" {
		ref.Key = defaultSecretKey
	}
	if strings.TrimSpace(ref.Name) == "" {
		return "", fmt.Errorf("external clickhouse dsn secret name is required")
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: infra.Namespace}, secret); err != nil {
		return "", fmt.Errorf("clickhouse dsn secret not found: %w", err)
	}
	dsn := strings.TrimSpace(string(secret.Data[ref.Key]))
	if dsn == "" {
		return "", fmt.Errorf("key %s not found in clickhouse dsn secret %q", ref.Key, ref.Name)
	}
	return dsn, nil
}

func builtinDSN(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: BuiltinSecretName(infra), Namespace: infra.Namespace}, secret); err != nil {
		return "", err
	}
	dsn := strings.TrimSpace(string(secret.Data["dsn"]))
	if dsn == "" {
		return "", fmt.Errorf("clickhouse secret %q missing dsn", BuiltinSecretName(infra))
	}
	return dsn, nil
}

func buildClickHouseDSN(username, password, host string, port int32, database string) string {
	u := url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(username, password),
		Host:   fmt.Sprintf("%s:%d", host, port),
		Path:   "/" + database,
	}
	return u.String()
}

func BuiltinName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-%s", infra.Name, componentName)
}

func BuiltinPVCName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-data", BuiltinName(infra))
}

func BuiltinSecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-credentials", BuiltinName(infra))
}

func builtinHost(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra.Namespace == "" {
		return BuiltinName(infra)
	}
	return fmt.Sprintf("%s.%s.svc", BuiltinName(infra), infra.Namespace)
}

func legacyBuiltinName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-%s", infra.Name, legacyComponentName)
}

func legacyBuiltinPVCName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-data", legacyBuiltinName(infra))
}

func legacyBuiltinSecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-credentials", legacyBuiltinName(infra))
}

func Labels(infra *infrav1alpha1.Sandbox0Infra) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       componentName,
		"app.kubernetes.io/instance":   infra.Name,
		"app.kubernetes.io/component":  componentName,
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}
}

func secretEnv(name, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

func tcpProbe(port int32, initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(int(port))},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       10,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonZero(values ...int32) int32 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
