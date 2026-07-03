package sandboxobservability

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

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	obsclickhouse "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/clickhouse"
)

const (
	componentName        = "sandbox-observability-clickhouse"
	defaultImage         = "clickhouse/clickhouse-server:24.8"
	defaultNativePort    = int32(9000)
	defaultHTTPPort      = int32(8123)
	defaultSecretKey     = "dsn"
	defaultUsername      = "sandbox0"
	clickHouseDataVolume = "data"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type RuntimeConfig struct {
	DSN                  string
	Database             string
	EventsTable          string
	LogsTable            string
	MetricsTable         string
	RetentionDays        int
	LogsRetentionDays    int
	MetricsRetentionDays int
	ConnectTimeout       metav1.Duration
	SkipSchemaMigration  bool
	Ingest               infrav1alpha1.SandboxObservabilityIngestConfig
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinResources(ctx, infra)
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	if infra == nil || infra.Spec.SandboxObservability == nil {
		return r.cleanupBuiltinResources(ctx, infra)
	}
	switch infra.Spec.SandboxObservability.Type {
	case infrav1alpha1.SandboxObservabilityTypeBuiltin:
		logger.Info("Reconciling builtin sandbox observability ClickHouse")
		if !resolveBuiltinConfig(infra).Enabled {
			return r.cleanupBuiltinResources(ctx, infra)
		}
		if err := r.reconcileBuiltinSecret(ctx, infra); err != nil {
			return err
		}
		if err := r.reconcileBuiltinPVC(ctx, infra); err != nil {
			return err
		}
		if err := r.reconcileBuiltinStatefulSet(ctx, infra); err != nil {
			return err
		}
		if err := r.reconcileBuiltinService(ctx, infra); err != nil {
			return err
		}
		return r.ensureBuiltinReady(ctx, infra)
	case infrav1alpha1.SandboxObservabilityTypeExternal:
		logger.Info("Using external sandbox observability ClickHouse")
		if err := r.cleanupBuiltinResources(ctx, infra); err != nil {
			return err
		}
		_, _, err := GetRuntimeConfig(ctx, r.Resources.Client, infra)
		return err
	case "", infrav1alpha1.SandboxObservabilityTypeDisabled:
		return r.cleanupBuiltinResources(ctx, infra)
	default:
		return fmt.Errorf("unsupported sandbox observability type: %s", infra.Spec.SandboxObservability.Type)
	}
}

func ApplyClusterGatewayConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, cfg *apiconfig.ClusterGatewayConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok {
		cfg.SandboxObservability = apiconfig.SandboxObservabilityConfig{Backend: apiconfig.SandboxObservabilityBackendDisabled}
		return nil
	}
	cfg.SandboxObservability = apiconfig.SandboxObservabilityConfig{
		Backend: apiconfig.SandboxObservabilityBackendClickHouse,
		ClickHouse: apiconfig.SandboxObservabilityClickHouseConfig{
			DSN:                  runtimeCfg.DSN,
			Database:             runtimeCfg.Database,
			EventsTable:          runtimeCfg.EventsTable,
			LogsTable:            runtimeCfg.LogsTable,
			MetricsTable:         runtimeCfg.MetricsTable,
			RetentionDays:        runtimeCfg.RetentionDays,
			LogsRetentionDays:    runtimeCfg.LogsRetentionDays,
			MetricsRetentionDays: runtimeCfg.MetricsRetentionDays,
			ConnectTimeout:       runtimeCfg.ConnectTimeout,
			SkipSchemaMigration:  runtimeCfg.SkipSchemaMigration,
		},
	}
	return nil
}

func ApplyNetdConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, clusterGatewayURL string, cfg *apiconfig.NetdConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(clusterGatewayURL) == "" {
		cfg.SandboxObservabilityIngestURL = ""
		return nil
	}
	cfg.SandboxObservabilityIngestURL = strings.TrimRight(clusterGatewayURL, "/") + "/internal/v1/sandbox-observability/events"
	applyIngestConfig(runtimeCfg.Ingest, cfg)
	return nil
}

func ApplyManagerConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra, clusterGatewayURL string, cfg *apiconfig.ManagerConfig) error {
	if cfg == nil {
		return nil
	}
	runtimeCfg, ok, err := GetRuntimeConfig(ctx, c, infra)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(clusterGatewayURL) == "" {
		cfg.SandboxObservabilityLogsIngestURL = ""
		cfg.SandboxObservabilityMetricsIngestURL = ""
		return nil
	}
	base := strings.TrimRight(clusterGatewayURL, "/") + "/internal/v1/sandbox-observability"
	cfg.SandboxObservabilityLogsIngestURL = base + "/logs"
	cfg.SandboxObservabilityMetricsIngestURL = base + "/metrics"
	applyManagerIngestConfig(runtimeCfg.Ingest, cfg)
	return nil
}

func GetRuntimeConfig(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (RuntimeConfig, bool, error) {
	if infra == nil || !infrav1alpha1.IsSandboxObservabilityEnabled(infra) {
		return RuntimeConfig{}, false, nil
	}
	cfg := RuntimeConfig{
		Database:             obsclickhouse.DefaultDatabase,
		EventsTable:          obsclickhouse.DefaultEventsTable,
		LogsTable:            obsclickhouse.DefaultLogsTable,
		MetricsTable:         obsclickhouse.DefaultMetricsTable,
		RetentionDays:        obsclickhouse.DefaultRetentionDays,
		LogsRetentionDays:    obsclickhouse.DefaultLogsRetentionDays,
		MetricsRetentionDays: obsclickhouse.DefaultMetricsRetentionDays,
		Ingest:               resolveIngestConfig(infra),
	}
	applyRetentionConfig(infra, &cfg)
	switch infra.Spec.SandboxObservability.Type {
	case infrav1alpha1.SandboxObservabilityTypeBuiltin:
		builtin := resolveClickHouseConfig(infra)
		cfg.Database = firstNonEmpty(builtin.Database, cfg.Database)
		cfg.EventsTable = firstNonEmpty(builtin.EventsTable, cfg.EventsTable)
		cfg.LogsTable = firstNonEmpty(builtin.LogsTable, cfg.LogsTable)
		cfg.MetricsTable = firstNonEmpty(builtin.MetricsTable, cfg.MetricsTable)
		dsn, err := builtinDSN(ctx, c, infra)
		if err != nil {
			return RuntimeConfig{}, false, err
		}
		cfg.DSN = dsn
	case infrav1alpha1.SandboxObservabilityTypeExternal:
		external := infra.Spec.SandboxObservability.External
		if external == nil {
			return RuntimeConfig{}, false, fmt.Errorf("external sandbox observability configuration is required")
		}
		cfg.Database = firstNonEmpty(external.ClickHouse.Database, cfg.Database)
		cfg.EventsTable = firstNonEmpty(external.ClickHouse.EventsTable, cfg.EventsTable)
		cfg.LogsTable = firstNonEmpty(external.ClickHouse.LogsTable, cfg.LogsTable)
		cfg.MetricsTable = firstNonEmpty(external.ClickHouse.MetricsTable, cfg.MetricsTable)
		cfg.ConnectTimeout = external.ClickHouse.ConnectTimeout
		cfg.SkipSchemaMigration = external.ClickHouse.SkipSchemaMigration
		dsn, err := externalDSN(ctx, c, infra)
		if err != nil {
			return RuntimeConfig{}, false, err
		}
		cfg.DSN = dsn
	default:
		return RuntimeConfig{}, false, nil
	}
	return cfg, true, nil
}

func (r *Reconciler) reconcileBuiltinSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinSecretName(infra)
	ch := resolveClickHouseConfig(infra)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		password := common.GenerateRandomString(32)
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"username": defaultUsername,
				"password": password,
				"database": firstNonEmpty(ch.Database, obsclickhouse.DefaultDatabase),
				"host":     builtinHost(infra),
				"port":     fmt.Sprintf("%d", firstNonZero(ch.NativePort, defaultNativePort)),
				"dsn":      buildClickHouseDSN(defaultUsername, password, builtinHost(infra), firstNonZero(ch.NativePort, defaultNativePort), firstNonEmpty(ch.Database, obsclickhouse.DefaultDatabase)),
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}
	return nil
}

func (r *Reconciler) reconcileBuiltinPVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	pvcName := builtinPVCName(infra)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if !errors.IsNotFound(err) {
		return nil
	}
	size := resource.MustParse("50Gi")
	if persistence := resolveClickHouseConfig(infra).Persistence; persistence != nil {
		size = persistence.Size
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
	if persistence := resolveClickHouseConfig(infra).Persistence; persistence != nil && strings.TrimSpace(persistence.StorageClass) != "" {
		pvc.Spec.StorageClassName = &persistence.StorageClass
	}
	if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
		return err
	}
	return r.Resources.Client.Create(ctx, pvc)
}

func (r *Reconciler) reconcileBuiltinStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinName(infra)
	ch := resolveClickHouseConfig(infra)
	labels := labels(infra)
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
							Image: firstNonEmpty(ch.Image, defaultImage),
							Ports: []corev1.ContainerPort{
								{Name: "native", ContainerPort: firstNonZero(ch.NativePort, defaultNativePort)},
								{Name: "http", ContainerPort: firstNonZero(ch.HTTPPort, defaultHTTPPort)},
							},
							Env: []corev1.EnvVar{
								secretEnv("CLICKHOUSE_USER", builtinSecretName(infra), "username"),
								secretEnv("CLICKHOUSE_PASSWORD", builtinSecretName(infra), "password"),
								secretEnv("CLICKHOUSE_DB", builtinSecretName(infra), "database"),
								{Name: "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", Value: "1"},
							},
							VolumeMounts: []corev1.VolumeMount{{
								Name:      clickHouseDataVolume,
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
							LivenessProbe:  tcpProbe(firstNonZero(ch.NativePort, defaultNativePort), 30),
							ReadinessProbe: tcpProbe(firstNonZero(ch.NativePort, defaultNativePort), 5),
						},
					},
					Volumes: []corev1.Volume{{
						Name: clickHouseDataVolume,
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: builtinPVCName(infra)},
						},
					}},
				},
			},
		},
	}
	return r.Resources.ApplyStatefulSet(ctx, infra, desired)
}

func (r *Reconciler) reconcileBuiltinService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	ch := resolveClickHouseConfig(infra)
	return r.Resources.ReconcileServicePorts(ctx, infra, builtinName(infra), labels(infra), corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{Name: "native", Port: firstNonZero(ch.NativePort, defaultNativePort), TargetPort: intstr.FromString("native")},
		{Name: "http", Port: firstNonZero(ch.HTTPPort, defaultHTTPPort), TargetPort: intstr.FromString("http")},
	})
}

func (r *Reconciler) ensureBuiltinReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := builtinName(infra)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	if sts.Status.ReadyReplicas < replicas {
		return fmt.Errorf("sandbox observability clickhouse statefulset %q not ready: %d/%d ready", name, sts.Status.ReadyReplicas, replicas)
	}
	return nil
}

func (r *Reconciler) cleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if infra == nil {
		return nil
	}
	for _, obj := range []client.Object{&appsv1.StatefulSet{}, &corev1.Service{}} {
		if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: builtinName(infra), Namespace: infra.Namespace}, obj); err == nil {
			if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
				return err
			}
		} else if !errors.IsNotFound(err) {
			return err
		}
	}
	if resolveClickHouseConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}
	for _, obj := range []client.Object{&corev1.PersistentVolumeClaim{}, &corev1.Secret{}} {
		name := builtinPVCName(infra)
		if _, ok := obj.(*corev1.Secret); ok {
			name = builtinSecretName(infra)
		}
		if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, obj); err == nil {
			if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
				return err
			}
		} else if !errors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func externalDSN(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	external := infra.Spec.SandboxObservability.External
	if external == nil {
		return "", fmt.Errorf("external sandbox observability configuration is required")
	}
	ref := external.ClickHouse.DSNSecret
	if ref.Key == "" {
		ref.Key = defaultSecretKey
	}
	if strings.TrimSpace(ref.Name) == "" {
		return "", fmt.Errorf("external sandbox observability dsn secret name is required")
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: infra.Namespace}, secret); err != nil {
		return "", fmt.Errorf("sandbox observability clickhouse dsn secret not found: %w", err)
	}
	dsn := strings.TrimSpace(string(secret.Data[ref.Key]))
	if dsn == "" {
		return "", fmt.Errorf("key %s not found in sandbox observability clickhouse dsn secret %q", ref.Key, ref.Name)
	}
	return dsn, nil
}

func builtinDSN(ctx context.Context, c client.Client, infra *infrav1alpha1.Sandbox0Infra) (string, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: builtinSecretName(infra), Namespace: infra.Namespace}, secret); err != nil {
		return "", err
	}
	dsn := strings.TrimSpace(string(secret.Data["dsn"]))
	if dsn == "" {
		return "", fmt.Errorf("sandbox observability clickhouse secret %q missing dsn", builtinSecretName(infra))
	}
	return dsn, nil
}

func applyRetentionConfig(infra *infrav1alpha1.Sandbox0Infra, cfg *RuntimeConfig) {
	retention := infra.Spec.SandboxObservability.Retention
	if retention.AuditDays > 0 {
		cfg.RetentionDays = retention.AuditDays
	}
	if retention.LogDays > 0 {
		cfg.LogsRetentionDays = retention.LogDays
	}
	if retention.MetricDays > 0 {
		cfg.MetricsRetentionDays = retention.MetricDays
	}
}

func applyIngestConfig(ingest infrav1alpha1.SandboxObservabilityIngestConfig, cfg *apiconfig.NetdConfig) {
	cfg.SandboxObservabilityIngestQueueSize = ingest.QueueSize
	cfg.SandboxObservabilityIngestBatchSize = ingest.BatchSize
	cfg.SandboxObservabilityIngestFlushInterval = ingest.FlushInterval
	cfg.SandboxObservabilityIngestRequestTimeout = ingest.RequestTimeout
	cfg.SandboxObservabilityIngestMaxRetries = ingest.MaxRetries
	cfg.SandboxObservabilityIngestRetryBackoff = ingest.RetryBackoff
}

func applyManagerIngestConfig(ingest infrav1alpha1.SandboxObservabilityIngestConfig, cfg *apiconfig.ManagerConfig) {
	cfg.SandboxObservabilityIngestQueueSize = ingest.QueueSize
	cfg.SandboxObservabilityIngestBatchSize = ingest.BatchSize
	cfg.SandboxObservabilityIngestFlushInterval = ingest.FlushInterval
	cfg.SandboxObservabilityIngestRequestTimeout = ingest.RequestTimeout
	cfg.SandboxObservabilityIngestMaxRetries = ingest.MaxRetries
	cfg.SandboxObservabilityIngestRetryBackoff = ingest.RetryBackoff
}

func resolveIngestConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.SandboxObservabilityIngestConfig {
	cfg := infrav1alpha1.SandboxObservabilityIngestConfig{}
	if infra != nil && infra.Spec.SandboxObservability != nil {
		cfg = infra.Spec.SandboxObservability.Ingest
	}
	return cfg
}

func resolveBuiltinConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinSandboxObservabilityConfig {
	cfg := infrav1alpha1.BuiltinSandboxObservabilityConfig{Enabled: true}
	if infra == nil || infra.Spec.SandboxObservability == nil || infra.Spec.SandboxObservability.Builtin == nil {
		return cfg
	}
	return *infra.Spec.SandboxObservability.Builtin
}

func resolveClickHouseConfig(infra *infrav1alpha1.Sandbox0Infra) infrav1alpha1.BuiltinSandboxObservabilityClickHouseConfig {
	cfg := infrav1alpha1.BuiltinSandboxObservabilityClickHouseConfig{
		Image:                  defaultImage,
		NativePort:             defaultNativePort,
		HTTPPort:               defaultHTTPPort,
		Database:               obsclickhouse.DefaultDatabase,
		EventsTable:            obsclickhouse.DefaultEventsTable,
		LogsTable:              obsclickhouse.DefaultLogsTable,
		MetricsTable:           obsclickhouse.DefaultMetricsTable,
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}
	builtin := resolveBuiltinConfig(infra)
	if strings.TrimSpace(builtin.ClickHouse.Image) != "" {
		cfg.Image = builtin.ClickHouse.Image
	}
	if builtin.ClickHouse.NativePort != 0 {
		cfg.NativePort = builtin.ClickHouse.NativePort
	}
	if builtin.ClickHouse.HTTPPort != 0 {
		cfg.HTTPPort = builtin.ClickHouse.HTTPPort
	}
	cfg.Database = firstNonEmpty(builtin.ClickHouse.Database, cfg.Database)
	cfg.EventsTable = firstNonEmpty(builtin.ClickHouse.EventsTable, cfg.EventsTable)
	cfg.LogsTable = firstNonEmpty(builtin.ClickHouse.LogsTable, cfg.LogsTable)
	cfg.MetricsTable = firstNonEmpty(builtin.ClickHouse.MetricsTable, cfg.MetricsTable)
	if builtin.ClickHouse.Persistence != nil {
		cfg.Persistence = builtin.ClickHouse.Persistence
	}
	if builtin.ClickHouse.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.ClickHouse.StatefulResourcePolicy
	}
	return cfg
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

func builtinName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-%s", infra.Name, componentName)
}

func builtinPVCName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-data", builtinName(infra))
}

func builtinSecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-credentials", builtinName(infra))
}

func builtinHost(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra.Namespace == "" {
		return builtinName(infra)
	}
	return fmt.Sprintf("%s.%s.svc", builtinName(infra), infra.Namespace)
}

func labels(infra *infrav1alpha1.Sandbox0Infra) map[string]string {
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
