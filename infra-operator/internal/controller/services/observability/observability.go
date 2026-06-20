package observability

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
)

const (
	defaultCollectorImage   = "otel/opentelemetry-collector-contrib:0.130.1"
	defaultClickHouseImage  = "clickhouse/clickhouse-server:25.6"
	defaultClickHouseDB     = "otel"
	defaultClickHouseUser   = "otel"
	defaultClickHouseNative = int32(9000)
	defaultClickHouseHTTP   = int32(8123)
	defaultClickHousePVC    = "20Gi"

	agentComponentName      = "otel-agent"
	collectorConfigFileName = "config.yaml"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

type clickHouseConfig struct {
	Image                  string
	NativePort             int32
	HTTPPort               int32
	Database               string
	Username               string
	Persistence            *infrav1alpha1.PersistenceConfig
	Resources              *corev1.ResourceRequirements
	StatefulResourcePolicy infrav1alpha1.BuiltinStatefulResourcePolicy
}

type collectorConfig struct {
	Image     string
	Replicas  int32
	Resources *corev1.ResourceRequirements
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	switch common.ResolveObservabilityBackendType(infra) {
	case infrav1alpha1.ObservabilityBackendTypeDisabled, "":
		logger.Info("Observability backend is disabled")
		if err := r.cleanupManagedCollectorResources(ctx, infra); err != nil {
			return err
		}
		return r.cleanupBuiltinClickHouseResources(ctx, infra)
	case infrav1alpha1.ObservabilityBackendTypeBuiltin:
		logger.Info("Reconciling builtin observability backend")
		if err := r.reconcileBuiltinClickHouse(ctx, infra); err != nil {
			return err
		}
		if collectorRequired(infra) {
			return r.reconcileManagedCollectors(ctx, infra)
		}
		return r.cleanupManagedCollectorResources(ctx, infra)
	case infrav1alpha1.ObservabilityBackendTypeExternal:
		logger.Info("Reconciling external observability backend integration")
		if err := r.cleanupBuiltinClickHouseResources(ctx, infra); err != nil {
			return err
		}
		if common.ResolveExternalObservabilityMode(infra) == infrav1alpha1.ObservabilityExternalModeManagedCollector {
			if collectorRequired(infra) {
				return r.reconcileManagedCollectors(ctx, infra)
			}
			return r.cleanupManagedCollectorResources(ctx, infra)
		}
		return r.cleanupManagedCollectorResources(ctx, infra)
	default:
		return fmt.Errorf("unsupported observability backend type: %s", common.ResolveObservabilityBackendType(infra))
	}
}

func (r *Reconciler) CleanupBuiltinResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	return r.cleanupBuiltinClickHouseResources(ctx, infra)
}

func (r *Reconciler) reconcileBuiltinClickHouse(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if err := r.reconcileClickHouseSecret(ctx, infra); err != nil {
		return err
	}
	if err := r.reconcileClickHousePVC(ctx, infra); err != nil {
		return err
	}
	if err := r.reconcileClickHouseStatefulSet(ctx, infra); err != nil {
		return err
	}
	if err := r.reconcileClickHouseService(ctx, infra); err != nil {
		return err
	}
	return r.ensureClickHouseReady(ctx, infra)
}

func (r *Reconciler) reconcileClickHouseSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHouseSecretName(infra.Name)
	cfg := resolveClickHouseConfig(infra)
	host := common.BuiltinObservabilityClickHouseName(infra.Name)
	nativeEndpoint := fmt.Sprintf("tcp://%s.%s.svc:%d", host, infra.Namespace, cfg.NativePort)
	httpEndpoint := fmt.Sprintf("http://%s.%s.svc:%d", host, infra.Namespace, cfg.HTTPPort)

	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if errors.IsNotFound(err) {
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
			Type:       corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"username":       cfg.Username,
				"password":       common.GenerateRandomString(32),
				"database":       cfg.Database,
				"host":           host,
				"nativePort":     fmt.Sprintf("%d", cfg.NativePort),
				"httpPort":       fmt.Sprintf("%d", cfg.HTTPPort),
				"nativeEndpoint": nativeEndpoint,
				"httpEndpoint":   httpEndpoint,
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, secret)
	}

	return r.Resources.UpdateObjectIfChanged(ctx, secret, func() {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data["host"] = []byte(host)
		secret.Data["nativeEndpoint"] = []byte(nativeEndpoint)
		secret.Data["httpEndpoint"] = []byte(httpEndpoint)
	})
}

func (r *Reconciler) reconcileClickHousePVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHousePVCName(infra.Name)
	cfg := resolveClickHouseConfig(infra)

	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	size := resource.MustParse(defaultClickHousePVC)
	var storageClass string
	if cfg.Persistence != nil {
		if !cfg.Persistence.Size.IsZero() {
			size = cfg.Persistence.Size
		}
		storageClass = strings.TrimSpace(cfg.Persistence.StorageClass)
	}
	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
	if storageClass != "" {
		pvc.Spec.StorageClassName = &storageClass
	}
	if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
		return err
	}
	return r.Resources.Client.Create(ctx, pvc)
}

func (r *Reconciler) reconcileClickHouseStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHouseName(infra.Name)
	secretName := common.BuiltinObservabilityClickHouseSecretName(infra.Name)
	pvcName := common.BuiltinObservabilityClickHousePVCName(infra.Name)
	cfg := resolveClickHouseConfig(infra)
	labels := common.GetServiceLabels(infra.Name, "clickhouse")
	replicas := int32(1)
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("2Gi"),
		},
	}
	if cfg.Resources != nil {
		resources = *cfg.Resources
	}

	sts := &appsv1.StatefulSet{
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
							Name:  "clickhouse",
							Image: cfg.Image,
							Ports: []corev1.ContainerPort{
								{Name: "native", ContainerPort: cfg.NativePort},
								{Name: "http", ContainerPort: cfg.HTTPPort},
							},
							Env: []corev1.EnvVar{
								secretEnv("CLICKHOUSE_DB", secretName, "database"),
								secretEnv("CLICKHOUSE_USER", secretName, "username"),
								secretEnv("CLICKHOUSE_PASSWORD", secretName, "password"),
								{Name: "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", Value: "1"},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/var/lib/clickhouse"},
							},
							Resources: resources,
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{Path: "/ping", Port: intstr.FromString("http")},
								},
								InitialDelaySeconds: 30,
								PeriodSeconds:       10,
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									HTTPGet: &corev1.HTTPGetAction{Path: "/ping", Port: intstr.FromString("http")},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
							},
						},
					},
				},
			},
		},
	}
	return r.Resources.ApplyStatefulSet(ctx, infra, sts)
}

func (r *Reconciler) reconcileClickHouseService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHouseName(infra.Name)
	cfg := resolveClickHouseConfig(infra)
	labels := common.GetServiceLabels(infra.Name, "clickhouse")
	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{Name: "native", Port: cfg.NativePort, TargetPort: intstr.FromString("native"), Protocol: corev1.ProtocolTCP},
		{Name: "http", Port: cfg.HTTPPort, TargetPort: intstr.FromString("http"), Protocol: corev1.ProtocolTCP},
	})
}

func (r *Reconciler) ensureClickHouseReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHouseName(infra.Name)
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, sts); err != nil {
		return err
	}
	if sts.Status.ReadyReplicas < 1 {
		return fmt.Errorf("clickhouse statefulset %q not ready: %d/1 ready", name, sts.Status.ReadyReplicas)
	}
	return nil
}

func (r *Reconciler) reconcileManagedCollectors(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if err := r.reconcileCollectorRBAC(ctx, infra); err != nil {
		return err
	}
	if common.ObservabilityLogsCollectionEnabled(infra) {
		if err := r.reconcileAgentCollector(ctx, infra); err != nil {
			return err
		}
	} else if err := r.cleanupAgentCollectorResources(ctx, infra); err != nil {
		return err
	}
	if err := r.reconcileGatewayCollector(ctx, infra); err != nil {
		return err
	}
	return r.ensureCollectorsReady(ctx, infra)
}

func (r *Reconciler) reconcileGatewayCollector(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.ManagedObservabilityCollectorName(infra.Name)
	labels := common.GetServiceLabels(infra.Name, common.ObservabilityCollectorComponentName)
	cfg := resolveCollectorConfig(infra)
	config, env, err := r.gatewayCollectorConfig(ctx, infra)
	if err != nil {
		return err
	}
	configHash, err := r.reconcileConfigSecret(ctx, infra, fmt.Sprintf("%s-config", name), labels, config)
	if err != nil {
		return err
	}
	annotations := common.ConfigHashAnnotationFromHash(configHash)

	env = append(env, corev1.EnvVar{Name: "GOMEMLIMIT", Value: "256MiB"})
	if err := r.Resources.ReconcileDeployment(ctx, infra, name, labels, cfg.Replicas, common.ServiceDefinition{
		Name:               common.ObservabilityCollectorComponentName,
		Image:              cfg.Image,
		Args:               []string{"--config=/etc/otelcol/config.yaml"},
		ServiceAccountName: name,
		EnvVars:            env,
		Ports: []corev1.ContainerPort{
			{Name: "otlp-grpc", ContainerPort: 4317},
			{Name: "otlp-http", ContainerPort: 4318},
			{Name: "health", ContainerPort: 13133},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "config", MountPath: "/etc/otelcol/config.yaml", SubPath: collectorConfigFileName, ReadOnly: true},
		},
		Volumes: []corev1.Volume{
			{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: fmt.Sprintf("%s-config", name)},
				},
			},
		},
		PodAnnotations: annotations,
		Resources:      cfg.Resources,
		LivenessProbe:  httpProbe("/"),
		ReadinessProbe: httpProbe("/"),
	}); err != nil {
		return err
	}

	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		{Name: "otlp-grpc", Port: 4317, TargetPort: intstr.FromString("otlp-grpc"), Protocol: corev1.ProtocolTCP},
		{Name: "otlp-http", Port: 4318, TargetPort: intstr.FromString("otlp-http"), Protocol: corev1.ProtocolTCP},
		{Name: "health", Port: 13133, TargetPort: intstr.FromString("health"), Protocol: corev1.ProtocolTCP},
	})
}

func (r *Reconciler) reconcileAgentCollector(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := agentCollectorName(infra.Name)
	serviceAccountName := common.ManagedObservabilityCollectorName(infra.Name)
	labels := common.GetServiceLabels(infra.Name, agentComponentName)
	cfg := resolveCollectorConfig(infra)
	config := agentCollectorConfig(infra)
	configHash, err := r.reconcileConfigSecret(ctx, infra, fmt.Sprintf("%s-config", name), labels, config)
	if err != nil {
		return err
	}

	desired := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace, Labels: common.EnsureManagedLabels(labels, name)},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      common.EnsureManagedLabels(labels, name),
					Annotations: common.ConfigHashAnnotationFromHash(configHash),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccountName,
					Containers: []corev1.Container{
						{
							Name:  agentComponentName,
							Image: cfg.Image,
							Args:  []string{"--config=/etc/otelcol/config.yaml"},
							Env: []corev1.EnvVar{
								{Name: "GOMEMLIMIT", Value: "128MiB"},
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "config", MountPath: "/etc/otelcol/config.yaml", SubPath: collectorConfigFileName, ReadOnly: true},
								{Name: "varlogpods", MountPath: "/var/log/pods", ReadOnly: true},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("50m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{SecretName: fmt.Sprintf("%s-config", name)},
							},
						},
						{
							Name: "varlogpods",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/var/log/pods"},
							},
						},
					},
				},
			},
		},
	}
	if cfg.Resources != nil {
		desired.Spec.Template.Spec.Containers[0].Resources = *cfg.Resources
	}
	return r.Resources.ApplyDaemonSet(ctx, infra, desired)
}

func (r *Reconciler) gatewayCollectorConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (map[string]any, []corev1.EnvVar, error) {
	receivers := map[string]any{}
	processors := defaultProcessors()
	exporters, env, err := r.gatewayExporters(ctx, infra)
	if err != nil {
		return nil, nil, err
	}
	pipelines := map[string]any{}
	if common.ObservabilityTracesCollectionEnabled(infra) || common.ObservabilityLogsCollectionEnabled(infra) {
		receivers["otlp"] = otlpReceiverConfig()
	}
	if common.ObservabilityMetricsCollectionEnabled(infra) {
		receivers["prometheus"] = prometheusReceiverConfig(infra)
		pipelines["metrics"] = pipeline([]string{"prometheus"}, []string{"memory_limiter", "k8sattributes", "batch"}, exporterNames(exporters))
	}
	if common.ObservabilityTracesCollectionEnabled(infra) {
		pipelines["traces"] = pipeline([]string{"otlp"}, []string{"memory_limiter", "k8sattributes", "batch"}, exporterNames(exporters))
	}
	if common.ObservabilityLogsCollectionEnabled(infra) {
		pipelines["logs"] = pipeline([]string{"otlp"}, []string{"memory_limiter", "k8sattributes", "batch"}, exporterNames(exporters))
	}

	return map[string]any{
		"receivers":  receivers,
		"processors": processors,
		"exporters":  exporters,
		"extensions": map[string]any{
			"health_check": map[string]any{"endpoint": "0.0.0.0:13133"},
		},
		"service": map[string]any{
			"extensions": []string{"health_check"},
			"pipelines":  pipelines,
		},
	}, env, nil
}

func (r *Reconciler) gatewayExporters(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (map[string]any, []corev1.EnvVar, error) {
	switch common.ResolveObservabilityBackendType(infra) {
	case infrav1alpha1.ObservabilityBackendTypeBuiltin:
		secretName := common.BuiltinObservabilityClickHouseSecretName(infra.Name)
		return map[string]any{
				"clickhouse": map[string]any{
					"endpoint": "${env:CLICKHOUSE_ENDPOINT}",
					"username": "${env:CLICKHOUSE_USERNAME}",
					"password": "${env:CLICKHOUSE_PASSWORD}",
					"database": "${env:CLICKHOUSE_DATABASE}",
				},
			}, []corev1.EnvVar{
				secretEnv("CLICKHOUSE_ENDPOINT", secretName, "nativeEndpoint"),
				secretEnv("CLICKHOUSE_USERNAME", secretName, "username"),
				secretEnv("CLICKHOUSE_PASSWORD", secretName, "password"),
				secretEnv("CLICKHOUSE_DATABASE", secretName, "database"),
			}, nil
	case infrav1alpha1.ObservabilityBackendTypeExternal:
		otlp := externalOTLPConfig(infra)
		headers, err := r.resolveExternalHeaders(ctx, infra, otlp)
		if err != nil {
			return nil, nil, err
		}
		exporter := map[string]any{
			"endpoint": strings.TrimSpace(otlp.Endpoint),
		}
		if otlp.Insecure != nil {
			exporter["tls"] = map[string]any{"insecure": *otlp.Insecure}
		}
		if otlp.Timeout.Duration > 0 {
			exporter["timeout"] = otlp.Timeout.Duration.String()
		}
		if len(headers) > 0 {
			exporter["headers"] = headers
		}
		return map[string]any{"otlp": exporter}, nil, nil
	default:
		return nil, nil, fmt.Errorf("observability backend is disabled")
	}
}

func agentCollectorConfig(infra *infrav1alpha1.Sandbox0Infra) map[string]any {
	return map[string]any{
		"receivers": map[string]any{
			"filelog": map[string]any{
				"include":           []string{"/var/log/pods/*/*/*.log"},
				"start_at":          "end",
				"include_file_path": true,
				"include_file_name": false,
				"operators": []map[string]any{
					{"type": "container"},
				},
			},
		},
		"processors": defaultProcessors(),
		"exporters": map[string]any{
			"otlp": map[string]any{
				"endpoint": strings.TrimPrefix(common.ManagedObservabilityCollectorOTLPEndpoint(infra), "http://"),
				"tls":      map[string]any{"insecure": true},
			},
		},
		"service": map[string]any{
			"pipelines": map[string]any{
				"logs": pipeline([]string{"filelog"}, []string{"memory_limiter", "k8sattributes", "batch"}, []string{"otlp"}),
			},
		},
	}
}

func defaultProcessors() map[string]any {
	return map[string]any{
		"memory_limiter": map[string]any{
			"check_interval": "1s",
			"limit_mib":      256,
		},
		"k8sattributes": map[string]any{
			"auth_type": "serviceAccount",
			"extract": map[string]any{
				"metadata": []string{
					"k8s.namespace.name",
					"k8s.pod.name",
					"k8s.pod.uid",
					"k8s.node.name",
					"k8s.container.name",
				},
				"labels": []map[string]string{
					{"tag_name": "app.kubernetes.io/component", "key": "app.kubernetes.io/component", "from": "pod"},
					{"tag_name": "app.kubernetes.io/instance", "key": "app.kubernetes.io/instance", "from": "pod"},
					{"tag_name": "sandbox0.ai/sandbox-id", "key": "sandbox0.ai/sandbox-id", "from": "pod"},
					{"tag_name": "sandbox0.ai/template-id", "key": "sandbox0.ai/template-id", "from": "pod"},
				},
			},
		},
		"batch": map[string]any{},
	}
}

func otlpReceiverConfig() map[string]any {
	return map[string]any{
		"protocols": map[string]any{
			"grpc": map[string]any{"endpoint": "0.0.0.0:4317"},
			"http": map[string]any{"endpoint": "0.0.0.0:4318"},
		},
	}
}

func prometheusReceiverConfig(infra *infrav1alpha1.Sandbox0Infra) map[string]any {
	components := "global-gateway|regional-gateway|ssh-gateway|scheduler|cluster-gateway|manager|storage-proxy|netd|ctld"
	return map[string]any{
		"config": map[string]any{
			"scrape_configs": []map[string]any{
				{
					"job_name": "sandbox0-services",
					"kubernetes_sd_configs": []map[string]any{
						{"role": "endpoints", "namespaces": map[string]any{"names": []string{infra.Namespace}}},
					},
					"relabel_configs": serviceDiscoveryRelabels(infra.Name, components, "__meta_kubernetes_endpoint_port_name"),
				},
				{
					"job_name": "sandbox0-pods",
					"kubernetes_sd_configs": []map[string]any{
						{"role": "pod", "namespaces": map[string]any{"names": []string{infra.Namespace}}},
					},
					"relabel_configs": serviceDiscoveryRelabels(infra.Name, components, "__meta_kubernetes_pod_container_port_name"),
				},
			},
		},
	}
}

func serviceDiscoveryRelabels(instance, components, portLabel string) []map[string]any {
	componentGroup := fmt.Sprintf("(%s)", components)
	return []map[string]any{
		{
			"source_labels": []string{"__meta_kubernetes_service_label_app_kubernetes_io_instance", "__meta_kubernetes_pod_label_app_kubernetes_io_instance"},
			"separator":     ";",
			"regex":         fmt.Sprintf("(%s;|;%s|%s;%s)", instance, instance, instance, instance),
			"action":        "keep",
		},
		{
			"source_labels": []string{"__meta_kubernetes_service_label_app_kubernetes_io_component", "__meta_kubernetes_pod_label_app_kubernetes_io_component"},
			"separator":     ";",
			"regex":         fmt.Sprintf("(%s;|;%s|%s;%s)", componentGroup, componentGroup, componentGroup, componentGroup),
			"action":        "keep",
		},
		{
			"source_labels": []string{portLabel},
			"regex":         "http|metrics",
			"action":        "keep",
		},
	}
}

func pipeline(receivers, processors, exporters []string) map[string]any {
	return map[string]any{
		"receivers":  receivers,
		"processors": processors,
		"exporters":  exporters,
	}
}

func exporterNames(exporters map[string]any) []string {
	names := make([]string, 0, len(exporters))
	for name := range exporters {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Reconciler) reconcileCollectorRBAC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.ManagedObservabilityCollectorName(infra.Name)
	labels := common.GetServiceLabels(infra.Name, common.ObservabilityCollectorComponentName)
	if err := r.reconcileServiceAccount(ctx, infra, name, labels); err != nil {
		return err
	}
	rules := []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"pods", "namespaces", "nodes", "services", "endpoints"},
			Verbs:     []string{"get", "list", "watch"},
		},
		{
			APIGroups: []string{"discovery.k8s.io"},
			Resources: []string{"endpointslices"},
			Verbs:     []string{"get", "list", "watch"},
		},
	}
	if err := r.reconcileClusterRole(ctx, name, labels, rules); err != nil {
		return err
	}
	return r.reconcileClusterRoleBinding(ctx, infra, name, labels)
}

func (r *Reconciler) reconcileServiceAccount(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string) error {
	desired := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace, Labels: common.EnsureManagedLabels(labels, name)},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}
	current := &corev1.ServiceAccount{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.OwnerReferences = desired.OwnerReferences
	})
}

func (r *Reconciler) reconcileClusterRole(ctx context.Context, name string, labels map[string]string, rules []rbacv1.PolicyRule) error {
	desired := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: common.EnsureManagedLabels(labels, name)},
		Rules:      rules,
	}
	current := &rbacv1.ClusterRole{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.Rules = desired.Rules
	})
}

func (r *Reconciler) reconcileClusterRoleBinding(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string) error {
	desired := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: common.EnsureManagedLabels(labels, name)},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name},
		Subjects: []rbacv1.Subject{
			{Kind: "ServiceAccount", Name: name, Namespace: infra.Namespace},
		},
	}
	current := &rbacv1.ClusterRoleBinding{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.RoleRef = desired.RoleRef
		current.Subjects = desired.Subjects
	})
}

func (r *Reconciler) reconcileConfigSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, config map[string]any) (string, error) {
	payload, err := yamlv3.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("marshal collector config: %w", err)
	}
	hash := common.ConfigHashFromPayload(payload)
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   infra.Namespace,
			Labels:      common.EnsureManagedLabels(labels, name),
			Annotations: map[string]string{common.PodTemplateConfigHashAnnotation: hash},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{collectorConfigFileName: payload},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return "", err
	}

	current := &corev1.Secret{}
	err = r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return hash, r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return "", err
	}
	return hash, r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.Annotations = desired.Annotations
		current.OwnerReferences = desired.OwnerReferences
		current.Type = desired.Type
		current.Data = desired.Data
	})
}

func (r *Reconciler) ensureCollectorsReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.ManagedObservabilityCollectorName(infra.Name)
	cfg := resolveCollectorConfig(infra)
	if err := r.Resources.EnsureDeploymentReady(ctx, infra, name, cfg.Replicas); err != nil {
		return err
	}
	if !common.ObservabilityLogsCollectionEnabled(infra) {
		return nil
	}
	ds := &appsv1.DaemonSet{}
	agentName := agentCollectorName(infra.Name)
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: agentName, Namespace: infra.Namespace}, ds); err != nil {
		return err
	}
	if ds.Status.DesiredNumberScheduled > 0 && ds.Status.NumberReady < ds.Status.DesiredNumberScheduled {
		return fmt.Errorf("daemonset %q not ready: %d/%d ready", agentName, ds.Status.NumberReady, ds.Status.DesiredNumberScheduled)
	}
	return nil
}

func (r *Reconciler) cleanupManagedCollectorResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	if err := r.cleanupGatewayCollectorResources(ctx, infra); err != nil {
		return err
	}
	if err := r.cleanupAgentCollectorResources(ctx, infra); err != nil {
		return err
	}
	name := common.ManagedObservabilityCollectorName(infra.Name)
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &corev1.ServiceAccount{}); err != nil {
		return err
	}
	if err := r.deleteClusterScoped(ctx, name, &rbacv1.ClusterRole{}); err != nil {
		return err
	}
	return r.deleteClusterScoped(ctx, name, &rbacv1.ClusterRoleBinding{})
}

func (r *Reconciler) cleanupGatewayCollectorResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.ManagedObservabilityCollectorName(infra.Name)
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &appsv1.Deployment{}); err != nil {
		return err
	}
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &corev1.Service{}); err != nil {
		return err
	}
	return r.deleteNamespaced(ctx, infra.Namespace, fmt.Sprintf("%s-config", name), &corev1.Secret{})
}

func (r *Reconciler) cleanupAgentCollectorResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := agentCollectorName(infra.Name)
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &appsv1.DaemonSet{}); err != nil {
		return err
	}
	return r.deleteNamespaced(ctx, infra.Namespace, fmt.Sprintf("%s-config", name), &corev1.Secret{})
}

func (r *Reconciler) cleanupBuiltinClickHouseResources(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := common.BuiltinObservabilityClickHouseName(infra.Name)
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &appsv1.StatefulSet{}); err != nil {
		return err
	}
	if err := r.deleteNamespaced(ctx, infra.Namespace, name, &corev1.Service{}); err != nil {
		return err
	}
	if resolveClickHouseConfig(infra).StatefulResourcePolicy != infrav1alpha1.BuiltinStatefulResourcePolicyDelete {
		return nil
	}
	if err := r.deleteNamespaced(ctx, infra.Namespace, common.BuiltinObservabilityClickHouseSecretName(infra.Name), &corev1.Secret{}); err != nil {
		return err
	}
	return r.deleteNamespaced(ctx, infra.Namespace, common.BuiltinObservabilityClickHousePVCName(infra.Name), &corev1.PersistentVolumeClaim{})
}

func (r *Reconciler) deleteNamespaced(ctx context.Context, namespace, name string, obj client.Object) error {
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, obj); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Reconciler) deleteClusterScoped(ctx context.Context, name string, obj client.Object) error {
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name}, obj); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := r.Resources.Client.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *Reconciler) resolveExternalHeaders(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, otlp *infrav1alpha1.ObservabilityOTLPConfig) (map[string]string, error) {
	if otlp == nil {
		return nil, nil
	}
	headers := common.CloneStringMap(otlp.Headers)
	if headers == nil {
		headers = map[string]string{}
	}
	if otlp.HeadersSecret == nil || strings.TrimSpace(otlp.HeadersSecret.Name) == "" {
		if len(headers) == 0 {
			return nil, nil
		}
		return headers, nil
	}
	key := strings.TrimSpace(otlp.HeadersSecret.Key)
	if key == "" {
		key = "headers"
	}
	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: otlp.HeadersSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
		return nil, fmt.Errorf("observability OTLP headers secret not found: %w", err)
	}
	value, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("observability OTLP headers secret %q missing key %q", otlp.HeadersSecret.Name, key)
	}
	for headerKey, headerValue := range parseHeaderList(string(value)) {
		headers[headerKey] = headerValue
	}
	if len(headers) == 0 {
		return nil, nil
	}
	return headers, nil
}

func parseHeaderList(value string) map[string]string {
	headers := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, raw, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		decoded, err := url.QueryUnescape(strings.TrimSpace(raw))
		if err != nil {
			decoded = strings.TrimSpace(raw)
		}
		headers[key] = decoded
	}
	return headers
}

func resolveClickHouseConfig(infra *infrav1alpha1.Sandbox0Infra) clickHouseConfig {
	cfg := clickHouseConfig{
		Image:                  defaultClickHouseImage,
		NativePort:             defaultClickHouseNative,
		HTTPPort:               defaultClickHouseHTTP,
		Database:               defaultClickHouseDB,
		Username:               defaultClickHouseUser,
		StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
	}
	if infra == nil || infra.Spec.Observability == nil || infra.Spec.Observability.Backend == nil ||
		infra.Spec.Observability.Backend.Builtin == nil || infra.Spec.Observability.Backend.Builtin.ClickHouse == nil {
		return cfg
	}
	builtin := infra.Spec.Observability.Backend.Builtin.ClickHouse
	if value := strings.TrimSpace(builtin.Image); value != "" {
		cfg.Image = value
	}
	if builtin.NativePort != 0 {
		cfg.NativePort = builtin.NativePort
	}
	if builtin.HTTPPort != 0 {
		cfg.HTTPPort = builtin.HTTPPort
	}
	if value := strings.TrimSpace(builtin.Database); value != "" {
		cfg.Database = value
	}
	if value := strings.TrimSpace(builtin.Username); value != "" {
		cfg.Username = value
	}
	cfg.Persistence = builtin.Persistence
	if builtin.Resources != nil {
		cfg.Resources = builtin.Resources.DeepCopy()
	}
	if builtin.StatefulResourcePolicy != "" {
		cfg.StatefulResourcePolicy = builtin.StatefulResourcePolicy
	}
	return cfg
}

func resolveCollectorConfig(infra *infrav1alpha1.Sandbox0Infra) collectorConfig {
	cfg := collectorConfig{Image: defaultCollectorImage, Replicas: 1}
	var spec *infrav1alpha1.ManagedObservabilityCollectorConfig
	if infra != nil && infra.Spec.Observability != nil && infra.Spec.Observability.Backend != nil {
		switch common.ResolveObservabilityBackendType(infra) {
		case infrav1alpha1.ObservabilityBackendTypeBuiltin:
			if infra.Spec.Observability.Backend.Builtin != nil {
				spec = infra.Spec.Observability.Backend.Builtin.Collector
			}
		case infrav1alpha1.ObservabilityBackendTypeExternal:
			if infra.Spec.Observability.Backend.External != nil {
				spec = infra.Spec.Observability.Backend.External.Collector
			}
		}
	}
	if spec == nil {
		return cfg
	}
	if image := strings.TrimSpace(spec.Image); image != "" {
		cfg.Image = image
	}
	if spec.Replicas != nil && *spec.Replicas > 0 {
		cfg.Replicas = *spec.Replicas
	}
	if spec.Resources != nil {
		cfg.Resources = spec.Resources.DeepCopy()
	}
	return cfg
}

func externalOTLPConfig(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.ObservabilityOTLPConfig {
	if infra == nil || infra.Spec.Observability == nil || infra.Spec.Observability.Backend == nil ||
		infra.Spec.Observability.Backend.External == nil || infra.Spec.Observability.Backend.External.OTLP == nil {
		return &infrav1alpha1.ObservabilityOTLPConfig{}
	}
	return infra.Spec.Observability.Backend.External.OTLP
}

func collectorRequired(infra *infrav1alpha1.Sandbox0Infra) bool {
	return common.ManagedObservabilityCollectorEnabled(infra) &&
		(common.ObservabilityLogsCollectionEnabled(infra) ||
			common.ObservabilityMetricsCollectionEnabled(infra) ||
			common.ObservabilityTracesCollectionEnabled(infra))
}

func agentCollectorName(infraName string) string {
	return fmt.Sprintf("%s-%s", infraName, agentComponentName)
}

func secretEnv(envName, secretName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: envName,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				Key:                  key,
			},
		},
	}
}

func httpProbe(path string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("health")},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
	}
}
