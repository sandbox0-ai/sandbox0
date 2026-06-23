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
	defaultCollectorImage = "otel/opentelemetry-collector-contrib:0.130.1"

	agentComponentName      = "otel-agent"
	collectorConfigFileName = "config.yaml"
)

type Reconciler struct {
	Resources *common.ResourceManager
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
		return r.cleanupManagedCollectorResources(ctx, infra)
	case infrav1alpha1.ObservabilityBackendTypeExternal:
		logger.Info("Reconciling external observability backend integration")
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

func resolveCollectorConfig(infra *infrav1alpha1.Sandbox0Infra) collectorConfig {
	cfg := collectorConfig{Image: defaultCollectorImage, Replicas: 1}
	var spec *infrav1alpha1.ManagedObservabilityCollectorConfig
	if infra != nil && infra.Spec.Observability != nil && infra.Spec.Observability.Backend != nil {
		if common.ResolveObservabilityBackendType(infra) == infrav1alpha1.ObservabilityBackendTypeExternal {
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

func httpProbe(path string) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString("health")},
		},
		InitialDelaySeconds: 5,
		PeriodSeconds:       10,
	}
}
