/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package observability

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
)

const (
	defaultCollectorImage  = "otel/opentelemetry-collector-contrib:0.139.0"
	defaultClickHouseImage = "clickhouse/clickhouse-server:25.8-alpine"
	clickHouseSecretSuffix = "clickhouse-credentials"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	if !infrav1alpha1.IsObservabilityEnabled(infra) {
		logger.Info("Observability is disabled, skipping")
		return nil
	}

	if useBuiltinClickHouse(infra) {
		if !builtinClickHouse(infra).Enabled {
			if collectorEnabled(infra) {
				return fmt.Errorf("observability collector requires builtin ClickHouse or external ClickHouse to be enabled")
			}
		} else {
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
			if err := r.ensureClickHouseReady(ctx, infra); err != nil {
				return err
			}
		}
	} else if err := r.validateExternalClickHouse(ctx, infra); err != nil {
		return err
	}

	if collectorEnabled(infra) {
		if err := r.reconcileCollector(ctx, infra); err != nil {
			return err
		}
	}

	logger.Info("Observability reconciled successfully")
	return nil
}

func (r *Reconciler) reconcileClickHouseSecret(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := clickHouseSecretName(infra)
	secret := &corev1.Secret{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	builtin := builtinClickHouse(infra)
	secret = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"username": builtin.Username,
			"password": common.GenerateRandomString(32),
			"database": builtin.Database,
		},
	}
	if err := ctrl.SetControllerReference(infra, secret, r.Resources.Scheme); err != nil {
		return err
	}
	return r.Resources.Client.Create(ctx, secret)
}

func (r *Reconciler) reconcileClickHousePVC(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := clickHousePVCName(infra)
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, pvc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}

	size := resource.MustParse("100Gi")
	builtin := builtinClickHouse(infra)
	if builtin.Persistence != nil {
		size = builtin.Persistence.Size
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
	if builtin.Persistence != nil && strings.TrimSpace(builtin.Persistence.StorageClass) != "" {
		pvc.Spec.StorageClassName = &builtin.Persistence.StorageClass
	}
	if err := ctrl.SetControllerReference(infra, pvc, r.Resources.Scheme); err != nil {
		return err
	}
	return r.Resources.Client.Create(ctx, pvc)
}

func (r *Reconciler) reconcileClickHouseStatefulSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := clickHouseName(infra)
	builtin := builtinClickHouse(infra)
	secretName := clickHouseSecretName(infra)
	labels := common.GetServiceLabels(infra.Name, "clickhouse")
	replicas := int32(1)
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	if builtin.Resources != nil {
		resources = *builtin.Resources
	}

	desired := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace, Labels: common.EnsureManagedLabels(labels, name)},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: common.EnsureManagedLabels(labels, name), Annotations: common.EnsurePodTemplateAnnotations(nil)},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "clickhouse",
						Image: builtin.Image,
						Ports: []corev1.ContainerPort{
							{Name: "http", ContainerPort: builtin.HTTPPort},
							{Name: "native", ContainerPort: builtin.NativePort},
						},
						Env: []corev1.EnvVar{
							{Name: "CLICKHOUSE_DB", ValueFrom: secretKeyRef(secretName, "database")},
							{Name: "CLICKHOUSE_USER", ValueFrom: secretKeyRef(secretName, "username")},
							{Name: "CLICKHOUSE_PASSWORD", ValueFrom: secretKeyRef(secretName, "password")},
						},
						VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/clickhouse"}},
						Resources:      resources,
						LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/ping", Port: intstr.FromString("http")}}, InitialDelaySeconds: 30, PeriodSeconds: 10},
						ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/ping", Port: intstr.FromString("http")}}, InitialDelaySeconds: 10, PeriodSeconds: 5},
					}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: clickHousePVCName(infra),
						}},
					}},
				},
			},
		},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	current := &appsv1.StatefulSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	current.Labels = desired.Labels
	current.Spec = desired.Spec
	return r.Resources.Client.Update(ctx, current)
}

func (r *Reconciler) reconcileClickHouseService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	builtin := builtinClickHouse(infra)
	name := clickHouseName(infra)
	labels := common.GetServiceLabels(infra.Name, "clickhouse")
	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		common.BuildServicePort("http", builtin.HTTPPort, builtin.HTTPPort, corev1.ServiceTypeClusterIP),
		common.BuildServicePort("native", builtin.NativePort, builtin.NativePort, corev1.ServiceTypeClusterIP),
	})
}

func (r *Reconciler) ensureClickHouseReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	sts := &appsv1.StatefulSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: clickHouseName(infra), Namespace: infra.Namespace}, sts); err != nil {
		return err
	}
	if sts.Status.ReadyReplicas < 1 {
		return fmt.Errorf("statefulset %q not ready: %d/1 ready", clickHouseName(infra), sts.Status.ReadyReplicas)
	}
	return nil
}

func (r *Reconciler) validateExternalClickHouse(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	external := infra.Spec.Observability.ClickHouse.External
	if external == nil {
		return fmt.Errorf("observability.clickHouse.external is required when type is external")
	}
	if strings.TrimSpace(external.Endpoint) == "" {
		return fmt.Errorf("observability.clickHouse.external.endpoint is required")
	}
	if strings.TrimSpace(external.Database) == "" {
		return fmt.Errorf("observability.clickHouse.external.database is required")
	}
	if strings.TrimSpace(external.Username) == "" {
		return fmt.Errorf("observability.clickHouse.external.username is required")
	}
	if strings.TrimSpace(external.PasswordSecret.Name) == "" {
		return fmt.Errorf("observability.clickHouse.external.passwordSecret.name is required")
	}
	secret := &corev1.Secret{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: external.PasswordSecret.Name, Namespace: infra.Namespace}, secret); err != nil {
		return fmt.Errorf("clickhouse password secret not found: %w", err)
	}
	key := external.PasswordSecret.Key
	if key == "" {
		key = "password"
	}
	if _, ok := secret.Data[key]; !ok {
		return fmt.Errorf("key %s not found in clickhouse password secret", key)
	}
	return nil
}

func (r *Reconciler) reconcileCollector(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	name := collectorName(infra)
	labels := common.GetServiceLabels(infra.Name, "otel-collector")
	cfg := collectorConfig(infra)
	if err := r.reconcileCollectorConfigMap(ctx, infra, name, labels, cfg); err != nil {
		return err
	}

	collector := collectorSpec(infra)
	clickHouseSecret := clickHouseSecretName(infra)
	env := []corev1.EnvVar{
		{Name: "CLICKHOUSE_ENDPOINT", Value: clickHouseEndpoint(infra)},
		{Name: "CLICKHOUSE_DATABASE", Value: clickHouseDatabase(infra)},
		{Name: "CLICKHOUSE_USERNAME", Value: clickHouseUsername(infra)},
	}
	if useBuiltinClickHouse(infra) {
		env = append(env, corev1.EnvVar{Name: "CLICKHOUSE_PASSWORD", ValueFrom: secretKeyRef(clickHouseSecret, "password")})
	} else {
		external := infra.Spec.Observability.ClickHouse.External
		key := external.PasswordSecret.Key
		if key == "" {
			key = "password"
		}
		env = append(env, corev1.EnvVar{Name: "CLICKHOUSE_PASSWORD", ValueFrom: secretKeyRef(external.PasswordSecret.Name, key)})
	}

	if err := r.Resources.ReconcileDeployment(ctx, infra, name, labels, collector.Replicas, common.ServiceDefinition{
		Name:  "otel-collector",
		Image: collector.Image,
		Args:  []string{"--config=/etc/otelcol/config.yaml"},
		Ports: []corev1.ContainerPort{
			{Name: "otlp-grpc", ContainerPort: collector.OTLPGRPCPort},
			{Name: "otlp-http", ContainerPort: collector.OTLPHTTPPort},
			{Name: "health", ContainerPort: collector.HealthPort},
		},
		EnvVars: env,
		VolumeMounts: []corev1.VolumeMount{{
			Name:      "config",
			MountPath: "/etc/otelcol/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		}},
		Volumes: []corev1.Volume{{
			Name: "config",
			VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: name},
			}},
		}},
		LivenessProbe:  &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("health")}}, InitialDelaySeconds: 10, PeriodSeconds: 10},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/", Port: intstr.FromString("health")}}, InitialDelaySeconds: 5, PeriodSeconds: 5},
		Resources:      collector.Resources,
	}); err != nil {
		return err
	}

	return r.Resources.ReconcileServicePorts(ctx, infra, name, labels, corev1.ServiceTypeClusterIP, nil, []corev1.ServicePort{
		common.BuildServicePort("otlp-grpc", collector.OTLPGRPCPort, collector.OTLPGRPCPort, corev1.ServiceTypeClusterIP),
		common.BuildServicePort("otlp-http", collector.OTLPHTTPPort, collector.OTLPHTTPPort, corev1.ServiceTypeClusterIP),
		common.BuildServicePort("health", collector.HealthPort, collector.HealthPort, corev1.ServiceTypeClusterIP),
	})
}

func (r *Reconciler) reconcileCollectorConfigMap(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, config string) error {
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace, Labels: common.EnsureManagedLabels(labels, name)},
		Data:       map[string]string{"config.yaml": config},
	}
	if err := ctrl.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	current := &corev1.ConfigMap{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, current)
	if errors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	current.Labels = desired.Labels
	current.Data = desired.Data
	return r.Resources.Client.Update(ctx, current)
}

func collectorConfig(infra *infrav1alpha1.Sandbox0Infra) string {
	collector := collectorSpec(infra)
	return fmt.Sprintf(`receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:%d
      http:
        endpoint: 0.0.0.0:%d

processors:
  batch:
    timeout: 5s
    send_batch_size: 1024
  memory_limiter:
    check_interval: 1s
    limit_mib: 512
  resource/sandbox0:
    attributes:
      - key: sandbox0.region_id
        value: %q
        action: upsert
      - key: sandbox0.cluster_id
        value: %q
        action: upsert

exporters:
  clickhouse:
    endpoint: ${env:CLICKHOUSE_ENDPOINT}
    database: ${env:CLICKHOUSE_DATABASE}
    username: ${env:CLICKHOUSE_USERNAME}
    password: ${env:CLICKHOUSE_PASSWORD}
    ttl: 168h

extensions:
  health_check:
    endpoint: 0.0.0.0:%d

service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, resource/sandbox0, batch]
      exporters: [clickhouse]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, resource/sandbox0, batch]
      exporters: [clickhouse]
    logs:
      receivers: [otlp]
      processors: [memory_limiter, resource/sandbox0, batch]
      exporters: [clickhouse]
`, collector.OTLPGRPCPort, collector.OTLPHTTPPort, infra.Spec.Region, clusterID(infra), collector.HealthPort)
}

func secretKeyRef(secretName, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
		Key:                  key,
	}}
}

func clickHouseName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-clickhouse", infra.Name)
}

func clickHousePVCName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-clickhouse-data", infra.Name)
}

func clickHouseSecretName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-%s", infra.Name, clickHouseSecretSuffix)
}

func collectorName(infra *infrav1alpha1.Sandbox0Infra) string {
	return fmt.Sprintf("%s-otel-collector", infra.Name)
}

func clusterID(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra != nil && infra.Spec.Cluster != nil {
		return infra.Spec.Cluster.ID
	}
	return ""
}

func collectorEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Observability == nil || !infra.Spec.Observability.Enabled {
		return false
	}
	return infra.Spec.Observability.Collector == nil || infra.Spec.Observability.Collector.Enabled
}

func useBuiltinClickHouse(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil || infra.Spec.Observability == nil || infra.Spec.Observability.ClickHouse == nil {
		return true
	}
	return infra.Spec.Observability.ClickHouse.Type != infrav1alpha1.ClickHouseTypeExternal
}

func builtinClickHouse(infra *infrav1alpha1.Sandbox0Infra) *infrav1alpha1.BuiltinClickHouseConfig {
	cfg := &infrav1alpha1.BuiltinClickHouseConfig{
		Enabled:    true,
		Image:      defaultClickHouseImage,
		HTTPPort:   8123,
		NativePort: 9000,
		Database:   "sandbox0_observability",
		Username:   "sandbox0",
	}
	if infra != nil && infra.Spec.Observability != nil && infra.Spec.Observability.ClickHouse != nil && infra.Spec.Observability.ClickHouse.Builtin != nil {
		src := infra.Spec.Observability.ClickHouse.Builtin
		*cfg = *src
		if src.Persistence != nil {
			cfg.Persistence = src.Persistence.DeepCopy()
		}
		if src.Resources != nil {
			cfg.Resources = src.Resources.DeepCopy()
		}
		if cfg.Image == "" {
			cfg.Image = defaultClickHouseImage
		}
		if cfg.HTTPPort == 0 {
			cfg.HTTPPort = 8123
		}
		if cfg.NativePort == 0 {
			cfg.NativePort = 9000
		}
		if cfg.Database == "" {
			cfg.Database = "sandbox0_observability"
		}
		if cfg.Username == "" {
			cfg.Username = "sandbox0"
		}
	}
	return cfg
}

type collectorRuntimeConfig struct {
	Enabled      bool
	Image        string
	Replicas     int32
	OTLPGRPCPort int32
	OTLPHTTPPort int32
	HealthPort   int32
	Resources    *corev1.ResourceRequirements
}

func collectorSpec(infra *infrav1alpha1.Sandbox0Infra) collectorRuntimeConfig {
	cfg := collectorRuntimeConfig{
		Enabled:      true,
		Image:        defaultCollectorImage,
		Replicas:     1,
		OTLPGRPCPort: 4317,
		OTLPHTTPPort: 4318,
		HealthPort:   13133,
	}
	if infra != nil && infra.Spec.Observability != nil && infra.Spec.Observability.Collector != nil {
		src := infra.Spec.Observability.Collector
		cfg.Enabled = src.Enabled
		if src.Image != "" {
			cfg.Image = src.Image
		}
		if src.Replicas != 0 {
			cfg.Replicas = src.Replicas
		}
		if src.OTLPGRPCPort != 0 {
			cfg.OTLPGRPCPort = src.OTLPGRPCPort
		}
		if src.OTLPHTTPPort != 0 {
			cfg.OTLPHTTPPort = src.OTLPHTTPPort
		}
		if src.HealthPort != 0 {
			cfg.HealthPort = src.HealthPort
		}
		if src.Resources != nil {
			cfg.Resources = src.Resources.DeepCopy()
		}
	}
	return cfg
}

func clickHouseEndpoint(infra *infrav1alpha1.Sandbox0Infra) string {
	if useBuiltinClickHouse(infra) {
		builtin := builtinClickHouse(infra)
		return fmt.Sprintf("tcp://%s.%s.svc.cluster.local:%d", clickHouseName(infra), infra.Namespace, builtin.NativePort)
	}
	return infra.Spec.Observability.ClickHouse.External.Endpoint
}

func clickHouseDatabase(infra *infrav1alpha1.Sandbox0Infra) string {
	if useBuiltinClickHouse(infra) {
		return builtinClickHouse(infra).Database
	}
	return infra.Spec.Observability.ClickHouse.External.Database
}

func clickHouseUsername(infra *infrav1alpha1.Sandbox0Infra) string {
	if useBuiltinClickHouse(infra) {
		return builtinClickHouse(infra).Username
	}
	return infra.Spec.Observability.ClickHouse.External.Username
}
