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

package common

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strconv"

	yamlv3 "gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

type ResourceManager struct {
	Client client.Client
	Scheme *runtime.Scheme
	// ImagePullPolicy overrides container pull policy when set.
	ImagePullPolicy *corev1.PullPolicy
	// LocalDev config for running operator outside the cluster.
	LocalDev LocalDevConfig
}

type LocalDevConfig struct {
	LocalDevMode   bool
	KubeconfigPath string
}

const PodTemplateSpecGenerationAnnotation = "infra.sandbox0.ai/spec-generation"

func NewResourceManager(client client.Client, scheme *runtime.Scheme, imagePullPolicy *corev1.PullPolicy, localDev LocalDevConfig) *ResourceManager {
	return &ResourceManager{
		Client:          client,
		Scheme:          scheme,
		ImagePullPolicy: imagePullPolicy,
		LocalDev:        localDev,
	}
}

// ServiceDefinition defines deployment/daemonset configuration for a service.
type ServiceDefinition struct {
	Name               string
	Port               int32
	TargetPort         int32
	Ports              []corev1.ContainerPort
	Image              string
	ImagePullPolicy    *corev1.PullPolicy
	Command            []string
	Args               []string
	EnvVars            []corev1.EnvVar
	VolumeMounts       []corev1.VolumeMount
	Volumes            []corev1.Volume
	LivenessProbe      *corev1.Probe
	ReadinessProbe     *corev1.Probe
	ServiceAccountName string
	Resources          *corev1.ResourceRequirements
}

// ReconcileDeployment creates or updates a deployment.
func (r *ResourceManager) ReconcileDeployment(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, replicas int32, def ServiceDefinition) error {
	deploy := &appsv1.Deployment{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	defaultResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	if def.Resources != nil {
		defaultResources = *def.Resources
	}

	desiredLabels := EnsureManagedLabels(labels, name)
	desiredDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      desiredLabels,
					Annotations: EnsurePodTemplateAnnotations(infra, nil),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: def.ServiceAccountName,
					Containers: []corev1.Container{
						{
							Name:            def.Name,
							Image:           def.Image,
							ImagePullPolicy: ResolveImagePullPolicy(def, r.ImagePullPolicy),
							Command:         def.Command,
							Args:            def.Args,
							Env:             def.EnvVars,
							VolumeMounts:    def.VolumeMounts,
							Ports:           ResolveContainerPorts(def),
							Resources:       defaultResources,
							LivenessProbe:   def.LivenessProbe,
							ReadinessProbe:  def.ReadinessProbe,
						},
					},
					Volumes: def.Volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredDeploy, r.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desiredDeploy)
	}

	deploy.Spec = desiredDeploy.Spec
	deploy.Labels = desiredLabels
	return r.Client.Update(ctx, deploy)
}

// EnsureDeploymentReady validates deployment readiness before reporting success.
func (r *ResourceManager) EnsureDeploymentReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, replicas int32) error {
	if replicas == 0 {
		return nil
	}

	deploy := &appsv1.Deployment{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy); err != nil {
		return err
	}

	desired := replicas
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	}
	if desired == 0 {
		return nil
	}
	if deploy.Status.ReadyReplicas < desired {
		return fmt.Errorf("deployment %q not ready: %d/%d ready", name, deploy.Status.ReadyReplicas, desired)
	}
	return nil
}

// ReconcileDaemonSet creates or updates a daemonset.
func (r *ResourceManager) ReconcileDaemonSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, def ServiceDefinition) error {
	ds := &appsv1.DaemonSet{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ds)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	defaultResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	if def.Resources != nil {
		defaultResources = *def.Resources
	}

	desiredLabels := EnsureManagedLabels(labels, name)
	desiredDs := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      desiredLabels,
					Annotations: EnsurePodTemplateAnnotations(infra, nil),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: def.ServiceAccountName,
					HostNetwork:        true,
					HostPID:            true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{
						{
							Name:            def.Name,
							Image:           def.Image,
							ImagePullPolicy: ResolveImagePullPolicy(def, r.ImagePullPolicy),
							Env:             def.EnvVars,
							VolumeMounts:    def.VolumeMounts,
							Ports:           ResolveContainerPorts(def),
							SecurityContext: &corev1.SecurityContext{
								Privileged: BoolPtr(true),
								Capabilities: &corev1.Capabilities{
									Add: []corev1.Capability{"NET_ADMIN", "SYS_ADMIN"},
								},
							},
							Resources:      defaultResources,
							LivenessProbe:  def.LivenessProbe,
							ReadinessProbe: def.ReadinessProbe,
						},
					},
					Volumes: def.Volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredDs, r.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desiredDs)
	}

	ds.Spec = desiredDs.Spec
	ds.Labels = desiredLabels
	return r.Client.Update(ctx, ds)
}

func ResolveContainerPorts(def ServiceDefinition) []corev1.ContainerPort {
	if len(def.Ports) > 0 {
		return def.Ports
	}
	if def.TargetPort == 0 {
		return nil
	}
	return []corev1.ContainerPort{
		{
			Name:          "http",
			ContainerPort: def.TargetPort,
		},
	}
}

func ResolveImagePullPolicy(def ServiceDefinition, fallback *corev1.PullPolicy) corev1.PullPolicy {
	if def.ImagePullPolicy != nil {
		return *def.ImagePullPolicy
	}
	if fallback != nil {
		return *fallback
	}
	return ""
}

func ResolveServiceType(config *infrav1alpha1.ServiceNetworkConfig) corev1.ServiceType {
	if config != nil && config.Type != "" {
		return config.Type
	}
	return corev1.ServiceTypeClusterIP
}

func ResolveServicePort(config *infrav1alpha1.ServiceNetworkConfig, fallback int32) int32 {
	if config != nil && config.Port != 0 {
		return config.Port
	}
	return fallback
}

// ReconcileService creates or updates a service.
// For NodePort type, the port is also used as NodePort.
func (r *ResourceManager) ReconcileService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, serviceType corev1.ServiceType, port, targetPort int32) error {
	return r.ReconcileServicePorts(ctx, infra, name, labels, serviceType, []corev1.ServicePort{
		BuildServicePort("http", port, targetPort, serviceType),
	})
}

// ReconcileServicePorts creates or updates a service with multiple ports.
func (r *ResourceManager) ReconcileServicePorts(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, serviceType corev1.ServiceType, ports []corev1.ServicePort) error {
	if len(ports) == 0 {
		return fmt.Errorf("service %q requires at least one port", name)
	}

	svc := &corev1.Service{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desiredLabels := EnsureManagedLabels(labels, name)
	desiredSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: labels,
			Ports:    ports,
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredSvc, r.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desiredSvc)
	}

	svc.Spec = desiredSvc.Spec
	svc.Labels = desiredLabels
	return r.Client.Update(ctx, svc)
}

// BuildServicePort returns a ServicePort with a target port.
// For NodePort type, the port is also used as NodePort.
func BuildServicePort(name string, port, targetPort int32, serviceType corev1.ServiceType) corev1.ServicePort {
	sp := corev1.ServicePort{
		Name:       name,
		Port:       port,
		TargetPort: intstr.FromInt(int(targetPort)),
	}
	if serviceType == corev1.ServiceTypeNodePort {
		sp.NodePort = port
	}
	return sp
}

// EnsureManagedLabels merges standard managed labels without overriding existing values.
func EnsureManagedLabels(labels map[string]string, fallbackName string) map[string]string {
	out := MergeLabels(labels, map[string]string{
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	})
	if out["app.kubernetes.io/name"] == "" {
		out["app.kubernetes.io/name"] = fallbackName
	}
	return out
}

// MergeLabels returns a copy of base with overrides applied.
func MergeLabels(base map[string]string, overrides map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(overrides))
	for key, value := range base {
		out[key] = value
	}
	for key, value := range overrides {
		out[key] = value
	}
	return out
}

// ReconcileIngress creates or updates an ingress.
func (r *ResourceManager) ReconcileIngress(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, serviceName string, servicePort int32, config *infrav1alpha1.IngressConfig) error {
	ingressName := serviceName

	ingress := &networkingv1.Ingress{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: ingressName, Namespace: infra.Namespace}, ingress)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	pathType := networkingv1.PathTypePrefix
	desiredLabels := EnsureManagedLabels(ingress.Labels, ingressName)
	desiredIngress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingressName,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &config.ClassName,
			Rules: []networkingv1.IngressRule{
				{
					Host: config.Host,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: serviceName,
											Port: networkingv1.ServiceBackendPort{
												Number: servicePort,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	if config.TLSSecret != "" {
		desiredIngress.Spec.TLS = []networkingv1.IngressTLS{
			{
				Hosts:      []string{config.Host},
				SecretName: config.TLSSecret,
			},
		}
	}

	if err := ctrl.SetControllerReference(infra, desiredIngress, r.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desiredIngress)
	}

	ingress.Spec = desiredIngress.Spec
	ingress.Labels = desiredLabels
	return r.Client.Update(ctx, ingress)
}

// ReconcileServiceConfigMap creates or updates a configmap for a service.
func (r *ResourceManager) ReconcileServiceConfigMap(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, config any) error {
	if config == nil {
		config = map[string]any{}
	}

	payload, err := yamlv3.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config for %s: %w", name, err)
	}

	desiredLabels := EnsureManagedLabels(labels, name)
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    desiredLabels,
		},
		Data: map[string]string{
			"config.yaml": string(payload),
		},
	}

	if err := ctrl.SetControllerReference(infra, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.ConfigMap{}
	err = r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, existing)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desired)
	}

	existing.Data = desired.Data
	existing.Labels = desired.Labels
	return r.Client.Update(ctx, existing)
}

// ReconcileNamespace creates a namespace if it does not exist.
// It ignores errors if the operator does not have permission to create the namespace.
func (r *ResourceManager) ReconcileNamespace(ctx context.Context, name string) error {
	ns := &corev1.Namespace{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name}, ns)
	if err == nil {
		return nil
	}

	if !errors.IsNotFound(err) {
		return err
	}

	desired := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	err = r.Client.Create(ctx, desired)
	if err != nil {
		if errors.IsForbidden(err) {
			log.FromContext(ctx).Info("Forbidden to create namespace, skipping", "namespace", name)
			return nil
		}
		return err
	}

	return nil
}

func GetOrInitMap(config map[string]any, key string) map[string]any {
	if val, ok := config[key]; ok {
		if typed, ok := val.(map[string]any); ok {
			return typed
		}
	}

	child := map[string]any{}
	config[key] = child
	return child
}

// GenerateRandomString generates a random string of specified length.
func GenerateRandomString(length int) string {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "defaultsecret123456789012"
	}

	encoded := base64.URLEncoding.EncodeToString(bytes)
	if len(encoded) > length {
		return encoded[:length]
	}
	return encoded
}

// EnsureSecretValue ensures a named secret contains a key, generating if needed.
func EnsureSecretValue(ctx context.Context, client client.Client, scheme *runtime.Scheme, infra *infrav1alpha1.Sandbox0Infra, name, key string, length int) (string, error) {
	secret := &corev1.Secret{}
	err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, secret)
	if err != nil && !errors.IsNotFound(err) {
		return "", err
	}

	if errors.IsNotFound(err) {
		value := GenerateRandomString(length)
		secret = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: infra.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				key: value,
			},
		}
		if err := ctrl.SetControllerReference(infra, secret, scheme); err != nil {
			return "", err
		}
		if err := client.Create(ctx, secret); err != nil {
			return "", err
		}
		return value, nil
	}

	if value, ok := secret.Data[key]; ok && len(value) > 0 {
		return string(value), nil
	}

	value := GenerateRandomString(length)
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[key] = []byte(value)
	if err := client.Update(ctx, secret); err != nil {
		return "", err
	}
	return value, nil
}

// GetSecretValue returns the value from a secret key reference.
func GetSecretValue(ctx context.Context, client client.Client, namespace string, ref infrav1alpha1.SecretKeyRef) (string, error) {
	if ref.Name == "" {
		return "", fmt.Errorf("secret name is required")
	}

	secret := &corev1.Secret{}
	if err := client.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, secret); err != nil {
		return "", err
	}

	key := ref.Key
	if key == "" {
		key = "password"
	}

	value, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %s not found in secret %s", key, ref.Name)
	}

	return string(value), nil
}

// ResolveSecretKeyRef returns a ref with defaults applied when fields are empty.
func ResolveSecretKeyRef(ref infrav1alpha1.SecretKeyRef, defaultName, defaultKey string) infrav1alpha1.SecretKeyRef {
	if ref.Name == "" {
		ref.Name = defaultName
	}
	if ref.Key == "" {
		ref.Key = defaultKey
	}
	return ref
}

// GetServiceLabels returns standard labels for a service.
func GetServiceLabels(instanceName, componentName string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       componentName,
		"app.kubernetes.io/instance":   instanceName,
		"app.kubernetes.io/component":  componentName,
		"app.kubernetes.io/managed-by": "sandbox0infra-operator",
	}
}

// BoolPtr returns a pointer to a bool.
func BoolPtr(b bool) *bool {
	return &b
}

// ResolveSandboxNodePlacement derives the shared scheduling constraints for
// sandbox workloads and node-local sandbox services. The shared spec-level
// placement takes precedence, with netd placement kept as a compatibility
// fallback for older manifests.
func ResolveSandboxNodePlacement(infra *infrav1alpha1.Sandbox0Infra) (map[string]string, []corev1.Toleration) {
	if infra == nil {
		return nil, nil
	}

	var nodeSelector map[string]string
	if infra.Spec.SandboxNodePlacement != nil && len(infra.Spec.SandboxNodePlacement.NodeSelector) > 0 {
		nodeSelector = cloneNodeSelector(infra.Spec.SandboxNodePlacement.NodeSelector)
	} else if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
		nodeSelector = cloneNodeSelector(infra.Spec.Services.Netd.NodeSelector)
	}

	var tolerations []corev1.Toleration
	if infra.Spec.SandboxNodePlacement != nil && len(infra.Spec.SandboxNodePlacement.Tolerations) > 0 {
		tolerations = cloneTolerations(infra.Spec.SandboxNodePlacement.Tolerations)
	} else if infra.Spec.Services != nil && infra.Spec.Services.Netd != nil {
		tolerations = cloneTolerations(infra.Spec.Services.Netd.Tolerations)
	}

	return nodeSelector, tolerations
}

func cloneNodeSelector(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}

	cloned := make(map[string]string, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}

func cloneTolerations(src []corev1.Toleration) []corev1.Toleration {
	if len(src) == 0 {
		return nil
	}

	cloned := make([]corev1.Toleration, len(src))
	copy(cloned, src)
	return cloned
}

// EnsurePodTemplateAnnotations returns annotations with the current CR generation marker.
func EnsurePodTemplateAnnotations(infra *infrav1alpha1.Sandbox0Infra, annotations map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range annotations {
		out[key] = value
	}
	if infra != nil {
		out[PodTemplateSpecGenerationAnnotation] = strconv.FormatInt(infra.Generation, 10)
	}
	return out
}
