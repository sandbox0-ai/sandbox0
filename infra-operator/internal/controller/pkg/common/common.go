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
	"sigs.k8s.io/yaml"

	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
)

type ResourceManager struct {
	Client client.Client
	Scheme *runtime.Scheme
}

func NewResourceManager(client client.Client, scheme *runtime.Scheme) *ResourceManager {
	return &ResourceManager{
		Client: client,
		Scheme: scheme,
	}
}

// ServiceDefinition defines deployment/daemonset configuration for a service.
type ServiceDefinition struct {
	Name               string
	Port               int32
	TargetPort         int32
	Ports              []corev1.ContainerPort
	Image              string
	Command            []string
	Args               []string
	EnvVars            []corev1.EnvVar
	VolumeMounts       []corev1.VolumeMount
	Volumes            []corev1.Volume
	LivenessProbe      *corev1.Probe
	ReadinessProbe     *corev1.Probe
	ServiceAccountName string
}

// ReconcileDeployment creates or updates a deployment.
func (r *ResourceManager) ReconcileDeployment(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, replicas int32, def ServiceDefinition) error {
	deploy := &appsv1.Deployment{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, deploy)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desiredDeploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: def.ServiceAccountName,
					Containers: []corev1.Container{
						{
							Name:         def.Name,
							Image:        def.Image,
							Command:      def.Command,
							Args:         def.Args,
							Env:          def.EnvVars,
							VolumeMounts: def.VolumeMounts,
							Ports:        ResolveContainerPorts(def),
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
							LivenessProbe:  def.LivenessProbe,
							ReadinessProbe: def.ReadinessProbe,
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
	return r.Client.Update(ctx, deploy)
}

// ReconcileDaemonSet creates or updates a daemonset.
func (r *ResourceManager) ReconcileDaemonSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, def ServiceDefinition) error {
	ds := &appsv1.DaemonSet{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, ds)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desiredDs := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: def.ServiceAccountName,
					HostNetwork:        true,
					HostPID:            true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					Containers: []corev1.Container{
						{
							Name:         def.Name,
							Image:        def.Image,
							Env:          def.EnvVars,
							VolumeMounts: def.VolumeMounts,
							Ports:        ResolveContainerPorts(def),
							SecurityContext: &corev1.SecurityContext{
								Privileged: BoolPtr(true),
								Capabilities: &corev1.Capabilities{
									Add: []corev1.Capability{"NET_ADMIN", "SYS_ADMIN"},
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("500m"),
									corev1.ResourceMemory: resource.MustParse("256Mi"),
								},
							},
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

// ReconcileService creates or updates a service.
func (r *ResourceManager) ReconcileService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, serviceType corev1.ServiceType, port, targetPort int32) error {
	svc := &corev1.Service{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, svc)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	desiredSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Type:     serviceType,
			Selector: labels,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       port,
					TargetPort: intstr.FromInt(int(targetPort)),
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(infra, desiredSvc, r.Scheme); err != nil {
		return err
	}

	if errors.IsNotFound(err) {
		return r.Client.Create(ctx, desiredSvc)
	}

	svc.Spec = desiredSvc.Spec
	return r.Client.Update(ctx, svc)
}

// ReconcileIngress creates or updates an ingress.
func (r *ResourceManager) ReconcileIngress(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, serviceName string, config *infrav1alpha1.IngressConfig) error {
	ingressName := serviceName

	ingress := &networkingv1.Ingress{}
	err := r.Client.Get(ctx, types.NamespacedName{Name: ingressName, Namespace: infra.Namespace}, ingress)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	pathType := networkingv1.PathTypePrefix
	desiredIngress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ingressName,
			Namespace: infra.Namespace,
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
												Number: 80,
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
	return r.Client.Update(ctx, ingress)
}

// ReconcileServiceConfigMap creates or updates a configmap for a service.
func (r *ResourceManager) ReconcileServiceConfigMap(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string, labels map[string]string, config any) error {
	if config == nil {
		config = map[string]any{}
	}

	payload, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config for %s: %w", name, err)
	}

	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: infra.Namespace,
			Labels:    labels,
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

func DecodeServiceConfig(raw *runtime.RawExtension, config any) error {
	if raw == nil || len(raw.Raw) == 0 {
		return nil
	}

	if err := yaml.Unmarshal(raw.Raw, config); err != nil {
		return fmt.Errorf("parse service config: %w", err)
	}

	return nil
}

func SetIfMissing(config map[string]any, key string, value any) {
	if _, ok := config[key]; ok {
		return
	}
	config[key] = value
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
