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

package functiongateway

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// Reconcile reconciles the function-gateway deployment.
func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}
	if !compiledPlan.FunctionGateway.Enabled {
		logger.Info("Function gateway is disabled, skipping")
		return nil
	}

	scope := compiledPlan.Scope
	deploymentName := fmt.Sprintf("%s-function-gateway", scope.Name)
	serviceName := deploymentName
	labels := common.GetServiceLabels(scope.Name, "function-gateway")
	keySecretName, privateKeyKey, _ := compiledPlan.ControlPlaneKeyRefs()

	config, err := r.buildConfig(ctx, compiledPlan)
	if err != nil {
		return err
	}
	podAnnotations, err := common.ConfigHashAnnotation(config)
	if err != nil {
		return err
	}
	if err := r.Resources.ReconcileServiceConfigMapWithScope(ctx, scope, deploymentName, labels, config); err != nil {
		return err
	}

	httpPort := int32(config.HTTPPort)
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "config",
			MountPath: "/config/config.yaml",
			SubPath:   "config.yaml",
			ReadOnly:  true,
		},
		{
			Name:      "internal-jwt-private-key",
			MountPath: pkginternalauth.DefaultInternalJWTPrivateKeyPath,
			SubPath:   "internal_jwt_private.key",
			ReadOnly:  true,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: deploymentName},
				},
			},
		},
		{
			Name: "internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: keySecretName,
					Items: []corev1.KeyToPath{
						{
							Key:  privateKeyKey,
							Path: "internal_jwt_private.key",
						},
					},
				},
			},
		},
	}

	if err := r.Resources.ReconcileDeploymentWithScope(ctx, scope, deploymentName, labels, compiledPlan.FunctionGateway.Replicas, common.ServiceDefinition{
		Name:       "function-gateway",
		Port:       httpPort,
		TargetPort: httpPort,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: httpPort,
			},
		},
		Image: fmt.Sprintf("%s:%s", imageRepo, imageTag),
		EnvVars: []corev1.EnvVar{
			{
				Name:  "SERVICE",
				Value: "function-gateway",
			},
			{
				Name:  "CONFIG_PATH",
				Value: "/config/config.yaml",
			},
		},
		VolumeMounts:   volumeMounts,
		Volumes:        volumes,
		PodAnnotations: podAnnotations,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/healthz",
					Port:   intstr.FromString("http"),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/readyz",
					Port:   intstr.FromString("http"),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       5,
		},
		Resources: compiledPlan.FunctionGateway.Resources,
	}); err != nil {
		return err
	}

	serviceType := common.ResolveServiceType(compiledPlan.FunctionGateway.ServiceConfig)
	servicePort := common.ResolveServicePort(compiledPlan.FunctionGateway.ServiceConfig, httpPort)
	serviceAnnotations := common.ResolveServiceAnnotations(compiledPlan.FunctionGateway.ServiceConfig)
	if err := r.Resources.ReconcileServicePortsWithScopeAndSpecMutator(ctx, scope, serviceName, labels, serviceType, serviceAnnotations, []corev1.ServicePort{
		{
			Name:       "http",
			Port:       servicePort,
			TargetPort: intstr.FromInt(int(httpPort)),
			Protocol:   corev1.ProtocolTCP,
			NodePort: func() int32 {
				if serviceType == corev1.ServiceTypeNodePort {
					return servicePort
				}
				return 0
			}(),
		},
	}, func(spec *corev1.ServiceSpec) {
		if serviceType == corev1.ServiceTypeLoadBalancer {
			spec.ExternalTrafficPolicy = corev1.ServiceExternalTrafficPolicyLocal
		}
	}); err != nil {
		return err
	}

	if compiledPlan.FunctionGateway.IngressConfig != nil && compiledPlan.FunctionGateway.IngressConfig.Enabled {
		if err := r.Resources.ReconcileIngressWithScope(ctx, scope, serviceName, servicePort, compiledPlan.FunctionGateway.IngressConfig); err != nil {
			return err
		}
	} else if err := r.deleteIngressIfExists(ctx, scope, serviceName); err != nil {
		return err
	}

	if err := r.Resources.EnsureDeploymentReadyWithScope(ctx, scope, deploymentName, compiledPlan.FunctionGateway.Replicas); err != nil {
		return err
	}

	logger.Info("Function gateway reconciled successfully")
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, compiledPlan *infraplan.InfraPlan) (*apiconfig.FunctionGatewayConfig, error) {
	cfg := &apiconfig.FunctionGatewayConfig{}
	if compiledPlan == nil {
		return nil, fmt.Errorf("compiled plan is required")
	}
	if compiledPlan.FunctionGateway.Config != nil {
		cloned := *compiledPlan.FunctionGateway.Config
		if gatewayConfig := compiledPlan.FunctionGateway.Config.GatewayConfig.DeepCopy(); gatewayConfig != nil {
			cloned.GatewayConfig = *gatewayConfig
		}
		cfg = &cloned
	}
	if dsn, err := compiledPlan.DatabaseDSN(ctx, r.Resources.Client); err == nil {
		cfg.DatabaseURL = dsn
	}
	cfg.DefaultClusterGatewayURL = compiledPlan.FunctionGateway.DefaultClusterGatewayURL
	if strings.TrimSpace(cfg.InternalAuthCaller) == "" {
		cfg.InternalAuthCaller = pkginternalauth.ServiceFunctionGateway
	}
	if strings.TrimSpace(cfg.JWTIssuer) == "" {
		cfg.JWTIssuer = "regional-gateway"
	}
	if strings.TrimSpace(cfg.JWTSecret) == "" && strings.TrimSpace(cfg.JWTPublicKeyPEM) == "" && strings.TrimSpace(cfg.JWTPublicKeyFile) == "" {
		jwtSecret, err := common.EnsureSecretValue(
			ctx,
			r.Resources.Client,
			r.Resources.Scheme,
			compiledPlan.Scope.Owner(),
			compiledPlan.RegionalGatewayJWTSecretName(),
			"jwt_secret",
			32,
		)
		if err != nil {
			return nil, err
		}
		cfg.JWTSecret = jwtSecret
	}
	return cfg, nil
}

func (r *Reconciler) deleteIngressIfExists(ctx context.Context, scope common.ObjectScope, name string) error {
	obj := &networkingv1.Ingress{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, obj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := r.Resources.Client.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
