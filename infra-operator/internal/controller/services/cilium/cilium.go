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

package cilium

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/infra/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/infra/infra-operator/internal/controller/pkg/common"
)

const (
	defaultInstallVersion   = "1.18.6"
	minSupportedVersion     = "1.14.0"
	defaultInstallNamespace = "kube-system"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

// IsEnabled returns true if the manager is configured to use Cilium.
func IsEnabled(infra *infrav1alpha1.Sandbox0Infra) bool {
	if infra == nil {
		return false
	}
	if infra.Spec.Mode == infrav1alpha1.DeploymentModeControlPlane {
		return false
	}
	if infra.Spec.Services != nil && infra.Spec.Services.Manager != nil && !infra.Spec.Services.Manager.Enabled {
		return false
	}
	provider := "cilium"
	if cfg := getNetworkConfig(infra); cfg != nil && cfg.Provider != "" {
		provider = cfg.Provider
	}
	return strings.EqualFold(provider, "cilium")
}

// Reconcile ensures Cilium is installed and at a supported version.
func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) error {
	logger := log.FromContext(ctx)
	if !IsEnabled(infra) {
		logger.Info("Cilium provider not enabled, skipping")
		return nil
	}

	cfg := resolveCiliumInstallConfig(infra)
	currentVersion, installed, err := r.detectVersion(ctx, cfg.InstallNamespace)
	if err != nil {
		return err
	}

	minVer, err := parseSemver(minSupportedVersion)
	if err != nil {
		return fmt.Errorf("parse minimum Cilium version: %w", err)
	}
	desiredVer, err := parseSemver(cfg.InstallVersion)
	if err != nil {
		return fmt.Errorf("parse desired Cilium version: %w", err)
	}
	if compareSemver(*desiredVer, *minVer) < 0 {
		return fmt.Errorf("requested Cilium version %s is below minimum %s", desiredVer.String(), minVer.String())
	}

	installAction := ""
	switch {
	case !installed:
		installAction = "install"
	case currentVersion == nil:
		return fmt.Errorf("detected Cilium but could not determine version in namespace %s", cfg.InstallNamespace)
	case compareSemver(*currentVersion, *minVer) < 0:
		if !cfg.AllowUpgrade {
			return fmt.Errorf("cilium version %s is below minimum %s and upgrades are disabled", currentVersion.String(), minVer.String())
		}
		installAction = "upgrade"
	}

	if installAction == "" {
		logger.Info("Cilium detected", "namespace", cfg.InstallNamespace, "version", currentVersion.String())
		return nil
	}

	if err := r.Resources.ReconcileNamespace(ctx, cfg.InstallNamespace); err != nil {
		return err
	}

	if err := r.reconcileInstallJob(ctx, infra, cfg, installAction, desiredVer.String()); err != nil {
		return err
	}

	logger.Info("Cilium install job reconciled",
		"action", installAction,
		"namespace", cfg.InstallNamespace,
		"version", cfg.InstallVersion,
	)
	return nil
}

type installConfig struct {
	InstallVersion   string
	InstallNamespace string
	AllowUpgrade     bool
}

func resolveCiliumInstallConfig(infra *infrav1alpha1.Sandbox0Infra) installConfig {
	cfg := installConfig{
		InstallVersion:   defaultInstallVersion,
		InstallNamespace: defaultInstallNamespace,
		AllowUpgrade:     false,
	}
	networkCfg := getNetworkConfig(infra)
	if networkCfg == nil {
		return cfg
	}
	ciliumCfg := networkCfg.Cilium
	if ciliumCfg.InstallVersion != "" {
		cfg.InstallVersion = ciliumCfg.InstallVersion
	}
	if ciliumCfg.InstallNamespace != "" {
		cfg.InstallNamespace = ciliumCfg.InstallNamespace
	}
	cfg.AllowUpgrade = ciliumCfg.AllowUpgrade
	return cfg
}

func getNetworkConfig(infra *infrav1alpha1.Sandbox0Infra) *apiconfig.NetworkProviderConfig {
	if infra == nil {
		return nil
	}
	return &infra.Spec.Network
}

func (r *Reconciler) detectVersion(ctx context.Context, namespace string) (*semver, bool, error) {
	ds := &appsv1.DaemonSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{
		Name:      "cilium",
		Namespace: namespace,
	}, ds)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	version, err := extractVersionFromDaemonSet(ds)
	if err != nil {
		return nil, true, err
	}
	return version, true, nil
}

func extractVersionFromDaemonSet(ds *appsv1.DaemonSet) (*semver, error) {
	if ds == nil {
		return nil, fmt.Errorf("nil daemonset")
	}
	for _, container := range ds.Spec.Template.Spec.Containers {
		if !strings.Contains(container.Image, "cilium") {
			continue
		}
		tag := extractImageTag(container.Image)
		if tag == "" {
			continue
		}
		return parseSemver(tag)
	}
	return nil, fmt.Errorf("no cilium container image tag found")
}

func extractImageTag(image string) string {
	image = strings.Split(image, "@")[0]
	parts := strings.Split(image, ":")
	if len(parts) < 2 {
		return ""
	}
	tag := parts[len(parts)-1]
	tag = strings.TrimPrefix(tag, "v")
	tag = strings.Split(tag, "-")[0]
	return tag
}

func (r *Reconciler) reconcileInstallJob(
	ctx context.Context,
	infra *infrav1alpha1.Sandbox0Infra,
	cfg installConfig,
	action string,
	version string,
) error {
	jobName := fmt.Sprintf("%s-cilium-install", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "cilium-installer")

	command := []string{
		"cilium",
		action,
		"--version",
		version,
		"--namespace",
		cfg.InstallNamespace,
		"--wait",
	}
	desired := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: infra.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: fmt.Sprintf("%s-cilium-installer", infra.Name),
					RestartPolicy:      corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "cilium-installer",
							Image:   buildCLIImage(cfg.InstallVersion),
							Command: command,
						},
					},
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(infra, desired, r.Resources.Scheme); err != nil {
		return err
	}

	existing := &batchv1.Job{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: jobName, Namespace: infra.Namespace}, existing)
	if err != nil {
		if errors.IsNotFound(err) {
			return r.Resources.Client.Create(ctx, desired)
		}
		return err
	}

	if existing.Status.Active > 0 {
		return nil
	}
	if existing.Status.Succeeded > 0 {
		if jobSpecEqual(existing, desired) {
			return nil
		}
		if err := r.Resources.Client.Delete(ctx, existing); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, desired)
	}

	if !jobSpecEqual(existing, desired) {
		if err := r.Resources.Client.Delete(ctx, existing); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, desired)
	}

	if existing.Status.Failed > 0 {
		if err := r.Resources.Client.Delete(ctx, existing); err != nil {
			return err
		}
		return r.Resources.Client.Create(ctx, desired)
	}

	return nil
}

func jobSpecEqual(current, desired *batchv1.Job) bool {
	if current == nil || desired == nil {
		return false
	}
	cur := current.Spec.Template.Spec
	des := desired.Spec.Template.Spec
	if cur.ServiceAccountName != des.ServiceAccountName {
		return false
	}
	if len(cur.Containers) != len(des.Containers) {
		return false
	}
	if len(cur.Containers) == 0 {
		return true
	}
	return cur.Containers[0].Image == des.Containers[0].Image &&
		strings.Join(cur.Containers[0].Command, " ") == strings.Join(des.Containers[0].Command, " ")
}

func int32Ptr(value int32) *int32 {
	return &value
}

type semver struct {
	major int
	minor int
	patch int
}

func (v semver) String() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
}

func parseSemver(value string) (*semver, error) {
	if value == "" {
		return nil, fmt.Errorf("empty version")
	}
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid version %q", value)
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid major version %q", value)
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid minor version %q", value)
	}
	patch := 0
	if len(parts) > 2 {
		patch, err = strconv.Atoi(parts[2])
		if err != nil {
			return nil, fmt.Errorf("invalid patch version %q", value)
		}
	}
	return &semver{major: major, minor: minor, patch: patch}, nil
}

func compareSemver(left, right semver) int {
	if left.major != right.major {
		return compareInt(left.major, right.major)
	}
	if left.minor != right.minor {
		return compareInt(left.minor, right.minor)
	}
	return compareInt(left.patch, right.patch)
}

func compareInt(left, right int) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func buildCLIImage(version string) string {
	// Use the dedicated cilium-cli image which contains the 'install' command.
	// The cilium/cilium image only contains the agent and cilium-dbg tool.
	return "quay.io/cilium/cilium-cli:v0.19.0"
}
