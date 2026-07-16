package netd

import (
	"context"
	"fmt"
	"net"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/activeguard"
)

type Reconciler struct {
	Resources *common.ResourceManager
}

const (
	LegacyHandoffStateAnnotation = "infra.sandbox0.ai/netd-handoff-state"
	LegacyHandoffStateActive     = "active"
	LegacyHandoffStateStandby    = "standby"
	LegacyStandbyInitialDelay    = "30s"
	LegacyStandbyMaxHold         = "2m"
)

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	return r.PrepareLegacyHandoff(ctx, imageRepo, imageTag, compiledPlan)
}

// PrepareLegacyHandoff upgrades an existing standalone netd DaemonSet to use
// the shared active lock. It intentionally does not create a DaemonSet on new
// installations, where ctld is the only netd workload.
func (r *Reconciler) PrepareLegacyHandoff(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	logger := log.FromContext(ctx)
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}
	if !compiledPlan.Netd.Enabled {
		logger.Info("netd is disabled, skipping")
		return nil
	}
	if !compiledPlan.Components.HasDataPlane {
		logger.Info("Data-plane services are disabled, skipping netd")
		return nil
	}

	scope := compiledPlan.Scope
	name := fmt.Sprintf("%s-netd", scope.Name)
	existing := &appsv1.DaemonSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("legacy netd daemonset is absent; embedded ctld owns netd")
			return nil
		}
		return err
	}
	// Once the legacy workload has entered standby, subsequent reconciles must
	// not turn it active again while ctld is still proving embedded netd ready.
	if legacyHandoffState(existing) == LegacyHandoffStateStandby {
		return nil
	}
	return r.applyLegacyDaemonSet(ctx, imageRepo, imageTag, compiledPlan)
}

// PrepareLegacyStandby keeps the guarded legacy DaemonSet as a delayed lock
// contender while embedded netd starts. If embedded initialization fails, the
// standby acquires the lock and restores the previous node-local runtime.
func (r *Reconciler) PrepareLegacyStandby(ctx context.Context, compiledPlan *infraplan.InfraPlan) error {
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}
	scope := compiledPlan.Scope
	name := fmt.Sprintf("%s-netd", scope.Name)
	existing := &appsv1.DaemonSet{}
	if err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: scope.Namespace}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	// Preserve the exact image, config, mounts, and security context that passed
	// the active handoff probes. Only lock acquisition behavior, probes, and
	// declarative port reservations differ in the standby template, so the
	// fallback runtime was already exercised end to end.
	desired := existing.DeepCopy()
	desired.Annotations = common.CloneStringMap(desired.Annotations)
	desired.Annotations[LegacyHandoffStateAnnotation] = LegacyHandoffStateStandby
	desired.Spec.Template.Annotations = common.CloneStringMap(desired.Spec.Template.Annotations)
	desired.Spec.Template.Annotations[LegacyHandoffStateAnnotation] = LegacyHandoffStateStandby
	if len(desired.Spec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("legacy netd daemonset has no container")
	}
	container := &desired.Spec.Template.Spec.Containers[0]
	container.LivenessProbe = nil
	container.ReadinessProbe = nil
	// A standby does not bind listeners until after its delay and active-lock
	// acquisition. Omitting host-network container ports lets it surge beside
	// the still-active predecessor without creating HostPort conflicts.
	container.Ports = nil
	container.Env = setContainerEnv(container.Env, activeguard.EnvInitialDelay, LegacyStandbyInitialDelay)
	container.Env = setContainerEnv(container.Env, activeguard.EnvMaxHold, LegacyStandbyMaxHold)
	maxSurge := intstr.FromInt(1)
	maxUnavailable := intstr.FromInt(0)
	desired.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{
		Type: appsv1.RollingUpdateDaemonSetStrategyType,
		RollingUpdate: &appsv1.RollingUpdateDaemonSet{
			MaxSurge:       &maxSurge,
			MaxUnavailable: &maxUnavailable,
		},
	}
	return r.Resources.ApplyDaemonSetWithScope(ctx, scope, desired)
}

func (r *Reconciler) applyLegacyDaemonSet(ctx context.Context, imageRepo, imageTag string, compiledPlan *infraplan.InfraPlan) error {
	scope := compiledPlan.Scope
	name := fmt.Sprintf("%s-netd", scope.Name)
	labels := common.GetServiceLabels(scope.Name, "netd")
	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	assets, err := r.BuildRuntimeAssets(ctx, compiledPlan)
	if err != nil {
		return err
	}

	desired := buildLegacyDaemonSet(legacyDaemonSetConfig{
		Name:       name,
		Namespace:  scope.Namespace,
		Labels:     labels,
		Image:      image,
		PullPolicy: pullPolicy,
		Assets:     assets,
		Plan:       compiledPlan,
	})
	return r.Resources.ApplyDaemonSetWithScope(ctx, scope, desired)
}

type legacyDaemonSetConfig struct {
	Name       string
	Namespace  string
	Labels     map[string]string
	Image      string
	PullPolicy corev1.PullPolicy
	Assets     *RuntimeAssets
	Plan       *infraplan.InfraPlan
}

func buildLegacyDaemonSet(cfg legacyDaemonSetConfig) *appsv1.DaemonSet {
	assets := cfg.Assets
	podAnnotations := common.EnsurePodTemplateAnnotations(assets.PodAnnotations)
	podAnnotations[LegacyHandoffStateAnnotation] = LegacyHandoffStateActive
	env := []corev1.EnvVar{
		{Name: "SERVICE", Value: "netd"},
		{Name: "CONFIG_PATH", Value: ConfigPath},
		{Name: activeguard.EnvPath, Value: assets.ActiveLockPath},
		{
			Name: "NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"},
			},
		},
	}
	container := corev1.Container{
		Name:            "netd",
		Image:           cfg.Image,
		ImagePullPolicy: cfg.PullPolicy,
		Env: common.AppendObservabilityEnvVars(env, cfg.Plan.Scope.Owner(), common.ObservabilityEnvConfig{
			ServiceName: "netd",
			RegionID:    cfg.Plan.Netd.RegionID,
			ClusterID:   cfg.Plan.Netd.ClusterID,
		}),
		Ports: assets.Ports,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: common.BoolPtr(false),
			Privileged:               common.BoolPtr(false),
			Capabilities: &corev1.Capabilities{
				Add: []corev1.Capability{"NET_ADMIN", "NET_RAW"},
			},
		},
		VolumeMounts: assets.VolumeMounts,
	}
	container.LivenessProbe = &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("health")}},
		InitialDelaySeconds: 10,
		PeriodSeconds:       10,
	}
	container.ReadinessProbe = &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("health")}},
		InitialDelaySeconds: 5,
		PeriodSeconds:       5,
	}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cfg.Name,
			Namespace: cfg.Namespace,
			Annotations: map[string]string{
				LegacyHandoffStateAnnotation: LegacyHandoffStateActive,
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: cfg.Labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      cfg.Labels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: cfg.Name,
					RuntimeClassName:   assets.RuntimeClassName,
					HostNetwork:        true,
					DNSPolicy:          corev1.DNSClusterFirstWithHostNet,
					NodeSelector:       assets.NodeSelector,
					Tolerations:        assets.Tolerations,
					Containers:         []corev1.Container{container},
					Volumes:            assets.Volumes,
				},
			},
		},
	}
}

// LegacyHandoffReady reports whether an existing guarded legacy DaemonSet has
// completed its rollout. An absent DaemonSet is ready on fresh installations.
func (r *Reconciler) LegacyHandoffReady(ctx context.Context, compiledPlan *infraplan.InfraPlan) (bool, error) {
	if compiledPlan == nil {
		return false, fmt.Errorf("compiled plan is required")
	}
	ds := &appsv1.DaemonSet{}
	key := types.NamespacedName{Name: fmt.Sprintf("%s-netd", compiledPlan.Scope.Name), Namespace: compiledPlan.Scope.Namespace}
	if err := r.Resources.Client.Get(ctx, key, ds); err != nil {
		if apierrors.IsNotFound(err) {
			return r.legacyPodsGone(ctx, compiledPlan)
		}
		return false, err
	}
	if ds.Status.DesiredNumberScheduled == 0 {
		return r.legacyPodsGone(ctx, compiledPlan)
	}
	if legacyHandoffState(ds) == LegacyHandoffStateStandby {
		return legacyDaemonSetRolloutReady(ds) && legacyDaemonSetIsStandby(ds) &&
			legacyDaemonSetUsesActiveLock(ds, ScopedActiveLockPath(compiledPlan.Scope.Namespace, compiledPlan.Scope.Name)), nil
	}
	ready := legacyDaemonSetRolloutReady(ds) &&
		legacyDaemonSetUsesActiveLock(ds, ScopedActiveLockPath(compiledPlan.Scope.Namespace, compiledPlan.Scope.Name))
	return ready, nil
}

// LegacyStandbyReady reports whether the delayed-lock standby template has a
// ready successor on every desired node. With maxSurge enabled, the previous
// active pods may still be running until their successors become ready.
func (r *Reconciler) LegacyStandbyReady(ctx context.Context, compiledPlan *infraplan.InfraPlan) (bool, error) {
	if compiledPlan == nil {
		return false, fmt.Errorf("compiled plan is required")
	}
	ds := &appsv1.DaemonSet{}
	key := types.NamespacedName{Name: fmt.Sprintf("%s-netd", compiledPlan.Scope.Name), Namespace: compiledPlan.Scope.Namespace}
	if err := r.Resources.Client.Get(ctx, key, ds); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if ds.Status.DesiredNumberScheduled == 0 {
		return r.legacyPodsGone(ctx, compiledPlan)
	}
	return legacyDaemonSetRolloutReady(ds) && legacyDaemonSetIsStandby(ds) &&
		legacyDaemonSetUsesActiveLock(ds, ScopedActiveLockPath(compiledPlan.Scope.Namespace, compiledPlan.Scope.Name)), nil
}

func legacyDaemonSetRolloutReady(ds *appsv1.DaemonSet) bool {
	return ds != nil && ds.Status.ObservedGeneration >= ds.Generation &&
		ds.Status.DesiredNumberScheduled > 0 &&
		ds.Status.UpdatedNumberScheduled == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberAvailable == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberUnavailable == 0
}

func legacyHandoffState(ds *appsv1.DaemonSet) string {
	if ds == nil {
		return ""
	}
	return ds.Annotations[LegacyHandoffStateAnnotation]
}

func legacyDaemonSetIsStandby(ds *appsv1.DaemonSet) bool {
	if ds == nil || legacyHandoffState(ds) != LegacyHandoffStateStandby || len(ds.Spec.Template.Spec.Containers) == 0 {
		return false
	}
	container := ds.Spec.Template.Spec.Containers[0]
	if len(container.Ports) != 0 {
		return false
	}
	if container.LivenessProbe != nil || container.ReadinessProbe != nil {
		return false
	}
	return containerEnvEquals(container.Env, activeguard.EnvInitialDelay, LegacyStandbyInitialDelay) &&
		containerEnvEquals(container.Env, activeguard.EnvMaxHold, LegacyStandbyMaxHold)
}

func containerEnvEquals(envs []corev1.EnvVar, name, value string) bool {
	for i := range envs {
		if envs[i].Name == name {
			return envs[i].Value == value
		}
	}
	return false
}

func setContainerEnv(envs []corev1.EnvVar, name, value string) []corev1.EnvVar {
	for i := range envs {
		if envs[i].Name == name {
			envs[i].Value = value
			envs[i].ValueFrom = nil
			return envs
		}
	}
	return append(envs, corev1.EnvVar{Name: name, Value: value})
}

func legacyDaemonSetUsesActiveLock(ds *appsv1.DaemonSet, wantPath string) bool {
	if ds == nil || len(ds.Spec.Template.Spec.Containers) == 0 {
		return false
	}
	for i := range ds.Spec.Template.Spec.Containers[0].Env {
		env := ds.Spec.Template.Spec.Containers[0].Env[i]
		if env.Name == activeguard.EnvPath {
			return env.Value == wantPath
		}
	}
	return false
}

// CleanupLegacyDaemonSet removes the standalone netd workload after embedded
// ctld netd is ready to acquire the shared active lock.
func (r *Reconciler) CleanupLegacyDaemonSet(ctx context.Context, compiledPlan *infraplan.InfraPlan) error {
	if compiledPlan == nil {
		return fmt.Errorf("compiled plan is required")
	}
	ds := &appsv1.DaemonSet{}
	key := types.NamespacedName{Name: fmt.Sprintf("%s-netd", compiledPlan.Scope.Name), Namespace: compiledPlan.Scope.Namespace}
	if err := r.Resources.Client.Get(ctx, key, ds); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
	} else if ds.DeletionTimestamp == nil {
		propagation := metav1.DeletePropagationForeground
		if err := r.Resources.Client.Delete(ctx, ds, &ctrlclient.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	gone, err := r.legacyPodsGone(ctx, compiledPlan)
	if err != nil {
		return err
	}
	if !gone {
		return fmt.Errorf("legacy netd pods are still terminating")
	}
	return nil
}

func (r *Reconciler) legacyPodsGone(ctx context.Context, compiledPlan *infraplan.InfraPlan) (bool, error) {
	pods := &corev1.PodList{}
	labels := common.GetServiceLabels(compiledPlan.Scope.Name, "netd")
	if err := r.Resources.Client.List(ctx, pods, ctrlclient.InNamespace(compiledPlan.Scope.Namespace), ctrlclient.MatchingLabels(labels)); err != nil {
		return false, err
	}
	return len(pods.Items) == 0, nil
}

func (r *Reconciler) resolveMITMCASecretName(ctx context.Context, compiledPlan *infraplan.InfraPlan, labels map[string]string) (string, error) {
	return EnsureMITMCASecretWithScope(ctx, r.Resources, compiledPlan.Scope, compiledPlan, labels)
}

func resolveClusterDNSCIDR(ctx context.Context, client ctrlclient.Client, logger logr.Logger) (string, error) {
	if client == nil {
		return "", fmt.Errorf("client is nil")
	}
	serviceNames := []string{"kube-dns", "coredns"}
	for _, name := range serviceNames {
		svc := &corev1.Service{}
		if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: "kube-system"}, svc); err != nil {
			continue
		}
		if svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == "None" {
			continue
		}
		ip := net.ParseIP(svc.Spec.ClusterIP)
		if ip == nil {
			continue
		}
		if ip.To4() != nil {
			return ip.String() + "/32", nil
		}
		return ip.String() + "/128", nil
	}
	return "", fmt.Errorf("failed to resolve cluster DNS CIDR from kube-dns/coredns, please specify it in the netd config")
}
