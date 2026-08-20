package ctld

import (
	"context"
	"fmt"
	"strings"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	ctldnetworkingassets "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/ctldnetworking"
	databasesvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	internalauthsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	meteringsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/metering"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

type Reconciler struct {
	Resources                *common.ResourceManager
	expectedRolloutRevisions map[string]string
}

const (
	containerdDataMountPath        = "/host-var-lib/containerd"
	defaultContainerdHostDataRoot  = "/var/lib/containerd"
	defaultContainerdHostStateRoot = "/run/containerd"
	ctldProbeTimeoutSeconds        = 15
	ctldProbeFailureThreshold      = 12
	ctldTerminationGraceSeconds    = int64(45)
	ctldCPURequest                 = "250m"
	ctldMemoryRequest              = "256Mi"
	networkRuntimeCPURequest       = "100m"
	networkRuntimeMemoryRequest    = "128Mi"
	ctldHAProbeSocket              = "/run/sandbox0/ctld-ha.sock"
	legacyVolumeCSIDriverName      = "volume.sandbox0.ai"
	ctldRolloutRevisionAnnotation  = "infra.sandbox0.ai/ctld-rollout-revision"
	networkMetricsServiceSuffix    = "-ctld-network-metrics"
	ctldHAMetricsPortA             = int32(9192)
	ctldHAMetricsPortB             = int32(9193)
	runtimeSlotControlHostRoot     = "/run/sandbox0"
	runtimeSlotControlMountRoot    = "/host-run/sandbox0"
	runtimeSlotNetNSHostRoot       = "/var/run/netns"
	runtimeSlotNetNSMountRoot      = "/host-run/netns"
)

func NewReconciler(resources *common.ResourceManager) *Reconciler {
	return &Reconciler{Resources: resources}
}

func (r *Reconciler) Reconcile(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, imageRepo, imageTag, clusterGatewayURL string) error {
	logger := log.FromContext(ctx)
	if !infrav1alpha1.HasDataPlaneServices(infra) {
		logger.Info("Data-plane services are disabled, skipping ctld")
		return nil
	}

	name := fmt.Sprintf("%s-ctld", infra.Name)
	labels := common.GetServiceLabels(infra.Name, "ctld")
	compiledPlan := infraplan.Compile(infra)
	compiledPlan.Services.ClusterGateway.URL = clusterGatewayURL
	config, err := r.buildConfig(ctx, infra, compiledPlan)
	if err != nil {
		return err
	}
	if err := sandboxobssvc.ApplyCtldConfig(ctx, r.Resources.Client, infra, clusterGatewayURL, config); err != nil {
		return err
	}
	configRef, err := r.Resources.ReconcileHashedServiceConfigMap(ctx, infra, name, labels, config)
	if err != nil {
		return err
	}
	podAnnotations := configRef.PodAnnotations()
	var networkingAssets *ctldnetworkingassets.RuntimeAssets
	if compiledPlan.Network.Enabled {
		networkingAssets, err = ctldnetworkingassets.NewReconciler(r.Resources).BuildRuntimeAssets(ctx, compiledPlan)
		if err != nil {
			return err
		}
		podAnnotations[ctldnetworkingassets.ConfigHashAnnotation] = networkingAssets.ConfigRef.Hash
	}
	if err := r.cleanupLegacyCSIDriver(ctx); err != nil {
		return err
	}
	if networkingAssets != nil {
		if err := r.ensureNetworkMetricsService(ctx, infra, int32(networkingAssets.Config.MetricsPort)); err != nil {
			return err
		}
	}
	if err := r.cleanupNetworkMetricsService(ctx, infra, networkingAssets != nil); err != nil {
		return err
	}

	image := fmt.Sprintf("%s:%s", imageRepo, imageTag)
	pullPolicy := corev1.PullIfNotPresent
	if r.Resources.ImagePullPolicy != nil {
		pullPolicy = *r.Resources.ImagePullPolicy
	}

	nodeSelector, tolerations := common.ResolveSandboxNodePlacement(infra)
	containerdHostDataRoot := ctldContainerdHostDataRoot(infra)
	args := ctldArgs(infra, containerdHostDataRoot)
	terminationGraceSeconds := ctldTerminationGraceSeconds
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumeMounts := []corev1.VolumeMount{
		{Name: "config", MountPath: "/config/config.yaml", SubPath: "config.yaml", ReadOnly: true},
		{Name: "ctld-data", MountPath: "/var/lib/sandbox0/ctld"},
		{Name: "containerd-sock", MountPath: "/host-run/containerd"},
		{Name: "containerd-data", MountPath: containerdDataMountPath, ReadOnly: true},
		{Name: ctldnetworkingassets.RunVolumeName, MountPath: ctldnetworkingassets.RunMountDirectory},
	}
	volumes := []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: configRef.ConfigMapName},
				},
			},
		},
		{
			Name: "ctld-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/sandbox0/ctld",
					Type: &hostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "containerd-sock",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: defaultContainerdHostStateRoot},
			},
		},
		{
			Name: "containerd-data",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: containerdHostDataRoot},
			},
		},
		{Name: ctldnetworkingassets.RunVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if strings.TrimSpace(config.SandboxObservabilityRuntimeSamplesIngestURL) != "" {
		keySecretName, privateKeyKey, _ := internalauthsvc.GetDataPlaneKeyRefs(infra)
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "internal-jwt-private-key",
			MountPath: pkginternalauth.DefaultInternalJWTPrivateKeyPath,
			SubPath:   "internal_jwt_private.key",
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "internal-jwt-private-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: keySecretName,
					Items: []corev1.KeyToPath{{
						Key:  privateKeyKey,
						Path: "internal_jwt_private.key",
					}},
				},
			},
		})
	}
	if config.RootFSObjectStorage.ObjectEncryptionEnabled {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "object-encryption-key",
			MountPath: common.ObjectEncryptionMountDir,
			ReadOnly:  true,
		})
		volumes = append(volumes, corev1.Volume{
			Name: "object-encryption-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: common.ObjectEncryptionSecretName(infra.Name),
					Items: []corev1.KeyToPath{{
						Key:  common.ObjectEncryptionSecretKey,
						Path: common.ObjectEncryptionKeyFilename,
					}},
				},
			},
		})
	}
	if networkingAssets != nil {
		volumeMounts = appendUniqueVolumeMounts(volumeMounts, networkingAssets.VolumeMounts...)
		volumes = appendUniqueVolumes(volumes, networkingAssets.Volumes...)
		volumeMounts = appendUniqueVolumeMounts(volumeMounts,
			corev1.VolumeMount{Name: "runtime-slot-control", MountPath: runtimeSlotControlMountRoot},
			corev1.VolumeMount{Name: "runtime-slot-netns", MountPath: runtimeSlotNetNSMountRoot, ReadOnly: true},
		)
		volumes = appendUniqueVolumes(volumes,
			corev1.Volume{Name: "runtime-slot-control", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: runtimeSlotControlHostRoot, Type: &hostPathDirectoryOrCreate},
			}},
			corev1.Volume{Name: "runtime-slot-netns", VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: runtimeSlotNetNSHostRoot, Type: &hostPathDirectoryOrCreate},
			}},
		)
	}
	desiredBySlot := make(map[string]*appsv1.DaemonSet, 2)
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		desired := buildCtldDaemonSet(ctldDaemonSetConfig{
			Name:                    name,
			Namespace:               infra.Namespace,
			Slot:                    slot,
			Labels:                  labels,
			PodAnnotations:          podAnnotations,
			Image:                   image,
			PullPolicy:              pullPolicy,
			Args:                    args,
			NodeSelector:            nodeSelector,
			Tolerations:             tolerations,
			TerminationGraceSeconds: terminationGraceSeconds,
			VolumeMounts:            volumeMounts,
			Volumes:                 volumes,
			Infra:                   infra,
			NetworkingEnabled:       networkingAssets != nil,
		})
		revision, err := common.ConfigHash(desired.Spec)
		if err != nil {
			return fmt.Errorf("hash ctld slot %s rollout: %w", slot, err)
		}
		desired.Spec.Template.Annotations[ctldRolloutRevisionAnnotation] = revision
		desiredBySlot[slot] = desired
	}
	r.expectedRolloutRevisions = map[string]string{
		dataplane.CtldHASlotA: desiredBySlot[dataplane.CtldHASlotA].Spec.Template.Annotations[ctldRolloutRevisionAnnotation],
		dataplane.CtldHASlotB: desiredBySlot[dataplane.CtldHASlotB].Spec.Template.Annotations[ctldRolloutRevisionAnnotation],
	}
	if err := r.reconcileHASlots(ctx, infra, desiredBySlot[dataplane.CtldHASlotA], desiredBySlot[dataplane.CtldHASlotB]); err != nil {
		return err
	}
	ready, err := r.Ready(ctx, infra)
	if err != nil {
		return err
	}
	if !ready {
		return nil
	}
	// The previous release used a separate netd config base name. Wait until
	// both ctld HA slots are current before deleting configs still mounted by
	// predecessor Pods.
	if err := r.Resources.CleanupUnusedServiceConfigMapsWithScope(ctx, compiledPlan.Scope, fmt.Sprintf("%s-netd", infra.Name)); err != nil {
		return err
	}
	if networkingAssets == nil {
		return r.Resources.CleanupUnusedServiceConfigMapsWithScope(ctx, compiledPlan.Scope, fmt.Sprintf("%s-ctld-networking", infra.Name))
	}
	return nil
}

// reconcileHASlots rolls ctld-b completely before changing ctld-a. Both
// DaemonSets use host networking, so a same-slot surge can conflict with the
// predecessor's listening ports. Serializing the slots keeps one HA peer
// running on every node while the other slot is replaced in place.
func (r *Reconciler) reconcileHASlots(
	ctx context.Context,
	infra *infrav1alpha1.Sandbox0Infra,
	desiredA, desiredB *appsv1.DaemonSet,
) error {
	currentA, aExists, err := r.getDaemonSet(ctx, desiredA)
	if err != nil {
		return err
	}
	currentB, bExists, err := r.getDaemonSet(ctx, desiredB)
	if err != nil {
		return err
	}

	// A fresh install has no availability to preserve, so create both peers in
	// one pass. If only one peer is missing, repair it without mutating the
	// surviving peer during the same reconciliation.
	if !aExists && !bExists {
		if err := r.Resources.ApplyDaemonSet(ctx, infra, desiredA); err != nil {
			return err
		}
		return r.Resources.ApplyDaemonSet(ctx, infra, desiredB)
	}
	if !aExists {
		return r.Resources.ApplyDaemonSet(ctx, infra, desiredA)
	}
	if !bExists {
		return r.Resources.ApplyDaemonSet(ctx, infra, desiredB)
	}

	aReady, err := r.daemonSetCurrentPodsReady(ctx, currentA)
	if err != nil {
		return err
	}
	bReady, err := r.daemonSetCurrentPodsReady(ctx, currentB)
	if err != nil {
		return err
	}
	aDesired := daemonSetHasDesiredRevision(currentA, desiredA)
	bDesired := daemonSetHasDesiredRevision(currentB, desiredB)

	if !bDesired {
		if aReady {
			// A healthy peer protects every node while B rolls. Return after the
			// write because controller-runtime cached reads are not read-your-writes.
			return r.Resources.ApplyDaemonSet(ctx, infra, desiredB)
		}
		if bReady {
			// Recover a degraded or stalled A under the still-healthy old B before
			// resuming the normal B-then-A order.
			return r.Resources.ApplyDaemonSet(ctx, infra, desiredA)
		}
		// Neither peer can currently protect the other. Leave both processes in
		// place and let their existing controllers recover readiness first.
		return nil
	}
	if !bReady {
		// B already carries the desired revision; wait for its DaemonSet and
		// current-template Pods to converge before touching A.
		return nil
	}
	if !aDesired || !aReady {
		return r.Resources.ApplyDaemonSet(ctx, infra, desiredA)
	}

	// Both slots are current and healthy. These calls are normally no-ops and
	// keep non-rollout metadata or strategy drift reconciled.
	if err := r.Resources.ApplyDaemonSet(ctx, infra, desiredB); err != nil {
		return err
	}
	return r.Resources.ApplyDaemonSet(ctx, infra, desiredA)
}

func daemonSetHasDesiredRevision(current, desired *appsv1.DaemonSet) bool {
	if current == nil || desired == nil {
		return false
	}
	desiredRevision := desired.Spec.Template.Annotations[ctldRolloutRevisionAnnotation]
	return desiredRevision != "" && current.Spec.Template.Annotations[ctldRolloutRevisionAnnotation] == desiredRevision
}

func (r *Reconciler) daemonSetCurrentPodsReady(ctx context.Context, ds *appsv1.DaemonSet) (bool, error) {
	if !daemonSetReady(ds) {
		return false, nil
	}
	return r.currentTemplatePodsReady(ctx, ds)
}

func (r *Reconciler) getDaemonSet(ctx context.Context, desired *appsv1.DaemonSet) (*appsv1.DaemonSet, bool, error) {
	current := &appsv1.DaemonSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, current)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return current, true, nil
}

type ctldDaemonSetConfig struct {
	Name                    string
	Namespace               string
	Slot                    string
	Labels                  map[string]string
	PodAnnotations          map[string]string
	Image                   string
	PullPolicy              corev1.PullPolicy
	Args                    []string
	NodeSelector            map[string]string
	Tolerations             []corev1.Toleration
	TerminationGraceSeconds int64
	VolumeMounts            []corev1.VolumeMount
	Volumes                 []corev1.Volume
	Infra                   *infrav1alpha1.Sandbox0Infra
	NetworkingEnabled       bool
}

func buildCtldDaemonSet(cfg ctldDaemonSetConfig) *appsv1.DaemonSet {
	labels := make(map[string]string, len(cfg.Labels)+1)
	for key, value := range cfg.Labels {
		labels[key] = value
	}
	labels[dataplane.CtldHASlotLabel] = cfg.Slot
	haMetricsPort := ctldHAMetricsPort(cfg.Slot)
	args := append([]string(nil), cfg.Args...)
	args = append(args,
		"-ha-slot="+cfg.Slot,
		"-ha-probe-socket="+ctldHAProbeSocket,
		fmt.Sprintf("-ha-metrics-addr=:%d", haMetricsPort),
	)
	probeCommand := func(kind string) []string {
		return []string{
			"/usr/local/bin/ctld",
			"-ha-probe=" + kind,
			"-ha-probe-socket=" + ctldHAProbeSocket,
			"-http-addr=:8095",
		}
	}
	requests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(ctldCPURequest),
		corev1.ResourceMemory: resource.MustParse(ctldMemoryRequest),
	}
	if cfg.NetworkingEnabled {
		cpu := requests[corev1.ResourceCPU]
		cpu.Add(resource.MustParse(networkRuntimeCPURequest))
		requests[corev1.ResourceCPU] = cpu
		memory := requests[corev1.ResourceMemory]
		memory.Add(resource.MustParse(networkRuntimeMemoryRequest))
		requests[corev1.ResourceMemory] = memory
	}
	ctldContainer := corev1.Container{
		Name:            "ctld",
		Image:           cfg.Image,
		ImagePullPolicy: cfg.PullPolicy,
		Args:            args,
		Env: common.AppendObservabilityEnvVars([]corev1.EnvVar{
			{Name: "SERVICE", Value: "ctld"},
			{Name: "CONFIG_PATH", Value: "/config/config.yaml"},
			{Name: "CTLD_HA_SLOT", Value: cfg.Slot},
			{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		}, cfg.Infra, common.ObservabilityEnvConfig{
			ServiceName: "ctld",
			RegionID:    common.ResolveRegionID(cfg.Infra),
			ClusterID:   common.ResolveClusterID(cfg.Infra),
		}),
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand("live")}},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			TimeoutSeconds:      ctldProbeTimeoutSeconds,
			FailureThreshold:    ctldProbeFailureThreshold,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand("ready")}},
			InitialDelaySeconds: 2,
			PeriodSeconds:       5,
			TimeoutSeconds:      ctldProbeTimeoutSeconds,
			FailureThreshold:    ctldProbeFailureThreshold,
		},
		SecurityContext: &corev1.SecurityContext{Privileged: common.BoolPtr(true)},
		Resources:       corev1.ResourceRequirements{Requests: requests},
		VolumeMounts:    cfg.VolumeMounts,
		Ports: []corev1.ContainerPort{{
			Name:          "ha-metrics",
			ContainerPort: haMetricsPort,
			Protocol:      corev1.ProtocolTCP,
		}},
	}
	if cfg.NetworkingEnabled {
		ctldContainer.Env = append(ctldContainer.Env,
			corev1.EnvVar{Name: "CTLD_NETWORK_CONFIG_PATH", Value: ctldnetworkingassets.ConfigPath},
		)
	}
	maxUnavailable := intstr.FromInt(1)
	maxSurge := intstr.FromInt(0)
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.Name + "-" + cfg.Slot, Namespace: cfg.Namespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &maxUnavailable,
					MaxSurge:       &maxSurge,
				},
			},
			MinReadySeconds: 2,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: common.EnsurePodTemplateAnnotations(cfg.PodAnnotations)},
				Spec: corev1.PodSpec{
					HostPID:                       true,
					ServiceAccountName:            cfg.Name,
					NodeSelector:                  cfg.NodeSelector,
					Tolerations:                   cfg.Tolerations,
					HostNetwork:                   true,
					DNSPolicy:                     corev1.DNSClusterFirstWithHostNet,
					TerminationGracePeriodSeconds: &cfg.TerminationGraceSeconds,
					Containers:                    []corev1.Container{ctldContainer},
					Volumes:                       cfg.Volumes,
				},
			},
		},
	}
}

func ctldHAMetricsPort(slot string) int32 {
	if slot == dataplane.CtldHASlotB {
		return ctldHAMetricsPortB
	}
	return ctldHAMetricsPortA
}

func appendUniqueVolumes(existing []corev1.Volume, additions ...corev1.Volume) []corev1.Volume {
	names := make(map[string]struct{}, len(existing)+len(additions))
	for i := range existing {
		names[existing[i].Name] = struct{}{}
	}
	for i := range additions {
		if _, ok := names[additions[i].Name]; ok {
			continue
		}
		existing = append(existing, additions[i])
		names[additions[i].Name] = struct{}{}
	}
	return existing
}

func appendUniqueVolumeMounts(existing []corev1.VolumeMount, additions ...corev1.VolumeMount) []corev1.VolumeMount {
	names := make(map[string]struct{}, len(existing)+len(additions))
	for i := range existing {
		names[existing[i].Name] = struct{}{}
	}
	for i := range additions {
		if _, ok := names[additions[i].Name]; ok {
			continue
		}
		existing = append(existing, additions[i])
		names[additions[i].Name] = struct{}{}
	}
	return existing
}

// Ready reports whether both ctld HA slots completed their rollout and all
// desired Pods pass the role-aware readiness probe.
func (r *Reconciler) Ready(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (bool, error) {
	if infra == nil {
		return false, fmt.Errorf("sandbox0infra is required")
	}
	name := fmt.Sprintf("%s-ctld", infra.Name)
	for _, slot := range []string{dataplane.CtldHASlotA, dataplane.CtldHASlotB} {
		ds := &appsv1.DaemonSet{}
		key := types.NamespacedName{Name: name + "-" + slot, Namespace: infra.Namespace}
		if err := r.Resources.Client.Get(ctx, key, ds); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if expectedRevision := r.expectedRolloutRevisions[slot]; expectedRevision != "" &&
			ds.Spec.Template.Annotations[ctldRolloutRevisionAnnotation] != expectedRevision {
			return false, nil
		}
		if !daemonSetReady(ds) {
			return false, nil
		}
		ready, err := r.currentTemplatePodsReady(ctx, ds)
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}
	}
	return true, nil
}

// NetworkReady reports network readiness through the ctld HA pair.
func (r *Reconciler) NetworkReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (bool, error) {
	return r.Ready(ctx, infra)
}

func daemonSetReady(ds *appsv1.DaemonSet) bool {
	return ds != nil &&
		ds.Status.ObservedGeneration >= ds.Generation &&
		ds.Status.UpdatedNumberScheduled == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberReady == ds.Status.DesiredNumberScheduled &&
		ds.Status.NumberUnavailable == 0
}

func (r *Reconciler) currentTemplatePodsReady(ctx context.Context, ds *appsv1.DaemonSet) (bool, error) {
	if ds == nil || ds.Status.DesiredNumberScheduled == 0 {
		return true, nil
	}
	pods := &corev1.PodList{}
	if err := r.Resources.Client.List(ctx, pods,
		ctrlclient.InNamespace(ds.Namespace),
		ctrlclient.MatchingLabels(ds.Spec.Selector.MatchLabels),
	); err != nil {
		return false, err
	}
	readyNodes := make(map[string]struct{}, ds.Status.DesiredNumberScheduled)
	for i := range pods.Items {
		if CtldContainerRunning(&pods.Items[i]) && !PodMatchesCurrentTemplate(&pods.Items[i], ds) {
			return false, nil
		}
		if pods.Items[i].DeletionTimestamp.IsZero() && pods.Items[i].Spec.NodeName != "" && PodReadyForCurrentTemplate(&pods.Items[i], ds) {
			readyNodes[pods.Items[i].Spec.NodeName] = struct{}{}
		}
	}
	return int32(len(readyNodes)) >= ds.Status.DesiredNumberScheduled, nil
}

// PodMatchesCurrentTemplate rejects a live predecessor and verifies that ctld
// is running with the desired network config, HA lock, mounts, and image.
func PodMatchesCurrentTemplate(pod *corev1.Pod, ds *appsv1.DaemonSet) bool {
	if pod == nil || ds == nil || !mapContains(pod.Labels, ds.Spec.Template.Labels) || !mapContains(pod.Annotations, ds.Spec.Template.Annotations) {
		return false
	}
	if pod.Annotations[ctldnetworkingassets.ConfigHashAnnotation] != ds.Spec.Template.Annotations[ctldnetworkingassets.ConfigHashAnnotation] {
		return false
	}
	desired := containerByName(ds.Spec.Template.Spec.Containers, "ctld")
	actual := containerByName(pod.Spec.Containers, "ctld")
	if desired == nil || actual == nil || desired.Image != actual.Image {
		return false
	}
	for _, name := range []string{"CTLD_NETWORK_CONFIG_PATH"} {
		desiredValue, desiredFound := envValue(desired.Env, name)
		actualValue, actualFound := envValue(actual.Env, name)
		if desiredFound != actualFound || desiredValue != actualValue {
			return false
		}
	}
	for _, name := range []string{ctldnetworkingassets.ConfigVolumeName} {
		desiredMount, desiredFound := volumeMountByName(desired.VolumeMounts, name)
		actualMount, actualFound := volumeMountByName(actual.VolumeMounts, name)
		if desiredFound != actualFound || (desiredFound && desiredMount.MountPath != actualMount.MountPath) {
			return false
		}
	}
	return CtldContainerRunning(pod)
}

// CtldContainerRunning reports whether a Pod can still own the node-local HA
// primary lock. A terminating predecessor remains relevant until its process
// has actually stopped.
func CtldContainerRunning(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	status := containerStatusByName(pod.Status.ContainerStatuses, "ctld")
	return status != nil && status.State.Running != nil
}

// PodReadyForCurrentTemplate adds the Kubernetes PodReady condition to the
// current-template and running-container checks.
func PodReadyForCurrentTemplate(pod *corev1.Pod, ds *appsv1.DaemonSet) bool {
	if !PodMatchesCurrentTemplate(pod, ds) {
		return false
	}
	for i := range pod.Status.Conditions {
		condition := pod.Status.Conditions[i]
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func mapContains(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}

func containerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func containerStatusByName(statuses []corev1.ContainerStatus, name string) *corev1.ContainerStatus {
	for i := range statuses {
		if statuses[i].Name == name {
			return &statuses[i]
		}
	}
	return nil
}

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for i := range env {
		if env[i].Name == name {
			return env[i].Value, true
		}
	}
	return "", false
}

func volumeMountByName(mounts []corev1.VolumeMount, name string) (corev1.VolumeMount, bool) {
	for i := range mounts {
		if mounts[i].Name == name {
			return mounts[i], true
		}
	}
	return corev1.VolumeMount{}, false
}

func ctldArgs(infra *infrav1alpha1.Sandbox0Infra, containerdHostDataRoot string) []string {
	if strings.TrimSpace(containerdHostDataRoot) == "" {
		containerdHostDataRoot = defaultContainerdHostDataRoot
	}
	args := []string{
		"-http-addr=:8095",
		fmt.Sprintf("-runtime-watch-addr=:%d", runtimecontrol.DefaultCtldWatchPort),
		"-cri-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-root=/host-run/containerd",
		"-containerd-host-root=" + defaultContainerdHostStateRoot,
		"-containerd-data-root=" + containerdDataMountPath,
		"-containerd-host-data-root=" + containerdHostDataRoot,
		"-state-root=/var/lib/sandbox0/ctld",
		"-runtime-slot-network-socket=" + runtimeSlotControlMountRoot + "/ctld-runtime-slot-network.sock",
		"-runtime-slot-netns-root=" + runtimeSlotNetNSMountRoot,
	}
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.Ctld != nil {
		cfg := infra.Spec.Services.Ctld
		if value := strings.TrimSpace(cfg.RootFSObjectCacheMaxBytes); value != "" {
			args = append(args, "-rootfs-object-cache-max-bytes="+value)
		}
		if value := strings.TrimSpace(cfg.RootFSObjectCacheMinFreeBytes); value != "" {
			args = append(args, "-rootfs-object-cache-min-free-bytes="+value)
		}
		if cfg.RootFSObjectCacheMaxAge.Duration > 0 {
			args = append(args, "-rootfs-object-cache-max-age="+cfg.RootFSObjectCacheMaxAge.Duration.String())
		}
		if cfg.RootFSObjectCacheSweepInterval.Duration > 0 {
			args = append(args, "-rootfs-object-cache-sweep-interval="+cfg.RootFSObjectCacheSweepInterval.Duration.String())
		}
	}
	return args
}

func ctldContainerdHostDataRoot(infra *infrav1alpha1.Sandbox0Infra) string {
	if infra == nil || infra.Spec.Services == nil || infra.Spec.Services.Ctld == nil {
		return defaultContainerdHostDataRoot
	}
	if root := strings.TrimSpace(infra.Spec.Services.Ctld.ContainerdHostDataRoot); root != "" {
		return root
	}
	return defaultContainerdHostDataRoot
}

func (r *Reconciler) cleanupLegacyCSIDriver(ctx context.Context) error {
	driver := &storagev1.CSIDriver{ObjectMeta: metav1.ObjectMeta{Name: legacyVolumeCSIDriverName}}
	if err := r.Resources.Client.Delete(ctx, driver); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete legacy volume CSI driver: %w", err)
	}
	return nil
}

// ensureNetworkMetricsService preserves named-port endpoint discovery without
// reserving the network runtime's node-local listener on both ctld HA Pods.
// Slot A supplies one endpoint per node; the numeric target reaches whichever
// lock-fenced ctld peer currently owns the node-local metrics listener.
func (r *Reconciler) ensureNetworkMetricsService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, metricsPort int32) error {
	labels := common.GetServiceLabels(infra.Name, "ctld")
	selector := common.GetServiceLabels(infra.Name, "ctld")
	selector[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotA
	return r.Resources.ReconcileServicePortsWithSpecMutator(
		ctx,
		infra,
		infra.Name+networkMetricsServiceSuffix,
		labels,
		corev1.ServiceTypeClusterIP,
		nil,
		[]corev1.ServicePort{common.BuildServicePort("metrics", metricsPort, metricsPort, corev1.ServiceTypeClusterIP)},
		func(spec *corev1.ServiceSpec) {
			spec.Selector = selector
		},
	)
}

func (r *Reconciler) cleanupNetworkMetricsService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, networkEnabled bool) error {
	if infra == nil || networkEnabled {
		return nil
	}
	name := infra.Name + networkMetricsServiceSuffix
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: infra.Namespace}}
	if err := r.Resources.Client.Delete(ctx, service); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete disabled network metrics service %s: %w", name, err)
	}
	return nil
}

func (r *Reconciler) buildConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, compiledPlan *infraplan.InfraPlan) (*apiconfig.CtldConfig, error) {
	if r == nil || r.Resources == nil {
		return nil, fmt.Errorf("ctld reconciler is required")
	}
	if infra == nil || compiledPlan == nil {
		return nil, fmt.Errorf("compiled infra plan is required")
	}
	cfg := &apiconfig.CtldConfig{
		RegionID:         common.ResolveRegionID(infra),
		DefaultClusterId: common.ResolveClusterID(infra),
	}
	if dsn, err := databasesvc.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
		cfg.DatabaseURL = dsn
	}
	rootFSObjectStorage, err := compiledPlan.RootFSObjectStorage(ctx, r.Resources.Client)
	if err != nil {
		return nil, fmt.Errorf("resolve rootfs object storage config: %w", err)
	}
	if rootFSObjectStorage != nil {
		cfg.RootFSObjectStorage = *rootFSObjectStorage
		if rootFSObjectStorage.ObjectEncryptionEnabled {
			if err := common.EnsureObjectEncryptionKeySecret(ctx, r.Resources, infra); err != nil {
				return nil, err
			}
		}
	}
	if err := meteringsvc.ApplyCtldConfig(ctx, r.Resources.Client, infra, cfg); err != nil {
		return nil, fmt.Errorf("apply ctld metering config: %w", err)
	}
	return cfg, nil
}
