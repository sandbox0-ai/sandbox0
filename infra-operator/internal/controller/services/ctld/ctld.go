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
	credentialstoresvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/credentialstore"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/database"
	internalauthsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/internalauth"
	netdsvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/netd"
	sandboxobssvc "github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/storage"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/runtimeconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
	pkginternalauth "github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
)

type Reconciler struct {
	Resources *common.ResourceManager
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
	embeddedNetdCPURequest         = "100m"
	embeddedNetdMemoryRequest      = "128Mi"
	ctldHAProbeSocket              = "/run/sandbox0/ctld-ha.sock"
	ctldKubeletRegistrationSocket  = "/var/lib/kubelet/plugins_registry/" + volumeportal.DriverName + "-reg.sock"
	ctldKubeletCSIEndpoint         = "/var/lib/kubelet/plugins/" + volumeportal.DriverName + "/csi.sock"
	ctldRolloutRevisionAnnotation  = "infra.sandbox0.ai/ctld-rollout-revision"
	netdMetricsServiceSuffix       = "-netd-metrics"
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
	storageConfig, err := r.buildStorageConfig(ctx, infra)
	if err != nil {
		return err
	}
	if storageConfig.ObjectEncryptionEnabled {
		if err := common.EnsureObjectEncryptionKeySecret(ctx, r.Resources, infra); err != nil {
			return err
		}
		storageConfig.ObjectEncryptionKeyPath = common.ObjectEncryptionKeyPath
	}
	if err := credentialstoresvc.ApplyEncryptedPGCredentialStoreConfig(ctx, r.Resources, common.NewObjectScope(infra), &storageConfig.CredentialStore); err != nil {
		return err
	}
	config := &apiconfig.CtldConfig{StorageProxyConfig: *storageConfig}
	if err := sandboxobssvc.ApplyCtldConfig(ctx, r.Resources.Client, infra, clusterGatewayURL, config); err != nil {
		return err
	}
	configRef, err := r.Resources.ReconcileHashedServiceConfigMap(ctx, infra, name, labels, config)
	if err != nil {
		return err
	}
	podAnnotations := configRef.PodAnnotations()
	compiledPlan := infraplan.Compile(infra)
	compiledPlan.Services.ClusterGateway.URL = clusterGatewayURL
	var netdAssets *netdsvc.RuntimeAssets
	if compiledPlan.Netd.Enabled {
		netdAssets, err = netdsvc.NewReconciler(r.Resources).BuildRuntimeAssets(ctx, compiledPlan)
		if err != nil {
			return err
		}
		podAnnotations[netdsvc.ConfigHashAnnotation] = netdAssets.ConfigRef.Hash
	}
	if err := r.ensureCSIDriver(ctx, labels); err != nil {
		return err
	}
	if netdAssets != nil {
		if err := r.ensureNetdMetricsService(ctx, infra, int32(netdAssets.Config.MetricsPort)); err != nil {
			return err
		}
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
	bidirectional := corev1.MountPropagationBidirectional
	hostPathDirectoryOrCreate := corev1.HostPathDirectoryOrCreate
	volumeMounts := []corev1.VolumeMount{
		{Name: "config", MountPath: "/config/config.yaml", SubPath: "config.yaml", ReadOnly: true},
		{Name: "csi-plugin", MountPath: "/csi"},
		{Name: "kubelet", MountPath: "/var/lib/kubelet", MountPropagation: &bidirectional},
		{Name: "ctld-data", MountPath: "/var/lib/sandbox0/ctld"},
		{Name: "containerd-sock", MountPath: "/host-run/containerd"},
		{Name: "containerd-data", MountPath: containerdDataMountPath, ReadOnly: true},
		{Name: netdsvc.RunVolumeName, MountPath: netdsvc.RunMountDirectory},
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
			Name: "csi-plugin",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/lib/kubelet/plugins/volume.sandbox0.ai",
					Type: &hostPathDirectoryOrCreate,
				},
			},
		},
		{
			Name: "kubelet",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/kubelet"},
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
		{Name: netdsvc.RunVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
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
	credentialStoreMounts, credentialStoreVolumes := credentialstoresvc.CredentialStoreVolumes(common.NewObjectScope(infra), &storageConfig.CredentialStore)
	volumeMounts = append(volumeMounts, credentialStoreMounts...)
	volumes = append(volumes, credentialStoreVolumes...)
	if storageConfig.ObjectEncryptionEnabled {
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
	if netdAssets != nil {
		volumeMounts = appendUniqueVolumeMounts(volumeMounts, netdAssets.VolumeMounts...)
		volumes = appendUniqueVolumes(volumes, netdAssets.Volumes...)
	}
	netdActiveLockPath := ""
	if netdAssets != nil {
		netdActiveLockPath = netdAssets.ActiveLockPath
	}

	if err := r.removeLegacyDaemonSet(ctx, infra, name); err != nil {
		return err
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
			NetdEnabled:             netdAssets != nil,
			NetdActiveLockPath:      netdActiveLockPath,
		})
		if netdAssets != nil {
			desired.Spec.Template.Spec.RuntimeClassName = netdAssets.RuntimeClassName
			// Do not declare netd listener ports on host-networked ctld Pods.
			// Kubernetes reserves every declared container port as a HostPort in
			// this mode, which prevents a replacement from coexisting with the
			// guarded legacy netd during handoff. The active lock still guarantees
			// that exactly one process binds each configured node-local port.
		}
		revision, err := common.ConfigHash(desired.Spec)
		if err != nil {
			return fmt.Errorf("hash ctld slot %s rollout: %w", slot, err)
		}
		desired.Spec.Template.Annotations[ctldRolloutRevisionAnnotation] = revision
		desiredBySlot[slot] = desired
	}
	return r.reconcileHASlots(ctx, infra, desiredBySlot[dataplane.CtldHASlotA], desiredBySlot[dataplane.CtldHASlotB])
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
	NetdEnabled             bool
	NetdActiveLockPath      string
}

func buildCtldDaemonSet(cfg ctldDaemonSetConfig) *appsv1.DaemonSet {
	labels := make(map[string]string, len(cfg.Labels)+1)
	for key, value := range cfg.Labels {
		labels[key] = value
	}
	labels[dataplane.CtldHASlotLabel] = cfg.Slot
	args := append([]string(nil), cfg.Args...)
	args = append(args,
		"-ha-enabled=true",
		"-ha-slot="+cfg.Slot,
		"-ha-probe-socket="+ctldHAProbeSocket,
		"-kubelet-registration-socket="+ctldKubeletRegistrationSocket,
		"-kubelet-registration-endpoint="+ctldKubeletCSIEndpoint,
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
	if cfg.NetdEnabled {
		cpu := requests[corev1.ResourceCPU]
		cpu.Add(resource.MustParse(embeddedNetdCPURequest))
		requests[corev1.ResourceCPU] = cpu
		memory := requests[corev1.ResourceMemory]
		memory.Add(resource.MustParse(embeddedNetdMemoryRequest))
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
	}
	if cfg.NetdEnabled {
		ctldContainer.Env = append(ctldContainer.Env,
			corev1.EnvVar{Name: "NETD_CONFIG_PATH", Value: netdsvc.ConfigPath},
			corev1.EnvVar{Name: netdsvc.ActiveLockEnv, Value: cfg.NetdActiveLockPath},
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

// EmbeddedNetdReady reports embedded netd readiness through the ctld primary
// probe. Standby probes validate FUSE synchronization without starting netd.
func (r *Reconciler) EmbeddedNetdReady(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (bool, error) {
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

// PodMatchesCurrentTemplate rejects a live predecessor and verifies that the
// ctld container carrying embedded netd is actually running with the desired
// config, lock, mounts, and image.
func PodMatchesCurrentTemplate(pod *corev1.Pod, ds *appsv1.DaemonSet) bool {
	if pod == nil || ds == nil || !mapContains(pod.Labels, ds.Spec.Template.Labels) || !mapContains(pod.Annotations, ds.Spec.Template.Annotations) {
		return false
	}
	if pod.Annotations[netdsvc.ConfigHashAnnotation] != ds.Spec.Template.Annotations[netdsvc.ConfigHashAnnotation] {
		return false
	}
	desired := containerByName(ds.Spec.Template.Spec.Containers, "ctld")
	actual := containerByName(pod.Spec.Containers, "ctld")
	if desired == nil || actual == nil || desired.Image != actual.Image {
		return false
	}
	for _, name := range []string{"NETD_CONFIG_PATH", netdsvc.ActiveLockEnv} {
		desiredValue, desiredFound := envValue(desired.Env, name)
		actualValue, actualFound := envValue(actual.Env, name)
		if desiredFound != actualFound || desiredValue != actualValue {
			return false
		}
	}
	for _, name := range []string{netdsvc.ConfigVolumeName, netdsvc.ActiveLockVolumeName} {
		desiredMount, desiredFound := volumeMountByName(desired.VolumeMounts, name)
		actualMount, actualFound := volumeMountByName(actual.VolumeMounts, name)
		if desiredFound != actualFound || (desiredFound && desiredMount.MountPath != actualMount.MountPath) {
			return false
		}
	}
	return CtldContainerRunning(pod)
}

// DaemonSetEmbedsNetd verifies the guarded netd runtime contract required
// before the standalone netd DaemonSet may yield its active lock.
func DaemonSetEmbedsNetd(ds *appsv1.DaemonSet, activeLockPath string) bool {
	if ds == nil || ds.Spec.Template.Annotations[netdsvc.ConfigHashAnnotation] == "" {
		return false
	}
	container := containerByName(ds.Spec.Template.Spec.Containers, "ctld")
	if container == nil {
		return false
	}
	configPath, hasConfigPath := envValue(container.Env, "NETD_CONFIG_PATH")
	lockPath, hasLockPath := envValue(container.Env, netdsvc.ActiveLockEnv)
	if !hasConfigPath || configPath != netdsvc.ConfigPath || !hasLockPath || lockPath != activeLockPath {
		return false
	}
	configMount, hasConfigMount := volumeMountByName(container.VolumeMounts, netdsvc.ConfigVolumeName)
	lockMount, hasLockMount := volumeMountByName(container.VolumeMounts, netdsvc.ActiveLockVolumeName)
	return hasConfigMount && configMount.MountPath == netdsvc.ConfigPath &&
		hasLockMount && lockMount.MountPath == netdsvc.ActiveLockMountDirectory
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

func (r *Reconciler) removeLegacyDaemonSet(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, name string) error {
	legacy := &appsv1.DaemonSet{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: infra.Namespace}, legacy)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.Resources.Client.Delete(ctx, legacy)
}

func ctldArgs(infra *infrav1alpha1.Sandbox0Infra, containerdHostDataRoot string) []string {
	if strings.TrimSpace(containerdHostDataRoot) == "" {
		containerdHostDataRoot = defaultContainerdHostDataRoot
	}
	args := []string{
		"-http-addr=:8095",
		"-cri-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-endpoint=/host-run/containerd/containerd.sock",
		"-containerd-root=/host-run/containerd",
		"-containerd-host-root=" + defaultContainerdHostStateRoot,
		"-containerd-data-root=" + containerdDataMountPath,
		"-containerd-host-data-root=" + containerdHostDataRoot,
		"-volume-portal-root=/var/lib/sandbox0/ctld",
		"-kubelet-pods-root=/var/lib/kubelet/pods",
		"-csi-socket=/csi/csi.sock",
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

func (r *Reconciler) ensureCSIDriver(ctx context.Context, labels map[string]string) error {
	attachRequired := false
	podInfoOnMount := true
	lifecycleModes := []storagev1.VolumeLifecycleMode{storagev1.VolumeLifecycleEphemeral}
	desired := &storagev1.CSIDriver{
		ObjectMeta: metav1.ObjectMeta{
			Name:   volumeportal.DriverName,
			Labels: labels,
		},
		Spec: storagev1.CSIDriverSpec{
			AttachRequired:       &attachRequired,
			PodInfoOnMount:       &podInfoOnMount,
			VolumeLifecycleModes: lifecycleModes,
		},
	}
	current := &storagev1.CSIDriver{}
	err := r.Resources.Client.Get(ctx, types.NamespacedName{Name: volumeportal.DriverName}, current)
	if apierrors.IsNotFound(err) {
		return r.Resources.Client.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	return r.Resources.UpdateObjectIfChanged(ctx, current, func() {
		current.Labels = desired.Labels
		current.Spec = desired.Spec
	})
}

// ensureNetdMetricsService preserves named-port endpoint discovery without
// reserving netd's node-local listener on both host-networked ctld HA Pods.
// Slot A supplies one endpoint per node; the numeric target reaches whichever
// lock-fenced ctld peer currently owns the node-local metrics listener.
func (r *Reconciler) ensureNetdMetricsService(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra, metricsPort int32) error {
	labels := common.GetServiceLabels(infra.Name, "netd")
	selector := common.GetServiceLabels(infra.Name, "ctld")
	selector[dataplane.CtldHASlotLabel] = dataplane.CtldHASlotA
	return r.Resources.ReconcileServicePortsWithSpecMutator(
		ctx,
		infra,
		infra.Name+netdMetricsServiceSuffix,
		labels,
		corev1.ServiceTypeClusterIP,
		nil,
		[]corev1.ServicePort{common.BuildServicePort("metrics", metricsPort, metricsPort, corev1.ServiceTypeClusterIP)},
		func(spec *corev1.ServiceSpec) {
			spec.Selector = selector
		},
	)
}

func (r *Reconciler) buildStorageConfig(ctx context.Context, infra *infrav1alpha1.Sandbox0Infra) (*apiconfig.StorageProxyConfig, error) {
	cfg := &apiconfig.StorageProxyConfig{}
	if infra != nil && infra.Spec.Services != nil && infra.Spec.Services.StorageProxy != nil {
		cfg = runtimeconfig.ToStorageProxy(infra.Spec.Services.StorageProxy.Config)
	}
	cfg.RegionID = common.ResolveRegionID(infra)
	cfg.DefaultClusterId = common.ResolveClusterID(infra)
	if infra != nil && infra.Spec.Database != nil && r != nil && r.Resources != nil && r.Resources.Client != nil {
		if dsn, err := database.GetDatabaseDSN(ctx, r.Resources.Client, infra); err == nil {
			cfg.DatabaseURL = dsn
		}
	}
	if infra == nil || infra.Spec.Storage == nil {
		return cfg, nil
	}
	storageConfig, err := storage.GetStorageConfig(ctx, r.Resources.Client, infra)
	if err != nil {
		return nil, err
	}
	cfg.ObjectStorageType = normalizeObjectStorageType(storageConfig.Type)
	cfg.S3Bucket = storageConfig.Bucket
	cfg.S3Region = storageConfig.Region
	cfg.S3Endpoint = storageConfig.Endpoint
	cfg.S3AccessKey = storageConfig.AccessKey
	cfg.S3SecretKey = storageConfig.SecretKey
	cfg.S3SessionToken = storageConfig.SessionToken
	return cfg, nil
}

func normalizeObjectStorageType(storageType infrav1alpha1.StorageType) string {
	if storageType == infrav1alpha1.StorageTypeBuiltin {
		return "s3"
	}
	return string(storageType)
}
