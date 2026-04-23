package http

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const defaultCtldPort = 8095

type volumeCtldResolver interface {
	ResolveLocalCtldURL(ctx context.Context) (string, error)
}

type kubernetesVolumeCtldResolver struct {
	client    kubernetes.Interface
	selfPodID string
	port      int
}

const (
	ctldNameLabel     = "app.kubernetes.io/name"
	ctldInstanceLabel = "app.kubernetes.io/instance"
	ctldComponentName = "ctld"
)

func newKubernetesVolumeCtldResolver(client kubernetes.Interface, selfPodID string) volumeCtldResolver {
	if client == nil || strings.TrimSpace(selfPodID) == "" {
		return nil
	}
	return &kubernetesVolumeCtldResolver{
		client:    client,
		selfPodID: strings.TrimSpace(selfPodID),
		port:      defaultCtldPort,
	}
}

func (r *kubernetesVolumeCtldResolver) ResolveLocalCtldURL(ctx context.Context) (string, error) {
	if r == nil || r.client == nil {
		return "", fmt.Errorf("ctld resolver unavailable")
	}
	selfPod, err := resolveKubernetesPod(ctx, r.client, r.selfPodID)
	if err != nil {
		return "", err
	}
	if selfPod == nil {
		return "", fmt.Errorf("storage-proxy pod %q not found", r.selfPodID)
	}
	if selfPod.Spec.NodeName == "" {
		return "", fmt.Errorf("storage-proxy pod %q is not scheduled", r.selfPodID)
	}

	candidates, err := r.listCtldCandidates(ctx, selfPod)
	if err != nil {
		return "", err
	}
	for _, candidate := range candidates {
		if candidate.Spec.NodeName == selfPod.Spec.NodeName {
			if addr, ok := podInternalURL(candidate, r.port); ok {
				return addr, nil
			}
		}
	}
	for _, candidate := range candidates {
		if addr, ok := podInternalURL(candidate, r.port); ok {
			return addr, nil
		}
	}
	return "", fmt.Errorf("no ready ctld pod available for storage-proxy pod %q", r.selfPodID)
}

func resolveKubernetesPod(ctx context.Context, client kubernetes.Interface, podID string) (*corev1.Pod, error) {
	if client == nil || strings.TrimSpace(podID) == "" {
		return nil, fmt.Errorf("pod resolver unavailable")
	}
	if strings.Contains(podID, "/") {
		parts := strings.SplitN(podID, "/", 2)
		return client.CoreV1().Pods(parts[0]).Get(ctx, parts[1], metav1.GetOptions{})
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + podID,
		Limit:         2,
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("pod %q not found", podID)
	}
	return &pods.Items[0], nil
}

func (r *kubernetesVolumeCtldResolver) listCtldCandidates(ctx context.Context, selfPod *corev1.Pod) ([]corev1.Pod, error) {
	if r == nil || r.client == nil || selfPod == nil {
		return nil, fmt.Errorf("ctld resolver unavailable")
	}

	labelSelector := ctldNameLabel + "=" + ctldComponentName
	if instance := strings.TrimSpace(selfPod.Labels[ctldInstanceLabel]); instance != "" {
		labelSelector += "," + ctldInstanceLabel + "=" + instance
	}

	pods, err := r.client.CoreV1().Pods(selfPod.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, err
	}

	candidates := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if isReadyCtldPod(&pod) {
			candidates = append(candidates, pod)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Spec.NodeName == candidates[j].Spec.NodeName {
			return candidates[i].Name < candidates[j].Name
		}
		if candidates[i].Spec.NodeName == selfPod.Spec.NodeName {
			return true
		}
		if candidates[j].Spec.NodeName == selfPod.Spec.NodeName {
			return false
		}
		return candidates[i].Name < candidates[j].Name
	})
	return candidates, nil
}

func isReadyCtldPod(pod *corev1.Pod) bool {
	if pod == nil || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || strings.TrimSpace(pod.Status.PodIP) == "" {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podInternalURL(pod corev1.Pod, port int) (string, bool) {
	if strings.TrimSpace(pod.Status.PodIP) == "" {
		return "", false
	}
	return fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port), true
}

func (s *Server) ensureCtldVolumeOwner(ctx context.Context, volumeRecord *db.SandboxVolume) error {
	if s == nil || s.repo == nil || s.ctldResolver == nil || volumeRecord == nil {
		return nil
	}
	if volume.NormalizeAccessMode(volumeRecord.AccessMode) != volume.AccessModeRWO {
		return nil
	}

	heartbeatTimeout := 15
	if s.cfg != nil && s.cfg.HeartbeatTimeout > 0 {
		heartbeatTimeout = s.cfg.HeartbeatTimeout
	}
	mounts, err := s.repo.GetActiveMounts(ctx, volumeRecord.ID, heartbeatTimeout)
	if err != nil {
		return err
	}
	if owner := s.selectPreferredVolumeOwner(mounts); owner != nil {
		return nil
	}

	ctldAddr, err := s.ctldResolver.ResolveLocalCtldURL(ctx)
	if err != nil {
		return err
	}
	if _, err := postCtldJSON[ctldapi.AttachVolumeOwnerResponse](ctx, ctldAddr, "/api/v1/volume-portals/owners/attach", ctldapi.AttachVolumeOwnerRequest{
		TeamID:          volumeRecord.TeamID,
		SandboxVolumeID: volumeRecord.ID,
	}); err != nil {
		mounts, reloadErr := s.repo.GetActiveMounts(ctx, volumeRecord.ID, heartbeatTimeout)
		if reloadErr == nil && s.selectPreferredVolumeOwner(mounts) != nil {
			return nil
		}
		return err
	}
	return nil
}
