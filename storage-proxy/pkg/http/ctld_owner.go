package http

import (
	"context"
	"fmt"
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
	pod, err := resolveKubernetesPod(ctx, r.client, r.selfPodID)
	if err != nil {
		return "", err
	}
	if pod == nil || pod.Spec.NodeName == "" {
		return "", fmt.Errorf("storage-proxy pod %q is not scheduled", r.selfPodID)
	}
	node, err := r.client.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeInternalIP && strings.TrimSpace(address.Address) != "" {
			return fmt.Sprintf("http://%s:%d", address.Address, r.port), nil
		}
	}
	return "", fmt.Errorf("node %s has no internal ip", node.Name)
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
