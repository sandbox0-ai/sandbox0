package power

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type PodResolver struct {
	K8sClient  kubernetes.Interface
	PodLister  corelisters.PodLister
	PodIndexer cache.Indexer
	NodeName   string
}

func NewPodResolver(k8sClient kubernetes.Interface, nodeName string) *PodResolver {
	return &PodResolver{
		K8sClient: k8sClient,
		NodeName:  strings.TrimSpace(nodeName),
	}
}

func (r *PodResolver) SetPodCache(lister corelisters.PodLister, indexer cache.Indexer) {
	if r == nil {
		return
	}
	r.PodLister = lister
	r.PodIndexer = indexer
}

func (r *PodResolver) Resolve(req *http.Request, sandboxID string) (Target, error) {
	if r == nil || !r.canLookupPods() {
		return Target{}, ErrNotImplemented
	}
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	pod, err := r.lookupSandboxPod(ctx, sandboxID)
	if err != nil {
		return Target{}, err
	}
	return r.resolvePodTarget(pod, sandboxID)
}

func (r *PodResolver) ResolvePod(req *http.Request, namespace, name string) (Target, error) {
	if r == nil || !r.canLookupPods() {
		return Target{}, ErrNotImplemented
	}
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	pod, err := r.lookupPod(ctx, namespace, name)
	if err != nil {
		return Target{}, err
	}
	return r.resolvePodTarget(pod, "")
}

func (r *PodResolver) canLookupPods() bool {
	return r != nil && (r.K8sClient != nil || r.PodLister != nil || r.PodIndexer != nil)
}

func (r *PodResolver) resolvePodTarget(pod *corev1.Pod, sandboxID string) (Target, error) {
	if pod == nil {
		return Target{}, ErrPodNotFound
	}
	if r.NodeName != "" && pod.Spec.NodeName != r.NodeName {
		return Target{}, fmt.Errorf("sandbox pod %s/%s is scheduled on node %s, not %s", pod.Namespace, pod.Name, pod.Spec.NodeName, r.NodeName)
	}
	if sandboxID == "" && pod.Labels != nil {
		sandboxID = strings.TrimSpace(pod.Labels[controller.LabelSandboxID])
	}
	return Target{
		SandboxID:    sandboxID,
		Runtime:      runtimeNameForPod(pod),
		PodNamespace: pod.Namespace,
		PodName:      pod.Name,
		PodUID:       string(pod.UID),
		PodIP:        pod.Status.PodIP,
		ProcdPort:    procdHTTPPort(pod),
	}, nil
}

func procdHTTPPort(pod *corev1.Pod) int32 {
	if pod == nil {
		return 0
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "procd" {
			continue
		}
		for _, port := range container.Ports {
			if port.Name == "http" && port.ContainerPort > 0 {
				return port.ContainerPort
			}
		}
	}
	return 0
}

func runtimeNameForPod(pod *corev1.Pod) string {
	if pod == nil || pod.Spec.RuntimeClassName == nil {
		return "runc"
	}
	raw := strings.ToLower(strings.TrimSpace(*pod.Spec.RuntimeClassName))
	switch {
	case raw == "":
		return "runc"
	case strings.Contains(raw, "gvisor") || strings.Contains(raw, "runsc"):
		return "gvisor"
	case strings.Contains(raw, "kata"):
		return "kata"
	case strings.Contains(raw, "runc"):
		return "runc"
	default:
		return raw
	}
}

func (r *PodResolver) lookupSandboxPod(ctx context.Context, sandboxID string) (*corev1.Pod, error) {
	if r.PodIndexer != nil {
		objects, err := r.PodIndexer.ByIndex(podSandboxIDIndex, sandboxID)
		if err != nil {
			return nil, fmt.Errorf("get cached sandbox pod: %w", err)
		}
		for _, obj := range objects {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod == nil {
				continue
			}
			return pod.DeepCopy(), nil
		}
	}

	if r.K8sClient == nil {
		return nil, ErrSandboxNotFound
	}

	selector := labels.SelectorFromSet(map[string]string{controller.LabelSandboxID: sandboxID}).String()
	pods, err := r.K8sClient.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("list sandbox pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return nil, ErrSandboxNotFound
	}
	return pods.Items[0].DeepCopy(), nil
}

func (r *PodResolver) lookupPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if r.PodLister != nil {
		pod, err := r.PodLister.Pods(namespace).Get(name)
		if err == nil {
			return pod.DeepCopy(), nil
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get cached sandbox pod %s/%s: %w", namespace, name, err)
		}
	}

	if r.K8sClient == nil {
		return nil, ErrPodNotFound
	}

	pod, err := r.K8sClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, ErrPodNotFound
		}
		return nil, fmt.Errorf("get sandbox pod %s/%s: %w", namespace, name, err)
	}
	return pod.DeepCopy(), nil
}
