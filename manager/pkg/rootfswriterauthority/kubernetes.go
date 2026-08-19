package rootfswriterauthority

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/containerd/errdefs"
	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	extraPodName  = "authentication.kubernetes.io/pod-name"
	extraPodUID   = "authentication.kubernetes.io/pod-uid"
	extraNodeName = "authentication.kubernetes.io/node-name"
	extraNodeUID  = "authentication.kubernetes.io/node-uid"
)

type KubernetesVerifierConfig struct {
	Client             kubernetes.Interface
	Audience           string
	ServiceAccountName string
	Namespace          string
}

type KubernetesVerifier struct {
	client             kubernetes.Interface
	audience           string
	serviceAccountName string
	namespace          string
}

func NewKubernetesVerifier(config KubernetesVerifierConfig) (*KubernetesVerifier, error) {
	if config.Client == nil || strings.TrimSpace(config.Audience) == "" ||
		strings.TrimSpace(config.ServiceAccountName) == "" || strings.TrimSpace(config.Namespace) == "" {
		return nil, fmt.Errorf("Kubernetes client, audience, namespace, and service account are required")
	}
	return &KubernetesVerifier{
		client: config.Client, audience: strings.TrimSpace(config.Audience),
		serviceAccountName: strings.TrimSpace(config.ServiceAccountName), namespace: strings.TrimSpace(config.Namespace),
	}, nil
}

func (v *KubernetesVerifier) Verify(ctx context.Context, bearer string) (CallerIdentity, error) {
	review, err := v.client.AuthenticationV1().TokenReviews().Create(ctx, &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{Token: bearer, Audiences: []string{v.audience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("TokenReview: %w: %w", err, errdefs.ErrUnavailable)
	}
	if !review.Status.Authenticated || !slices.Contains(review.Status.Audiences, v.audience) {
		return CallerIdentity{}, fmt.Errorf("projected service account token was not authenticated for the required audience: %w", errdefs.ErrPermissionDenied)
	}
	expectedUsername := "system:serviceaccount:" + v.namespace + ":" + v.serviceAccountName
	if review.Status.User.Username != expectedUsername || strings.TrimSpace(review.Status.User.UID) == "" {
		return CallerIdentity{}, fmt.Errorf("unexpected service account identity: %w", errdefs.ErrPermissionDenied)
	}
	podName, err := singleExtra(review.Status.User.Extra, extraPodName)
	if err != nil {
		return CallerIdentity{}, err
	}
	podUID, err := singleExtra(review.Status.User.Extra, extraPodUID)
	if err != nil {
		return CallerIdentity{}, err
	}
	nodeName, err := singleExtra(review.Status.User.Extra, extraNodeName)
	if err != nil {
		return CallerIdentity{}, err
	}
	nodeUID, err := singleExtra(review.Status.User.Extra, extraNodeUID)
	if err != nil {
		return CallerIdentity{}, err
	}

	serviceAccount, err := v.client.CoreV1().ServiceAccounts(v.namespace).Get(ctx, v.serviceAccountName, metav1.GetOptions{})
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("get ctld service account: %w: %w", err, errdefs.ErrUnavailable)
	}
	if string(serviceAccount.UID) != review.Status.User.UID {
		return CallerIdentity{}, fmt.Errorf("service account UID changed: %w", errdefs.ErrPermissionDenied)
	}
	pod, err := v.client.CoreV1().Pods(v.namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("get ctld Pod: %w: %w", err, errdefs.ErrUnavailable)
	}
	if string(pod.UID) != podUID || pod.Spec.ServiceAccountName != v.serviceAccountName ||
		pod.Spec.NodeName != nodeName || pod.DeletionTimestamp != nil {
		return CallerIdentity{}, fmt.Errorf("ctld Pod identity is stale or does not match TokenReview: %w", errdefs.ErrPermissionDenied)
	}
	node, err := v.client.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return CallerIdentity{}, fmt.Errorf("get ctld Node: %w: %w", err, errdefs.ErrUnavailable)
	}
	if string(node.UID) != nodeUID {
		return CallerIdentity{}, fmt.Errorf("ctld Node UID does not match TokenReview: %w", errdefs.ErrPermissionDenied)
	}
	return CallerIdentity{NodeUID: nodeUID, PodUID: podUID}, nil
}

func singleExtra(values map[string]authenticationv1.ExtraValue, key string) (string, error) {
	value := values[key]
	if len(value) != 1 || strings.TrimSpace(value[0]) == "" {
		return "", fmt.Errorf("TokenReview extra %q must contain exactly one value: %w", key, errdefs.ErrPermissionDenied)
	}
	return value[0], nil
}
