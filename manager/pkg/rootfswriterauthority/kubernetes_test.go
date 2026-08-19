package rootfswriterauthority

import (
	"context"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/stretchr/testify/require"
)

func TestKubernetesVerifierBindsServiceAccountPodAndNode(t *testing.T) {
	client := fake.NewClientset(
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "ctld", Namespace: "sandbox0", UID: types.UID("sa-uid")}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ctld-a", Namespace: "sandbox0", UID: types.UID("pod-uid")}, Spec: corev1.PodSpec{ServiceAccountName: "ctld", NodeName: "node-a"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("node-uid")}},
	)
	client.PrependReactor("create", "tokenreviews", tokenReviewReactor(tokenReviewUser()))
	verifier, err := NewKubernetesVerifier(KubernetesVerifierConfig{
		Client: client, Audience: "sandbox0-rootfs-writer:cluster-1",
		Namespace: "sandbox0", ServiceAccountName: "ctld",
	})
	require.NoError(t, err)
	identity, err := verifier.Verify(context.Background(), "token")
	require.NoError(t, err)
	require.Equal(t, CallerIdentity{NodeUID: "node-uid", PodUID: "pod-uid"}, identity)
}

func TestKubernetesVerifierRejectsAmbiguousNodeUID(t *testing.T) {
	client := fake.NewClientset()
	user := tokenReviewUser()
	user.Extra[extraNodeUID] = authenticationv1.ExtraValue{"node-uid", "other-node"}
	client.PrependReactor("create", "tokenreviews", tokenReviewReactor(user))
	verifier, err := NewKubernetesVerifier(KubernetesVerifierConfig{
		Client: client, Audience: "sandbox0-rootfs-writer:cluster-1",
		Namespace: "sandbox0", ServiceAccountName: "ctld",
	})
	require.NoError(t, err)
	_, err = verifier.Verify(context.Background(), "token")
	require.Error(t, err)
}

func tokenReviewUser() authenticationv1.UserInfo {
	return authenticationv1.UserInfo{
		Username: "system:serviceaccount:sandbox0:ctld", UID: "sa-uid",
		Extra: map[string]authenticationv1.ExtraValue{
			extraPodName: {"ctld-a"}, extraPodUID: {"pod-uid"},
			extraNodeName: {"node-a"}, extraNodeUID: {"node-uid"},
		},
	}
}

func tokenReviewReactor(user authenticationv1.UserInfo) ktesting.ReactionFunc {
	return func(action ktesting.Action) (bool, runtime.Object, error) {
		create := action.(ktesting.CreateAction)
		request := create.GetObject().(*authenticationv1.TokenReview)
		return true, &authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{
			Authenticated: true, Audiences: append([]string(nil), request.Spec.Audiences...), User: user,
		}}, nil
	}
}
