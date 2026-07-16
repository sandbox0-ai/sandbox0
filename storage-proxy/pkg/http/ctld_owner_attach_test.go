package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeCtldResolver struct {
	url     string
	err     error
	resolve func(context.Context, string, string) (string, error)
}

func (f fakeCtldResolver) ResolveLocalCtldURL(context.Context) (string, error) {
	return f.url, f.err
}

func (f fakeCtldResolver) ResolveCtldURL(ctx context.Context, nodeName, podNamespace string) (string, error) {
	if f.resolve != nil {
		return f.resolve(ctx, nodeName, podNamespace)
	}
	return f.url, f.err
}

func TestEnsureCtldVolumeOwnerAttachesLocalCtldForRWOWithoutOwner(t *testing.T) {
	repo := newFakeHTTPRepo()
	repo.volumes["vol-1"] = &db.SandboxVolume{ID: "vol-1", TeamID: "team-a", AccessMode: string(volume.AccessModeRWO)}

	var attachCalls int32
	var attachReq ctldapi.AttachVolumeOwnerRequest
	ctld := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/volume-portals/owners/attach" {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&attachCalls, 1)
		if err := json.NewDecoder(r.Body).Decode(&attachReq); err != nil {
			t.Fatalf("decode attach request: %v", err)
		}
		repo.activeMounts["vol-1"] = []*db.VolumeMount{{
			VolumeID:     "vol-1",
			ClusterID:    "cluster-a",
			PodID:        "sandbox0-system/ctld-a",
			MountOptions: mustMountOptionsRaw(t, volume.MountOptions{AccessMode: volume.AccessModeRWO, OwnerKind: volume.OwnerKindCtld}),
		}}
		_ = json.NewEncoder(w).Encode(ctldapi.AttachVolumeOwnerResponse{Attached: true})
	}))
	defer ctld.Close()

	server := &Server{
		logger:       logrus.New(),
		repo:         repo,
		cfg:          &config.StorageProxyConfig{HeartbeatTimeout: 15},
		ctldResolver: fakeCtldResolver{url: ctld.URL},
	}

	if err := server.ensureCtldVolumeOwner(context.Background(), repo.volumes["vol-1"]); err != nil {
		t.Fatalf("ensureCtldVolumeOwner() error = %v", err)
	}
	if got := atomic.LoadInt32(&attachCalls); got != 1 {
		t.Fatalf("attach calls = %d, want 1", got)
	}
	if attachReq.TeamID != "team-a" || attachReq.SandboxVolumeID != "vol-1" {
		t.Fatalf("attach request = %+v, want team-a/vol-1", attachReq)
	}
}

func TestResolveVolumeMountURLUsesOwnerPort(t *testing.T) {
	server := &Server{
		podResolver: &fakeVolumeFilePodResolver{urls: map[string]string{
			"sandbox0-system/ctld-a": "http://10.0.0.9:8080",
		}},
	}
	mount := &db.VolumeMount{
		VolumeID: "vol-1",
		PodID:    "sandbox0-system/ctld-a",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{
			OwnerKind: volume.OwnerKindCtld,
			OwnerPort: 8095,
		}),
	}

	got, err := server.resolveVolumeMountURL(context.Background(), mount)
	if err != nil {
		t.Fatalf("resolveVolumeMountURL() error = %v", err)
	}
	if got == nil || got.String() != "http://10.0.0.9:8095" {
		t.Fatalf("resolveVolumeMountURL() = %v, want http://10.0.0.9:8095", got)
	}
}

func TestResolveVolumeMountURLUsesNodeScopedCtldOwner(t *testing.T) {
	var resolvedNode string
	var resolvedNamespace string
	server := &Server{
		ctldResolver: fakeCtldResolver{resolve: func(_ context.Context, nodeName, podNamespace string) (string, error) {
			resolvedNode = nodeName
			resolvedNamespace = podNamespace
			return "http://10.0.0.9:8095", nil
		}},
	}
	mount := &db.VolumeMount{
		VolumeID: "vol-1",
		PodID:    "ctld-node/node-a",
		MountOptions: mustMountOptionsRaw(t, volume.MountOptions{
			OwnerKind:    volume.OwnerKindCtld,
			OwnerPort:    8095,
			NodeName:     "node-a",
			PodNamespace: "sandbox0-system",
		}),
	}

	got, err := server.resolveVolumeMountURL(context.Background(), mount)
	if err != nil {
		t.Fatalf("resolveVolumeMountURL() error = %v", err)
	}
	if got == nil || got.String() != "http://10.0.0.9:8095" {
		t.Fatalf("resolveVolumeMountURL() = %v, want http://10.0.0.9:8095", got)
	}
	if resolvedNode != "node-a" || resolvedNamespace != "sandbox0-system" {
		t.Fatalf("resolved owner = %s/%s, want sandbox0-system/node-a", resolvedNamespace, resolvedNode)
	}
}

func TestKubernetesVolumeCtldResolverUsesOwnerNode(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "manager-a",
				Namespace: "sandbox0-system",
				Labels:    map[string]string{ctldInstanceLabel: "fullmode"},
			},
			Spec: corev1.PodSpec{NodeName: "node-a"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "fullmode-ctld-a-node-b",
				Namespace: "sandbox0-system",
				Labels: map[string]string{
					ctldNameLabel:     ctldComponentName,
					ctldInstanceLabel: "fullmode",
				},
			},
			Spec: corev1.PodSpec{NodeName: "node-b"},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				PodIP: "10.0.0.9",
				Conditions: []corev1.PodCondition{{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				}},
			},
		},
	)
	resolver := newKubernetesVolumeCtldResolver(client, "sandbox0-system/manager-a")

	got, err := resolver.ResolveCtldURL(context.Background(), "node-b", "sandbox0-system")
	if err != nil {
		t.Fatalf("ResolveCtldURL() error = %v", err)
	}
	if got != "http://10.0.0.9:8095" {
		t.Fatalf("ResolveCtldURL() = %q, want http://10.0.0.9:8095", got)
	}
}
