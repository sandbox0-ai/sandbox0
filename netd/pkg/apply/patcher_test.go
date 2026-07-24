package apply

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/watcher"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestSyncAppliedHashesPatchesPendingSandboxesConcurrently(t *testing.T) {
	const sandboxCount = appliedHashPatchConcurrency * 2
	sandboxes := make([]*watcher.SandboxInfo, 0, sandboxCount)
	for index := range sandboxCount {
		name := fmt.Sprintf("sandbox-%d", index)
		sandboxes = append(sandboxes, &watcher.SandboxInfo{
			Namespace:         "default",
			Name:              name,
			NetworkPolicyHash: "policy-hash",
		})
	}

	var active atomic.Int64
	var maximum atomic.Int64
	var patches atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPatch {
			http.Error(writer, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		patches.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(&corev1.Pod{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      path.Base(request.URL.Path),
			},
		})
	}))
	defer server.Close()

	client, err := kubernetes.NewForConfig(&rest.Config{
		Host:  server.URL,
		QPS:   1_000,
		Burst: 1_000,
	})
	if err != nil {
		t.Fatalf("NewForConfig() error = %v", err)
	}

	if err := NewPatcher(client, nil).SyncAppliedHashes(context.Background(), sandboxes); err != nil {
		t.Fatalf("SyncAppliedHashes() error = %v", err)
	}
	if got := maximum.Load(); got <= 1 {
		t.Fatalf("maximum concurrent patches = %d, want greater than 1", got)
	} else if got > appliedHashPatchConcurrency {
		t.Fatalf("maximum concurrent patches = %d, want at most %d", got, appliedHashPatchConcurrency)
	}
	if got := patches.Load(); got != sandboxCount {
		t.Fatalf("patch count = %d, want %d", got, sandboxCount)
	}
}

func TestSyncAppliedHashesSkipsAcknowledgedPolicies(t *testing.T) {
	client := fake.NewSimpleClientset()
	sandbox := &watcher.SandboxInfo{
		Namespace:          "default",
		Name:               "sandbox",
		NetworkPolicyHash:  "policy-hash",
		NetworkAppliedHash: "policy-hash",
	}

	if err := NewPatcher(client, nil).SyncAppliedHashes(context.Background(), []*watcher.SandboxInfo{sandbox}); err != nil {
		t.Fatalf("SyncAppliedHashes() error = %v", err)
	}
	if actions := client.Actions(); len(actions) != 0 {
		t.Fatalf("client actions = %v, want none", actions)
	}
}

func TestSyncAppliedHashesWritesPolicyHash(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default",
		Name:      "sandbox",
	}})
	sandbox := &watcher.SandboxInfo{
		Namespace:         "default",
		Name:              "sandbox",
		NetworkPolicyHash: "policy-hash",
	}

	if err := NewPatcher(client, nil).SyncAppliedHashes(context.Background(), []*watcher.SandboxInfo{sandbox}); err != nil {
		t.Fatalf("SyncAppliedHashes() error = %v", err)
	}
	pod, err := client.CoreV1().Pods("default").Get(context.Background(), "sandbox", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if got := pod.Annotations[controller.AnnotationNetworkPolicyAppliedHash]; got != sandbox.NetworkPolicyHash {
		t.Fatalf("applied hash = %q, want %q", got, sandbox.NetworkPolicyHash)
	}
}
