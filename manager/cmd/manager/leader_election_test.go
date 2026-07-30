package main

import (
	"context"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestManagerLeaderElectionUsesPodIdentityAndConfiguredLease(t *testing.T) {
	t.Setenv("POD_NAME", "manager-a")
	t.Setenv("POD_NAMESPACE", "sandbox0-system")
	t.Setenv(config.ManagerLeaderElectionNameEnv, "demo-manager")

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	client := fake.NewSimpleClientset()
	go func() {
		done <- runManagerLeaderElection(ctx, client, zap.NewNop(), func(context.Context) {
			close(started)
		}, cancel)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("manager did not acquire controller leadership")
	}
	lease, err := client.CoordinationV1().Leases("sandbox0-system").Get(
		context.Background(),
		"demo-manager",
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get leader election lease: %v", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "manager-a" {
		t.Fatalf("lease holder = %v, want manager-a", lease.Spec.HolderIdentity)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runManagerLeaderElection() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("manager leader election did not stop")
	}
}

func TestManagerLeaderElectionDefaults(t *testing.T) {
	t.Setenv(config.ManagerLeaderElectionNameEnv, "")
	t.Setenv("POD_NAMESPACE", "default-a")

	if got := managerLeaderElectionName(); got != defaultManagerLeaderElectionName {
		t.Fatalf("leader election name = %q, want %q", got, defaultManagerLeaderElectionName)
	}
	if got := managerLeaderElectionNamespace(); got != "default-a" {
		t.Fatalf("leader election namespace = %q, want default-a", got)
	}
}
