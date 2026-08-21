package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	crdfake "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	"go.uber.org/zap"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func TestBuildManagerInformerRuntimeDoesNotConstructKubernetesSandboxOperatorForNomad(t *testing.T) {
	runtime, err := buildManagerInformerRuntime(
		&managerKubernetesClients{
			client:    k8sfake.NewSimpleClientset(),
			crdClient: crdfake.NewSimpleClientset(),
		},
		&config.ManagerConfig{SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad},
		nil,
		nil,
		nil,
		zap.NewNop(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.operator != nil || runtime.recorder != nil || runtime.sandboxIndex != nil ||
		runtime.teardownCoordinator != nil || len(runtime.autoscalerAnnotationKeys) != 0 {
		t.Fatalf("Nomad informer runtime retained Kubernetes sandbox owners: %+v", runtime)
	}
	if runtime.templateLister == nil {
		t.Fatal("Nomad informer runtime has no read-only template lister")
	}
	templates, err := runtime.templateLister.List()
	if err != nil || len(templates) != 0 {
		t.Fatalf("template lister returned templates=%v error=%v", templates, err)
	}
}
