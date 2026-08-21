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
	if runtime.secretLister == nil || runtime.secretInformer == nil {
		t.Fatal("Nomad informer runtime has no registry Secret cache")
	}
	if runtime.podInformer != nil || runtime.nodeInformer != nil || runtime.replicaSetInformer != nil ||
		runtime.networkPolicyInformer != nil || runtime.templateInformer != nil || runtime.crdFactory != nil ||
		runtime.templateLister != nil || len(runtime.cacheSyncs()) != 1 {
		t.Fatalf("Nomad informer runtime retained Pod or CRD caches: %+v", runtime)
	}
}
