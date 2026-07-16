package controller

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/pkg/common"
	"github.com/sandbox0-ai/sandbox0/infra-operator/internal/controller/services/nodereadiness"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
	"github.com/sandbox0-ai/sandbox0/pkg/dataplane"
)

func TestRunStepsRefreshesNodeReadinessAfterEarlierFailure(t *testing.T) {
	ctx := context.Background()
	infra := newWorkflowNodeReadinessInfra()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node-a",
		Labels: map[string]string{
			"sandbox0.ai/node-role":           "sandbox",
			dataplane.NodeDataPlaneReadyLabel: dataplane.ReadyLabelValue,
			dataplane.NodeNetdReadyLabel:      dataplane.ReadyLabelValue,
			dataplane.NodeCtldReadyLabel:      dataplane.ReadyLabelValue,
		},
	}}
	scheme := newWorkflowNodeReadinessScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(infra.DeepCopy(), node).Build()
	reconciler := &Sandbox0InfraReconciler{Client: client, Scheme: scheme}
	readiness := nodereadiness.NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))
	primaryFailure := errors.New("ctld rollout failed")
	laterStepRan := false
	compiledPlan := infraplan.Compile(infra)

	result, err := reconciler.runStepsWithNodeReadinessRefresh(ctx, infra, compiledPlan, readiness, []reconcileStep{
		{
			Name:          "ctld",
			Run:           func(context.Context) error { return primaryFailure },
			ConditionType: infrav1alpha1.ConditionTypeCtldReady,
			ErrorReason:   "CtldFailed",
		},
		{
			Name: "netd-ready",
			Run: func(context.Context) error {
				laterStepRan = true
				return nil
			},
		},
	})
	if err != nil {
		t.Fatalf("runStepsWithNodeReadinessRefresh() error = %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("requeue after = %v, want %v", result.RequeueAfter, requeueInterval)
	}
	if laterStepRan {
		t.Fatal("workflow continued after the primary ctld failure")
	}
	if infra.Status.LastMessage != primaryFailure.Error() {
		t.Fatalf("last message = %q, want primary failure %q", infra.Status.LastMessage, primaryFailure)
	}
	got := &corev1.Node{}
	if err := client.Get(ctx, types.NamespacedName{Name: node.Name}, got); err != nil {
		t.Fatalf("get refreshed node: %v", err)
	}
	assertWorkflowNodeLabel(t, got, dataplane.NodeDataPlaneReadyLabel, dataplane.NotReadyLabelValue)
	assertWorkflowNodeLabel(t, got, dataplane.NodeNetdReadyLabel, dataplane.NotReadyLabelValue)
	assertWorkflowNodeLabel(t, got, dataplane.NodeCtldReadyLabel, dataplane.NotReadyLabelValue)
}

func TestNodeReadinessRefreshFailureDoesNotMaskWorkflowError(t *testing.T) {
	ctx := context.Background()
	infra := newWorkflowNodeReadinessInfra()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-a",
		Labels: map[string]string{"sandbox0.ai/node-role": "sandbox"},
	}}
	scheme := newWorkflowNodeReadinessScheme(t)
	workflowErr := errors.New("workflow freshness failed")
	refreshErr := errors.New("node label patch failed")
	infraGetCalls := 0
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(infra.DeepCopy(), node).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client ctrlclient.WithWatch, key ctrlclient.ObjectKey, obj ctrlclient.Object, opts ...ctrlclient.GetOption) error {
				if _, ok := obj.(*infrav1alpha1.Sandbox0Infra); ok {
					infraGetCalls++
					if infraGetCalls == 1 {
						return workflowErr
					}
				}
				return client.Get(ctx, key, obj, opts...)
			},
			Patch: func(ctx context.Context, client ctrlclient.WithWatch, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
				if _, ok := obj.(*corev1.Node); ok {
					return refreshErr
				}
				return client.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	reconciler := &Sandbox0InfraReconciler{Client: client, Scheme: scheme}
	readiness := nodereadiness.NewReconciler(common.NewResourceManager(client, scheme, nil, common.LocalDevConfig{}))

	_, err := reconciler.runStepsWithNodeReadinessRefresh(
		ctx,
		infra,
		infraplan.Compile(infra),
		readiness,
		[]reconcileStep{{Name: "ctld", Run: func(context.Context) error { return nil }}},
	)
	if !errors.Is(err, workflowErr) {
		t.Fatalf("error = %v, want original workflow error %v", err, workflowErr)
	}
	if errors.Is(err, refreshErr) {
		t.Fatalf("refresh error %v masked workflow error", refreshErr)
	}
}

func newWorkflowNodeReadinessScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add apps scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := storagev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add storage scheme: %v", err)
	}
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	return scheme
}

func newWorkflowNodeReadinessInfra() *infrav1alpha1.Sandbox0Infra {
	return &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			SandboxNodePlacement: &infrav1alpha1.SandboxNodePlacementConfig{
				NodeSelector: map[string]string{"sandbox0.ai/node-role": "sandbox"},
			},
			Services: &infrav1alpha1.ServicesConfig{
				Manager: &infrav1alpha1.ManagerServiceConfig{WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
					EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
				}},
				Netd: &infrav1alpha1.NetdServiceConfig{EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true}},
			},
		},
	}
}

func assertWorkflowNodeLabel(t *testing.T, node *corev1.Node, key, want string) {
	t.Helper()
	if got := node.Labels[key]; got != want {
		t.Fatalf("node %s label %s = %q, want %q", node.Name, key, got, want)
	}
}
