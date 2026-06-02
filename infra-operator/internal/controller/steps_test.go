/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	infraplan "github.com/sandbox0-ai/sandbox0/infra-operator/internal/plan"
)

func TestRunStepsSetsConditions(t *testing.T) {
	ctx := context.Background()
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{}

	steps := []reconcileStep{
		{
			Name:           "database",
			Run:            func(context.Context) error { return nil },
			ConditionType:  infrav1alpha1.ConditionTypeDatabaseReady,
			SuccessReason:  "DatabaseReady",
			SuccessMessage: "Database is ready",
			ErrorReason:    "DatabaseFailed",
		},
	}

	result, err := reconciler.runSteps(ctx, infra, steps)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.RequeueAfter != requeueInterval {
		t.Fatalf("expected requeue interval %v, got %v", requeueInterval, result.RequeueAfter)
	}

	condition := meta.FindStatusCondition(infra.Status.Conditions, infrav1alpha1.ConditionTypeDatabaseReady)
	if condition == nil {
		t.Fatalf("expected database condition to be set")
	} else {
		status := condition.Status
		if status != metav1.ConditionTrue {
			t.Fatalf("expected condition status true, got %v", status)
		}
	}
}

func TestRunStepsErrorStopsAndSetsCondition(t *testing.T) {
	ctx := context.Background()
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{}
	stepErr := errors.New("boom")

	steps := []reconcileStep{
		{
			Name:          "internal-auth",
			Run:           func(context.Context) error { return stepErr },
			ConditionType: infrav1alpha1.ConditionTypeInternalAuthReady,
			ErrorReason:   "KeyGenerationFailed",
			ErrorResult:   &ctrl.Result{},
		},
		{
			Name:           "database",
			Run:            func(context.Context) error { return nil },
			ConditionType:  infrav1alpha1.ConditionTypeDatabaseReady,
			SuccessReason:  "DatabaseReady",
			SuccessMessage: "Database is ready",
			ErrorReason:    "DatabaseFailed",
		},
	}

	result, err := reconciler.runSteps(ctx, infra, steps)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("expected explicit zero requeue result, got %v", result.RequeueAfter)
	}

	condition := meta.FindStatusCondition(infra.Status.Conditions, infrav1alpha1.ConditionTypeInternalAuthReady)
	if condition == nil {
		t.Fatalf("expected internal auth condition to be set")
	} else {
		status := condition.Status
		if status != metav1.ConditionFalse {
			t.Fatalf("expected condition status false, got %v", status)
		}
	}
}

func TestIsReadyPod(t *testing.T) {
	readyPod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}
	if !isReadyPod(readyPod) {
		t.Fatalf("expected running ready pod to be reported as ready")
	}

	notReadyPod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}
	if isReadyPod(notReadyPod) {
		t.Fatalf("expected pending pod to be reported as not ready")
	}
}

func TestWaitBuiltinTemplatePodsReadySkipsWhenRootFSPersistenceEnabled(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ManagerConfig{
						RootFSPersistenceEnabled: true,
					},
				},
			},
		},
	}
	reconciler := &Sandbox0InfraReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}

	if err := reconciler.waitBuiltinTemplatePodsReady(ctx, infra, infraplan.Compile(infra)); err != nil {
		t.Fatalf("wait builtin template pods with rootfs persistence enabled: %v", err)
	}
}

func TestWaitBuiltinTemplatePodsReadyRequiresPodsWhenIdlePoolEnabled(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "sandbox0-system"},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Services: &infrav1alpha1.ServicesConfig{
				ClusterGateway: &infrav1alpha1.ClusterGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
					Config: &infrav1alpha1.ClusterGatewayConfig{
						AuthMode: "public",
					},
				},
				Manager: &infrav1alpha1.ManagerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: true},
					},
				},
			},
		},
	}
	reconciler := &Sandbox0InfraReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).Build(),
		Scheme: scheme,
	}

	err := reconciler.waitBuiltinTemplatePodsReady(ctx, infra, infraplan.Compile(infra))
	if err == nil || !strings.Contains(err.Error(), `builtin template "default" pods not ready`) {
		t.Fatalf("expected builtin template pod readiness error, got %v", err)
	}
}
