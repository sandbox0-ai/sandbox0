package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestUpdateOverallStatusMarksReadyAndCompletesOperation(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "demo",
			Namespace:  "sandbox0-system",
			Generation: 3,
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase: infrav1alpha1.PhaseInstalling,
			LastOperation: &infrav1alpha1.LastOperation{
				Type:      "Install",
				Status:    "InProgress",
				StartedAt: &metav1.Time{Time: metav1.Now().Time},
			},
			Conditions: []metav1.Condition{
				{
					Type:    infrav1alpha1.ConditionTypeDatabaseReady,
					Status:  metav1.ConditionTrue,
					Reason:  "DatabaseReady",
					Message: "Database is ready",
				},
			},
		},
	}

	reconciler, client := newStatusTestReconciler(t, infra)
	if err := reconciler.updateOverallStatus(context.Background(), infra); err != nil {
		t.Fatalf("update overall status: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(context.Background(), ctrlclient.ObjectKeyFromObject(infra), stored); err != nil {
		t.Fatalf("get updated infra: %v", err)
	}

	if stored.Status.Phase != infrav1alpha1.PhaseReady {
		t.Fatalf("expected phase %q, got %q", infrav1alpha1.PhaseReady, stored.Status.Phase)
	}
	if stored.Status.Progress != "1/1" {
		t.Fatalf("expected progress 1/1, got %q", stored.Status.Progress)
	}
	if stored.Status.LastMessage != "All services are healthy" {
		t.Fatalf("expected healthy status message, got %q", stored.Status.LastMessage)
	}
	if stored.Status.LastOperation == nil || stored.Status.LastOperation.Status != "Succeeded" {
		t.Fatalf("expected last operation to succeed, got %#v", stored.Status.LastOperation)
	}
	if stored.Status.LastOperation.CompletedAt == nil {
		t.Fatal("expected completedAt to be populated")
	}
	readyCondition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeReady)
	if readyCondition == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if readyCondition.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready condition true, got %s", readyCondition.Status)
	}
}

func TestUpdateOverallStatusMarksDegradedAndTracksRetainedResources(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "sandbox0-system",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				Builtin: &infrav1alpha1.BuiltinDatabaseConfig{
					StatefulResourcePolicy: infrav1alpha1.BuiltinStatefulResourcePolicyRetain,
				},
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase: infrav1alpha1.PhaseReady,
			Cluster: &infrav1alpha1.ClusterStatus{
				ID:         "stale-cluster",
				Registered: true,
				RegisteredAt: &metav1.Time{
					Time: metav1.Now().Time,
				},
			},
			Conditions: []metav1.Condition{
				{
					Type:    infrav1alpha1.ConditionTypeDatabaseReady,
					Status:  metav1.ConditionFalse,
					Reason:  "DatabaseFailed",
					Message: "database unreachable",
				},
			},
		},
	}

	reconciler, client := newStatusTestReconciler(t, infra,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "demo-sandbox0-database-credentials", Namespace: "sandbox0-system"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "demo-postgres-data", Namespace: "sandbox0-system"}},
	)
	if err := reconciler.updateOverallStatus(context.Background(), infra); err != nil {
		t.Fatalf("update overall status: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(context.Background(), ctrlclient.ObjectKeyFromObject(infra), stored); err != nil {
		t.Fatalf("get updated infra: %v", err)
	}

	if stored.Status.Phase != infrav1alpha1.PhaseDegraded {
		t.Fatalf("expected phase %q, got %q", infrav1alpha1.PhaseDegraded, stored.Status.Phase)
	}
	if stored.Status.Progress != "0/1" {
		t.Fatalf("expected progress 0/1, got %q", stored.Status.Progress)
	}
	if stored.Status.LastMessage != "database unreachable" {
		t.Fatalf("expected failing status message, got %q", stored.Status.LastMessage)
	}
	if stored.Status.Cluster != nil {
		t.Fatalf("expected stale cluster status to be cleared, got %#v", stored.Status.Cluster)
	}
	if len(stored.Status.RetainedResources) != 2 {
		t.Fatalf("expected 2 retained resources, got %#v", stored.Status.RetainedResources)
	}
	readyCondition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeReady)
	if readyCondition == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if readyCondition.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready condition false, got %s", readyCondition.Status)
	}
}

func TestUpdateOverallStatusPrunesDisabledServiceStatusProjection(t *testing.T) {
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "demo",
			Namespace:  "sandbox0-system",
			Generation: 7,
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalGateway: &infrav1alpha1.GlobalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
					},
				},
				RegionalGateway: &infrav1alpha1.RegionalGatewayServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
					},
				},
				Scheduler: &infrav1alpha1.SchedulerServiceConfig{
					WorkloadServiceConfig: infrav1alpha1.WorkloadServiceConfig{
						EnabledServiceConfig: infrav1alpha1.EnabledServiceConfig{Enabled: false},
					},
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase: infrav1alpha1.PhaseDegraded,
			Endpoints: &infrav1alpha1.EndpointsStatus{
				GlobalGateway:           "http://demo-global-gateway:19083",
				RegionalGateway:         "https://edge.example.com",
				RegionalGatewayInternal: "http://demo-regional-gateway:8080",
			},
			Conditions: []metav1.Condition{
				{
					Type:               infrav1alpha1.ConditionTypeDatabaseReady,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: 7,
					Reason:             "DatabaseReady",
					Message:            "Database is ready",
				},
				{
					Type:               infrav1alpha1.ConditionTypeGlobalGatewayReady,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: 6,
					Reason:             "GlobalGatewayReady",
					Message:            "Global gateway is ready",
				},
				{
					Type:               infrav1alpha1.ConditionTypeRegionalGatewayReady,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: 6,
					Reason:             "RegionalGatewayReady",
					Message:            "Regional gateway is ready",
				},
				{
					Type:               infrav1alpha1.ConditionTypeSchedulerReady,
					Status:             metav1.ConditionFalse,
					ObservedGeneration: 6,
					Reason:             "SchedulerFailed",
					Message:            "license secret missing",
				},
			},
		},
	}

	reconciler, client := newStatusTestReconciler(t, infra)
	if err := reconciler.updateOverallStatus(context.Background(), infra); err != nil {
		t.Fatalf("update overall status: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(context.Background(), ctrlclient.ObjectKeyFromObject(infra), stored); err != nil {
		t.Fatalf("get updated infra: %v", err)
	}

	if stored.Status.Phase != infrav1alpha1.PhaseReady {
		t.Fatalf("expected phase %q, got %q", infrav1alpha1.PhaseReady, stored.Status.Phase)
	}
	if stored.Status.Progress != "1/1" {
		t.Fatalf("expected progress 1/1 after pruning disabled services, got %q", stored.Status.Progress)
	}
	if stored.Status.Endpoints != nil {
		t.Fatalf("expected disabled service endpoints to be cleared, got %#v", stored.Status.Endpoints)
	}
	if condition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeGlobalGatewayReady); condition != nil {
		t.Fatalf("expected GlobalGatewayReady to be pruned, got %#v", condition)
	}
	if condition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeRegionalGatewayReady); condition != nil {
		t.Fatalf("expected RegionalGatewayReady to be pruned, got %#v", condition)
	}
	if condition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeSchedulerReady); condition != nil {
		t.Fatalf("expected SchedulerReady to be pruned, got %#v", condition)
	}
	readyCondition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeReady)
	if readyCondition == nil || readyCondition.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=true after pruning disabled services, got %#v", readyCondition)
	}
}

func TestUpdateOverallStatusUsesLatestObjectWhenInputIsStale(t *testing.T) {
	ctx := context.Background()
	infra := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "demo",
			Namespace:  "sandbox0-system",
			Generation: 3,
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase: infrav1alpha1.PhaseInstalling,
			LastOperation: &infrav1alpha1.LastOperation{
				Type:      "Install",
				Status:    "InProgress",
				StartedAt: &metav1.Time{Time: metav1.Now().Time},
			},
			Conditions: []metav1.Condition{
				{
					Type:    infrav1alpha1.ConditionTypeDatabaseReady,
					Status:  metav1.ConditionTrue,
					Reason:  "DatabaseReady",
					Message: "Database is ready",
				},
			},
		},
	}

	reconciler, client := newStatusTestReconciler(t, infra)
	stale := infra.DeepCopy()

	live := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(infra), live); err != nil {
		t.Fatalf("get live infra: %v", err)
	}
	live.Generation = 4
	live.Spec.InitUser = &infrav1alpha1.InitUserConfig{Email: "admin@example.com"}
	if err := client.Update(ctx, live); err != nil {
		t.Fatalf("update live infra: %v", err)
	}

	stale.Generation = 4
	stale.ResourceVersion = live.ResourceVersion
	if err := reconciler.updateOverallStatus(ctx, stale); err != nil {
		t.Fatalf("update overall status with stale object: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(live), stored); err != nil {
		t.Fatalf("get updated infra: %v", err)
	}

	if stored.Status.Phase != infrav1alpha1.PhaseReady {
		t.Fatalf("expected phase %q, got %q", infrav1alpha1.PhaseReady, stored.Status.Phase)
	}
	readyCondition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeReady)
	if readyCondition == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if readyCondition.ObservedGeneration != 4 {
		t.Fatalf("expected Ready observedGeneration 4, got %d", readyCondition.ObservedGeneration)
	}
}

func TestUpdateOverallStatusDoesNotOverwriteNewerGenerationStatus(t *testing.T) {
	ctx := context.Background()
	live := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "demo",
			Namespace:  "sandbox0-system",
			Generation: 4,
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase:       infrav1alpha1.PhaseDegraded,
			LastMessage: "newer generation wins",
			Conditions: []metav1.Condition{
				{
					Type:               infrav1alpha1.ConditionTypeDatabaseReady,
					Status:             metav1.ConditionFalse,
					ObservedGeneration: 4,
					Reason:             "DatabaseFailed",
					Message:            "new generation status",
				},
			},
		},
	}

	stale := live.DeepCopy()
	stale.Generation = 3
	stale.Status.Phase = infrav1alpha1.PhaseReady
	stale.Status.LastMessage = "old generation"
	stale.Status.Conditions = []metav1.Condition{
		{
			Type:               infrav1alpha1.ConditionTypeDatabaseReady,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: 3,
			Reason:             "DatabaseReady",
			Message:            "Database is ready",
		},
	}

	reconciler, client := newStatusTestReconcilerWithInterceptors(t, live, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, client ctrlclient.Client, subResourceName string, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
			if subResourceName == "status" {
				return fmt.Errorf("unexpected status patch for stale generation")
			}
			return client.Status().Patch(ctx, obj, patch, opts...)
		},
	})

	if err := reconciler.updateOverallStatus(ctx, stale); err != nil {
		t.Fatalf("update overall status with stale generation: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(live), stored); err != nil {
		t.Fatalf("get stored infra: %v", err)
	}

	if stored.Status.LastMessage != "newer generation wins" {
		t.Fatalf("expected newer generation status to be preserved, got %q", stored.Status.LastMessage)
	}
	condition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeDatabaseReady)
	if condition == nil {
		t.Fatal("expected database condition to be present")
	}
	if condition.ObservedGeneration != 4 {
		t.Fatalf("expected observedGeneration 4 to be preserved, got %d", condition.ObservedGeneration)
	}
}

func TestUpdateOverallStatusAllowsSameGenerationStatusToConverge(t *testing.T) {
	ctx := context.Background()
	live := &infrav1alpha1.Sandbox0Infra{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "demo",
			Namespace:       "sandbox0-system",
			Generation:      4,
			ResourceVersion: "2",
		},
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Port:     5432,
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
		},
		Status: infrav1alpha1.Sandbox0InfraStatus{
			Phase:       infrav1alpha1.PhaseInstalling,
			LastMessage: "installing",
			Conditions: []metav1.Condition{
				{
					Type:               infrav1alpha1.ConditionTypeDatabaseReady,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: 4,
					Reason:             "DatabaseReady",
					Message:            "Database is ready",
				},
			},
		},
	}

	stale := live.DeepCopy()
	stale.ResourceVersion = "1"
	stale.Status.LastOperation = &infrav1alpha1.LastOperation{
		Type:      "Install",
		Status:    "InProgress",
		StartedAt: &metav1.Time{Time: metav1.Now().Time},
	}

	reconciler, client := newStatusTestReconcilerWithInterceptors(t, live, interceptor.Funcs{})

	if err := reconciler.updateOverallStatus(ctx, stale); err != nil {
		t.Fatalf("update overall status with same-generation stale resource version: %v", err)
	}

	stored := &infrav1alpha1.Sandbox0Infra{}
	if err := client.Get(ctx, ctrlclient.ObjectKeyFromObject(live), stored); err != nil {
		t.Fatalf("get stored infra: %v", err)
	}

	if stored.Status.Phase != infrav1alpha1.PhaseReady {
		t.Fatalf("expected phase %q, got %q", infrav1alpha1.PhaseReady, stored.Status.Phase)
	}
	if stored.Status.LastMessage != "All services are healthy" {
		t.Fatalf("expected converged healthy status, got %q", stored.Status.LastMessage)
	}
	readyCondition := findCondition(stored.Status.Conditions, infrav1alpha1.ConditionTypeReady)
	if readyCondition == nil {
		t.Fatal("expected Ready condition to be present")
	}
	if readyCondition.ObservedGeneration != 4 {
		t.Fatalf("expected Ready observedGeneration 4, got %d", readyCondition.ObservedGeneration)
	}
}

func newStatusTestReconciler(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, objects ...ctrlclient.Object) (*Sandbox0InfraReconciler, ctrlclient.Client) {
	t.Helper()
	return newStatusTestReconcilerWithInterceptors(t, infra, interceptor.Funcs{}, objects...)
}

func newStatusTestReconcilerWithInterceptors(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, interceptors interceptor.Funcs, objects ...ctrlclient.Object) (*Sandbox0InfraReconciler, ctrlclient.Client) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := infrav1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add infra scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}

	allObjects := append([]ctrlclient.Object{infra}, objects...)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(allObjects...).
		WithInterceptorFuncs(interceptors).
		WithStatusSubresource(infra).
		Build()

	return &Sandbox0InfraReconciler{
		Client: client,
		Scheme: scheme,
	}, client
}
