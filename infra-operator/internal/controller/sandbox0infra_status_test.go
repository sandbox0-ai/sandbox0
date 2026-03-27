package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func newStatusTestReconciler(t *testing.T, infra *infrav1alpha1.Sandbox0Infra, objects ...ctrlclient.Object) (*Sandbox0InfraReconciler, ctrlclient.Client) {
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
		WithStatusSubresource(infra).
		Build()

	return &Sandbox0InfraReconciler{
		Client: client,
		Scheme: scheme,
	}, client
}
