package controller

import (
	"testing"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestExpectedConditionTypesIncludesGlobalDirectory(t *testing.T) {
	reconciler := &Sandbox0InfraReconciler{}
	infra := &infrav1alpha1.Sandbox0Infra{
		Spec: infrav1alpha1.Sandbox0InfraSpec{
			Database: &infrav1alpha1.DatabaseConfig{
				Type: infrav1alpha1.DatabaseTypeExternal,
				External: &infrav1alpha1.ExternalDatabaseConfig{
					Host:     "db.example.com",
					Database: "sandbox0",
					Username: "sandbox0",
				},
			},
			Services: &infrav1alpha1.ServicesConfig{
				GlobalDirectory: &infrav1alpha1.GlobalDirectoryServiceConfig{
					BaseServiceConfig: infrav1alpha1.BaseServiceConfig{
						Enabled: true,
					},
				},
			},
		},
	}

	conditions := reconciler.expectedConditionTypes(infra)
	if len(conditions) != 2 {
		t.Fatalf("expected 2 conditions, got %d: %#v", len(conditions), conditions)
	}
	if conditions[0] != infrav1alpha1.ConditionTypeDatabaseReady {
		t.Fatalf("expected database condition first, got %q", conditions[0])
	}
	if conditions[1] != infrav1alpha1.ConditionTypeGlobalDirectoryReady {
		t.Fatalf("expected global-directory condition second, got %q", conditions[1])
	}
}
