package framework

import (
	"errors"
	"reflect"
	"testing"
)

func TestCleanupManagerNamespacesBeforeStoppingManagerPreservesStorageUntilCleanupCompletes(t *testing.T) {
	var calls []string

	namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
		func() error {
			calls = append(calls, "quiesce-manager")
			return nil
		},
		func() error {
			calls = append(calls, "cleanup-namespaces")
			return nil
		},
		func() error {
			calls = append(calls, "stop-manager")
			return nil
		},
	)

	if namespaceCleanupErr != nil {
		t.Fatalf("namespace cleanup returned error: %v", namespaceCleanupErr)
	}
	if managerStopErr != nil {
		t.Fatalf("manager stop returned error: %v", managerStopErr)
	}
	wantCalls := []string{"quiesce-manager", "cleanup-namespaces", "stop-manager"}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("cleanup order = %v, want %v", calls, wantCalls)
	}
}

func TestCleanupManagerNamespacesBeforeStoppingManagerKeepsManagerRunningOnCleanupFailure(t *testing.T) {
	wantErr := errors.New("namespace still terminating")
	managerStopped := false

	namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
		func() error { return nil },
		func() error { return wantErr },
		func() error {
			managerStopped = true
			return nil
		},
	)

	if !errors.Is(namespaceCleanupErr, wantErr) {
		t.Fatalf("namespace cleanup error = %v, want %v", namespaceCleanupErr, wantErr)
	}
	if managerStopErr != nil {
		t.Fatalf("manager stop returned error: %v", managerStopErr)
	}
	if managerStopped {
		t.Fatal("manager was stopped while namespace cleanup still needed its embedded storage API")
	}
}

func TestCleanupManagerNamespacesBeforeStoppingManagerReportsStopFailureAfterCleanup(t *testing.T) {
	wantErr := errors.New("scale failed")

	namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
		func() error { return nil },
		func() error { return nil },
		func() error { return wantErr },
	)

	if namespaceCleanupErr != nil {
		t.Fatalf("namespace cleanup returned error: %v", namespaceCleanupErr)
	}
	if !errors.Is(managerStopErr, wantErr) {
		t.Fatalf("manager stop error = %v, want %v", managerStopErr, wantErr)
	}
}

func TestCleanupManagerNamespacesBeforeStoppingManagerDoesNotDeleteWhenQuiesceFails(t *testing.T) {
	wantErr := errors.New("signal failed")
	cleanupCalled := false
	stopCalled := false

	namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
		func() error { return wantErr },
		func() error {
			cleanupCalled = true
			return nil
		},
		func() error {
			stopCalled = true
			return nil
		},
	)

	if !errors.Is(namespaceCleanupErr, wantErr) {
		t.Fatalf("namespace cleanup error = %v, want wrapped %v", namespaceCleanupErr, wantErr)
	}
	if managerStopErr != nil {
		t.Fatalf("manager stop error = %v", managerStopErr)
	}
	if cleanupCalled || stopCalled {
		t.Fatalf("cleanup called = %t, stop called = %t", cleanupCalled, stopCalled)
	}
}
