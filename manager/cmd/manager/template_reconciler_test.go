package main

import (
	"testing"

	httpserver "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
)

func TestDisabledManagerTemplateReconcilerRemainsNilForConsumers(t *testing.T) {
	reconciler := optionalManagerTemplateReconciler(nil)
	if reconciler != nil {
		t.Fatal("disabled template reconciler must be represented by a nil interface")
	}

	controllers := managerControllerSet{templateReconciler: reconciler}
	if controllers.templateReconciler != nil {
		t.Fatal("disabled template reconciler must remain nil for controller startup")
	}

	var quiescer templateReconcilerQuiescer = reconciler
	if quiescer != nil {
		t.Fatal("disabled template reconciler must remain nil for quiesce signals")
	}

	var httpReconciler httpserver.TemplateReconciler = reconciler
	if httpReconciler != nil {
		t.Fatal("disabled template reconciler must remain nil for HTTP handlers")
	}
}
