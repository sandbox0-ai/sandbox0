package common

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

// ObjectScope carries the minimum metadata and ownership context required for
// reconciling namespaced child resources without exposing the full infra spec.
type ObjectScope struct {
	Name      string
	Namespace string
	owner     *infrav1alpha1.Sandbox0Infra
}

func NewObjectScope(infra *infrav1alpha1.Sandbox0Infra) ObjectScope {
	if infra == nil {
		return ObjectScope{}
	}
	return ObjectScope{
		Name:      infra.Name,
		Namespace: infra.Namespace,
		owner:     infra,
	}
}

func (s ObjectScope) SetControllerReference(obj client.Object, scheme *runtime.Scheme) error {
	if s.owner == nil {
		return fmt.Errorf("object scope owner is required")
	}
	return ctrl.SetControllerReference(s.owner, obj, scheme)
}

func (s ObjectScope) Owner() *infrav1alpha1.Sandbox0Infra {
	return s.owner
}
