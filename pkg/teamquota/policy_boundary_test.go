package teamquota

import (
	"reflect"
	"testing"
)

func TestRepositoryIsReadOnlyAtPolicyInterfaceBoundary(t *testing.T) {
	var repository any = (*Repository)(nil)
	if _, ok := repository.(PolicyReader); !ok {
		t.Fatal("*Repository must implement PolicyReader")
	}
	if _, ok := repository.(PolicyManager); ok {
		t.Fatal("*Repository must not implement PolicyManager")
	}

	repositoryType := reflect.TypeOf((*Repository)(nil))
	for _, method := range []string{
		"PutTeamPolicy",
		"DeleteTeamPolicy",
		"ReplaceDefaultPolicies",
		"ReplaceDefaultPoliciesVersioned",
		"PutTeamPolicyTx",
		"DeleteTeamPolicyTx",
		"ReplaceDefaultPoliciesTx",
	} {
		if _, ok := repositoryType.MethodByName(method); ok {
			t.Errorf("*Repository unexpectedly exposes policy writer method %s", method)
		}
	}
}

func TestPolicyCoordinatorOwnsPolicyManagerBoundary(t *testing.T) {
	var coordinator any = (*PolicyCoordinator)(nil)
	if _, ok := coordinator.(PolicyManager); !ok {
		t.Fatal("*PolicyCoordinator must implement PolicyManager")
	}
}
