package controller

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

func TestOperatorRBACCanDelegateRuntimeClassReadPermission(t *testing.T) {
	manifest, err := os.ReadFile("../../chart/files/clusterrole.yaml")
	require.NoError(t, err)

	role := &rbacv1.ClusterRole{}
	require.NoError(t, yaml.Unmarshal(manifest, role))

	found := false
	for _, rule := range role.Rules {
		if !containsRBACValue(rule.APIGroups, "node.k8s.io") {
			continue
		}
		if !containsRBACValue(rule.Resources, "runtimeclasses") {
			continue
		}
		assert.ElementsMatch(t, []string{"get", "list", "watch"}, rule.Verbs)
		found = true
	}
	assert.True(t, found, "expected operator cluster role to include RuntimeClass read permission")
}

func containsRBACValue(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
