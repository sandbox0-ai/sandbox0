// Package naming owns manager-specific Kubernetes resource names.
package naming

import (
	"fmt"

	sharednaming "github.com/sandbox0-ai/sandbox0/pkg/naming"
)

// ProcdConfigSecretName generates the manager-owned Secret name for a
// template's procd configuration.
// Format: procd-secret-<clusterKey>-<templateKey>.
func ProcdConfigSecretName(clusterID, templateName string) (string, error) {
	clusterKey, err := sharednaming.ClusterKey(clusterID)
	if err != nil {
		return "", err
	}
	base := fmt.Sprintf("procd-secret-%s-%s", clusterKey, templateName)
	return sharednaming.DNSLabelWithHash(base, sharednaming.DNSLabelMaxLen)
}

// ReplicaSetName generates the manager-owned ReplicaSet name for a template.
func ReplicaSetName(clusterID, templateName string) (string, error) {
	workloadName, err := sharednaming.NewSandboxWorkloadName(clusterID, templateName)
	if err != nil {
		return "", err
	}
	return workloadName.ReplicaSetName(), nil
}
