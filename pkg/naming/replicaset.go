package naming

import (
	"fmt"
	"strings"
)

// SandboxWorkloadName contains the stable name components shared by a
// sandbox ReplicaSet and the pods it owns.
//
// Manager owns the resource-specific ReplicaSet facade. This type keeps the
// underlying name construction shared with SandboxName so the two cannot
// drift apart.
type SandboxWorkloadName struct {
	replicaSetName string
}

// NewSandboxWorkloadName derives the shared ReplicaSet and pod name base for
// one cluster-scoped template.
func NewSandboxWorkloadName(clusterID, templateName string) (SandboxWorkloadName, error) {
	clusterKey, err := ClusterKey(clusterID)
	if err != nil {
		return SandboxWorkloadName{}, err
	}
	prefix := fmt.Sprintf("%s-%s-", sandboxNamePrefix, clusterKey)
	remaining := replicaSetMaxLen - len(prefix)
	if remaining <= 0 {
		return SandboxWorkloadName{}, fmt.Errorf("cluster key too long to build replicaset name")
	}
	templateKey, err := slugWithHash(templateName, remaining)
	if err != nil {
		return SandboxWorkloadName{}, err
	}
	name := prefix + templateKey
	if err := validateDNSLabel(name); err != nil {
		return SandboxWorkloadName{}, err
	}
	return SandboxWorkloadName{
		replicaSetName: name,
	}, nil
}

// ReplicaSetName returns the stable ReplicaSet name shared by the sandbox pods.
func (n SandboxWorkloadName) ReplicaSetName() string {
	return n.replicaSetName
}

// SandboxName generates a sandbox (pod) name using the ReplicaSet name and a random suffix.
func SandboxName(clusterID, templateName, randSuffix string) (string, error) {
	if err := validateSandboxRandSuffix(randSuffix); err != nil {
		return "", err
	}
	workloadName, err := NewSandboxWorkloadName(clusterID, templateName)
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("%s-%s", workloadName.ReplicaSetName(), randSuffix)
	if err := validateDNSLabel(name); err != nil {
		return "", err
	}
	if len(name) > sandboxNameMaxLen {
		return "", fmt.Errorf("sandbox name too long for exposure routing (%d > %d)", len(name), sandboxNameMaxLen)
	}
	return name, nil
}

func validateSandboxRandSuffix(randSuffix string) error {
	if randSuffix == "" {
		return fmt.Errorf("randSuffix is empty")
	}
	if strings.Contains(randSuffix, "-") {
		return fmt.Errorf("randSuffix cannot contain hyphens")
	}
	if len(randSuffix) > podRandSuffixLen {
		return fmt.Errorf("randSuffix is too long (%d > %d)", len(randSuffix), podRandSuffixLen)
	}
	return nil
}
