package rootfsimporter

import "github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"

// ValidateMappingGroupPolicy binds the delivery policy to new format-two image
// imports. Empty preserves old operation and attestation identities exactly.
func ValidateMappingGroupPolicy(policy string, formatGeneration int) error {
	if policy == "" {
		return nil
	}
	_, err := NormalizeBlockOptions(formatGeneration, rootfsblock.BuildOptions{MappingGroupPolicy: policy})
	return err
}
