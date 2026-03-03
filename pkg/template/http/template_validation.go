package http

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
)

func validateTemplateSpecForClaims(spec v1alpha1.SandboxTemplateSpec, claims *internalauth.Claims) error {
	if isPrivilegedTemplateWriter(claims) {
		return nil
	}

	if spec.Pod != nil {
		return fmt.Errorf("spec.pod requires system identity")
	}
	if spec.MainContainer.SecurityContext != nil {
		return fmt.Errorf("spec.mainContainer.securityContext requires system identity")
	}
	if len(spec.Sidecars) > 0 {
		return fmt.Errorf("spec.sidecars requires system identity")
	}
	if spec.RuntimeClassName != nil {
		return fmt.Errorf("spec.runtimeClassName requires system identity")
	}
	if spec.ClusterId != nil {
		return fmt.Errorf("spec.clusterId requires system identity")
	}
	if spec.Public {
		return fmt.Errorf("spec.public=true requires system identity")
	}
	if len(spec.AllowedTeams) > 0 {
		return fmt.Errorf("spec.allowedTeams requires system identity")
	}

	return nil
}

func isPrivilegedTemplateWriter(claims *internalauth.Claims) bool {
	if claims == nil {
		return false
	}
	return claims.IsSystemToken()
}
