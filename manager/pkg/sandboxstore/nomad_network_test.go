package sandboxstore

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestNormalizeBeginNomadSandboxNetworkMutationRejectsEmbeddedBindings(t *testing.T) {
	desiredPolicy, err := v1alpha1.NetworkPolicyToAnnotation(&v1alpha1.NetworkPolicySpec{
		Version: "v1", SandboxID: "sandbox", TeamID: "team",
		Mode: v1alpha1.NetworkModeAllowAll,
	})
	if err != nil {
		t.Fatalf("serialize desired policy: %v", err)
	}
	_, _, err = normalizeBeginNomadSandboxNetworkMutationRequest(
		&BeginNomadSandboxNetworkMutationRequest{
			SandboxID: "sandbox", OperationID: "operation", ExpectedTeamID: "team",
			ExpectedCurrentPolicyDigest: protocol.NetworkPolicyDigest("current"),
			DesiredPolicy:               desiredPolicy, DesiredPolicyDigest: protocol.NetworkPolicyDigest(desiredPolicy),
			RequestPolicy: &v1alpha1.SandboxNetworkPolicy{
				Mode: v1alpha1.NetworkModeAllowAll,
				CredentialBindings: []v1alpha1.CredentialBinding{{
					Ref: "api", SourceRef: "source",
				}},
			},
			CredentialBindingDigest: credentialbinding.EmptyDigest,
		},
	)
	if err == nil {
		t.Fatal("embedded credential bindings were accepted as durable request config")
	}
}
