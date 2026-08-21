package credentialbinding

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
)

func TestDigestPublicIsOrderIndependentAndRotationIndependent(t *testing.T) {
	bindings := []v1alpha1.CredentialBinding{
		{Ref: "second", SourceRef: "source-b", Projection: v1alpha1.ProjectionSpec{Type: v1alpha1.CredentialProjectionTypeUsernamePassword, UsernamePassword: &v1alpha1.UsernamePasswordProjection{}}},
		{Ref: "first", SourceRef: "source-a", Projection: v1alpha1.ProjectionSpec{Type: v1alpha1.CredentialProjectionTypeHTTPHeaders, HTTPHeaders: &v1alpha1.HTTPHeadersProjection{Headers: []v1alpha1.ProjectedHeader{{Name: "Authorization", ValueTemplate: "Bearer {{.token}}"}}}}},
	}
	reversed := []v1alpha1.CredentialBinding{bindings[1], bindings[0]}
	if DigestPublic(bindings) != DigestPublic(reversed) {
		t.Fatal("binding digest changed with caller ordering")
	}
	materialized := ToStore(bindings)
	for index := range materialized {
		materialized[index].SourceID = int64(index + 10)
		materialized[index].SourceVersion = int64(index + 20)
	}
	if DigestPublic(bindings) != DigestStore(materialized) {
		t.Fatal("binding digest included materialized source identity")
	}
}

func TestEmptyDigestIsStable(t *testing.T) {
	if got := DigestPublic(nil); got != EmptyDigest {
		t.Fatalf("empty digest = %q, want %q", got, EmptyDigest)
	}
}

func TestConversionDeepCopiesNestedFields(t *testing.T) {
	bindings := []v1alpha1.CredentialBinding{{
		Ref: "ssh", SourceRef: "source",
		Projection: v1alpha1.ProjectionSpec{Type: v1alpha1.CredentialProjectionTypeSSHProxy,
			SSHProxy: &v1alpha1.SSHProxyProjection{SandboxPublicKeys: []string{"key"}, KnownHosts: []string{"host"}}},
	}}
	stored := ToStore(bindings)
	bindings[0].Projection.SSHProxy.SandboxPublicKeys[0] = "changed"
	if stored[0].Projection.SSHProxy.SandboxPublicKeys[0] != "key" {
		t.Fatal("public-to-store conversion aliased nested fields")
	}
	public := FromStore(stored)
	stored[0].Projection.SSHProxy.KnownHosts[0] = "changed"
	if public[0].Projection.SSHProxy.KnownHosts[0] != "host" {
		t.Fatal("store-to-public conversion aliased nested fields")
	}
}
