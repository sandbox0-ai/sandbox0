package naming

import (
	"strings"
	"testing"

	sharednaming "github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestManagerResourceNamesUseSharedSandboxWorkloadName(t *testing.T) {
	const (
		clusterID    = "default"
		templateName = "template-a"
		randSuffix   = "abcde"
	)

	replicaSetName, err := ReplicaSetName(clusterID, templateName)
	if err != nil {
		t.Fatalf("ReplicaSetName: %v", err)
	}
	if want := "rs-mrswmylvnr2a-template-a"; replicaSetName != want {
		t.Fatalf("ReplicaSetName = %q, want %q", replicaSetName, want)
	}
	sandboxName, err := sharednaming.SandboxName(clusterID, templateName, randSuffix)
	if err != nil {
		t.Fatalf("SandboxName: %v", err)
	}
	if want := strings.TrimSuffix(sandboxName, "-"+randSuffix); replicaSetName != want {
		t.Fatalf("ReplicaSetName = %q, want shared sandbox base %q", replicaSetName, want)
	}

	secretName, err := ProcdConfigSecretName(clusterID, templateName)
	if err != nil {
		t.Fatalf("ProcdConfigSecretName: %v", err)
	}
	if want := "procd-secret-mrswmylvnr2a-template-a"; secretName != want {
		t.Fatalf("ProcdConfigSecretName = %q, want %q", secretName, want)
	}
}
