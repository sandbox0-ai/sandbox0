package naming

import (
	"strings"
	"testing"
)

func TestReplicasetAndSandboxNames(t *testing.T) {
	clusterID := "aws-us-east-1"
	templateName := "basic-template"

	rsName, err := ReplicasetName(clusterID, templateName)
	if err != nil {
		t.Fatalf("replicaset name: %v", err)
	}
	if len(rsName) > replicaSetMaxLen {
		t.Fatalf("replicaset name too long: %d", len(rsName))
	}

	sandboxName, err := SandboxName(clusterID, templateName, "abcde")
	if err != nil {
		t.Fatalf("sandbox name: %v", err)
	}
	if len(sandboxName) > sandboxNameMaxLen {
		t.Fatalf("sandbox name too long: %d", len(sandboxName))
	}

	parsed, err := ParseSandboxName(sandboxName)
	if err != nil {
		t.Fatalf("parse sandbox name: %v", err)
	}
	if parsed.ClusterID != clusterID {
		t.Fatalf("expected clusterID %s, got %s", clusterID, parsed.ClusterID)
	}
}

func TestExposureHostLabel(t *testing.T) {
	sandboxName := "rs-mfwha2dbfzzsayjaomxwg2dbon2gc-zd0b8631-abcde"
	label, err := BuildExposureHostLabel(sandboxName, 3000)
	if err != nil {
		t.Fatalf("BuildExposureHostLabel: %v", err)
	}

	gotSandbox, gotPort, err := ParseExposureHostLabel(label)
	if err != nil {
		t.Fatalf("ParseExposureHostLabel: %v", err)
	}
	if gotSandbox != sandboxName {
		t.Fatalf("expected sandbox %q, got %q", sandboxName, gotSandbox)
	}
	if gotPort != 3000 {
		t.Fatalf("expected port %d, got %d", 3000, gotPort)
	}
}

func TestExposureHostLabelRejectsInvalid(t *testing.T) {
	if _, err := BuildExposureHostLabel("bad_name", 3000); err == nil {
		t.Fatalf("expected invalid sandboxName error")
	}
	if _, err := BuildExposureHostLabel("rs-valid-name", 0); err == nil {
		t.Fatalf("expected invalid port error")
	}
	if _, _, err := ParseExposureHostLabel("rs-valid-name-p3000"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestTemplateNameForCluster(t *testing.T) {
	name := TemplateNameForCluster(ScopeTeam, "team-123", "my-template-name")
	if name == "" {
		t.Fatalf("expected template name to be non-empty")
	}
	if len(name) > dnsLabelMaxLen {
		t.Fatalf("template name too long: %d", len(name))
	}
	if err := validateDNSLabel(name); err != nil {
		t.Fatalf("template name invalid: %v", err)
	}
}

func TestSlugWithHashTruncates(t *testing.T) {
	input := "This-Is-A-Very-Long-Template-Name-With-Invalid---Chars"
	name, err := slugWithHash(input, 20)
	if err != nil {
		t.Fatalf("slugWithHash: %v", err)
	}
	if len(name) > 20 {
		t.Fatalf("expected length <= 20, got %d", len(name))
	}
	if err := validateDNSLabel(name); err != nil {
		t.Fatalf("generated name invalid: %v", err)
	}
}

func TestClusterIDFromName(t *testing.T) {
	clusterID, err := ClusterIDFromName("My Cluster East 1")
	if err != nil {
		t.Fatalf("ClusterIDFromName: %v", err)
	}
	if len(clusterID) > clusterIDMaxLen {
		t.Fatalf("cluster_id too long: %d", len(clusterID))
	}
	if err := validateDNSLabel(clusterID); err != nil {
		t.Fatalf("cluster_id invalid: %v", err)
	}
}

func TestValidateClusterID(t *testing.T) {
	valid := []string{
		"default",
		"aws-us-east-1",
		"cluster-a",
		strings.Repeat("a", ClusterIDMaxLen),
	}
	for _, clusterID := range valid {
		if err := ValidateClusterID(clusterID); err != nil {
			t.Fatalf("expected clusterID %q to be valid: %v", clusterID, err)
		}
	}

	invalid := []string{
		"",
		"Sandbox0-GCP-USE4-GKE",
		"bad_name",
		"-starts-with-dash",
		"ends-with-dash-",
		strings.Repeat("a", ClusterIDMaxLen+1),
	}
	for _, clusterID := range invalid {
		if err := ValidateClusterID(clusterID); err == nil {
			t.Fatalf("expected clusterID %q to be invalid", clusterID)
		}
	}
}

func TestTemplateNamespaceForBuiltin(t *testing.T) {
	namespace, err := TemplateNamespaceForBuiltin("My Template ID")
	if err != nil {
		t.Fatalf("TemplateNamespaceForBuiltin: %v", err)
	}
	if len(namespace) > dnsLabelMaxLen {
		t.Fatalf("namespace too long: %d", len(namespace))
	}
	if err := validateDNSLabel(namespace); err != nil {
		t.Fatalf("namespace invalid: %v", err)
	}
}

func TestTemplateNamespaceForTeam(t *testing.T) {
	namespace, err := TemplateNamespaceForTeam("team-123")
	if err != nil {
		t.Fatalf("TemplateNamespaceForTeam: %v", err)
	}
	if len(namespace) > dnsLabelMaxLen {
		t.Fatalf("namespace too long: %d", len(namespace))
	}
	if err := validateDNSLabel(namespace); err != nil {
		t.Fatalf("namespace invalid: %v", err)
	}
}

func TestCanonicalTemplateID(t *testing.T) {
	templateID, err := CanonicalTemplateID("My-Template-ID")
	if err != nil {
		t.Fatalf("CanonicalTemplateID: %v", err)
	}
	if templateID != "my-template-id" {
		t.Fatalf("expected lowercase template_id, got %s", templateID)
	}
}
