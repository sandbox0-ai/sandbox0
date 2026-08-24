package naming

import (
	"strings"
	"testing"
)

func TestOperationSandboxNameIsParseable(t *testing.T) {
	clusterID := "aws-us-east-1"
	sandboxName, err := SandboxNameForOperation(clusterID, "basic-template", "operation-1")
	if err != nil {
		t.Fatalf("SandboxNameForOperation: %v", err)
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

func TestSandboxNameForLongTeamTemplateFitsExposureHostLabel(t *testing.T) {
	sandboxName, err := SandboxNameForOperation(DefaultClusterID, "e2e-fullmode-rc-123456789", "operation-1")
	if err != nil {
		t.Fatalf("SandboxName: %v", err)
	}
	if len(sandboxName) > sandboxNameMaxLen {
		t.Fatalf("sandbox name too long: %d", len(sandboxName))
	}
	if _, err := BuildExposureHostLabel(sandboxName, 65535); err != nil {
		t.Fatalf("BuildExposureHostLabel: %v", err)
	}
}

func TestSandboxNameForOperationIsStableAndRouteable(t *testing.T) {
	first, err := SandboxNameForOperation("aws-us-east-1", "basic-template", "operation-1")
	if err != nil {
		t.Fatal(err)
	}
	retried, err := SandboxNameForOperation("aws-us-east-1", "basic-template", "operation-1")
	if err != nil {
		t.Fatal(err)
	}
	other, err := SandboxNameForOperation("aws-us-east-1", "basic-template", "operation-2")
	if err != nil {
		t.Fatal(err)
	}
	if first != retried || first == other {
		t.Fatalf("operation names = %q, %q, %q", first, retried, other)
	}
	parsed, err := ParseSandboxName(first)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ClusterID != "aws-us-east-1" {
		t.Fatalf("cluster ID = %q", parsed.ClusterID)
	}
	if _, err := BuildExposureHostLabel(first, 49983); err != nil {
		t.Fatalf("build exposure host: %v", err)
	}
}

func TestSandboxNameForOperationSupportsLongestClusterID(t *testing.T) {
	clusterID := strings.Repeat("a", ClusterIDMaxLen)
	name, err := SandboxNameForOperation(clusterID, strings.Repeat("template", 20), "operation-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(name) > sandboxNameMaxLen {
		t.Fatalf("name length = %d, want <= %d", len(name), sandboxNameMaxLen)
	}
	parsed, err := ParseSandboxName(name)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ClusterID != clusterID {
		t.Fatalf("cluster ID = %q", parsed.ClusterID)
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

func TestDNSLabelWithHashTruncates(t *testing.T) {
	input := "This-Is-A-Very-Long-Template-Name-With-Invalid---Chars"
	name, err := DNSLabelWithHash(input, 20)
	if err != nil {
		t.Fatalf("DNSLabelWithHash: %v", err)
	}
	if len(name) > 20 {
		t.Fatalf("expected length <= 20, got %d", len(name))
	}
	if err := validateDNSLabel(name); err != nil {
		t.Fatalf("generated name invalid: %v", err)
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

func TestCanonicalTemplateID(t *testing.T) {
	templateID, err := CanonicalTemplateID("My-Template-ID")
	if err != nil {
		t.Fatalf("CanonicalTemplateID: %v", err)
	}
	if templateID != "my-template-id" {
		t.Fatalf("expected lowercase template_id, got %s", templateID)
	}
}
