package config

import (
	"encoding/json"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDurationScalarRoundTrip(t *testing.T) {
	type document struct {
		Timeout Duration `yaml:"timeout" json:"timeout"`
	}

	var fromYAML document
	if err := yaml.Unmarshal([]byte("timeout: 750ms\n"), &fromYAML); err != nil {
		t.Fatalf("unmarshal YAML duration: %v", err)
	}
	if fromYAML.Timeout.Duration != 750*time.Millisecond {
		t.Fatalf("YAML duration = %s, want 750ms", fromYAML.Timeout.Duration)
	}

	encodedYAML, err := yaml.Marshal(fromYAML)
	if err != nil {
		t.Fatalf("marshal YAML duration: %v", err)
	}
	if string(encodedYAML) != "timeout: 750ms\n" {
		t.Fatalf("YAML = %q, want scalar duration", encodedYAML)
	}

	encodedJSON, err := json.Marshal(fromYAML)
	if err != nil {
		t.Fatalf("marshal JSON duration: %v", err)
	}
	if string(encodedJSON) != `{"timeout":"750ms"}` {
		t.Fatalf("JSON = %s, want string duration", encodedJSON)
	}

	var fromJSON document
	if err := json.Unmarshal(encodedJSON, &fromJSON); err != nil {
		t.Fatalf("unmarshal JSON duration: %v", err)
	}
	if fromJSON.Timeout.Duration != fromYAML.Timeout.Duration {
		t.Fatalf("JSON duration = %s, want %s", fromJSON.Timeout.Duration, fromYAML.Timeout.Duration)
	}
}

func TestDurationRejectsNonScalarYAML(t *testing.T) {
	var target struct {
		Timeout Duration `yaml:"timeout"`
	}
	if err := yaml.Unmarshal([]byte("timeout:\n  duration: 1s\n"), &target); err == nil {
		t.Fatal("legacy Kubernetes duration object was accepted")
	}
}
