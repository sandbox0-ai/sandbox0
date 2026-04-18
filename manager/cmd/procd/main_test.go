package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

func TestResolveCtldBaseURLUsesExplicitBaseURL(t *testing.T) {
	got := resolveCtldBaseURL(&config.ProcdConfig{
		CtldBaseURL: "http://ctld.local:8095",
		NodeHostIP:  "10.0.0.1",
		CtldPort:    8095,
	})
	if got != "http://ctld.local:8095" {
		t.Fatalf("resolveCtldBaseURL() = %q", got)
	}
}

func TestResolveCtldBaseURLBuildsFromNodeHostIP(t *testing.T) {
	got := resolveCtldBaseURL(&config.ProcdConfig{
		NodeHostIP: "10.0.0.1",
		CtldPort:   8095,
	})
	if got != "http://10.0.0.1:8095" {
		t.Fatalf("resolveCtldBaseURL() = %q", got)
	}
}

func TestResolveCtldBaseURLSupportsIPv6(t *testing.T) {
	got := resolveCtldBaseURL(&config.ProcdConfig{
		NodeHostIP: "fd00::1",
		CtldPort:   8095,
	})
	if got != "http://[fd00::1]:8095" {
		t.Fatalf("resolveCtldBaseURL() = %q", got)
	}
}
