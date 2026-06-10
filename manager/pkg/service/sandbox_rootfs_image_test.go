package service

import (
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
)

func TestRegistryCredentialsMatchReference(t *testing.T) {
	ref, err := name.ParseReference("registry.example.com/team/base@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", name.WeakValidation)
	if err != nil {
		t.Fatalf("ParseReference() error = %v", err)
	}

	creds := &registry.Credential{
		PushRegistry: "https://registry.example.com/team-1",
		Username:     "user",
		Password:     "pass",
	}
	if !registryCredentialsMatchReference(creds, ref) {
		t.Fatalf("expected credentials to match base image registry host")
	}
}

func TestRegistryCredentialsMatchReferenceRejectsOtherHost(t *testing.T) {
	ref, err := name.ParseReference("docker.io/library/busybox:1.36", name.WeakValidation)
	if err != nil {
		t.Fatalf("ParseReference() error = %v", err)
	}

	creds := &registry.Credential{
		PushRegistry: "registry.example.com/team-1",
		Username:     "user",
		Password:     "pass",
	}
	if registryCredentialsMatchReference(creds, ref) {
		t.Fatalf("expected credentials not to match a different registry host")
	}
}

func TestRegistryCredentialsMatchReferenceRejectsEmptyAuth(t *testing.T) {
	ref, err := name.ParseReference("registry.example.com/team/base:latest", name.WeakValidation)
	if err != nil {
		t.Fatalf("ParseReference() error = %v", err)
	}

	creds := &registry.Credential{PushRegistry: "registry.example.com/team-1"}
	if registryCredentialsMatchReference(creds, ref) {
		t.Fatalf("expected empty credentials not to match")
	}
}
