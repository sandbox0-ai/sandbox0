package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestRuntimeCompatibilityDigestKeepsVersionOneWireContract(t *testing.T) {
	profile := RuntimeCompatibility{
		Version: RuntimeCompatibilityVersion, Architecture: "amd64",
		DriverVersion: "0.1.0", RunscVersion: "runsc version release-20260820.0",
		Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true,
		Command: "/procd", ProcdPort: NomadProcdPort, RuntimeMode: runtimecontrol.ControlModeStatic,
		CPUPeriod: 100000, CPUQuota: 100000, CPUShares: 1024, MemoryLimitBytes: 1 << 30,
	}

	got, err := profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
}

func TestRuntimeCompatibilityRejectsAmbiguousProfiles(t *testing.T) {
	valid := RuntimeCompatibility{
		Version: RuntimeCompatibilityVersion, Architecture: "amd64",
		DriverVersion: "0.1.0", RunscVersion: "runsc-1", Platform: "systrap",
		Overlay2: "none", FileAccess: "shared", Command: "/procd",
		ProcdPort: NomadProcdPort, RuntimeMode: runtimecontrol.ControlModeStatic,
	}
	tests := map[string]func(*RuntimeCompatibility){
		"wrong version":         func(profile *RuntimeCompatibility) { profile.Version++ },
		"spaced architecture":   func(profile *RuntimeCompatibility) { profile.Architecture = " amd64" },
		"missing runsc version": func(profile *RuntimeCompatibility) { profile.RunscVersion = "" },
		"wrong command":         func(profile *RuntimeCompatibility) { profile.Command = "/bin/sh" },
		"wrong port":            func(profile *RuntimeCompatibility) { profile.ProcdPort++ },
		"wrong mode":            func(profile *RuntimeCompatibility) { profile.RuntimeMode = "watch" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			profile := valid
			mutate(&profile)
			if _, err := profile.Digest(); err == nil {
				t.Fatal("invalid compatibility profile was accepted")
			}
		})
	}
}
