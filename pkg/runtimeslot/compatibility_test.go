package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestRuntimeCompatibilityDigestKeepsResourceNeutralVersionTwoWireContract(t *testing.T) {
	runtimeClass := RuntimeCompatibility{
		Version: RuntimeCompatibilityVersion, Architecture: "amd64",
		DriverVersion: "0.1.0", RunscVersion: "runsc version release-20260820.0",
		Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true,
		Command: "/procd", ProcdPort: NomadProcdPort, RuntimeMode: runtimecontrol.ControlModeStatic,
		SecurityClass: "standard",
	}

	got, err := runtimeClass.Digest()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(runtimeClass)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("digest = %q, want %q", got, want)
	}
}

func TestRuntimeCompatibilityRejectsAmbiguousClasses(t *testing.T) {
	valid := RuntimeCompatibility{
		Version: RuntimeCompatibilityVersion, Architecture: "amd64",
		DriverVersion: "0.1.0", RunscVersion: "runsc-1", Platform: "systrap",
		Overlay2: "none", FileAccess: "shared", Command: "/procd",
		ProcdPort: NomadProcdPort, RuntimeMode: runtimecontrol.ControlModeStatic,
		SecurityClass: "standard",
	}
	tests := map[string]func(*RuntimeCompatibility){
		"wrong version":          func(runtimeClass *RuntimeCompatibility) { runtimeClass.Version++ },
		"spaced architecture":    func(runtimeClass *RuntimeCompatibility) { runtimeClass.Architecture = " amd64" },
		"missing runsc version":  func(runtimeClass *RuntimeCompatibility) { runtimeClass.RunscVersion = "" },
		"missing security class": func(runtimeClass *RuntimeCompatibility) { runtimeClass.SecurityClass = "" },
		"unknown security class": func(runtimeClass *RuntimeCompatibility) { runtimeClass.SecurityClass = "host" },
		"wrong command":          func(runtimeClass *RuntimeCompatibility) { runtimeClass.Command = "/bin/sh" },
		"wrong port":             func(runtimeClass *RuntimeCompatibility) { runtimeClass.ProcdPort++ },
		"wrong mode":             func(runtimeClass *RuntimeCompatibility) { runtimeClass.RuntimeMode = "watch" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			runtimeClass := valid
			mutate(&runtimeClass)
			if _, err := runtimeClass.Digest(); err == nil {
				t.Fatal("invalid compatibility class was accepted")
			}
		})
	}
}
