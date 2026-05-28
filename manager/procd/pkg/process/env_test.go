package process

import (
	"reflect"
	"testing"
)

func TestMergeEnvVarsAppliesLaterLayers(t *testing.T) {
	got := MergeEnvVars(
		map[string]string{
			"BASE":     "base",
			"OVERRIDE": "sandbox",
			"BAD=KEY":  "ignored",
			" EMPTY ":  "trimmed",
			"":         "ignored",
		},
		map[string]string{
			"OVERRIDE": "context",
			"PROCESS":  "process",
		},
	)

	want := map[string]string{
		"BASE":     "base",
		"OVERRIDE": "context",
		"EMPTY":    "trimmed",
		"PROCESS":  "process",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MergeEnvVars() = %#v, want %#v", got, want)
	}
}

func TestMergeEnvironmentOverlaysBaseInOrder(t *testing.T) {
	got := MergeEnvironment(
		[]string{"BASE=base", "OVERRIDE=base", "MALFORMED"},
		map[string]string{
			"OVERRIDE": "sandbox",
			"SANDBOX":  "sandbox",
		},
		map[string]string{
			"OVERRIDE": "context",
			"PROCESS":  "process",
			" BAD ":    "trimmed",
			"BAD=KEY":  "ignored",
		},
	)

	want := []string{
		"BASE=base",
		"OVERRIDE=context",
		"SANDBOX=sandbox",
		"BAD=trimmed",
		"PROCESS=process",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MergeEnvironment() = %#v, want %#v", got, want)
	}
}
