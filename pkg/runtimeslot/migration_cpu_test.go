package runtimeslot

import (
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
)

func testMigrationCPUProfile() MigrationCPUProfile {
	return MigrationCPUProfile{Version: MigrationCPUProfileVersion, Architecture: "amd64",
		RunscVersion: "runsc version release-20260817.0", Features: []string{"aes", "sse2", "xsave"},
		CacheLineBytes: 64, XStateLayoutDigest: digest.FromString("measured-layout").String()}
}

func TestParseMigrationCPUFeatures(t *testing.T) {
	for _, input := range []string{"xsave,aes,sse2", "xsave,aes,sse2\n"} {
		features, err := ParseMigrationCPUFeatures([]byte(input))
		if err != nil || !slices.Equal(features, []string{"aes", "sse2", "xsave"}) {
			t.Fatalf("parse %q: %v, %v", input, features, err)
		}
	}
	for _, input := range []string{"", "\n", "aes,", ",aes", "aes,,sse2", "aes,aes",
		"aes, sse2", "AES", "aes\r\n", "aes\n\n", "aes\x00", "warning: cpu not found",
		strings.Repeat("x", 65), strings.Repeat("x", 64<<10+1), strings.Repeat("aes,", 513)} {
		if _, err := ParseMigrationCPUFeatures([]byte(input)); err == nil {
			t.Fatalf("accepted malformed CPU output %q", input[:min(len(input), 80)])
		}
	}
}

func TestMigrationCPUProfileCompatibility(t *testing.T) {
	source := testMigrationCPUProfile()
	for _, tc := range []struct {
		name       string
		change     func(*MigrationCPUProfile)
		compatible bool
	}{
		{"identical", func(*MigrationCPUProfile) {}, true},
		{"additional target feature", func(p *MigrationCPUProfile) { p.Features = []string{"aes", "avx", "sse2", "xsave"} }, true},
		{"missing source feature", func(p *MigrationCPUProfile) { p.Features = []string{"sse2", "xsave"} }, false},
		{"different runsc", func(p *MigrationCPUProfile) { p.RunscVersion = "runsc version other" }, false},
		{"different cache line", func(p *MigrationCPUProfile) { p.CacheLineBytes = 128 }, false},
		{"different xstate", func(p *MigrationCPUProfile) { p.XStateLayoutDigest = digest.FromString("other").String() }, false},
		{"different architecture", func(p *MigrationCPUProfile) {
			p.Architecture = "arm64"
			p.CacheLineBytes = 0
			p.XStateLayoutDigest = ""
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := source
			tc.change(&target)
			if err := CheckMigrationCPUProfiles(source, target); (err == nil) != tc.compatible {
				t.Fatalf("compatibility = %v, want %t", err, tc.compatible)
			}
		})
	}
	arm := MigrationCPUProfile{Version: 1, Architecture: "arm64", RunscVersion: source.RunscVersion, Features: []string{"aes", "asimd", "fp"}}
	if err := CheckMigrationCPUProfiles(arm, arm); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationCPUProfileRejectsIncompleteOrNoncanonicalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*MigrationCPUProfile)
	}{
		{"unknown version", func(p *MigrationCPUProfile) { p.Version++ }},
		{"missing runtime", func(p *MigrationCPUProfile) { p.RunscVersion = "" }},
		{"runtime whitespace", func(p *MigrationCPUProfile) { p.RunscVersion += " " }},
		{"runtime newline", func(p *MigrationCPUProfile) { p.RunscVersion += "\nother" }},
		{"runtime NUL", func(p *MigrationCPUProfile) { p.RunscVersion += "\x00" }},
		{"runtime too long", func(p *MigrationCPUProfile) { p.RunscVersion = strings.Repeat("x", 513) }},
		{"unsupported arch", func(p *MigrationCPUProfile) { p.Architecture = "riscv64" }},
		{"missing cache line", func(p *MigrationCPUProfile) { p.CacheLineBytes = 0 }},
		{"invalid cache line", func(p *MigrationCPUProfile) { p.CacheLineBytes = 65 }},
		{"missing xstate", func(p *MigrationCPUProfile) { p.XStateLayoutDigest = "" }},
		{"malformed xstate", func(p *MigrationCPUProfile) { p.XStateLayoutDigest = "sha256:short" }},
		{"arm xstate", func(p *MigrationCPUProfile) { p.Architecture = "arm64"; p.CacheLineBytes = 0 }},
		{"arm cache line", func(p *MigrationCPUProfile) { p.Architecture = "arm64"; p.XStateLayoutDigest = "" }},
		{"missing features", func(p *MigrationCPUProfile) { p.Features = nil }},
		{"unordered features", func(p *MigrationCPUProfile) { p.Features = []string{"xsave", "aes"} }},
		{"duplicate features", func(p *MigrationCPUProfile) { p.Features = []string{"aes", "aes"} }},
		{"invalid feature", func(p *MigrationCPUProfile) { p.Features = []string{"aes,sse2"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := testMigrationCPUProfile()
			tc.change(&bad)
			if _, err := bad.Digest(); err == nil {
				t.Fatal("invalid profile produced digest")
			}
			if err := CheckMigrationCPUProfiles(bad, testMigrationCPUProfile()); err == nil {
				t.Fatal("invalid source accepted")
			}
			if err := CheckMigrationCPUProfiles(testMigrationCPUProfile(), bad); err == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
}

func TestMigrationCPUProfileDigestBindsEvidence(t *testing.T) {
	profile := testMigrationCPUProfile()
	first, err := profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := profile.Digest()
	if err != nil || repeated != first {
		t.Fatalf("unstable digest: %s %v", repeated, err)
	}
	for _, change := range []func(*MigrationCPUProfile){
		func(p *MigrationCPUProfile) { p.Features = []string{"aes", "avx", "sse2", "xsave"} },
		func(p *MigrationCPUProfile) { p.RunscVersion = "runsc version other" },
		func(p *MigrationCPUProfile) { p.CacheLineBytes = 128 },
		func(p *MigrationCPUProfile) { p.XStateLayoutDigest = digest.FromString("different-layout").String() },
	} {
		altered := profile
		change(&altered)
		got, err := altered.Digest()
		if err != nil || got == first {
			t.Fatalf("changed evidence not bound: %s %v", got, err)
		}
	}
}

func TestMigrationCPUObservationBindsCoverageSeparatelyFromCompatibility(t *testing.T) {
	first := MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "0-3,8"}
	second := MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "16-19"}
	firstDigest, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := second.Digest()
	if err != nil || firstDigest == secondDigest {
		t.Fatalf("CPU coverage not bound: %s %s %v", firstDigest, secondDigest, err)
	}
	if err := CheckMigrationCPUProfiles(first.Profile, second.Profile); err != nil {
		t.Fatalf("node-local CPU numbers changed compatibility: %v", err)
	}
	for _, covered := range []string{"0", "0-3", "1,3,8"} {
		if err := first.Covers(covered); err != nil {
			t.Fatalf("covered %q: %v", covered, err)
		}
	}
	for _, uncovered := range []string{"", "0-4", "8-9", "0,0"} {
		if err := first.Covers(uncovered); err == nil {
			t.Fatalf("accepted uncovered set %q", uncovered)
		}
	}
	first.CPUSet = ""
	if _, err := first.Digest(); err == nil {
		t.Fatal("empty coverage produced digest")
	}
}
