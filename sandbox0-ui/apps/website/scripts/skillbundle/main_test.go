package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBundlerBuild(t *testing.T) {
	root := t.TempDir()
	skillDir := filepath.Join(root, "agent-skills", "sandbox0")
	docsDir := filepath.Join(root, "dist", "docs-bundles", "sandbox0-docs-bundle-1.2.3")

	mustMkdirAll(t, filepath.Join(skillDir, "references"))
	mustWriteFile(t, filepath.Join(skillDir, "SKILL.md"), "# Skill\n")
	mustWriteFile(t, filepath.Join(skillDir, "references", "source-priority.md"), "priority\n")
	mustWriteFile(t, filepath.Join(skillDir, "references", "task-routing.md"), "routing\n")

	mustMkdirAll(t, filepath.Join(docsDir, "docs-source"))
	mustWriteFile(t, filepath.Join(docsDir, "manifest.json"), "{\n  \"kind\": \"sandbox0.docs-bundle\"\n}\n")
	mustWriteFile(t, filepath.Join(docsDir, "SHA256SUMS"), "abc  manifest.json\n")
	mustWriteFile(t, filepath.Join(docsDir, "docs-source", "page.mdx"), "hello\n")

	result, err := (&bundler{
		repoRoot:      root,
		skillSource:   skillDir,
		docsBundleDir: docsDir,
		bundleRoot:    filepath.Join(root, "dist", "skill-bundles"),
		version:       "1.2.3",
		now:           time.Unix(1700000000, 0).UTC(),
	}).build()
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	manifestData, err := os.ReadFile(result.manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest bundleManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if manifest.SkillName != "sandbox0" {
		t.Fatalf("unexpected skill name %q", manifest.SkillName)
	}
	if manifest.Sandbox0Version != "1.2.3" {
		t.Fatalf("unexpected version %q", manifest.Sandbox0Version)
	}
	if _, err := os.Stat(result.manifestAssetPath); err != nil {
		t.Fatalf("stat manifest asset: %v", err)
	}

	checksums, err := os.ReadFile(filepath.Join(result.bundleDir, "SHA256SUMS"))
	if err != nil {
		t.Fatalf("read checksums: %v", err)
	}
	if !strings.Contains(string(checksums), "skill/SKILL.md") {
		t.Fatalf("checksums missing skill file")
	}
	if !strings.Contains(string(checksums), "bundled-docs/manifest.json") {
		t.Fatalf("checksums missing docs manifest")
	}

	archiveEntries := readArchiveEntries(t, result.archivePath)
	if _, ok := archiveEntries["sandbox0-agent-skill-1.2.3/skill/SKILL.md"]; !ok {
		t.Fatalf("archive missing SKILL.md")
	}
	if _, ok := archiveEntries["sandbox0-agent-skill-1.2.3/bundled-docs/manifest.json"]; !ok {
		t.Fatalf("archive missing bundled docs manifest")
	}

	archiveSHA, err := os.ReadFile(result.archiveSHA256Path)
	if err != nil {
		t.Fatalf("read archive checksum: %v", err)
	}
	if !strings.Contains(string(archiveSHA), "sandbox0-agent-skill-1.2.3.tar.gz") {
		t.Fatalf("archive checksum missing archive name")
	}
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readArchiveEntries(t *testing.T, archivePath string) map[string]struct{} {
	t.Helper()
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("new gzip reader: %v", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	entries := make(map[string]struct{})
	for {
		header, err := tarReader.Next()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("read tar entry: %v", err)
		}
		entries[header.Name] = struct{}{}
	}
	return entries
}
