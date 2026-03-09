package main

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	skillBundleSchemaVersion = "1"
	skillBundleKind          = "sandbox0.agent-skill"
	skillName                = "sandbox0"
	skillBundleTimeFormat    = "2006-01-02T15:04:05Z"
)

type bundleManifest struct {
	SchemaVersion   string          `json:"schemaVersion"`
	Kind            string          `json:"kind"`
	SkillName       string          `json:"skillName"`
	Sandbox0Version string          `json:"sandbox0Version"`
	GeneratedAt     string          `json:"generatedAt"`
	SourcePriority  []string        `json:"sourcePriority"`
	Authority       bundleAuthority `json:"authorityBoundary"`
	Bundle          bundleLayout    `json:"bundle"`
}

type bundleAuthority struct {
	AuthoritativeSources []string `json:"authoritativeSources"`
	BundledDocsRole      string   `json:"bundledDocsRole"`
}

type bundleLayout struct {
	Root             string   `json:"root"`
	SkillDir         string   `json:"skillDir"`
	BundledDocsDir   string   `json:"bundledDocsDir"`
	NavigationFiles  []string `json:"navigationFiles"`
	ChecksumFile     string   `json:"checksumFile"`
	ManifestFile     string   `json:"manifestFile"`
	ArchiveFile      string   `json:"archiveFile"`
	ArchiveSHA256    string   `json:"archiveSha256File"`
	DocsManifestFile string   `json:"docsManifestFile"`
}

type bundler struct {
	repoRoot      string
	skillSource   string
	docsBundleDir string
	bundleRoot    string
	version       string
	now           time.Time
}

type bundleResult struct {
	bundleDir         string
	archivePath       string
	archiveSHA256Path string
	manifestPath      string
	manifestAssetPath string
}

func main() {
	var (
		version       = flag.String("version", "", "Sandbox0 release version for the skill bundle")
		repoRoot      = flag.String("repo-root", "", "Repository root path")
		skillSource   = flag.String("skill-source", "", "Skill source directory")
		docsBundleDir = flag.String("docs-bundle-dir", "", "Expanded docs bundle directory")
		bundleRoot    = flag.String("bundle-root", "", "Output directory for generated skill bundles")
	)
	flag.Parse()

	if strings.TrimSpace(*version) == "" {
		fail(fmt.Errorf("missing required -version"))
	}

	root := strings.TrimSpace(*repoRoot)
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fail(err)
		}
		root = cwd
	}

	skillDir := firstNonEmpty(*skillSource, filepath.Join(root, "agent-skills", skillName))
	docsDir := firstNonEmpty(
		*docsBundleDir,
		filepath.Join(root, "sandbox0-ui", "apps", "website", "dist", "docs-bundles", "sandbox0-docs-bundle-"+strings.TrimSpace(*version)),
	)
	outputRoot := firstNonEmpty(
		*bundleRoot,
		filepath.Join(root, "sandbox0-ui", "apps", "website", "dist", "skill-bundles"),
	)

	result, err := (&bundler{
		repoRoot:      root,
		skillSource:   skillDir,
		docsBundleDir: docsDir,
		bundleRoot:    outputRoot,
		version:       strings.TrimSpace(*version),
		now:           bundleTime(),
	}).build()
	if err != nil {
		fail(err)
	}

	fmt.Printf("bundle_dir=%s\n", result.bundleDir)
	fmt.Printf("manifest=%s\n", result.manifestPath)
	fmt.Printf("manifest_asset=%s\n", result.manifestAssetPath)
	fmt.Printf("archive=%s\n", result.archivePath)
	fmt.Printf("archive_sha256=%s\n", result.archiveSHA256Path)
}

func (b *bundler) build() (*bundleResult, error) {
	if err := requireDir(b.skillSource); err != nil {
		return nil, err
	}
	if err := requireDir(b.docsBundleDir); err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(b.docsBundleDir, "manifest.json")); err != nil {
		return nil, fmt.Errorf("docs bundle manifest missing: %w", err)
	}

	bundleBaseName := "sandbox0-agent-skill-" + b.version
	bundleDir := filepath.Join(b.bundleRoot, bundleBaseName)
	if err := os.RemoveAll(bundleDir); err != nil {
		return nil, fmt.Errorf("remove previous bundle dir: %w", err)
	}
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return nil, fmt.Errorf("create bundle dir: %w", err)
	}

	skillDest := filepath.Join(bundleDir, "skill")
	if err := copyDir(b.skillSource, skillDest); err != nil {
		return nil, fmt.Errorf("copy skill source: %w", err)
	}

	docsDest := filepath.Join(bundleDir, "bundled-docs")
	if err := copyDir(b.docsBundleDir, docsDest); err != nil {
		return nil, fmt.Errorf("copy bundled docs: %w", err)
	}

	archivePath := filepath.Join(b.bundleRoot, bundleBaseName+".tar.gz")
	archiveSHA256Path := archivePath + ".sha256"
	manifestPath := filepath.Join(bundleDir, "manifest.json")
	manifestAssetPath := filepath.Join(b.bundleRoot, bundleBaseName+".manifest.json")
	manifest := bundleManifest{
		SchemaVersion:   skillBundleSchemaVersion,
		Kind:            skillBundleKind,
		SkillName:       skillName,
		Sandbox0Version: b.version,
		GeneratedAt:     b.now.UTC().Format(skillBundleTimeFormat),
		SourcePriority: []string{
			"source-code",
			"pkg/apispec/openapi.yaml",
			"s0-cli-help-and-implementation",
			"bundled-docs",
			"hosted-website-docs",
		},
		Authority: bundleAuthority{
			AuthoritativeSources: []string{
				"source-code",
				"pkg/apispec/openapi.yaml",
				"s0-cli-help-and-implementation",
			},
			BundledDocsRole: "Bundled docs are release-matched reference material and must not override source code, OpenAPI, or CLI behavior when they disagree.",
		},
		Bundle: bundleLayout{
			Root:             bundleBaseName,
			SkillDir:         "skill",
			BundledDocsDir:   "bundled-docs",
			NavigationFiles:  []string{"skill/SKILL.md", "skill/references/source-priority.md", "skill/references/task-routing.md"},
			ChecksumFile:     "SHA256SUMS",
			ManifestFile:     filepath.Base(manifestAssetPath),
			ArchiveFile:      filepath.Base(archivePath),
			ArchiveSHA256:    filepath.Base(archiveSHA256Path),
			DocsManifestFile: "bundled-docs/manifest.json",
		},
	}
	if err := writeJSON(manifestPath, manifest); err != nil {
		return nil, err
	}
	if err := writeJSON(manifestAssetPath, manifest); err != nil {
		return nil, err
	}
	if err := writeChecksums(filepath.Join(bundleDir, "SHA256SUMS"), bundleDir); err != nil {
		return nil, err
	}
	if err := createTarGz(archivePath, b.bundleRoot, bundleBaseName, b.now); err != nil {
		return nil, err
	}
	sum, err := sha256File(archivePath)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(archiveSHA256Path, []byte(sum+"  "+filepath.Base(archivePath)+"\n"), 0o644); err != nil {
		return nil, fmt.Errorf("write archive checksum: %w", err)
	}

	return &bundleResult{
		bundleDir:         bundleDir,
		archivePath:       archivePath,
		archiveSHA256Path: archiveSHA256Path,
		manifestPath:      manifestPath,
		manifestAssetPath: manifestAssetPath,
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func bundleTime() time.Time {
	if epoch := strings.TrimSpace(os.Getenv("SOURCE_DATE_EPOCH")); epoch != "" {
		if seconds, err := parseUnixEpoch(epoch); err == nil {
			return time.Unix(seconds, 0).UTC()
		}
	}
	return time.Now().UTC().Truncate(time.Second)
}

func parseUnixEpoch(value string) (int64, error) {
	var seconds int64
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid SOURCE_DATE_EPOCH %q", value)
		}
		seconds = seconds*10 + int64(r-'0')
	}
	return seconds, nil
}

func requireDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func copyDir(src, dest string) error {
	if err := os.RemoveAll(dest); err != nil {
		return fmt.Errorf("remove existing destination: %w", err)
	}
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(src, dest string, perm fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func writeChecksums(path, root string) error {
	var entries []string
	err := filepath.WalkDir(root, func(current string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(current) == "SHA256SUMS" {
			return nil
		}
		sum, err := sha256File(current)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		entries = append(entries, fmt.Sprintf("%s  %s", sum, filepath.ToSlash(rel)))
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk checksums: %w", err)
	}
	sort.Strings(entries)
	return os.WriteFile(path, []byte(strings.Join(entries, "\n")+"\n"), 0o644)
}

func createTarGz(dest, root, includeName string, modTime time.Time) error {
	file, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("create archive: %w", err)
	}
	defer file.Close()

	gzipWriter, err := gzip.NewWriterLevel(file, gzip.BestCompression)
	if err != nil {
		return fmt.Errorf("create gzip writer: %w", err)
	}
	gzipWriter.Name = filepath.Base(dest)
	gzipWriter.ModTime = modTime.UTC()
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	baseDir := filepath.Join(root, includeName)
	return filepath.Walk(baseDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = rel
		header.ModTime = modTime.UTC()
		header.AccessTime = modTime.UTC()
		header.ChangeTime = modTime.UTC()
		if info.IsDir() {
			if header.Name != "." && !strings.HasSuffix(header.Name, "/") {
				header.Name += "/"
			}
			return tarWriter.WriteHeader(header)
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}
		source, err := os.Open(path)
		if err != nil {
			return err
		}
		defer source.Close()
		_, err = io.Copy(tarWriter, source)
		return err
	})
}

func sha256File(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
