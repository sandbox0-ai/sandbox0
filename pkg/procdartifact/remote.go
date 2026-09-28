package procdartifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

const DefaultSourceFile = "/etc/sandbox0/procd-artifact-source.json"

const ossObjectHelper = "/usr/local/sbin/sandbox0-oss-object"

var safeBucket = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
var fetches singleflight.Group

// Source is host-controlled, never supplied by a sandbox or a checkpoint.
// Object keys are derived only from a validated SHA-256 digest.
type Source struct {
	Endpoint string `json:"oss_endpoint"`
	Bucket   string `json:"oss_bucket"`
}

func (s Source) Validate() error {
	u, err := url.Parse(s.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" ||
		!safeBucket.MatchString(s.Bucket) || strings.Contains(u.Host, "@") {
		return errors.New("invalid procd artifact OSS source")
	}
	return nil
}

func ObjectKey(a Artifact) (string, error) {
	if err := a.Validate(); err != nil {
		return "", err
	}
	return "sandbox0-nomad-runtime/procd/sha256/" + strings.TrimPrefix(a.Digest, "sha256:") + "/procd", nil
}

func LoadSource(file string) (Source, error) {
	if !filepath.IsAbs(file) || filepath.Clean(file) != file || file == "/" {
		return Source{}, errors.New("procd source path must be canonical and absolute")
	}
	for current := file; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return Source{}, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !trustedOwner(info) || info.Mode().Perm()&0022 != 0 {
			return Source{}, fmt.Errorf("untrusted procd source path: %s", current)
		}
		if current == file {
			if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 4096 {
				return Source{}, errors.New("procd source file is invalid")
			}
		} else if !info.IsDir() {
			return Source{}, errors.New("procd source parent is not a directory")
		}
		if current == "/" {
			break
		}
	}
	f, err := os.Open(file)
	if err != nil {
		return Source{}, err
	}
	defer f.Close()
	var source Source
	decoder := json.NewDecoder(io.LimitReader(f, 4097))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&source); err != nil {
		return Source{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Source{}, errors.New("procd source has trailing data")
	}
	return source, source.Validate()
}

// Ensure resolves a local artifact or fetches the exact digest from the
// authenticated private release bucket. A bad local entry fails closed: only
// a missing file triggers a download, and no existing inode is replaced.
func Ensure(ctx context.Context, root, sourceFile string, a Artifact) (string, error) {
	return ensure(ctx, root, sourceFile, a, downloadObject)
}

func ensure(ctx context.Context, root, sourceFile string, a Artifact, download func(context.Context, Source, string, string, string) error) (string, error) {
	path, err := Resolve(root, a)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return path, err
	}
	result, err, _ := fetches.Do(root+"\x00"+sourceFile+"\x00"+a.Digest, func() (any, error) {
		return fetchMissing(ctx, root, sourceFile, a, download)
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

func fetchMissing(ctx context.Context, root, sourceFile string, a Artifact, download func(context.Context, Source, string, string, string) error) (string, error) {
	path, err := Resolve(root, a)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return path, err
	}
	source, err := LoadSource(sourceFile)
	if err != nil {
		return "", fmt.Errorf("load procd artifact source: %w", err)
	}
	key, err := ObjectKey(a)
	if err != nil {
		return "", err
	}
	limited, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	staged, err := os.CreateTemp("/run", "sandbox0-procd-fetch-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(staged.Name())
	if err := staged.Close(); err != nil {
		return "", err
	}
	if err := download(limited, source, key, strings.TrimPrefix(a.Digest, "sha256:"), staged.Name()); err != nil {
		return "", fmt.Errorf("fetch procd artifact %s: %w", a.Digest, err)
	}
	return Install(root, staged.Name(), a.Digest)
}

func downloadObject(ctx context.Context, source Source, key, sha256, target string) error {
	command := exec.CommandContext(ctx, ossObjectHelper, "get", "--endpoint", source.Endpoint,
		"--bucket", source.Bucket, "--key", key, "--file", target,
		"--sha256", sha256, "--mode", "0600")
	command.Stdout = io.Discard
	command.Stderr = io.Discard
	if err := command.Run(); err != nil {
		return fmt.Errorf("private OSS download failed: %w", err)
	}
	return nil
}
