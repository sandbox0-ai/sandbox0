// Package runtimecheckpoint stores immutable execution images for system-owned
// runtime migration. Images are custody artifacts, not user-visible snapshots
// or authorization to execute a second copy of a sandbox.
package runtimecheckpoint

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	ManifestVersion  = 1
	ChunkBytes       = 8 << 20
	MaxFiles         = 256
	MaxManifestBytes = 4 << 20
	MaxImageBytes    = int64(256) << 30
)

// Binding ties execution state to the same filesystem cut and immutable source
// assignment. SourceBindingDigest is the digest of the existing durable RootFS
// StageRequest, including its exact node, boot, allocation and writer epoch.
// The regional authority must compare every field before publishing the image;
// a valid digest alone does not prove that a checkpoint is authorized.
type Binding struct {
	OperationID                string `json:"operation_id"`
	SandboxID                  string `json:"sandbox_id"`
	TeamID                     string `json:"team_id"`
	SourceBindingDigest        string `json:"source_binding_digest"`
	RuntimeCompatibilityDigest string `json:"runtime_compatibility_digest"`
	AssignmentRevision         string `json:"assignment_revision"`
	CPUFeaturesDigest          string `json:"cpu_features_digest"`
	RootFSGenerationID         string `json:"rootfs_generation_id"`
	RootFSDescriptorDigest     string `json:"rootfs_descriptor_digest"`
}

func (b Binding) Validate() error {
	for name, value := range map[string]string{
		"operation_id": b.OperationID, "sandbox_id": b.SandboxID,
		"team_id": b.TeamID, "rootfs_generation_id": b.RootFSGenerationID,
	} {
		if value == "" || len(value) > 512 || strings.TrimSpace(value) != value ||
			strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("%s must be a canonical bounded identity", name)
		}
	}
	for name, value := range map[string]string{
		"source_binding_digest":        b.SourceBindingDigest,
		"runtime_compatibility_digest": b.RuntimeCompatibilityDigest,
		"assignment_revision":          b.AssignmentRevision, "cpu_features_digest": b.CPUFeaturesDigest,
		"rootfs_descriptor_digest": b.RootFSDescriptorDigest,
	} {
		if err := validateDigest(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func (b Binding) Digest() (string, error) {
	if err := b.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

type Chunk struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type File struct {
	Path   string  `json:"path"`
	Size   int64   `json:"size"`
	Chunks []Chunk `json:"chunks"`
}

// Manifest has no bearer tokens, host paths or executable configuration. Image
// data can contain workload credentials and must use the regional encrypted
// object store, with the same access restrictions as durable RootFS objects.
type Manifest struct {
	Version int     `json:"version"`
	Binding Binding `json:"binding"`
	Files   []File  `json:"files"`
}

func (m Manifest) Validate(maxBytes int64) error {
	if maxBytes <= 0 || maxBytes > MaxImageBytes || m.Version != ManifestVersion ||
		len(m.Files) == 0 || len(m.Files) > MaxFiles {
		return fmt.Errorf("invalid checkpoint version, file count or image limit")
	}
	if err := m.Binding.Validate(); err != nil {
		return err
	}
	var total int64
	seen := make(map[string]bool, len(m.Files))
	directories := map[string]bool{".": true}
	previous := ""
	for _, file := range m.Files {
		if !validPath(file.Path) || file.Path <= previous || file.Size < 0 || file.Size > maxBytes-total {
			return fmt.Errorf("invalid checkpoint file path, order or size")
		}
		for parent := path.Dir(file.Path); parent != "."; parent = path.Dir(parent) {
			directories[parent] = true
			if seen[parent] {
				return fmt.Errorf("checkpoint file overlaps a parent file")
			}
		}
		seen[file.Path] = true
		if len(seen)+len(directories) > MaxFiles*8 {
			return fmt.Errorf("checkpoint image exceeds staging inode bound")
		}
		previous = file.Path
		total += file.Size
		if int64(len(file.Chunks)) != (file.Size+ChunkBytes-1)/ChunkBytes {
			return fmt.Errorf("checkpoint chunk count does not match file size")
		}
		remaining := file.Size
		for _, chunk := range file.Chunks {
			if chunk.Size != min(int64(ChunkBytes), remaining) || validateDigest(chunk.Digest) != nil {
				return fmt.Errorf("invalid checkpoint chunk size or digest")
			}
			remaining -= chunk.Size
		}
	}
	if total == 0 {
		return fmt.Errorf("checkpoint image is empty")
	}
	return nil
}

func (m Manifest) Encode(maxBytes int64) ([]byte, error) {
	if err := m.Validate(maxBytes); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxManifestBytes {
		return nil, fmt.Errorf("checkpoint manifest exceeds size limit")
	}
	return payload, nil
}

func Decode(payload []byte, maxBytes int64) (Manifest, error) {
	var m Manifest
	if len(payload) == 0 || len(payload) > MaxManifestBytes {
		return m, fmt.Errorf("invalid checkpoint manifest size")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return Manifest{}, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Manifest{}, fmt.Errorf("checkpoint manifest contains trailing data")
	}
	canonical, err := m.Encode(maxBytes)
	if err != nil {
		return Manifest{}, err
	}
	// Canonical comparison also rejects duplicate JSON keys, which otherwise
	// allow different implementations to bind different identities to one blob.
	if !bytes.Equal(payload, canonical) {
		return Manifest{}, fmt.Errorf("checkpoint manifest is not canonical")
	}
	return m, nil
}

func validPath(value string) bool {
	return value != "" && value != "." && value != ".." && len(value) <= 1024 &&
		!strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "../") &&
		!strings.ContainsAny(value, "\\\x00\r\n") && path.Clean(value) == value
}

func validateDigest(value string) error {
	parsed, err := digest.Parse(value)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return fmt.Errorf("expected a canonical sha256 digest")
	}
	return nil
}
