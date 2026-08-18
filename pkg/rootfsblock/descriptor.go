// Package rootfsblock defines the bounded, versioned control descriptor for a
// durable RootFS block-map generation. Bulk data and mapping pages remain in
// immutable object-store packs.
package rootfsblock

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
	DescriptorVersion   = 1
	MappingPageVersion  = 1
	LogicalBlockSize    = 4096
	MaxDescriptorBytes  = 64 << 10
	MaxMappingRootBytes = 8 << 20
	// MaxCompositeTailBytes leaves room for JSON/base64 envelope metadata so
	// the complete descriptor remains within MaxDescriptorBytes.
	MaxCompositeTailBytes  = 46 << 10
	MaxObjectKeyBytes      = 1024
	DurabilityS3           = "s3_materialized"
	DurabilityComposite    = "composite_durable"
	CompositeTailEncoding  = "sandbox0-block-tail-v1"
	MappingPageContentType = "application/vnd.sandbox0.block-map.v1"
)

// Descriptor points at one immutable complete block-map root. A bounded
// composite tail may override blocks after that root without changing the
// object. Neither field contains object-store credentials.
type Descriptor struct {
	Version          int                `json:"version"`
	LogicalSizeBytes int64              `json:"logical_size_bytes"`
	BlockSizeBytes   int64              `json:"block_size_bytes"`
	MappingRoot      MappingRootLocator `json:"mapping_root"`
	CompositeTail    *CompositeTail     `json:"composite_tail,omitempty"`
}

type MappingRootLocator struct {
	Version    int         `json:"version"`
	RootDigest string      `json:"root_digest"`
	Object     ObjectRange `json:"object"`
}

type ObjectRange struct {
	Key      string `json:"key"`
	Offset   int64  `json:"offset"`
	Length   int64  `json:"length"`
	Checksum string `json:"checksum"`
}

type CompositeTail struct {
	Encoding string `json:"encoding"`
	Checksum string `json:"checksum"`
	Payload  []byte `json:"payload"`
}

func DecodeDescriptor(payload []byte) (Descriptor, error) {
	if len(payload) == 0 || len(payload) > MaxDescriptorBytes {
		return Descriptor{}, fmt.Errorf("descriptor must contain 1..%d bytes", MaxDescriptorBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var descriptor Descriptor
	if err := decoder.Decode(&descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("decode block descriptor: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Descriptor{}, fmt.Errorf("block descriptor contains trailing data")
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func EncodeDescriptor(descriptor Descriptor) ([]byte, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(descriptor)
	if err != nil {
		return nil, fmt.Errorf("encode block descriptor: %w", err)
	}
	if len(payload) > MaxDescriptorBytes {
		return nil, fmt.Errorf("encoded block descriptor exceeds %d bytes", MaxDescriptorBytes)
	}
	return payload, nil
}

func (d Descriptor) Validate() error {
	if d.Version != DescriptorVersion {
		return fmt.Errorf("unsupported block descriptor version %d", d.Version)
	}
	if d.BlockSizeBytes != LogicalBlockSize || d.LogicalSizeBytes <= 0 || d.LogicalSizeBytes%d.BlockSizeBytes != 0 {
		return fmt.Errorf("logical size must be a positive multiple of the %d-byte block size", LogicalBlockSize)
	}
	if err := d.MappingRoot.Validate(); err != nil {
		return fmt.Errorf("mapping_root: %w", err)
	}
	if d.CompositeTail != nil {
		if err := d.CompositeTail.Validate(); err != nil {
			return fmt.Errorf("composite_tail: %w", err)
		}
		if _, _, err := decodeCompositeTailPayload(d.CompositeTail.Payload, uint64(d.LogicalSizeBytes/d.BlockSizeBytes)); err != nil {
			return fmt.Errorf("composite_tail: %w", err)
		}
	}
	return nil
}

func (l MappingRootLocator) Validate() error {
	if l.Version != MappingPageVersion {
		return fmt.Errorf("unsupported mapping page version %d", l.Version)
	}
	if err := validateDigest("root_digest", l.RootDigest); err != nil {
		return err
	}
	if err := l.Object.Validate(MaxMappingRootBytes); err != nil {
		return err
	}
	return nil
}

func (r ObjectRange) Validate(maxLength int64) error {
	if r.Key == "" || strings.TrimSpace(r.Key) != r.Key || len(r.Key) > MaxObjectKeyBytes || strings.HasPrefix(r.Key, "/") || strings.Contains(r.Key, "\\") ||
		path.Clean(r.Key) != r.Key || r.Key == "." || strings.HasPrefix(r.Key, "../") {
		return fmt.Errorf("object key is not a canonical relative key")
	}
	if r.Offset < 0 || r.Length <= 0 || r.Length > maxLength {
		return fmt.Errorf("object range offset or length is invalid")
	}
	return validateDigest("object checksum", r.Checksum)
}

func (t CompositeTail) Validate() error {
	if t.Encoding != CompositeTailEncoding {
		return fmt.Errorf("unsupported tail encoding %q", t.Encoding)
	}
	if len(t.Payload) == 0 || len(t.Payload) > MaxCompositeTailBytes {
		return fmt.Errorf("tail payload must contain 1..%d bytes", MaxCompositeTailBytes)
	}
	if err := validateDigest("tail checksum", t.Checksum); err != nil {
		return err
	}
	if digest.FromBytes(t.Payload).String() != t.Checksum {
		return fmt.Errorf("tail checksum does not match payload")
	}
	if _, _, err := decodeCompositeTailPayload(t.Payload, ^uint64(0)); err != nil {
		return err
	}
	return nil
}

func validateDigest(name, value string) error {
	parsed, err := digest.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return fmt.Errorf("%s must be a canonical sha256 digest", name)
	}
	return nil
}
