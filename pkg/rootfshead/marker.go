package rootfshead

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	markerPAXKey = "SANDBOX0.rootfs-head"

	// MarkerMediaType identifies the immutable lookup copy of an OCI marker.
	// The registry and object-store copies have identical bytes and digest.
	MarkerMediaType = "application/vnd.sandbox0.rootfs.head-marker.v2.tar"
)

// MaxMarkerBytes bounds content-store reads used to identify marker layers.
const MaxMarkerBytes int64 = 64 * 1024

// ErrNotMarker reports that an OCI layer is not a Sandbox0 rootfs head marker.
var ErrNotMarker = errors.New("not a sandbox0 rootfs head marker")

// EncodeMarker returns a filesystem-empty tar layer carrying only a bounded
// immutable head reference. Keeping the reference inside the marker makes
// correctness independent of CRI snapshot-annotation propagation.
func EncodeMarker(reference HeadReference) ([]byte, error) {
	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	writer := tar.NewWriter(&payload)
	if err := writer.WriteHeader(&tar.Header{
		Typeflag: tar.TypeXGlobalHeader,
		Name:     "Sandbox0.rootfs-head",
		Format:   tar.FormatPAX,
		PAXRecords: map[string]string{
			markerPAXKey: annotation,
		},
	}); err != nil {
		return nil, fmt.Errorf("encode rootfs head marker: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close rootfs head marker: %w", err)
	}
	return payload.Bytes(), nil
}

// MarkerObject returns the deterministic object-store descriptor and payload
// for a rootfs head marker. Keeping this tiny content-addressed copy makes
// marker discovery independent of optional CRI annotation propagation and
// containerd content-GC timing.
func MarkerObject(reference HeadReference) (Object, []byte, error) {
	payload, err := EncodeMarker(reference)
	if err != nil {
		return Object{}, nil, err
	}
	digestValue := digest.FromBytes(payload)
	key, err := MarkerObjectKey(digestValue.String())
	if err != nil {
		return Object{}, nil, err
	}
	return Object{
		Key:       key,
		Digest:    digestValue.String(),
		Size:      int64(len(payload)),
		MediaType: MarkerMediaType,
	}, payload, nil
}

// MarkerObjectKey maps an OCI marker digest to its immutable object-store key.
func MarkerObjectKey(digestString string) (string, error) {
	digestValue, err := digest.Parse(strings.TrimSpace(digestString))
	if err != nil {
		return "", fmt.Errorf("invalid rootfs head marker digest %q: %w", digestString, err)
	}
	return path.Join("sandbox-rootfs", "markers", digestValue.Algorithm().String(), digestValue.Encoded()+".tar"), nil
}

// DecodeMarker reads a filesystem-empty marker layer and returns its bounded
// head reference. Ordinary OCI tar layers return ErrNotMarker.
func DecodeMarker(reader io.Reader) (HeadReference, error) {
	if reader == nil {
		return HeadReference{}, ErrNotMarker
	}
	tarReader := tar.NewReader(reader)
	header, err := tarReader.Next()
	if errors.Is(err, io.EOF) {
		return HeadReference{}, ErrNotMarker
	}
	if err != nil {
		return HeadReference{}, fmt.Errorf("read rootfs head marker: %w", err)
	}
	if header.Typeflag != tar.TypeXGlobalHeader {
		return HeadReference{}, ErrNotMarker
	}
	annotation := strings.TrimSpace(header.PAXRecords[markerPAXKey])
	if annotation == "" {
		return HeadReference{}, ErrNotMarker
	}
	if _, err := tarReader.Next(); !errors.Is(err, io.EOF) {
		if err == nil {
			return HeadReference{}, fmt.Errorf("rootfs head marker contains a filesystem entry")
		}
		return HeadReference{}, fmt.Errorf("finish rootfs head marker: %w", err)
	}
	reference, err := DecodeHeadAnnotation(annotation)
	if err != nil {
		return HeadReference{}, fmt.Errorf("decode rootfs head marker reference: %w", err)
	}
	return reference, nil
}
