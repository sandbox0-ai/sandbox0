package rootfshead

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/opencontainers/go-digest"
)

const (
	markerPAXKey = "SANDBOX0.rootfs-head-v3"

	MarkerMediaType = "application/vnd.sandbox0.rootfs.head-marker.v3.tar"
	MaxMarkerBytes  = 64 * 1024
)

var ErrNotMarker = errors.New("not a sandbox0 rootfs head marker")

func EncodeMarker(reference HeadReference) ([]byte, error) {
	annotation, err := EncodeHeadAnnotation(reference)
	if err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	writer := tar.NewWriter(&payload)
	if err := writer.WriteHeader(&tar.Header{
		Typeflag: tar.TypeXGlobalHeader,
		Name:     "Sandbox0.rootfs-head-v3",
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

func MarkerObject(prefix string, reference HeadReference) (Object, []byte, error) {
	payload, err := EncodeMarker(reference)
	if err != nil {
		return Object{}, nil, err
	}
	digestValue := digest.FromBytes(payload)
	key, err := ObjectKey(prefix, MarkerMediaType, digestValue.String())
	if err != nil {
		return Object{}, nil, err
	}
	object := Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: MarkerMediaType}
	if err := ValidateObjectScope(prefix, object); err != nil {
		return Object{}, nil, err
	}
	return object, payload, nil
}

func DecodeMarker(reader io.Reader) (HeadReference, error) {
	if reader == nil {
		return HeadReference{}, ErrNotMarker
	}
	tarReader := tar.NewReader(io.LimitReader(reader, MaxMarkerBytes+1))
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
