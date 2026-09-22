package runtimecheckpoint

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/opencontainers/go-digest"
)

const peerMagic = "S0CKPT\x00\x01"

// WritePeerImage streams a retained source image using its committed manifest.
// The caller must authenticate the destination and hold source custody until
// this call finishes. It reads only the manifest from regional storage; image
// bytes flow from the source disk directly to the peer. Each chunk is checked
// before release, so local corruption cannot silently change the saved cut.
// Transport must enforce cancellation/deadlines on blocked writes.
func (s *Store) WritePeerImage(ctx context.Context, binding Binding, ref Reference, directory string, output io.Writer) error {
	manifest, err := s.loadManifest(ctx, binding, ref)
	if err != nil {
		return err
	}
	return writePeerImage(ctx, manifest, directory, output, s.maxBytes)
}

func writePeerImage(ctx context.Context, manifest Manifest, directory string, output io.Writer, maxBytes int64) error {
	payload, err := manifest.Encode(maxBytes)
	if err != nil {
		return err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	files, err := (&Store{maxBytes: maxBytes}).imageFiles(ctx, root)
	if err != nil {
		return err
	}
	if len(files) != len(manifest.Files) {
		return fmt.Errorf("peer image inventory changed")
	}
	for i, file := range files {
		if file.Path != manifest.Files[i].Path || file.Size != manifest.Files[i].Size {
			return fmt.Errorf("peer image file identity changed")
		}
	}
	var header [12]byte
	copy(header[:8], peerMagic)
	binary.BigEndian.PutUint32(header[8:], uint32(len(payload)))
	if err := writePeerBytes(ctx, output, header[:]); err != nil {
		return err
	}
	if err := writePeerBytes(ctx, output, payload); err != nil {
		return err
	}
	buffer := make([]byte, ChunkBytes)
	for _, file := range manifest.Files {
		if err := writePeerFile(ctx, root, file, output, buffer); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func writePeerFile(ctx context.Context, root *os.Root, file File, output io.Writer, buffer []byte) error {
	input, err := root.Open(file.Path)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != file.Size {
		return fmt.Errorf("peer image file changed")
	}
	for _, chunk := range file.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		data := buffer[:int(chunk.Size)]
		if _, err := io.ReadFull(input, data); err != nil {
			return err
		}
		if digest.FromBytes(data).String() != chunk.Digest {
			return fmt.Errorf("peer image chunk digest mismatch")
		}
		if err := writePeerBytes(ctx, output, data); err != nil {
			return err
		}
	}
	var extra [1]byte
	if n, err := input.Read(extra[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("peer image file grew")
	}
	return nil
}

func writePeerBytes(ctx context.Context, output io.Writer, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := output.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}

// ReceivePeerImage verifies an authenticated peer stream against its authorized
// reference without downloading image chunks from object storage. It proves
// local image preparation, never regional publication or restore authority. The manifest
// is bounded, hash-bound and admitted before any destination file is created.
// Partial, corrupt or overlong streams never produce a successful receipt.
// Callers own transport deadlines, exclusive staging and cleanup on failure.
func ReceivePeerImage(ctx context.Context, expected Binding, ref Reference, directory string, input io.Reader,
	maxBytes int64, admit func(int64, uint64) error) (Manifest, error) {
	if admit == nil {
		return Manifest{}, fmt.Errorf("peer image requires bounded staging admission")
	}
	manifest, err := readPeerManifest(ctx, expected, ref, input, maxBytes, peerMagic)
	if err != nil {
		return Manifest{}, err
	}
	buffer := make([]byte, ChunkBytes)
	manifest, err = materializeImage(ctx, manifest, directory, admit, func(ctx context.Context, chunk Chunk) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data := buffer[:int(chunk.Size)]
		_, err := io.ReadFull(input, data)
		return data, err
	})
	if err != nil {
		return Manifest{}, err
	}
	var extra [1]byte
	if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
		return Manifest{}, fmt.Errorf("peer image has trailing data or incomplete stream termination")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Both full and capture-repair streams bind the same canonical final manifest.
func readPeerManifest(ctx context.Context, expected Binding, ref Reference, input io.Reader, maxBytes int64, magic string) (Manifest, error) {
	if err := ref.ValidateFor(expected); err != nil {
		return Manifest{}, err
	}
	if maxBytes <= 0 || maxBytes > MaxImageBytes {
		return Manifest{}, fmt.Errorf("peer image requires bounded staging admission")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	var header [12]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return Manifest{}, err
	}
	size := binary.BigEndian.Uint32(header[8:])
	if string(header[:8]) != magic || size == 0 || size > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("invalid peer image header")
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(input, payload); err != nil {
		return Manifest{}, err
	}
	if digest.FromBytes(payload).String() != ref.ManifestDigest {
		return Manifest{}, fmt.Errorf("peer manifest digest mismatch")
	}
	manifest, err := Decode(payload, maxBytes)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Binding != expected {
		return Manifest{}, fmt.Errorf("peer image binding mismatch")
	}
	return manifest, nil
}
