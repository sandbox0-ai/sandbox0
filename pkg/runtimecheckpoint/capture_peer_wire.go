package runtimecheckpoint

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/opencontainers/go-digest"
)

const captureGrowingMagic = "S0GROW\x00\x01"

// Version 3 permits source verification after frames and requires its trailer.
// A mixed-version peer rejects repair and falls back to the full-image protocol.
const captureFinalMagic = "S0CKPT\x00\x03"
const captureFinalComplete = "S0DONE\x00\x01"

// WritePublishedCapturePeerFinal preserves the exact committed manifest,
// including staged chunk keys, when publication finishes before peer repair.
func (s *Store) WritePublishedCapturePeerFinal(ctx context.Context, binding Binding, ref Reference, directory string,
	inventory CapturePeerInventory, output io.Writer) error {
	manifest, err := s.loadManifest(ctx, binding, ref)
	if err != nil {
		return err
	}
	return s.WriteCapturePeerFinal(ctx, binding, LocalImagePlan{Manifest: manifest, Reference: ref}, directory, inventory, output)
}

// CapturePeerWriter sends tentative full chunks under an authenticated exact
// capture grant. The caller owns transport cancellation/write deadlines and
// joins concurrent WriteChunk calls before finishing or releasing source custody.
// A completed tentative stream never proves that the source capture succeeded.
type CapturePeerWriter struct {
	mu     sync.Mutex
	output io.Writer
	closed bool
	failed error
}

func NewCapturePeerWriter(ctx context.Context, scope CaptureScope, output io.Writer) (*CapturePeerWriter, error) {
	scopeDigest, err := scope.Digest()
	if err != nil {
		return nil, err
	}
	hash, _ := hex.DecodeString(digest.Digest(scopeDigest).Encoded())
	header := append([]byte(captureGrowingMagic), hash...)
	if err := writePeerBytes(ctx, output, header); err != nil {
		return nil, err
	}
	return &CapturePeerWriter{output: output}, nil
}

func (w *CapturePeerWriter) WriteChunk(ctx context.Context, name string, offset int64, data []byte) (err error) {
	// The caller retains an immutable borrowed buffer until this call returns.
	// Hash independent chunks before serializing their wire frames, so the
	// bounded source workers can overlap checks without retaining extra buffers.
	var hash [sha256.Size]byte
	if len(data) == ChunkBytes && ctx.Err() == nil {
		hash = sha256.Sum256(data)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.failed != nil {
		return fmt.Errorf("tentative capture stream is closed")
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	if !validPath(name) || len(data) != ChunkBytes || offset < 0 || offset%ChunkBytes != 0 || offset > MaxImageBytes-ChunkBytes {
		return fmt.Errorf("invalid tentative capture range")
	}
	var header [42]byte
	binary.BigEndian.PutUint16(header[:2], uint16(len(name)))
	binary.BigEndian.PutUint64(header[2:10], uint64(offset))
	copy(header[10:], hash[:])
	for _, payload := range [][]byte{header[:], []byte(name), data} {
		if err := writePeerBytes(ctx, w.output, payload); err != nil {
			return err
		}
	}
	return nil
}

func (w *CapturePeerWriter) Finish(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.failed != nil {
		return fmt.Errorf("tentative capture stream is closed")
	}
	w.closed = true
	return writePeerBytes(ctx, w.output, []byte{0, 0})
}

// ReceiveGrowing checks framing and source scope before accepting tentative
// ranges. Interrupted streams leave a bounded cache for final verification;
// even a clean EOF conveys no publication or execution authority. Cancellation
// requires the transport owner to interrupt any blocked read.
func (c *CapturePeerCache) ReceiveGrowing(ctx context.Context, input io.Reader) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.root == nil || c.finalizing {
		return fmt.Errorf("capture peer cache is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var header [40]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return err
	}
	scope, _ := c.scope.Digest()
	if string(header[:8]) != captureGrowingMagic || "sha256:"+hex.EncodeToString(header[8:]) != scope {
		return fmt.Errorf("tentative stream changed capture scope")
	}
	buffer := make([]byte, ChunkBytes)
	// Charge wire traffic as well as disk capacity. A malicious/retrying peer
	// cannot send an unbounded sequence of overwrites within a small disk quota.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var size [2]byte
		if _, err := io.ReadFull(input, size[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint16(size[:])
		if n == 0 {
			var extra [1]byte
			if n, err := io.ReadFull(input, extra[:]); n != 0 || err != io.EOF {
				return fmt.Errorf("overlong or incomplete tentative stream")
			}
			return ctx.Err()
		}
		if n > 1024 || c.framesRemaining == 0 {
			return fmt.Errorf("tentative stream exceeds frame budget")
		}
		c.framesRemaining--
		var frame [40]byte
		if _, err := io.ReadFull(input, frame[:]); err != nil {
			return err
		}
		offset := binary.BigEndian.Uint64(frame[:8])
		if offset > uint64(c.maxBytes-ChunkBytes) || offset%ChunkBytes != 0 {
			return fmt.Errorf("tentative offset exceeds admission")
		}
		name := make([]byte, int(n))
		if _, err := io.ReadFull(input, name); err != nil {
			return err
		}
		if !validPath(string(name)) {
			return fmt.Errorf("invalid tentative file path")
		}
		if _, err := io.ReadFull(input, buffer); err != nil {
			return err
		}
		hash := digest.FromBytes(buffer).String()
		if hash != "sha256:"+hex.EncodeToString(frame[8:]) {
			return fmt.Errorf("tentative chunk digest mismatch")
		}
		if err := c.putVerified(ctx, string(name), int64(offset), buffer, hash); err != nil {
			return err
		}
	}
}

// WriteCapturePeerFinal rereads the final retained source and sends only ranges
// absent from the destination's tentative inventory. Even skipped bytes are
// checked at the source, and the receiver independently rehashes its local copy.
func (s *Store) WriteCapturePeerFinal(ctx context.Context, expected Binding, plan LocalImagePlan, directory string,
	inventory CapturePeerInventory, output io.Writer) error {
	manifest, payload, err := plan.snapshot(expected, s.maxBytes)
	if err != nil {
		return err
	}
	scope, err := captureScopeForBinding(expected)
	if err != nil {
		return err
	}
	if err := inventory.validate(scope, s.maxBytes); err != nil {
		return err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	files, err := s.imageFiles(ctx, root)
	if err != nil {
		return err
	}
	if len(files) != len(manifest.Files) {
		return fmt.Errorf("final source inventory changed")
	}
	for i, file := range files {
		if file.Path != manifest.Files[i].Path || file.Size != manifest.Files[i].Size {
			return fmt.Errorf("final source file changed")
		}
	}
	cached := make(map[string]File, len(inventory.Files))
	for _, file := range inventory.Files {
		cached[file.Path] = file
	}
	var header [12]byte
	copy(header[:8], captureFinalMagic)
	binary.BigEndian.PutUint32(header[8:], uint32(len(payload)))
	if err := writePeerBytes(ctx, output, header[:]); err != nil {
		return err
	}
	if err := writePeerBytes(ctx, output, payload); err != nil {
		return err
	}
	buffer := make([]byte, ChunkBytes)
	for _, file := range manifest.Files {
		prior := cached[file.Path]
		reused := false
		for i, chunk := range file.Chunks {
			if i < len(prior.Chunks) && prior.Chunks[i] == chunk {
				reused = true
				break
			}
		}
		input, err := root.Open(file.Path)
		if err != nil {
			return err
		}
		before, err := input.Stat()
		if err != nil {
			_ = input.Close()
			return err
		}
		err = func() error {
			defer input.Close()
			var offset int64
			for index, chunk := range file.Chunks {
				if err := ctx.Err(); err != nil {
					return err
				}
				mode := byte(0)
				if index < len(prior.Chunks) && prior.Chunks[index] == chunk {
					mode = 1
				}
				data := buffer[:int(chunk.Size)]
				if mode == 0 {
					if _, err := input.ReadAt(data, offset); err != nil {
						return err
					}
					if digest.FromBytes(data).String() != chunk.Digest {
						return fmt.Errorf("final source chunk changed")
					}
				}
				if err := writePeerBytes(ctx, output, []byte{mode}); err != nil {
					return err
				}
				if mode == 0 {
					if err := writePeerBytes(ctx, output, data); err != nil {
						return err
					}
				}
				offset += chunk.Size
			}
			var extra [1]byte
			if n, err := input.ReadAt(extra[:], file.Size); n != 0 || err != io.EOF {
				return fmt.Errorf("final source file grew")
			}
			after, err := input.Stat()
			if err != nil {
				return err
			}
			if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
				return fmt.Errorf("final source file changed during transfer")
			}
			return nil
		}()
		if err != nil {
			return err
		}
		// Send repair frames before checking reused source ranges so the target
		// can hash its retained file concurrently. Successful transport EOF is
		// still withheld until this complete source check passes. The transport
		// owner must abort, never close cleanly, on any returned error.
		if reused {
			// Buffered transports must expose the frames before the source scan,
			// otherwise the target would still wait for both checks in series.
			if flusher, ok := output.(interface{ Flush() error }); ok {
				if err := flusher.Flush(); err != nil {
					return err
				}
			}
			if _, err := scanImageFile(ctx, root, file, nil); err != nil {
				return err
			}
		}
	}
	return writePeerBytes(ctx, output, []byte(captureFinalComplete))
}
