package s0fs

import (
	"bufio"
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

var stateV2Magic = [8]byte{'S', '0', 'F', 'S', 'V', '2', '\r', '\n'}

const (
	stateV2Compression         = "zstd"
	stateV2TargetChunkBytes    = 1 << 20
	stateV2MaxHeaderBytes      = 16 << 20
	stateV2MaxEncodedChunkSize = 64 << 20
	stateV2MaxStoredChunkSize  = 72 << 20
	stateV2MaxPreallocatedRows = 1 << 20
)

type stateV2Metadata struct {
	Role          StateV2Role
	ManifestSeq   uint64
	CheckpointSeq uint64
	CreatedAt     time.Time
}

type stateV2DecodeResult struct {
	State    *SnapshotState
	Metadata stateV2Metadata
	Bytes    int64
}

type stateV2StreamResult struct {
	Header   *StateV2Header
	Metadata stateV2Metadata
	Bytes    int64
}

type stateV2RawChunk struct {
	kind        StateV2ChunkKind
	message     *StateV2Chunk
	recordCount uint64
	firstInode  uint64
	lastInode   uint64
}

type stateV2EncodedChunk struct {
	descriptor *StateV2ChunkDescriptor
	compressed []byte
}

func encodeStateV2(volumeID string, binding []byte, state *SnapshotState, metadata stateV2Metadata, encryption *EncryptionConfig) ([]byte, error) {
	if strings.TrimSpace(volumeID) == "" {
		return nil, fmt.Errorf("%w: volume id is required", ErrInvalidInput)
	}
	if state == nil {
		return nil, fmt.Errorf("%w: snapshot state is required", ErrInvalidInput)
	}
	if metadata.Role == StateV2Role_STATE_V2_ROLE_UNSPECIFIED {
		return nil, fmt.Errorf("%w: state v2 role is required", ErrInvalidInput)
	}
	rawChunks, err := buildStateV2Chunks(state)
	if err != nil {
		return nil, err
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return nil, fmt.Errorf("create state v2 compressor: %w", err)
	}
	defer encoder.Close()

	header := &StateV2Header{
		FormatVersion:    StateFormatV2,
		Role:             metadata.Role,
		VolumeId:         volumeID,
		BindingSha256:    hashBytes(binding),
		NextSeq:          state.NextSeq,
		NextInode:        state.NextInode,
		ManifestSeq:      metadata.ManifestSeq,
		CheckpointSeq:    metadata.CheckpointSeq,
		CreatedAtSeconds: metadata.CreatedAt.Unix(),
		CreatedAtNanos:   int32(metadata.CreatedAt.Nanosecond()),
		Compression:      stateV2Compression,
	}

	var aead cipher.AEAD
	if encryption.enabled() {
		key, err := encryption.newDataKey()
		if err != nil {
			return nil, err
		}
		wrappedKey, err := encryption.KeyEncryptor.Encrypt(key)
		if err != nil {
			return nil, fmt.Errorf("wrap state v2 data key: %w", err)
		}
		aead, err = encryption.newAEAD(key)
		if err != nil {
			return nil, err
		}
		noncePrefix := make([]byte, aead.NonceSize()-8)
		if _, err := io.ReadFull(rand.Reader, noncePrefix); err != nil {
			return nil, err
		}
		header.EncryptionAlgorithm = encryption.normalizedAlgorithm()
		header.WrappedKey = wrappedKey
		header.NoncePrefix = noncePrefix
	}

	marshal := proto.MarshalOptions{Deterministic: true}
	encodedChunks := make([]stateV2EncodedChunk, 0, len(rawChunks))
	var offset uint64
	for index, rawChunk := range rawChunks {
		encoded, err := marshal.Marshal(rawChunk.message)
		if err != nil {
			return nil, fmt.Errorf("encode state v2 chunk %d: %w", index, err)
		}
		if len(encoded) > stateV2MaxEncodedChunkSize {
			return nil, fmt.Errorf("%w: state v2 chunk %d is too large", ErrInvalidInput, index)
		}
		compressed := encoder.EncodeAll(encoded, nil)
		storedSize := len(compressed)
		if aead != nil {
			storedSize += aead.Overhead()
		}
		descriptor := &StateV2ChunkDescriptor{
			Kind:        rawChunk.kind,
			Index:       uint32(index),
			Offset:      offset,
			EncodedSize: uint64(len(encoded)),
			StoredSize:  uint64(storedSize),
			RecordCount: rawChunk.recordCount,
			FirstInode:  rawChunk.firstInode,
			LastInode:   rawChunk.lastInode,
		}
		if aead == nil {
			descriptor.EncodedSha256 = hashBytes(encoded)
		}
		header.Chunks = append(header.Chunks, descriptor)
		encodedChunks = append(encodedChunks, stateV2EncodedChunk{descriptor: descriptor, compressed: compressed})
		offset += uint64(storedSize)
	}

	headerBytes, err := marshal.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encode state v2 header: %w", err)
	}
	if len(headerBytes) == 0 || len(headerBytes) > stateV2MaxHeaderBytes {
		return nil, fmt.Errorf("%w: invalid state v2 header size %d", ErrInvalidInput, len(headerBytes))
	}
	headerDigest := sha256.Sum256(headerBytes)
	var out bytes.Buffer
	out.Grow(len(stateV2Magic) + 4 + len(headerBytes) + int(offset))
	out.Write(stateV2Magic[:])
	_ = binary.Write(&out, binary.LittleEndian, uint32(len(headerBytes)))
	out.Write(headerBytes)
	for _, encodedChunk := range encodedChunks {
		payload := encodedChunk.compressed
		if aead != nil {
			descriptor := encodedChunk.descriptor
			nonce := segmentChunkNonce(aead.NonceSize(), header.NoncePrefix, uint64(descriptor.Index))
			payload = aead.Seal(nil, nonce, payload, stateV2ChunkAAD(headerDigest, descriptor))
		}
		out.Write(payload)
	}
	return out.Bytes(), nil
}

func decodeStateV2(reader io.Reader, expectedVolumeID string, binding []byte, expectedRole StateV2Role, encryption *EncryptionConfig) (*stateV2DecodeResult, error) {
	return decodeStateV2Context(context.Background(), reader, expectedVolumeID, binding, expectedRole, encryption)
}

func decodeStateV2Context(ctx context.Context, reader io.Reader, expectedVolumeID string, binding []byte, expectedRole StateV2Role, encryption *EncryptionConfig) (*stateV2DecodeResult, error) {
	var state *SnapshotState
	stream, err := streamStateV2Chunks(ctx, reader, expectedVolumeID, binding, expectedRole, encryption, func(header *StateV2Header, descriptor *StateV2ChunkDescriptor, chunk *StateV2Chunk) error {
		if state == nil {
			state = newStateV2Snapshot(header)
		}
		return applyStateV2Chunk(state, descriptor, chunk)
	})
	if err != nil {
		return nil, err
	}
	if state == nil {
		state = newStateV2Snapshot(stream.Header)
	}
	normalizeState(state)
	return &stateV2DecodeResult{State: state, Metadata: stream.Metadata, Bytes: stream.Bytes}, nil
}

func newStateV2Snapshot(header *StateV2Header) *SnapshotState {
	return &SnapshotState{
		NextSeq:   header.NextSeq,
		NextInode: header.NextInode,
		Nodes:     make(map[uint64]*Node, stateV2RecordCapacity(header.Chunks, StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES)),
		Children:  make(map[uint64]map[string]uint64),
		Data:      make(map[uint64][]byte),
		ColdFiles: make(map[uint64][]FileExtent),
		Segments:  make(map[string]*Segment, stateV2RecordCapacity(header.Chunks, StateV2ChunkKind_STATE_V2_CHUNK_KIND_SEGMENTS)),
	}
}

func streamStateV2Chunks(
	ctx context.Context,
	reader io.Reader,
	expectedVolumeID string,
	binding []byte,
	expectedRole StateV2Role,
	encryption *EncryptionConfig,
	visit func(*StateV2Header, *StateV2ChunkDescriptor, *StateV2Chunk) error,
) (*stateV2StreamResult, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	counted := &countingReader{reader: reader}
	magic := make([]byte, len(stateV2Magic))
	if _, err := io.ReadFull(counted, magic); err != nil {
		return nil, fmt.Errorf("read state v2 magic: %w", err)
	}
	if !bytes.Equal(magic, stateV2Magic[:]) {
		return nil, fmt.Errorf("%w: invalid state v2 magic", ErrInvalidInput)
	}
	var headerSize uint32
	if err := binary.Read(counted, binary.LittleEndian, &headerSize); err != nil {
		return nil, fmt.Errorf("read state v2 header size: %w", err)
	}
	if headerSize == 0 || headerSize > stateV2MaxHeaderBytes {
		return nil, fmt.Errorf("%w: invalid state v2 header size %d", ErrInvalidInput, headerSize)
	}
	headerBytes := make([]byte, int(headerSize))
	if _, err := io.ReadFull(counted, headerBytes); err != nil {
		return nil, fmt.Errorf("read state v2 header: %w", err)
	}
	var header StateV2Header
	if err := proto.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("decode state v2 header: %w", err)
	}
	if err := validateStateV2Header(&header, expectedVolumeID, binding, expectedRole); err != nil {
		return nil, err
	}
	headerDigest := sha256.Sum256(headerBytes)
	var aead cipher.AEAD
	if header.EncryptionAlgorithm != "" {
		if !encryption.enabled() {
			return nil, fmt.Errorf("%w: encrypted state v2 requires encryption config", ErrInvalidInput)
		}
		key, err := encryption.KeyEncryptor.Decrypt(header.WrappedKey)
		if err != nil {
			return nil, fmt.Errorf("unwrap state v2 data key: %w", err)
		}
		aead, err = newAEADForAlgorithm(header.EncryptionAlgorithm, key)
		if err != nil {
			return nil, err
		}
		if len(header.NoncePrefix) != aead.NonceSize()-8 {
			return nil, fmt.Errorf("%w: invalid state v2 nonce prefix", ErrInvalidInput)
		}
	} else if len(header.WrappedKey) != 0 || len(header.NoncePrefix) != 0 {
		return nil, fmt.Errorf("%w: unencrypted state v2 has encryption metadata", ErrInvalidInput)
	}

	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(stateV2MaxEncodedChunkSize*2))
	if err != nil {
		return nil, fmt.Errorf("create state v2 decompressor: %w", err)
	}
	defer decoder.Close()
	var expectedOffset uint64
	var previousKind StateV2ChunkKind
	for index, descriptor := range header.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateStateV2ChunkDescriptor(descriptor, index, expectedOffset, aead != nil); err != nil {
			return nil, err
		}
		if descriptor.Kind < previousKind {
			return nil, fmt.Errorf("%w: state v2 chunks are not ordered by kind", ErrInvalidInput)
		}
		previousKind = descriptor.Kind
		stored := make([]byte, int(descriptor.StoredSize))
		if _, err := io.ReadFull(counted, stored); err != nil {
			return nil, fmt.Errorf("read state v2 chunk %d: %w", index, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expectedOffset += descriptor.StoredSize
		compressed := stored
		if aead != nil {
			nonce := segmentChunkNonce(aead.NonceSize(), header.NoncePrefix, uint64(descriptor.Index))
			compressed, err = aead.Open(nil, nonce, stored, stateV2ChunkAAD(headerDigest, descriptor))
			if err != nil {
				return nil, fmt.Errorf("decrypt state v2 chunk %d: %w", index, err)
			}
		}
		encoded, err := decoder.DecodeAll(compressed, nil)
		if err != nil {
			return nil, fmt.Errorf("decompress state v2 chunk %d: %w", index, err)
		}
		if uint64(len(encoded)) != descriptor.EncodedSize || (aead == nil && !bytes.Equal(hashBytes(encoded), descriptor.EncodedSha256)) {
			return nil, fmt.Errorf("%w: state v2 chunk %d checksum mismatch", ErrInvalidInput, index)
		}
		var chunk StateV2Chunk
		if err := proto.Unmarshal(encoded, &chunk); err != nil {
			return nil, fmt.Errorf("decode state v2 chunk %d: %w", index, err)
		}
		if visit != nil {
			if err := visit(&header, descriptor, &chunk); err != nil {
				return nil, fmt.Errorf("apply state v2 chunk %d: %w", index, err)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	var trailing [1]byte
	if n, err := counted.Read(trailing[:]); n != 0 || (err != nil && !errors.Is(err, io.EOF)) {
		return nil, fmt.Errorf("%w: state v2 has trailing payload", ErrInvalidInput)
	}
	return &stateV2StreamResult{
		Header: &header,
		Metadata: stateV2Metadata{
			Role:          header.Role,
			ManifestSeq:   header.ManifestSeq,
			CheckpointSeq: header.CheckpointSeq,
			CreatedAt:     time.Unix(header.CreatedAtSeconds, int64(header.CreatedAtNanos)).UTC(),
		},
		Bytes: counted.count,
	}, nil
}

func buildStateV2Chunks(state *SnapshotState) ([]stateV2RawChunk, error) {
	chunks := make([]stateV2RawChunk, 0)
	nodeIDs := sortedUint64Keys(state.Nodes)
	for start := 0; start < len(nodeIDs); {
		chunk := &StateV2Chunk{}
		estimated := 0
		first := nodeIDs[start]
		last := first
		for start < len(nodeIDs) {
			inode := nodeIDs[start]
			node := state.Nodes[inode]
			if node == nil || node.Inode != inode {
				return nil, fmt.Errorf("%w: node map key %d does not match its inode", ErrInvalidInput, inode)
			}
			record := encodeStateV2Node(node)
			recordSize := 96 + len(node.Target)
			if estimated > 0 && estimated+recordSize > stateV2TargetChunkBytes {
				break
			}
			chunk.Nodes = append(chunk.Nodes, record)
			estimated += recordSize
			last = inode
			start++
		}
		chunks = append(chunks, stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES, message: chunk, recordCount: uint64(len(chunk.Nodes)), firstInode: first, lastInode: last})
	}

	parentIDs := sortedUint64Keys(state.Children)
	directoryChunk := &StateV2Chunk{}
	directoryChunkBytes := 0
	var directoryFirst, directoryLast uint64
	flushDirectories := func() {
		if len(directoryChunk.Directories) == 0 {
			return
		}
		chunks = append(chunks, stateV2RawChunk{
			kind:        StateV2ChunkKind_STATE_V2_CHUNK_KIND_DIRECTORIES,
			message:     directoryChunk,
			recordCount: uint64(len(directoryChunk.Directories)),
			firstInode:  directoryFirst,
			lastInode:   directoryLast,
		})
		directoryChunk = &StateV2Chunk{}
		directoryChunkBytes = 0
		directoryFirst = 0
		directoryLast = 0
	}
	appendDirectory := func(record *StateV2Directory, estimated int) {
		if len(directoryChunk.Directories) > 0 && directoryChunkBytes+estimated > stateV2TargetChunkBytes {
			flushDirectories()
		}
		if len(directoryChunk.Directories) == 0 {
			directoryFirst = record.ParentInode
		}
		directoryLast = record.ParentInode
		directoryChunk.Directories = append(directoryChunk.Directories, record)
		directoryChunkBytes += estimated
	}
	for _, parent := range parentIDs {
		names := sortedStringKeys(state.Children[parent])
		if len(names) == 0 {
			appendDirectory(&StateV2Directory{ParentInode: parent}, 16)
			continue
		}
		for start := 0; start < len(names); {
			record := &StateV2Directory{ParentInode: parent}
			estimated := 16
			for start < len(names) {
				name := names[start]
				recordSize := 16 + len(name)
				if len(record.Children) > 0 && estimated+recordSize > stateV2TargetChunkBytes {
					break
				}
				record.Children = append(record.Children, &StateV2Child{Name: name, Inode: state.Children[parent][name]})
				estimated += recordSize
				start++
			}
			appendDirectory(record, estimated)
		}
	}
	flushDirectories()

	dataIDs := sortedUint64Keys(state.Data)
	dataChunk := &StateV2Chunk{}
	dataChunkBytes := 0
	var dataFirst, dataLast uint64
	flushData := func() {
		if len(dataChunk.Data) == 0 {
			return
		}
		chunks = append(chunks, stateV2RawChunk{
			kind:        StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA,
			message:     dataChunk,
			recordCount: uint64(len(dataChunk.Data)),
			firstInode:  dataFirst,
			lastInode:   dataLast,
		})
		dataChunk = &StateV2Chunk{}
		dataChunkBytes = 0
		dataFirst = 0
		dataLast = 0
	}
	appendData := func(record *StateV2Data, estimated int) {
		if len(dataChunk.Data) > 0 && dataChunkBytes+estimated > stateV2TargetChunkBytes {
			flushData()
		}
		if len(dataChunk.Data) == 0 {
			dataFirst = record.Inode
		}
		dataLast = record.Inode
		dataChunk.Data = append(dataChunk.Data, record)
		dataChunkBytes += estimated
	}
	for _, inode := range dataIDs {
		payload := state.Data[inode]
		if len(payload) == 0 {
			appendData(&StateV2Data{Inode: inode}, 16)
			continue
		}
		for offset := 0; offset < len(payload); offset += stateV2TargetChunkBytes {
			end := offset + stateV2TargetChunkBytes
			if end > len(payload) {
				end = len(payload)
			}
			appendData(&StateV2Data{Inode: inode, Offset: uint64(offset), Payload: payload[offset:end]}, 24+end-offset)
		}
	}
	flushData()

	coldIDs := sortedUint64Keys(state.ColdFiles)
	coldChunk := &StateV2Chunk{}
	coldChunkBytes := 0
	var coldFirst, coldLast uint64
	flushColdFiles := func() {
		if len(coldChunk.ColdFiles) == 0 {
			return
		}
		chunks = append(chunks, stateV2RawChunk{
			kind:        StateV2ChunkKind_STATE_V2_CHUNK_KIND_COLD_FILES,
			message:     coldChunk,
			recordCount: uint64(len(coldChunk.ColdFiles)),
			firstInode:  coldFirst,
			lastInode:   coldLast,
		})
		coldChunk = &StateV2Chunk{}
		coldChunkBytes = 0
		coldFirst = 0
		coldLast = 0
	}
	appendColdFile := func(record *StateV2ColdFile, estimated int) {
		if len(coldChunk.ColdFiles) > 0 && coldChunkBytes+estimated > stateV2TargetChunkBytes {
			flushColdFiles()
		}
		if len(coldChunk.ColdFiles) == 0 {
			coldFirst = record.Inode
		}
		coldLast = record.Inode
		coldChunk.ColdFiles = append(coldChunk.ColdFiles, record)
		coldChunkBytes += estimated
	}
	for _, inode := range coldIDs {
		extents := state.ColdFiles[inode]
		if len(extents) == 0 {
			appendColdFile(&StateV2ColdFile{Inode: inode}, 16)
			continue
		}
		for start := 0; start < len(extents); {
			record := &StateV2ColdFile{Inode: inode}
			estimated := 16
			for start < len(extents) {
				extent := extents[start]
				recordSize := 32 + len(extent.SegmentID)
				if len(record.Extents) > 0 && estimated+recordSize > stateV2TargetChunkBytes {
					break
				}
				record.Extents = append(record.Extents, &StateV2Extent{SegmentId: extent.SegmentID, Offset: extent.Offset, Length: extent.Length})
				estimated += recordSize
				start++
			}
			appendColdFile(record, estimated)
		}
	}
	flushColdFiles()

	segmentIDs := sortedStringKeys(state.Segments)
	for start := 0; start < len(segmentIDs); {
		chunk := &StateV2Chunk{}
		estimated := 0
		for start < len(segmentIDs) {
			id := segmentIDs[start]
			segment := state.Segments[id]
			if segment == nil || segment.ID != id {
				return nil, fmt.Errorf("%w: segment map key %s does not match its id", ErrInvalidInput, id)
			}
			recordSize := 128 + len(id) + len(segment.VolumeID) + len(segment.Key) + len(segment.SHA256) + len(segment.InlineData)
			if estimated > 0 && estimated+recordSize > stateV2TargetChunkBytes {
				break
			}
			chunk.Segments = append(chunk.Segments, encodeStateV2Segment(segment))
			estimated += recordSize
			start++
		}
		chunks = append(chunks, stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_SEGMENTS, message: chunk, recordCount: uint64(len(chunk.Segments))})
	}
	if len(chunks) == 0 {
		chunks = append(chunks, stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES, message: &StateV2Chunk{}})
	}
	return chunks, nil
}

func validateStateV2Header(header *StateV2Header, expectedVolumeID string, binding []byte, expectedRole StateV2Role) error {
	if header == nil || header.FormatVersion != StateFormatV2 {
		return fmt.Errorf("%w: unsupported state v2 format version", ErrInvalidInput)
	}
	if header.Role != expectedRole || header.Role == StateV2Role_STATE_V2_ROLE_UNSPECIFIED {
		return fmt.Errorf("%w: state v2 role mismatch", ErrInvalidInput)
	}
	if header.VolumeId != expectedVolumeID || strings.TrimSpace(header.VolumeId) == "" {
		return fmt.Errorf("%w: state v2 volume mismatch", ErrInvalidInput)
	}
	if !bytes.Equal(header.BindingSha256, hashBytes(binding)) {
		return fmt.Errorf("%w: state v2 binding mismatch", ErrInvalidInput)
	}
	if header.Compression != stateV2Compression {
		return fmt.Errorf("%w: unsupported state v2 compression %q", ErrInvalidInput, header.Compression)
	}
	if header.CreatedAtNanos < 0 || header.CreatedAtNanos >= int32(time.Second) {
		return fmt.Errorf("%w: invalid state v2 creation time", ErrInvalidInput)
	}
	if header.Role == StateV2Role_STATE_V2_ROLE_MANIFEST {
		if header.ManifestSeq == 0 || header.CheckpointSeq != header.ManifestSeq || header.CreatedAtSeconds == 0 {
			return fmt.Errorf("%w: invalid state v2 manifest metadata", ErrInvalidInput)
		}
	} else if header.ManifestSeq != 0 || header.CheckpointSeq != 0 {
		return fmt.Errorf("%w: non-manifest state v2 has manifest metadata", ErrInvalidInput)
	}
	return nil
}

func validateStateV2ChunkDescriptor(descriptor *StateV2ChunkDescriptor, index int, expectedOffset uint64, encrypted bool) error {
	if descriptor == nil || descriptor.Index != uint32(index) || descriptor.Offset != expectedOffset {
		return fmt.Errorf("%w: invalid state v2 chunk descriptor %d", ErrInvalidInput, index)
	}
	if descriptor.Kind == StateV2ChunkKind_STATE_V2_CHUNK_KIND_UNSPECIFIED || descriptor.EncodedSize > stateV2MaxEncodedChunkSize || descriptor.StoredSize == 0 || descriptor.StoredSize > stateV2MaxStoredChunkSize {
		return fmt.Errorf("%w: invalid state v2 chunk size or kind at %d", ErrInvalidInput, index)
	}
	if (!encrypted && len(descriptor.EncodedSha256) != sha256.Size) || (encrypted && len(descriptor.EncodedSha256) != 0) {
		return fmt.Errorf("%w: invalid state v2 chunk checksum at %d", ErrInvalidInput, index)
	}
	if descriptor.StoredSize > ^uint64(0)-expectedOffset {
		return fmt.Errorf("%w: state v2 chunk offset overflow at %d", ErrInvalidInput, index)
	}
	return nil
}

func applyStateV2Chunk(state *SnapshotState, descriptor *StateV2ChunkDescriptor, chunk *StateV2Chunk) error {
	if state == nil || descriptor == nil || chunk == nil {
		return fmt.Errorf("%w: state v2 chunk is required", ErrInvalidInput)
	}
	switch descriptor.Kind {
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES:
		if len(chunk.Directories)+len(chunk.Data)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Nodes)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed node chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Nodes {
			node, err := decodeStateV2Node(record)
			if err != nil {
				return err
			}
			if _, exists := state.Nodes[node.Inode]; exists {
				return fmt.Errorf("%w: duplicate inode %d", ErrInvalidInput, node.Inode)
			}
			state.Nodes[node.Inode] = node
		}
		if err := validateStateV2InodeRange(descriptor, nodeChunkInodeRange(chunk.Nodes)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_DIRECTORIES:
		if len(chunk.Nodes)+len(chunk.Data)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Directories)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed directory chunk", ErrInvalidInput)
		}
		for _, directory := range chunk.Directories {
			if directory == nil || directory.ParentInode == 0 {
				return fmt.Errorf("%w: invalid directory record", ErrInvalidInput)
			}
			children, exists := state.Children[directory.ParentInode]
			if exists && (len(children) == 0 || len(directory.Children) == 0) {
				return fmt.Errorf("%w: duplicate empty directory record", ErrInvalidInput)
			}
			if !exists {
				children = make(map[string]uint64, len(directory.Children))
				state.Children[directory.ParentInode] = children
			}
			for _, child := range directory.Children {
				if child == nil || child.Name == "" || child.Inode == 0 {
					return fmt.Errorf("%w: invalid directory child", ErrInvalidInput)
				}
				if _, exists := children[child.Name]; exists {
					return fmt.Errorf("%w: duplicate directory child %q", ErrInvalidInput, child.Name)
				}
				children[child.Name] = child.Inode
			}
		}
		if err := validateStateV2InodeRange(descriptor, directoryChunkInodeRange(chunk.Directories)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.ColdFiles)+len(chunk.Segments) != 0 || uint64(len(chunk.Data)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed data chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Data {
			if record == nil || record.Inode == 0 {
				return fmt.Errorf("%w: invalid inline data record", ErrInvalidInput)
			}
			data, exists := state.Data[record.Inode]
			if (exists && (len(data) == 0 || len(record.Payload) == 0)) || uint64(len(data)) != record.Offset {
				return fmt.Errorf("%w: duplicate or non-contiguous inline data record", ErrInvalidInput)
			}
			if !exists {
				state.Data[record.Inode] = make([]byte, 0, len(record.Payload))
			}
			state.Data[record.Inode] = append(state.Data[record.Inode], record.Payload...)
		}
		if err := validateStateV2InodeRange(descriptor, dataChunkInodeRange(chunk.Data)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_COLD_FILES:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.Data)+len(chunk.Segments) != 0 || uint64(len(chunk.ColdFiles)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed cold-file chunk", ErrInvalidInput)
		}
		for _, record := range chunk.ColdFiles {
			if record == nil || record.Inode == 0 {
				return fmt.Errorf("%w: invalid cold-file record", ErrInvalidInput)
			}
			extents, exists := state.ColdFiles[record.Inode]
			if exists && (len(extents) == 0 || len(record.Extents) == 0) {
				return fmt.Errorf("%w: duplicate empty cold-file record", ErrInvalidInput)
			}
			if !exists {
				extents = make([]FileExtent, 0, len(record.Extents))
			}
			for _, extent := range record.Extents {
				if extent == nil {
					return fmt.Errorf("%w: nil cold-file extent", ErrInvalidInput)
				}
				extents = append(extents, FileExtent{SegmentID: extent.SegmentId, Offset: extent.Offset, Length: extent.Length})
			}
			state.ColdFiles[record.Inode] = extents
		}
		if err := validateStateV2InodeRange(descriptor, coldFileChunkInodeRange(chunk.ColdFiles)); err != nil {
			return err
		}
	case StateV2ChunkKind_STATE_V2_CHUNK_KIND_SEGMENTS:
		if len(chunk.Nodes)+len(chunk.Directories)+len(chunk.Data)+len(chunk.ColdFiles) != 0 || uint64(len(chunk.Segments)) != descriptor.RecordCount {
			return fmt.Errorf("%w: malformed segment chunk", ErrInvalidInput)
		}
		for _, record := range chunk.Segments {
			segment, err := decodeStateV2Segment(record)
			if err != nil {
				return err
			}
			if _, exists := state.Segments[segment.ID]; exists {
				return fmt.Errorf("%w: duplicate segment %s", ErrInvalidInput, segment.ID)
			}
			state.Segments[segment.ID] = segment
		}
		if descriptor.FirstInode != 0 || descriptor.LastInode != 0 {
			return fmt.Errorf("%w: segment chunk has an inode range", ErrInvalidInput)
		}
	default:
		return fmt.Errorf("%w: unsupported state v2 chunk kind %d", ErrInvalidInput, descriptor.Kind)
	}
	return nil
}

type stateV2InodeRange struct {
	first uint64
	last  uint64
	valid bool
}

func validateStateV2InodeRange(descriptor *StateV2ChunkDescriptor, inodeRange stateV2InodeRange) error {
	if !inodeRange.valid {
		if descriptor.FirstInode == 0 && descriptor.LastInode == 0 {
			return nil
		}
		return fmt.Errorf("%w: empty state v2 chunk has an inode range", ErrInvalidInput)
	}
	if descriptor.FirstInode != inodeRange.first || descriptor.LastInode != inodeRange.last || inodeRange.first == 0 || inodeRange.last < inodeRange.first {
		return fmt.Errorf("%w: state v2 chunk inode range mismatch", ErrInvalidInput)
	}
	return nil
}

func nodeChunkInodeRange(records []*StateV2Node) stateV2InodeRange {
	if len(records) == 0 || records[0] == nil || records[len(records)-1] == nil {
		return stateV2InodeRange{}
	}
	return stateV2InodeRange{first: records[0].Inode, last: records[len(records)-1].Inode, valid: true}
}

func directoryChunkInodeRange(records []*StateV2Directory) stateV2InodeRange {
	if len(records) == 0 || records[0] == nil || records[len(records)-1] == nil {
		return stateV2InodeRange{}
	}
	return stateV2InodeRange{first: records[0].ParentInode, last: records[len(records)-1].ParentInode, valid: true}
}

func dataChunkInodeRange(records []*StateV2Data) stateV2InodeRange {
	if len(records) == 0 || records[0] == nil || records[len(records)-1] == nil {
		return stateV2InodeRange{}
	}
	return stateV2InodeRange{first: records[0].Inode, last: records[len(records)-1].Inode, valid: true}
}

func coldFileChunkInodeRange(records []*StateV2ColdFile) stateV2InodeRange {
	if len(records) == 0 || records[0] == nil || records[len(records)-1] == nil {
		return stateV2InodeRange{}
	}
	return stateV2InodeRange{first: records[0].Inode, last: records[len(records)-1].Inode, valid: true}
}

func encodeStateV2Node(node *Node) *StateV2Node {
	return &StateV2Node{
		Inode:        node.Inode,
		Type:         encodeStateV2FileType(node.Type),
		Mode:         node.Mode,
		Uid:          node.UID,
		Gid:          node.GID,
		Nlink:        node.Nlink,
		Size:         node.Size,
		Target:       node.Target,
		AtimeSeconds: node.Atime.Unix(),
		AtimeNanos:   int32(node.Atime.Nanosecond()),
		MtimeSeconds: node.Mtime.Unix(),
		MtimeNanos:   int32(node.Mtime.Nanosecond()),
		CtimeSeconds: node.Ctime.Unix(),
		CtimeNanos:   int32(node.Ctime.Nanosecond()),
	}
}

func decodeStateV2Node(record *StateV2Node) (*Node, error) {
	if record == nil || record.Inode == 0 {
		return nil, fmt.Errorf("%w: invalid state v2 node", ErrInvalidInput)
	}
	fileType, err := decodeStateV2FileType(record.Type)
	if err != nil {
		return nil, err
	}
	for _, nanos := range []int32{record.AtimeNanos, record.MtimeNanos, record.CtimeNanos} {
		if nanos < 0 || nanos >= int32(time.Second) {
			return nil, fmt.Errorf("%w: invalid state v2 node timestamp", ErrInvalidInput)
		}
	}
	return &Node{
		Inode:  record.Inode,
		Type:   fileType,
		Mode:   record.Mode,
		UID:    record.Uid,
		GID:    record.Gid,
		Nlink:  record.Nlink,
		Size:   record.Size,
		Target: record.Target,
		Atime:  time.Unix(record.AtimeSeconds, int64(record.AtimeNanos)).UTC(),
		Mtime:  time.Unix(record.MtimeSeconds, int64(record.MtimeNanos)).UTC(),
		Ctime:  time.Unix(record.CtimeSeconds, int64(record.CtimeNanos)).UTC(),
	}, nil
}

func encodeStateV2Segment(segment *Segment) *StateV2Segment {
	record := &StateV2Segment{
		Id:         segment.ID,
		VolumeId:   segment.VolumeID,
		Key:        segment.Key,
		Length:     segment.Length,
		Sha256:     segment.SHA256,
		InlineData: segment.InlineData,
	}
	if segment.Encryption != nil {
		record.Encryption = &StateV2SegmentEncryption{
			Version:        uint32(segment.Encryption.Version),
			Algorithm:      segment.Encryption.Algorithm,
			ChunkSize:      segment.Encryption.ChunkSize,
			PlaintextSize:  segment.Encryption.PlaintextSize,
			CiphertextSize: segment.Encryption.CiphertextSize,
			WrappedKey:     segment.Encryption.WrappedKey,
			NoncePrefix:    segment.Encryption.NoncePrefix,
		}
	}
	return record
}

func decodeStateV2Segment(record *StateV2Segment) (*Segment, error) {
	if record == nil || strings.TrimSpace(record.Id) == "" {
		return nil, fmt.Errorf("%w: invalid state v2 segment", ErrInvalidInput)
	}
	segment := &Segment{
		ID:         record.Id,
		VolumeID:   record.VolumeId,
		Key:        record.Key,
		Length:     record.Length,
		SHA256:     record.Sha256,
		InlineData: record.InlineData,
	}
	if record.Encryption != nil {
		segment.Encryption = &SegmentEncryption{
			Version:        int(record.Encryption.Version),
			Algorithm:      record.Encryption.Algorithm,
			ChunkSize:      record.Encryption.ChunkSize,
			PlaintextSize:  record.Encryption.PlaintextSize,
			CiphertextSize: record.Encryption.CiphertextSize,
			WrappedKey:     record.Encryption.WrappedKey,
			NoncePrefix:    record.Encryption.NoncePrefix,
		}
	}
	return segment, nil
}

func encodeStateV2FileType(fileType FileType) StateV2FileType {
	switch fileType {
	case TypeDirectory:
		return StateV2FileType_STATE_V2_FILE_TYPE_DIRECTORY
	case TypeFile:
		return StateV2FileType_STATE_V2_FILE_TYPE_FILE
	case TypeSymlink:
		return StateV2FileType_STATE_V2_FILE_TYPE_SYMLINK
	default:
		return StateV2FileType_STATE_V2_FILE_TYPE_UNSPECIFIED
	}
}

func decodeStateV2FileType(fileType StateV2FileType) (FileType, error) {
	switch fileType {
	case StateV2FileType_STATE_V2_FILE_TYPE_DIRECTORY:
		return TypeDirectory, nil
	case StateV2FileType_STATE_V2_FILE_TYPE_FILE:
		return TypeFile, nil
	case StateV2FileType_STATE_V2_FILE_TYPE_SYMLINK:
		return TypeSymlink, nil
	default:
		return "", fmt.Errorf("%w: unsupported state v2 file type %d", ErrInvalidInput, fileType)
	}
}

func stateV2ChunkAAD(headerDigest [sha256.Size]byte, descriptor *StateV2ChunkDescriptor) []byte {
	aad := make([]byte, 0, 16+sha256.Size+32)
	aad = append(aad, []byte("s0fs.state.v2")...)
	aad = append(aad, headerDigest[:]...)
	var fields [32]byte
	binary.LittleEndian.PutUint32(fields[0:4], descriptor.Index)
	binary.LittleEndian.PutUint32(fields[4:8], uint32(descriptor.Kind))
	binary.LittleEndian.PutUint64(fields[8:16], descriptor.Offset)
	binary.LittleEndian.PutUint64(fields[16:24], descriptor.EncodedSize)
	binary.LittleEndian.PutUint64(fields[24:32], descriptor.StoredSize)
	return append(aad, fields[:]...)
}

func stateV2RoleForStateRole(role string) StateV2Role {
	if role == "head" {
		return StateV2Role_STATE_V2_ROLE_HEAD
	}
	if strings.HasPrefix(role, "snapshot:") {
		return StateV2Role_STATE_V2_ROLE_SNAPSHOT
	}
	return StateV2Role_STATE_V2_ROLE_UNSPECIFIED
}

func hasStateV2Magic(reader *bufio.Reader) (bool, error) {
	prefix, err := reader.Peek(len(stateV2Magic))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, bufio.ErrBufferFull) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(prefix, stateV2Magic[:]), nil
}

func stateV2RecordCapacity(descriptors []*StateV2ChunkDescriptor, kind StateV2ChunkKind) int {
	var total uint64
	for _, descriptor := range descriptors {
		if descriptor != nil && descriptor.Kind == kind {
			total += descriptor.RecordCount
		}
	}
	if total > stateV2MaxPreallocatedRows {
		return 0
	}
	return int(total)
}

func sortedUint64Keys[T any](values map[uint64]T) []uint64 {
	keys := make([]uint64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sortedStringKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func hashBytes(payload []byte) []byte {
	digest := sha256.Sum256(payload)
	return digest[:]
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (r *countingReader) Read(payload []byte) (int, error) {
	n, err := r.reader.Read(payload)
	r.count += int64(n)
	return n, err
}
