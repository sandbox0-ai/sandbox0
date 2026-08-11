package s0fs

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
)

type stateV2SpoolEncoder struct {
	ctx     context.Context
	header  *StateV2Header
	aead    cipher.AEAD
	encoder *zstd.Encoder
	spool   *os.File
	offset  uint64
}

func writeMetadataStateV2(
	ctx context.Context,
	writer io.Writer,
	volumeID string,
	binding []byte,
	metadata metadataStore,
	nextSeq uint64,
	nextInode uint64,
	stateMetadata stateV2Metadata,
	encryption *EncryptionConfig,
) error {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(volumeID) == "" || metadata == nil {
		return fmt.Errorf("%w: volume id and metadata are required", ErrInvalidInput)
	}
	if stateMetadata.Role == StateV2Role_STATE_V2_ROLE_UNSPECIFIED {
		return fmt.Errorf("%w: state v2 role is required", ErrInvalidInput)
	}
	header := &StateV2Header{
		FormatVersion:    StateFormatV2,
		Role:             stateMetadata.Role,
		VolumeId:         volumeID,
		BindingSha256:    hashBytes(binding),
		NextSeq:          nextSeq,
		NextInode:        nextInode,
		ManifestSeq:      stateMetadata.ManifestSeq,
		CheckpointSeq:    stateMetadata.CheckpointSeq,
		CreatedAtSeconds: stateMetadata.CreatedAt.Unix(),
		CreatedAtNanos:   int32(stateMetadata.CreatedAt.Nanosecond()),
		Compression:      stateV2Compression,
	}
	var aead cipher.AEAD
	if encryption.enabled() {
		key, err := encryption.newDataKey()
		if err != nil {
			return err
		}
		wrappedKey, err := encryption.KeyEncryptor.Encrypt(key)
		if err != nil {
			return fmt.Errorf("wrap state v2 data key: %w", err)
		}
		aead, err = encryption.newAEAD(key)
		if err != nil {
			return err
		}
		noncePrefix := make([]byte, aead.NonceSize()-8)
		if _, err := io.ReadFull(rand.Reader, noncePrefix); err != nil {
			return err
		}
		header.EncryptionAlgorithm = encryption.normalizedAlgorithm()
		header.WrappedKey = wrappedKey
		header.NoncePrefix = noncePrefix
	}
	spool, err := os.CreateTemp("", "s0fs-state-v2-*.chunks")
	if err != nil {
		return fmt.Errorf("create state v2 chunk spool: %w", err)
	}
	spoolPath := spool.Name()
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spoolPath)
	}()
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("create state v2 compressor: %w", err)
	}
	defer encoder.Close()
	stream := &stateV2SpoolEncoder{ctx: ctx, header: header, aead: aead, encoder: encoder, spool: spool}
	if err := streamMetadataStateV2Chunks(metadata, stream.emit); err != nil {
		return err
	}
	if len(header.Chunks) == 0 {
		if err := stream.emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES, message: &StateV2Chunk{}}); err != nil {
			return err
		}
	}
	headerBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(header)
	if err != nil {
		return fmt.Errorf("encode state v2 header: %w", err)
	}
	if len(headerBytes) == 0 || len(headerBytes) > stateV2MaxHeaderBytes {
		return fmt.Errorf("%w: invalid state v2 header size %d", ErrInvalidInput, len(headerBytes))
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind state v2 chunk spool: %w", err)
	}
	if _, err := writer.Write(stateV2Magic[:]); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(headerBytes))); err != nil {
		return err
	}
	if _, err := writer.Write(headerBytes); err != nil {
		return err
	}
	headerDigest := sha256.Sum256(headerBytes)
	for _, descriptor := range header.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		compressedSize := descriptor.StoredSize
		if aead != nil {
			compressedSize -= uint64(aead.Overhead())
		}
		compressed := make([]byte, int(compressedSize))
		if _, err := io.ReadFull(spool, compressed); err != nil {
			return fmt.Errorf("read state v2 chunk spool: %w", err)
		}
		payload := compressed
		if aead != nil {
			nonce := segmentChunkNonce(aead.NonceSize(), header.NoncePrefix, uint64(descriptor.Index))
			payload = aead.Seal(nil, nonce, compressed, stateV2ChunkAAD(headerDigest, descriptor))
		}
		if _, err := writer.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (e *stateV2SpoolEncoder) emit(raw stateV2RawChunk) error {
	if err := e.ctx.Err(); err != nil {
		return err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(raw.message)
	if err != nil {
		return err
	}
	if len(encoded) > stateV2MaxEncodedChunkSize {
		return fmt.Errorf("%w: state v2 chunk is too large", ErrInvalidInput)
	}
	compressed := e.encoder.EncodeAll(encoded, nil)
	storedSize := len(compressed)
	if e.aead != nil {
		storedSize += e.aead.Overhead()
	}
	descriptor := &StateV2ChunkDescriptor{
		Kind: raw.kind, Index: uint32(len(e.header.Chunks)), Offset: e.offset,
		EncodedSize: uint64(len(encoded)), StoredSize: uint64(storedSize), RecordCount: raw.recordCount,
		FirstInode: raw.firstInode, LastInode: raw.lastInode,
	}
	if e.aead == nil {
		descriptor.EncodedSha256 = hashBytes(encoded)
	}
	if _, err := e.spool.Write(compressed); err != nil {
		return err
	}
	e.header.Chunks = append(e.header.Chunks, descriptor)
	e.offset += uint64(storedSize)
	return nil
}

func streamMetadataStateV2Chunks(metadata metadataStore, emit func(stateV2RawChunk) error) error {
	var generationErr error
	check := func() bool { return generationErr == nil && metadata.Err() == nil }

	nodes := &StateV2Chunk{}
	nodeBytes := 0
	var nodeFirst, nodeLast uint64
	flushNodes := func() {
		if generationErr != nil || len(nodes.Nodes) == 0 {
			return
		}
		generationErr = emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_NODES, message: nodes, recordCount: uint64(len(nodes.Nodes)), firstInode: nodeFirst, lastInode: nodeLast})
		nodes, nodeBytes, nodeFirst, nodeLast = &StateV2Chunk{}, 0, 0, 0
	}
	metadata.RangeNodes(func(inode uint64, node *Node) bool {
		if node == nil || node.Inode != inode {
			generationErr = fmt.Errorf("%w: node map key %d does not match its inode", ErrInvalidInput, inode)
			return false
		}
		recordSize := 96 + len(node.Target)
		if len(nodes.Nodes) > 0 && nodeBytes+recordSize > stateV2TargetChunkBytes {
			flushNodes()
		}
		if len(nodes.Nodes) == 0 {
			nodeFirst = inode
		}
		nodeLast = inode
		nodes.Nodes = append(nodes.Nodes, encodeStateV2Node(node))
		nodeBytes += recordSize
		return check()
	})
	flushNodes()
	if !check() {
		return firstMetadataStreamError(generationErr, metadata.Err())
	}

	directories := &StateV2Chunk{}
	directoryBytes := 0
	var directoryFirst, directoryLast uint64
	flushDirectories := func() {
		if generationErr != nil || len(directories.Directories) == 0 {
			return
		}
		generationErr = emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_DIRECTORIES, message: directories, recordCount: uint64(len(directories.Directories)), firstInode: directoryFirst, lastInode: directoryLast})
		directories, directoryBytes, directoryFirst, directoryLast = &StateV2Chunk{}, 0, 0, 0
	}
	appendDirectory := func(record *StateV2Directory, estimated int) {
		if len(directories.Directories) > 0 && directoryBytes+estimated > stateV2TargetChunkBytes {
			flushDirectories()
		}
		if generationErr != nil {
			return
		}
		if len(directories.Directories) == 0 {
			directoryFirst = record.ParentInode
		}
		directoryLast = record.ParentInode
		directories.Directories = append(directories.Directories, record)
		directoryBytes += estimated
	}
	var directory *StateV2Directory
	directoryRecordBytes := 0
	flushDirectoryRecord := func() {
		if directory != nil {
			appendDirectory(directory, directoryRecordBytes)
		}
		directory, directoryRecordBytes = nil, 0
	}
	metadata.RangeDirectoryRecords(func(parent uint64, name string, inode uint64, first bool) bool {
		if first {
			flushDirectoryRecord()
			directory = &StateV2Directory{ParentInode: parent}
			directoryRecordBytes = 16
		}
		if directory == nil || directory.ParentInode != parent {
			generationErr = fmt.Errorf("%w: invalid directory record order", ErrInvalidInput)
			return false
		}
		if name == "" {
			flushDirectoryRecord()
			return check()
		}
		recordSize := 16 + len(name)
		if len(directory.Children) > 0 && directoryRecordBytes+recordSize > stateV2TargetChunkBytes {
			flushDirectoryRecord()
			directory = &StateV2Directory{ParentInode: parent}
			directoryRecordBytes = 16
		}
		directory.Children = append(directory.Children, &StateV2Child{Name: name, Inode: inode})
		directoryRecordBytes += recordSize
		return check()
	})
	flushDirectoryRecord()
	flushDirectories()
	if !check() {
		return firstMetadataStreamError(generationErr, metadata.Err())
	}

	data := &StateV2Chunk{}
	dataBytes := 0
	var dataFirst, dataLast uint64
	flushData := func() {
		if generationErr != nil || len(data.Data) == 0 {
			return
		}
		generationErr = emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_DATA, message: data, recordCount: uint64(len(data.Data)), firstInode: dataFirst, lastInode: dataLast})
		data, dataBytes, dataFirst, dataLast = &StateV2Chunk{}, 0, 0, 0
	}
	appendData := func(record *StateV2Data, estimated int) {
		if len(data.Data) > 0 && dataBytes+estimated > stateV2TargetChunkBytes {
			flushData()
		}
		if generationErr != nil {
			return
		}
		if len(data.Data) == 0 {
			dataFirst = record.Inode
		}
		dataLast = record.Inode
		data.Data = append(data.Data, record)
		dataBytes += estimated
	}
	metadata.RangeData(func(inode uint64, payload []byte) bool {
		if len(payload) == 0 {
			appendData(&StateV2Data{Inode: inode}, 16)
			return check()
		}
		for offset := 0; offset < len(payload) && check(); offset += stateV2TargetChunkBytes {
			end := min(offset+stateV2TargetChunkBytes, len(payload))
			appendData(&StateV2Data{Inode: inode, Offset: uint64(offset), Payload: payload[offset:end]}, 24+end-offset)
		}
		return check()
	})
	flushData()
	if !check() {
		return firstMetadataStreamError(generationErr, metadata.Err())
	}

	cold := &StateV2Chunk{}
	coldBytes := 0
	var coldFirst, coldLast uint64
	flushCold := func() {
		if generationErr != nil || len(cold.ColdFiles) == 0 {
			return
		}
		generationErr = emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_COLD_FILES, message: cold, recordCount: uint64(len(cold.ColdFiles)), firstInode: coldFirst, lastInode: coldLast})
		cold, coldBytes, coldFirst, coldLast = &StateV2Chunk{}, 0, 0, 0
	}
	appendCold := func(record *StateV2ColdFile, estimated int) {
		if len(cold.ColdFiles) > 0 && coldBytes+estimated > stateV2TargetChunkBytes {
			flushCold()
		}
		if generationErr != nil {
			return
		}
		if len(cold.ColdFiles) == 0 {
			coldFirst = record.Inode
		}
		coldLast = record.Inode
		cold.ColdFiles = append(cold.ColdFiles, record)
		coldBytes += estimated
	}
	metadata.RangeColdFiles(func(inode uint64, extents []FileExtent) bool {
		if len(extents) == 0 {
			appendCold(&StateV2ColdFile{Inode: inode}, 16)
			return check()
		}
		for start := 0; start < len(extents) && check(); {
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
			appendCold(record, estimated)
		}
		return check()
	})
	flushCold()
	if !check() {
		return firstMetadataStreamError(generationErr, metadata.Err())
	}

	segments := &StateV2Chunk{}
	segmentBytes := 0
	flushSegments := func() {
		if generationErr != nil || len(segments.Segments) == 0 {
			return
		}
		generationErr = emit(stateV2RawChunk{kind: StateV2ChunkKind_STATE_V2_CHUNK_KIND_SEGMENTS, message: segments, recordCount: uint64(len(segments.Segments))})
		segments, segmentBytes = &StateV2Chunk{}, 0
	}
	metadata.RangeSegments(func(id string, segment *Segment) bool {
		if segment == nil || segment.ID != id {
			generationErr = fmt.Errorf("%w: segment map key %s does not match its id", ErrInvalidInput, id)
			return false
		}
		recordSize := 128 + len(id) + len(segment.VolumeID) + len(segment.Key) + len(segment.SHA256) + len(segment.InlineData)
		if len(segments.Segments) > 0 && segmentBytes+recordSize > stateV2TargetChunkBytes {
			flushSegments()
		}
		segments.Segments = append(segments.Segments, encodeStateV2Segment(segment))
		segmentBytes += recordSize
		return check()
	})
	flushSegments()
	return firstMetadataStreamError(generationErr, metadata.Err())
}

func firstMetadataStreamError(generationErr, metadataErr error) error {
	if generationErr != nil {
		return generationErr
	}
	return metadataErr
}
