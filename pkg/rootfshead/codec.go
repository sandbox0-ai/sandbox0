package rootfshead

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

var ErrMetadataObjectTooLarge = fmt.Errorf("rootfs metadata exceeds %d bytes", MaxMetadataObjectBytes)

var gzipWriterPool = sync.Pool{New: func() any {
	writer, _ := gzip.NewWriterLevel(io.Discard, gzip.BestSpeed)
	return writer
}}

func EncodeHead(value Head) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeHead(reader io.Reader) (Head, error) {
	var value Head
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return Head{}, fmt.Errorf("decode rootfs head: %w", err)
	}
	if err := value.Validate(); err != nil {
		return Head{}, err
	}
	return value, nil
}

func EncodeDirectoryIndex(value DirectoryIndex) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeDirectoryIndex(reader io.Reader) (DirectoryIndex, error) {
	var value DirectoryIndex
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return DirectoryIndex{}, fmt.Errorf("decode rootfs directory index: %w", err)
	}
	if err := value.Validate(); err != nil {
		return DirectoryIndex{}, err
	}
	return value, nil
}

func EncodeDirectoryShard(value DirectoryShard) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeDirectoryShard(reader io.Reader) (DirectoryShard, error) {
	var value DirectoryShard
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return DirectoryShard{}, fmt.Errorf("decode rootfs directory shard: %w", err)
	}
	if err := value.Validate(); err != nil {
		return DirectoryShard{}, err
	}
	return value, nil
}

func EncodeFileManifest(value FileManifest) ([]byte, error) {
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return encodeGZIPJSON(value)
}

func DecodeFileManifest(reader io.Reader) (FileManifest, error) {
	var value FileManifest
	if err := decodeGZIPJSON(reader, &value); err != nil {
		return FileManifest{}, fmt.Errorf("decode rootfs file manifest: %w", err)
	}
	if err := value.Validate(); err != nil {
		return FileManifest{}, err
	}
	return value, nil
}

func EncodeHeadAnnotation(reference HeadReference) (string, error) {
	if err := reference.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(reference)
	if err != nil {
		return "", fmt.Errorf("encode rootfs head reference: %w", err)
	}
	value := base64.RawURLEncoding.EncodeToString(payload)
	if len(value) > maxAnnotationBytes {
		return "", fmt.Errorf("rootfs head annotation is %d bytes, exceeds %d-byte limit", len(value), maxAnnotationBytes)
	}
	return value, nil
}

func DecodeHeadAnnotation(value string) (HeadReference, error) {
	var reference HeadReference
	value = strings.TrimSpace(value)
	if len(value) > maxAnnotationBytes {
		return reference, fmt.Errorf("rootfs head annotation is %d bytes, exceeds %d-byte limit", len(value), maxAnnotationBytes)
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return reference, fmt.Errorf("decode rootfs head annotation: %w", err)
	}
	if err := json.Unmarshal(payload, &reference); err != nil {
		return reference, fmt.Errorf("decode rootfs head reference: %w", err)
	}
	if err := reference.Validate(); err != nil {
		return HeadReference{}, err
	}
	return reference, nil
}

func encodeGZIPJSON(value any) ([]byte, error) {
	var payload bytes.Buffer
	writer := gzipWriterPool.Get().(*gzip.Writer)
	writer.Reset(&payload)
	writer.ModTime = time.Unix(0, 0).UTC()
	writer.OS = 255
	release := func() {
		writer.Reset(io.Discard)
		gzipWriterPool.Put(writer)
	}
	limited := &metadataLimitWriter{writer: writer, remaining: MaxMetadataObjectBytes}
	encoder := json.NewEncoder(limited)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = writer.Close()
		release()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		release()
		return nil, err
	}
	release()
	if int64(payload.Len()) > MaxMetadataObjectBytes {
		return nil, ErrMetadataObjectTooLarge
	}
	return payload.Bytes(), nil
}

type metadataLimitWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *metadataLimitWriter) Write(payload []byte) (int, error) {
	if w == nil || w.writer == nil {
		return 0, fmt.Errorf("rootfs metadata writer is nil")
	}
	if int64(len(payload)) > w.remaining {
		return 0, ErrMetadataObjectTooLarge
	}
	written, err := w.writer.Write(payload)
	w.remaining -= int64(written)
	return written, err
}

func decodeGZIPJSON(reader io.Reader, target any) error {
	if reader == nil {
		return fmt.Errorf("rootfs metadata reader is nil")
	}
	compressed := io.LimitReader(reader, MaxMetadataObjectBytes+1)
	gzipReader, err := gzip.NewReader(compressed)
	if err != nil {
		return err
	}
	decompressed := io.LimitReader(gzipReader, MaxMetadataObjectBytes+1)
	decoder := json.NewDecoder(decompressed)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		_ = gzipReader.Close()
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		_ = gzipReader.Close()
		if err == nil {
			return fmt.Errorf("rootfs metadata contains multiple JSON values")
		}
		return err
	}
	return gzipReader.Close()
}
