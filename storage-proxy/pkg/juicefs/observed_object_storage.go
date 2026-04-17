package juicefs

import (
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

var httpStatusPattern = regexp.MustCompile(`\b(4[0-9]{2}|5[0-9]{2})\b`)

type observedObjectStorage struct {
	object.ObjectStorage
	provider string
	bucket   string
	metrics  *obsmetrics.StorageProxyMetrics
}

func newObservedObjectStorage(store object.ObjectStorage, provider, bucket string, metrics *obsmetrics.StorageProxyMetrics) object.ObjectStorage {
	if store == nil || metrics == nil {
		return store
	}
	return &observedObjectStorage{
		ObjectStorage: store,
		provider:      nonEmptyMetricLabel(NormalizeObjectStorageType(provider), "unknown"),
		bucket:        nonEmptyMetricLabel(bucket, "unknown"),
		metrics:       metrics,
	}
}

func (s *observedObjectStorage) Create() error {
	start := time.Now()
	err := s.ObjectStorage.Create()
	s.observeRequest("create", "bucket", objectStoreStatus(err), start)
	return err
}

func (s *observedObjectStorage) Get(key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	reader, err := s.ObjectStorage.Get(key, off, limit, getters...)
	if err != nil {
		s.observeRequest("get", prefixClass, objectStoreStatus(err), start)
		return nil, err
	}
	return &observedObjectReadCloser{
		ReadCloser:  reader,
		store:       s,
		operation:   "get",
		prefixClass: prefixClass,
		start:       start,
	}, nil
}

func (s *observedObjectStorage) Put(key string, in io.Reader, getters ...object.AttrGetter) error {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	counted := &countingReader{Reader: in}
	err := s.ObjectStorage.Put(key, counted, getters...)
	s.observeBytes("put", prefixClass, "write", counted.bytes)
	s.observeRequest("put", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedObjectStorage) Copy(dst, src string) error {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(dst)
	err := s.ObjectStorage.Copy(dst, src)
	s.observeRequest("copy", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedObjectStorage) Delete(key string, getters ...object.AttrGetter) error {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	err := s.ObjectStorage.Delete(key, getters...)
	s.observeRequest("delete", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedObjectStorage) Head(key string) (object.Object, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	obj, err := s.ObjectStorage.Head(key)
	s.observeRequest("head", prefixClass, objectStoreStatus(err), start)
	return obj, err
}

func (s *observedObjectStorage) List(prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(prefix)
	objects, hasMore, nextToken, err := s.ObjectStorage.List(prefix, startAfter, token, delimiter, limit, followLink)
	s.observeRequest("list", prefixClass, objectStoreStatus(err), start)
	return objects, hasMore, nextToken, err
}

func (s *observedObjectStorage) ListAll(prefix, marker string, followLink bool) (<-chan object.Object, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(prefix)
	ch, err := s.ObjectStorage.ListAll(prefix, marker, followLink)
	s.observeRequest("list_all", prefixClass, objectStoreStatus(err), start)
	return ch, err
}

func (s *observedObjectStorage) CreateMultipartUpload(key string) (*object.MultipartUpload, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	upload, err := s.ObjectStorage.CreateMultipartUpload(key)
	s.observeRequest("create_multipart_upload", prefixClass, objectStoreStatus(err), start)
	return upload, err
}

func (s *observedObjectStorage) UploadPart(key string, uploadID string, num int, body []byte) (*object.Part, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	part, err := s.ObjectStorage.UploadPart(key, uploadID, num, body)
	s.observeBytes("upload_part", prefixClass, "write", int64(len(body)))
	s.observeRequest("upload_part", prefixClass, objectStoreStatus(err), start)
	return part, err
}

func (s *observedObjectStorage) UploadPartCopy(key string, uploadID string, num int, srcKey string, off, size int64) (*object.Part, error) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	part, err := s.ObjectStorage.UploadPartCopy(key, uploadID, num, srcKey, off, size)
	s.observeBytes("upload_part_copy", prefixClass, "copy", size)
	s.observeRequest("upload_part_copy", prefixClass, objectStoreStatus(err), start)
	return part, err
}

func (s *observedObjectStorage) AbortUpload(key string, uploadID string) {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	s.ObjectStorage.AbortUpload(key, uploadID)
	s.observeRequest("abort_upload", prefixClass, "success", start)
}

func (s *observedObjectStorage) CompleteUpload(key string, uploadID string, parts []*object.Part) error {
	start := time.Now()
	prefixClass := classifyObjectStorePrefix(key)
	err := s.ObjectStorage.CompleteUpload(key, uploadID, parts)
	s.observeRequest("complete_upload", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedObjectStorage) ListUploads(marker string) ([]*object.PendingPart, string, error) {
	start := time.Now()
	uploads, nextMarker, err := s.ObjectStorage.ListUploads(marker)
	s.observeRequest("list_uploads", "multipart", objectStoreStatus(err), start)
	return uploads, nextMarker, err
}

func (s *observedObjectStorage) observeRequest(operation, prefixClass, status string, start time.Time) {
	s.metrics.ObserveObjectStoreRequest(s.provider, s.bucket, prefixClass, operation, status, time.Since(start))
}

func (s *observedObjectStorage) observeBytes(operation, prefixClass, direction string, bytes int64) {
	s.metrics.ObserveObjectStoreBytes(s.provider, s.bucket, prefixClass, operation, direction, bytes)
}

type countingReader struct {
	io.Reader
	bytes int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

type observedObjectReadCloser struct {
	io.ReadCloser
	store       *observedObjectStorage
	operation   string
	prefixClass string
	start       time.Time
	bytes       int64
	once        sync.Once
}

func (r *observedObjectReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes += int64(n)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.finish("success")
		} else {
			r.finish(objectStoreStatus(err))
		}
	}
	return n, err
}

func (r *observedObjectReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish(objectStoreStatus(err))
	return err
}

func (r *observedObjectReadCloser) finish(status string) {
	r.once.Do(func() {
		r.store.observeBytes(r.operation, r.prefixClass, "read", r.bytes)
		r.store.observeRequest(r.operation, r.prefixClass, status, r.start)
	})
}

func objectStoreStatus(err error) string {
	if err == nil {
		return "success"
	}

	var httpStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &httpStatus) && httpStatus.HTTPStatusCode() > 0 {
		return httpStatusLabel(httpStatus.HTTPStatusCode())
	}

	var statusCode interface{ StatusCode() int }
	if errors.As(err, &statusCode) && statusCode.StatusCode() > 0 {
		return httpStatusLabel(statusCode.StatusCode())
	}

	text := strings.ToLower(err.Error())
	if strings.Contains(text, "ratelimitexceeded") ||
		strings.Contains(text, "rate limit") ||
		strings.Contains(text, "too many requests") {
		return "429"
	}
	if match := httpStatusPattern.FindStringSubmatch(text); len(match) == 2 {
		return match[1]
	}
	return "error"
}

func httpStatusLabel(code int) string {
	if code >= 100 && code <= 599 {
		return strconv.Itoa(code)
	}
	return "error"
}

func classifyObjectStorePrefix(key string) string {
	value := strings.TrimLeft(strings.TrimSpace(key), "/")
	switch {
	case value == "":
		return "none"
	case value == ".juicefs" || strings.HasPrefix(value, ".juicefs-"):
		return "juicefs_metadata"
	case strings.HasPrefix(value, "sandboxvolumes-sync/"):
		if strings.Contains(value, "/replay/") {
			return "volume_sync_replay"
		}
		return "volume_sync"
	case strings.HasPrefix(value, "sandboxvolumes/"):
		return "volume_data"
	default:
		return "other"
	}
}

func nonEmptyMetricLabel(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}
