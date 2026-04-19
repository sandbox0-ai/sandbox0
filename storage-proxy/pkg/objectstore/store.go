package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
)

const (
	TypeS3  = "s3"
	TypeOSS = "oss"
	TypeGCS = "gs"
	TypeMem = "mem"
)

type Config struct {
	Type         string
	Bucket       string
	Region       string
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Metrics      *obsmetrics.StorageProxyMetrics
}

type Info struct {
	Key      string
	Size     int64
	Modified time.Time
}

type Store interface {
	String() string
	Create() error
	Get(key string, off, limit int64) (io.ReadCloser, error)
	Put(key string, in io.Reader) error
	Delete(key string) error
	Head(key string) (Info, error)
	List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error)
}

func NormalizeType(raw string) string {
	value := strings.TrimSpace(strings.ToLower(raw))
	switch value {
	case "", "builtin", TypeS3:
		return TypeS3
	case TypeOSS:
		return TypeOSS
	case "gcs", TypeGCS:
		return TypeGCS
	case TypeMem:
		return TypeMem
	default:
		return value
	}
}

func BuildEndpoint(cfg Config) (string, string, error) {
	storageType := NormalizeType(cfg.Type)
	bucket := strings.TrimSpace(cfg.Bucket)
	if bucket == "" && storageType != TypeMem {
		return "", "", fmt.Errorf("object storage bucket is required")
	}

	switch storageType {
	case TypeS3:
		endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
		if endpoint == "" {
			region := strings.TrimSpace(cfg.Region)
			if region == "" {
				return "", "", fmt.Errorf("object storage region or endpoint is required for s3")
			}
			endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
		}
		return storageType, endpoint, nil
	case TypeOSS:
		endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
		if endpoint == "" {
			return "", "", fmt.Errorf("object storage endpoint is required for oss")
		}
		return storageType, endpoint, nil
	case TypeGCS:
		return storageType, fmt.Sprintf("gs://%s", bucket), nil
	case TypeMem:
		return storageType, "mem://" + bucket, nil
	default:
		return "", "", fmt.Errorf("unsupported object storage type: %s", storageType)
	}
}

func Create(cfg Config) (Store, error) {
	storageType := NormalizeType(cfg.Type)
	switch storageType {
	case TypeMem:
		return NewMemoryStore(strings.TrimSpace(cfg.Bucket)), nil
	case TypeS3, TypeOSS:
		store, err := newS3Store(cfg)
		if err != nil {
			return nil, err
		}
		return newObservedStore(store, storageType, cfg.Bucket, cfg.Metrics), nil
	case TypeGCS:
		return nil, fmt.Errorf("gcs object storage is not implemented")
	default:
		return nil, fmt.Errorf("unsupported object storage type: %s", storageType)
	}
}

func Prefix(store Store, prefix string) Store {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if store == nil || prefix == "" {
		return store
	}
	return &prefixedStore{
		store:  store,
		prefix: prefix + "/",
	}
}

func NewMemoryStore(namespace string) Store {
	return &memoryStore{
		namespace: strings.TrimSpace(namespace),
		objects:   make(map[string][]byte),
	}
}

type s3Store struct {
	client    *s3.Client
	bucket    string
	endpoint  string
	provider  string
	pathStyle bool
}

func newS3Store(cfg Config) (Store, error) {
	storageType, endpoint, err := BuildEndpoint(cfg)
	if err != nil {
		return nil, err
	}

	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = "us-east-1"
	}

	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" || cfg.SessionToken != "" {
		loadOptions = append(loadOptions, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			cfg.SessionToken,
		)))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load object storage config: %w", err)
	}

	options := func(o *s3.Options) {
		o.UsePathStyle = true
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}

	return &s3Store{
		client:    s3.NewFromConfig(awsCfg, options),
		bucket:    strings.TrimSpace(cfg.Bucket),
		endpoint:  endpoint,
		provider:  storageType,
		pathStyle: true,
	}, nil
}

func (s *s3Store) String() string {
	return fmt.Sprintf("%s://%s", s.provider, s.bucket)
}

func (s *s3Store) Create() error {
	_, err := s.client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	return err
}

func (s *s3Store) Get(key string, off, limit int64) (io.ReadCloser, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(key, "/")),
	}
	if off > 0 || limit >= 0 {
		switch {
		case limit < 0:
			input.Range = aws.String(fmt.Sprintf("bytes=%d-", off))
		case limit == 0:
			return io.NopCloser(bytes.NewReader(nil)), nil
		default:
			input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+limit-1))
		}
	}
	resp, err := s.client.GetObject(context.Background(), input)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (s *s3Store) Put(key string, in io.Reader) error {
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(key, "/")),
		Body:   in,
	})
	return err
}

func (s *s3Store) Delete(key string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(key, "/")),
	})
	return err
}

func (s *s3Store) Head(key string) (Info, error) {
	if strings.TrimSpace(key) == "" {
		_, err := s.client.HeadBucket(context.Background(), &s3.HeadBucketInput{
			Bucket: aws.String(s.bucket),
		})
		return Info{}, err
	}
	resp, err := s.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(strings.TrimLeft(key, "/")),
	})
	if err != nil {
		return Info{}, err
	}
	return Info{
		Key:      strings.TrimLeft(key, "/"),
		Size:     aws.ToInt64(resp.ContentLength),
		Modified: aws.ToTime(resp.LastModified),
	}, nil
}

func (s *s3Store) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	maxKeys := int32(limit)
	if limit <= 0 || limit > 1000 {
		maxKeys = 1000
	}
	resp, err := s.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket:            aws.String(s.bucket),
		Prefix:            aws.String(strings.TrimLeft(prefix, "/")),
		StartAfter:        aws.String(strings.TrimLeft(startAfter, "/")),
		ContinuationToken: emptyStringPtr(token),
		Delimiter:         emptyStringPtr(delimiter),
		MaxKeys:           aws.Int32(maxKeys),
	})
	if err != nil {
		return nil, false, "", err
	}
	objects := make([]Info, 0, len(resp.Contents))
	for _, item := range resp.Contents {
		objects = append(objects, Info{
			Key:      aws.ToString(item.Key),
			Size:     aws.ToInt64(item.Size),
			Modified: aws.ToTime(item.LastModified),
		})
	}
	return objects, resp.IsTruncated != nil && *resp.IsTruncated, aws.ToString(resp.NextContinuationToken), nil
}

func emptyStringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return aws.String(value)
}

type prefixedStore struct {
	store  Store
	prefix string
}

func (s *prefixedStore) String() string {
	return s.store.String()
}

func (s *prefixedStore) Create() error {
	return s.store.Create()
}

func (s *prefixedStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	return s.store.Get(s.prefixed(key), off, limit)
}

func (s *prefixedStore) Put(key string, in io.Reader) error {
	return s.store.Put(s.prefixed(key), in)
}

func (s *prefixedStore) Delete(key string) error {
	return s.store.Delete(s.prefixed(key))
}

func (s *prefixedStore) Head(key string) (Info, error) {
	info, err := s.store.Head(s.prefixed(key))
	if err != nil {
		return Info{}, err
	}
	info.Key = strings.TrimPrefix(info.Key, s.prefix)
	return info, nil
}

func (s *prefixedStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	objects, hasMore, nextToken, err := s.store.List(s.prefixed(prefix), s.prefixed(startAfter), token, delimiter, limit)
	if err != nil {
		return nil, false, "", err
	}
	for i := range objects {
		objects[i].Key = strings.TrimPrefix(objects[i].Key, s.prefix)
	}
	return objects, hasMore, nextToken, nil
}

func (s *prefixedStore) prefixed(key string) string {
	key = strings.TrimLeft(strings.TrimSpace(key), "/")
	if key == "" {
		return strings.TrimRight(s.prefix, "/")
	}
	return s.prefix + key
}

type memoryStore struct {
	mu        sync.RWMutex
	namespace string
	objects   map[string][]byte
}

func (s *memoryStore) String() string {
	if s.namespace == "" {
		return "mem://"
	}
	return "mem://" + s.namespace
}

func (s *memoryStore) Create() error {
	return nil
}

func (s *memoryStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payload, ok := s.objects[strings.TrimLeft(key, "/")]
	if !ok {
		return nil, errors.New("object not found")
	}
	if off > int64(len(payload)) {
		off = int64(len(payload))
	}
	end := int64(len(payload))
	if limit >= 0 && off+limit < end {
		end = off + limit
	}
	copyPayload := append([]byte(nil), payload[off:end]...)
	return io.NopCloser(bytes.NewReader(copyPayload)), nil
}

func (s *memoryStore) Put(key string, in io.Reader) error {
	payload, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[strings.TrimLeft(key, "/")] = payload
	return nil
}

func (s *memoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, strings.TrimLeft(key, "/"))
	return nil
}

func (s *memoryStore) Head(key string) (Info, error) {
	if strings.TrimSpace(key) == "" {
		return Info{}, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	payload, ok := s.objects[strings.TrimLeft(key, "/")]
	if !ok {
		return Info{}, errors.New("object not found")
	}
	return Info{
		Key:  strings.TrimLeft(key, "/"),
		Size: int64(len(payload)),
	}, nil
}

func (s *memoryStore) List(prefix, startAfter, _ string, _ string, limit int64) ([]Info, bool, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefix = strings.TrimLeft(prefix, "/")
	startAfter = strings.TrimLeft(startAfter, "/")
	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) && key > startAfter {
			keys = append(keys, key)
		}
	}
	slicesSort(keys)
	max := len(keys)
	hasMore := false
	nextToken := ""
	if limit > 0 && int(limit) < max {
		hasMore = true
		max = int(limit)
		nextToken = keys[max-1]
	}
	objects := make([]Info, 0, max)
	for _, key := range keys[:max] {
		objects = append(objects, Info{Key: key, Size: int64(len(s.objects[key]))})
	}
	return objects, hasMore, nextToken, nil
}

func slicesSort(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

type observedStore struct {
	store    Store
	provider string
	bucket   string
	metrics  *obsmetrics.StorageProxyMetrics
}

func newObservedStore(store Store, provider, bucket string, metrics *obsmetrics.StorageProxyMetrics) Store {
	if store == nil || metrics == nil {
		return store
	}
	return &observedStore{
		store:    store,
		provider: nonEmptyMetricLabel(NormalizeType(provider), "unknown"),
		bucket:   nonEmptyMetricLabel(bucket, "unknown"),
		metrics:  metrics,
	}
}

func (s *observedStore) String() string {
	return s.store.String()
}

func (s *observedStore) Create() error {
	start := time.Now()
	err := s.store.Create()
	s.observeRequest("create", "bucket", objectStoreStatus(err), start)
	return err
}

func (s *observedStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	start := time.Now()
	prefixClass := classifyPrefix(key)
	reader, err := s.store.Get(key, off, limit)
	if err != nil {
		s.observeRequest("get", prefixClass, objectStoreStatus(err), start)
		return nil, err
	}
	return &observedReadCloser{
		ReadCloser:  reader,
		store:       s,
		operation:   "get",
		prefixClass: prefixClass,
		start:       start,
	}, nil
}

func (s *observedStore) Put(key string, in io.Reader) error {
	start := time.Now()
	prefixClass := classifyPrefix(key)
	counted := countingReaderFor(in)
	err := s.store.Put(key, counted)
	s.observeBytes("put", prefixClass, "write", counted.BytesRead())
	s.observeRequest("put", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedStore) Delete(key string) error {
	start := time.Now()
	prefixClass := classifyPrefix(key)
	err := s.store.Delete(key)
	s.observeRequest("delete", prefixClass, objectStoreStatus(err), start)
	return err
}

func (s *observedStore) Head(key string) (Info, error) {
	start := time.Now()
	prefixClass := classifyPrefix(key)
	info, err := s.store.Head(key)
	s.observeRequest("head", prefixClass, objectStoreStatus(err), start)
	return info, err
}

func (s *observedStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	start := time.Now()
	prefixClass := classifyPrefix(prefix)
	objects, hasMore, nextToken, err := s.store.List(prefix, startAfter, token, delimiter, limit)
	s.observeRequest("list", prefixClass, objectStoreStatus(err), start)
	return objects, hasMore, nextToken, err
}

func (s *observedStore) observeRequest(operation, prefixClass, status string, start time.Time) {
	s.metrics.ObserveObjectStoreRequest(s.provider, s.bucket, prefixClass, operation, status, time.Since(start))
}

func (s *observedStore) observeBytes(operation, prefixClass, direction string, bytes int64) {
	s.metrics.ObserveObjectStoreBytes(s.provider, s.bucket, prefixClass, operation, direction, bytes)
}

type byteCountingReader interface {
	io.Reader
	BytesRead() int64
}

func countingReaderFor(in io.Reader) byteCountingReader {
	if rs, ok := in.(io.ReadSeeker); ok {
		return &countingReadSeeker{ReadSeeker: rs}
	}
	return &countingReader{Reader: in}
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

func (r *countingReader) BytesRead() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

type countingReadSeeker struct {
	io.ReadSeeker
	bytes int64
}

func (r *countingReadSeeker) Read(p []byte) (int, error) {
	n, err := r.ReadSeeker.Read(p)
	r.bytes += int64(n)
	return n, err
}

func (r *countingReadSeeker) BytesRead() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

type observedReadCloser struct {
	io.ReadCloser
	store       *observedStore
	operation   string
	prefixClass string
	start       time.Time
	bytes       int64
	once        sync.Once
}

func (r *observedReadCloser) Read(p []byte) (int, error) {
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

func (r *observedReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.finish(objectStoreStatus(err))
	return err
}

func (r *observedReadCloser) finish(status string) {
	r.once.Do(func() {
		r.store.observeBytes(r.operation, r.prefixClass, "read", r.bytes)
		r.store.observeRequest(r.operation, r.prefixClass, status, r.start)
	})
}

func classifyPrefix(key string) string {
	value := strings.TrimLeft(strings.TrimSpace(key), "/")
	switch {
	case value == "":
		return "none"
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

func objectStoreStatus(err error) string {
	if err == nil {
		return "success"
	}
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		code := strings.TrimSpace(apiErr.ErrorCode())
		if code != "" {
			return code
		}
	}
	var httpErr interface{ HTTPStatusCode() int }
	if errors.As(err, &httpErr) && httpErr.HTTPStatusCode() > 0 {
		return fmt.Sprintf("%d", httpErr.HTTPStatusCode())
	}
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "too many requests"), strings.Contains(text, "rate limit"):
		return "429"
	default:
		return "error"
	}
}

type Encryptor interface {
	Encrypt([]byte) ([]byte, error)
	Decrypt([]byte) ([]byte, error)
}

type encryptedStore struct {
	store Store
	enc   Encryptor
}

func WrapEncrypted(store Store, enc Encryptor) Store {
	if store == nil || enc == nil {
		return store
	}
	return &encryptedStore{store: store, enc: enc}
}

func (s *encryptedStore) String() string { return s.store.String() + "(encrypted)" }
func (s *encryptedStore) Create() error  { return s.store.Create() }
func (s *encryptedStore) Delete(key string) error {
	return s.store.Delete(key)
}
func (s *encryptedStore) Head(key string) (Info, error) {
	return s.store.Head(key)
}
func (s *encryptedStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	return s.store.List(prefix, startAfter, token, delimiter, limit)
}
func (s *encryptedStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	reader, err := s.store.Get(key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	ciphertext, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	plaintext, err := s.enc.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	if off > int64(len(plaintext)) {
		off = int64(len(plaintext))
	}
	end := int64(len(plaintext))
	if limit >= 0 && off+limit < end {
		end = off + limit
	}
	return io.NopCloser(bytes.NewReader(plaintext[off:end])), nil
}
func (s *encryptedStore) Put(key string, in io.Reader) error {
	payload, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	ciphertext, err := s.enc.Encrypt(payload)
	if err != nil {
		return err
	}
	return s.store.Put(key, bytes.NewReader(ciphertext))
}
