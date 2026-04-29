package objectstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"golang.org/x/oauth2/google"
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
		store, err := newGCSStore(cfg)
		if err != nil {
			return nil, err
		}
		return newObservedStore(store, storageType, cfg.Bucket, cfg.Metrics), nil
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

var sharedMemoryNamespaces sync.Map

func NewMemoryStore(namespace string) Store {
	namespace = strings.TrimSpace(namespace)
	state := &memoryStoreState{
		objects: make(map[string][]byte),
	}
	if namespace != "" {
		shared, _ := sharedMemoryNamespaces.LoadOrStore(namespace, state)
		state = shared.(*memoryStoreState)
	}
	return &memoryStore{
		namespace: namespace,
		state:     state,
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

type gcsStore struct {
	client    *http.Client
	bucket    string
	projectID string
	baseURL   string
}

func newGCSStore(cfg Config) (Store, error) {
	bucket := strings.TrimSpace(cfg.Bucket)
	if bucket == "" {
		return nil, fmt.Errorf("object storage bucket is required")
	}
	client, err := google.DefaultClient(context.Background(), "https://www.googleapis.com/auth/devstorage.full_control")
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	return &gcsStore{
		client:    client,
		bucket:    bucket,
		projectID: gcsProjectID(cfg),
		baseURL:   gcsBaseURL(cfg),
	}, nil
}

func gcsBaseURL(cfg Config) string {
	if endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/"); endpoint != "" {
		return endpoint
	}
	return "https://storage.googleapis.com"
}

func gcsProjectID(cfg Config) string {
	for _, value := range []string{
		cfg.Region,
		os.Getenv("GOOGLE_CLOUD_PROJECT"),
		os.Getenv("GCLOUD_PROJECT"),
		os.Getenv("GCP_PROJECT"),
	} {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func (s *gcsStore) String() string {
	return fmt.Sprintf("gs://%s", s.bucket)
}

func (s *gcsStore) Create() error {
	if s.projectID == "" {
		return fmt.Errorf("gcs project id is required to create bucket %q; pre-create the bucket or set GOOGLE_CLOUD_PROJECT", s.bucket)
	}
	body, err := json.Marshal(map[string]string{"name": s.bucket})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.bucketCollectionURL("project", s.projectID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return s.doNoContent(req)
}

func (s *gcsStore) Get(key string, off, limit int64) (io.ReadCloser, error) {
	if limit == 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.objectURL(gcsObjectName(key), "alt", "media"), nil)
	if err != nil {
		return nil, err
	}
	if off > 0 || limit >= 0 {
		switch {
		case limit < 0:
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
		default:
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+limit-1))
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, gcsHTTPError(resp)
	}
	return resp.Body, nil
}

func (s *gcsStore) Put(key string, in io.Reader) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.uploadURL(gcsObjectName(key)), in)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return s.doNoContent(req)
}

func (s *gcsStore) Delete(key string) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodDelete, s.objectURL(gcsObjectName(key)), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return gcsHTTPError(resp)
	}
	return nil
}

func (s *gcsStore) Head(key string) (Info, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.bucketURL(), nil)
		if err != nil {
			return Info{}, err
		}
		return Info{}, s.doNoContent(req)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.objectURL(gcsObjectName(key)), nil)
	if err != nil {
		return Info{}, err
	}
	var attrs gcsObjectAttrs
	err = s.doJSON(req, &attrs)
	if err != nil {
		return Info{}, err
	}
	size, err := strconv.ParseInt(attrs.Size, 10, 64)
	if err != nil {
		return Info{}, fmt.Errorf("parse gcs object size %q: %w", attrs.Size, err)
	}
	return Info{
		Key:      gcsObjectName(attrs.Name),
		Size:     size,
		Modified: attrs.Updated,
	}, nil
}

func (s *gcsStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	pageSize := int(limit)
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	values := url.Values{}
	values.Set("maxResults", strconv.Itoa(pageSize))
	values.Set("prefix", strings.TrimLeft(prefix, "/"))
	if delimiter = strings.TrimSpace(delimiter); delimiter != "" {
		values.Set("delimiter", delimiter)
	}
	if startAfter = strings.TrimLeft(startAfter, "/"); startAfter != "" {
		values.Set("startOffset", startAfter+"\x00")
	}
	if token = strings.TrimSpace(token); token != "" {
		values.Set("pageToken", token)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.bucketObjectsURL(values), nil)
	if err != nil {
		return nil, false, "", err
	}
	var result gcsListResult
	err = s.doJSON(req, &result)
	if err != nil {
		return nil, false, "", err
	}
	objects := make([]Info, 0, len(result.Items))
	for _, item := range result.Items {
		if item.Name == "" {
			continue
		}
		size, err := strconv.ParseInt(item.Size, 10, 64)
		if err != nil {
			return nil, false, "", fmt.Errorf("parse gcs object size %q: %w", item.Size, err)
		}
		objects = append(objects, Info{
			Key:      gcsObjectName(item.Name),
			Size:     size,
			Modified: item.Updated,
		})
	}
	return objects, result.NextPageToken != "", result.NextPageToken, nil
}

func gcsObjectName(key string) string {
	return strings.TrimLeft(key, "/")
}

type gcsObjectAttrs struct {
	Name    string    `json:"name"`
	Size    string    `json:"size"`
	Updated time.Time `json:"updated"`
}

type gcsListResult struct {
	Items         []gcsObjectAttrs `json:"items"`
	NextPageToken string           `json:"nextPageToken"`
}

func (s *gcsStore) bucketURL() string {
	return strings.TrimRight(s.baseURL, "/") + "/storage/v1/b/" + url.PathEscape(s.bucket)
}

func (s *gcsStore) bucketCollectionURL(keyValues ...string) string {
	values := url.Values{}
	for i := 0; i+1 < len(keyValues); i += 2 {
		if value := strings.TrimSpace(keyValues[i+1]); value != "" {
			values.Set(keyValues[i], value)
		}
	}
	raw := strings.TrimRight(s.baseURL, "/") + "/storage/v1/b"
	if encoded := values.Encode(); encoded != "" {
		raw += "?" + encoded
	}
	return raw
}

func (s *gcsStore) bucketObjectsURL(values url.Values) string {
	raw := s.bucketURL() + "/o"
	if encoded := values.Encode(); encoded != "" {
		raw += "?" + encoded
	}
	return raw
}

func (s *gcsStore) objectURL(name string, keyValues ...string) string {
	values := url.Values{}
	for i := 0; i+1 < len(keyValues); i += 2 {
		if value := strings.TrimSpace(keyValues[i+1]); value != "" {
			values.Set(keyValues[i], value)
		}
	}
	raw := s.bucketURL() + "/o/" + url.PathEscape(name)
	if encoded := values.Encode(); encoded != "" {
		raw += "?" + encoded
	}
	return raw
}

func (s *gcsStore) uploadURL(name string) string {
	values := url.Values{}
	values.Set("uploadType", "media")
	values.Set("name", name)
	return strings.TrimRight(s.baseURL, "/") + "/upload/storage/v1/b/" + url.PathEscape(s.bucket) + "/o?" + values.Encode()
}

func (s *gcsStore) doNoContent(req *http.Request) error {
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return gcsHTTPError(resp)
	}
	return nil
}

func (s *gcsStore) doJSON(req *http.Request, out any) error {
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return gcsHTTPError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func gcsHTTPError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("gcs request failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
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
	namespace string
	state     *memoryStoreState
}

type memoryStoreState struct {
	mu      sync.RWMutex
	objects map[string][]byte
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
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	payload, ok := s.state.objects[strings.TrimLeft(key, "/")]
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
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	s.state.objects[strings.TrimLeft(key, "/")] = payload
	return nil
}

func (s *memoryStore) Delete(key string) error {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	delete(s.state.objects, strings.TrimLeft(key, "/"))
	return nil
}

func (s *memoryStore) Head(key string) (Info, error) {
	if strings.TrimSpace(key) == "" {
		return Info{}, nil
	}
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	payload, ok := s.state.objects[strings.TrimLeft(key, "/")]
	if !ok {
		return Info{}, errors.New("object not found")
	}
	return Info{
		Key:  strings.TrimLeft(key, "/"),
		Size: int64(len(payload)),
	}, nil
}

func (s *memoryStore) List(prefix, startAfter, _ string, _ string, limit int64) ([]Info, bool, string, error) {
	s.state.mu.RLock()
	defer s.state.mu.RUnlock()
	prefix = strings.TrimLeft(prefix, "/")
	startAfter = strings.TrimLeft(startAfter, "/")
	keys := make([]string, 0, len(s.state.objects))
	for key := range s.state.objects {
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
		objects = append(objects, Info{Key: key, Size: int64(len(s.state.objects[key]))})
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
