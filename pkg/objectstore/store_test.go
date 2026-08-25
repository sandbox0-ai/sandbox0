package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBuildEndpointGCS(t *testing.T) {
	storageType, endpoint, err := BuildEndpoint(Config{Type: "gcs", Bucket: "sandbox0-data"})
	if err != nil {
		t.Fatalf("BuildEndpoint() error = %v", err)
	}
	if storageType != TypeGCS {
		t.Fatalf("storage type = %q, want %q", storageType, TypeGCS)
	}
	if endpoint != "gs://sandbox0-data" {
		t.Fatalf("endpoint = %q, want gs://sandbox0-data", endpoint)
	}
}

func TestGCSProjectIDPrefersConfigRegion(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "env-project")

	got := gcsProjectID(Config{Region: "config-project"})
	if got != "config-project" {
		t.Fatalf("project id = %q, want config-project", got)
	}
}

func TestGCSProjectIDFallsBackToEnvironment(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", "env-project")

	got := gcsProjectID(Config{})
	if got != "env-project" {
		t.Fatalf("project id = %q, want env-project", got)
	}
}

func TestGCSBaseURLUsesConfiguredEndpoint(t *testing.T) {
	got := gcsBaseURL(Config{Endpoint: "https://storage.example.test/"})
	if got != "https://storage.example.test" {
		t.Fatalf("base URL = %q, want https://storage.example.test", got)
	}
}

func TestGCSObjectURLEscapesObjectNameAsSinglePathSegment(t *testing.T) {
	store := &gcsStore{bucket: "sandbox0-data", baseURL: "https://storage.googleapis.com"}

	got := store.objectURL("team-a/volume-a/meta.json")
	want := "https://storage.googleapis.com/storage/v1/b/sandbox0-data/o/team-a%2Fvolume-a%2Fmeta.json"
	if got != want {
		t.Fatalf("object URL = %q, want %q", got, want)
	}
}

func TestNewKeyEncryptorRoundTrip(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	encryptor, err := NewKeyEncryptor(string(keyPEM), "")
	if err != nil {
		t.Fatalf("NewKeyEncryptor() error = %v", err)
	}
	plaintext := []byte("sandbox0 object data key")
	ciphertext, err := encryptor.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt() error = %v", err)
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext should not equal plaintext")
	}
	got, err := encryptor.Decrypt(ciphertext)
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("Decrypt() = %q, want %q", got, plaintext)
	}
}

func TestNewMemoryStoreSharesNamespace(t *testing.T) {
	t.Parallel()

	first := NewMemoryStore("shared-test")
	second := NewMemoryStore("shared-test")

	if err := first.Put("objects/one.txt", bytes.NewReader([]byte("alpha"))); err != nil {
		t.Fatalf("first.Put() error = %v", err)
	}

	reader, err := second.Get("objects/one.txt", 0, -1)
	if err != nil {
		t.Fatalf("second.Get() error = %v", err)
	}
	defer reader.Close()

	payload, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	if got := string(payload); got != "alpha" {
		t.Fatalf("payload = %q, want alpha", got)
	}
}

func TestMemoryStoreListDelimiterReturnsCommonPrefixes(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore(t.Name())
	for key, data := range map[string]string{
		"root.txt":           "root",
		"dir/child.txt":      "child",
		"dir/nested/a.txt":   "nested",
		"other/another.txt":  "other",
		"other/deeper/b.txt": "deep",
	} {
		if err := store.Put(key, bytes.NewReader([]byte(data))); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	infos, more, _, err := store.List("", "", "", "/", 100)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if more {
		t.Fatal("List() more = true, want false")
	}
	got := make([]string, 0, len(infos))
	for _, info := range infos {
		kind := "file"
		if info.IsPrefix {
			kind = "prefix"
		}
		got = append(got, kind+":"+info.Key)
	}
	sort.Strings(got)
	want := []string{"file:root.txt", "prefix:dir/", "prefix:other/"}
	if !equalStringSlices(got, want) {
		t.Fatalf("List() = %#v, want %#v", got, want)
	}
}

func TestPrefixedStoreListPreservesCommonPrefixes(t *testing.T) {
	t.Parallel()

	base := NewMemoryStore(t.Name())
	store := Prefix(base, "tenant-a/volume-a")
	if err := store.Put("visible.txt", bytes.NewReader([]byte("visible"))); err != nil {
		t.Fatalf("Put(visible.txt) error = %v", err)
	}
	if err := store.Put("dir/child.txt", bytes.NewReader([]byte("child"))); err != nil {
		t.Fatalf("Put(dir/child.txt) error = %v", err)
	}
	if err := base.Put("tenant-a/other/hidden.txt", bytes.NewReader([]byte("hidden"))); err != nil {
		t.Fatalf("base.Put(hidden) error = %v", err)
	}

	infos, more, _, err := store.List("", "", "", "/", 100)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if more {
		t.Fatal("List() more = true, want false")
	}
	got := make([]string, 0, len(infos))
	for _, info := range infos {
		kind := "file"
		if info.IsPrefix {
			kind = "prefix"
		}
		got = append(got, kind+":"+info.Key)
	}
	sort.Strings(got)
	want := []string{"file:visible.txt", "prefix:dir/"}
	if !equalStringSlices(got, want) {
		t.Fatalf("List() = %#v, want %#v", got, want)
	}
}

func TestPrefixedStoreConditionalCreateUsesPrefixedKey(t *testing.T) {
	base := NewMemoryStore(t.Name())
	store := Prefix(base, "tenant-a/rootfs").(ConditionalStore)
	created, err := store.PutIfAbsent("packs/object", strings.NewReader("first"))
	if err != nil || !created {
		t.Fatalf("first PutIfAbsent() = %v, %v", created, err)
	}
	created, err = store.PutIfAbsent("packs/object", strings.NewReader("second"))
	if err != nil || created {
		t.Fatalf("second PutIfAbsent() = %v, %v", created, err)
	}
	reader, err := base.Get("tenant-a/rootfs/packs/object", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(payload) != "first" {
		t.Fatalf("stored payload = %q, %v", payload, err)
	}
}

func TestContextConditionalStoreCancellationFollowsPrefix(t *testing.T) {
	base := NewMemoryStore(t.Name())
	prefixed := Prefix(base, "tenant-a/rootfs")
	store, ok := prefixed.(ContextConditionalStore)
	if !ok || !SupportsContextConditionalCreate(prefixed) {
		t.Fatal("prefix wrapper lost contextual conditional access")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PutIfAbsentContext(ctx, "object", strings.NewReader("payload")); err != context.Canceled {
		t.Fatalf("canceled PutIfAbsentContext() error = %v", err)
	}
	if _, err := store.GetContext(ctx, "object", 0, -1); err != context.Canceled {
		t.Fatalf("canceled GetContext() error = %v", err)
	}
	if _, err := base.Get("tenant-a/rootfs/object", 0, -1); err == nil {
		t.Fatal("canceled conditional write created an object")
	}
}

func TestRemoteConditionalCreateStopsOnInFlightCancellation(t *testing.T) {
	for _, provider := range []string{TypeS3, TypeGCS} {
		t.Run(provider, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				once.Do(func() { close(started) })
				select {
				case <-request.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)

			var store ContextConditionalStore
			if provider == TypeS3 {
				created, err := Create(Config{
					Type: TypeS3, Bucket: "rootfs", Region: "us-east-1", Endpoint: server.URL,
					AccessKey: "test-access-key", SecretKey: "test-secret-key",
				})
				if err != nil {
					t.Fatal(err)
				}
				store = created.(ContextConditionalStore)
			} else {
				store = &gcsStore{client: server.Client(), bucket: "rootfs", baseURL: server.URL}
			}

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, err := store.PutIfAbsentContext(ctx, "objects/one", strings.NewReader("payload"))
				result <- err
			}()
			select {
			case <-started:
			case err := <-result:
				cancel()
				t.Fatalf("conditional create stopped before the test endpoint: %v", err)
			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("conditional create did not reach the test endpoint")
			}
			cancel()
			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("PutIfAbsentContext() error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("conditional create ignored context cancellation")
			}
		})
	}
}

func TestOSSConditionalCreateUsesNativeAtomicHeaderAndVirtualHost(t *testing.T) {
	type observedRequest struct {
		host, path, forbidOverwrite, ifNoneMatch, authorization string
	}
	requests := make([]observedRequest, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests = append(requests, observedRequest{
			host: request.Host, path: request.URL.Path,
			forbidOverwrite: request.Header.Get("x-oss-forbid-overwrite"),
			ifNoneMatch:     request.Header.Get("If-None-Match"),
			authorization:   request.Header.Get("Authorization"),
		})
		if len(requests) == 1 {
			response.WriteHeader(http.StatusOK)
			return
		}
		response.Header().Set("Content-Type", "application/xml")
		response.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(response,
			`<Error><Code>FileAlreadyExists</Code><Message>Object already exists.</Message></Error>`)
	}))
	defer server.Close()

	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	createdStore, err := Create(Config{
		Type: TypeOSS, Bucket: "rootfs", Region: "us-east-1", Endpoint: endpoint,
		AccessKey: "test-access-key", SecretKey: "test-secret-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	store := createdStore.(*s3Store)
	if store.pathStyle {
		t.Fatal("OSS store uses path-style bucket addressing")
	}
	created, err := store.PutIfAbsentContext(t.Context(), "objects/one", strings.NewReader("payload"))
	if err != nil || !created {
		t.Fatalf("first PutIfAbsentContext() = %v, %v", created, err)
	}
	created, err = store.PutIfAbsentContext(t.Context(), "objects/one", strings.NewReader("payload"))
	if err != nil || created {
		t.Fatalf("collision PutIfAbsentContext() = %v, %v", created, err)
	}
	if len(requests) != 2 {
		t.Fatalf("request count = %d, want 2", len(requests))
	}
	for index, request := range requests {
		if !strings.HasPrefix(request.host, "rootfs.localhost:") || request.path != "/objects/one" {
			t.Fatalf("request %d address = %s%s, want virtual-hosted bucket", index, request.host, request.path)
		}
		if request.forbidOverwrite != "true" || request.ifNoneMatch != "" {
			t.Fatalf("request %d conditional headers = forbid:%q if-none-match:%q",
				index, request.forbidOverwrite, request.ifNoneMatch)
		}
		if !strings.Contains(strings.ToLower(request.authorization), "x-oss-forbid-overwrite") {
			t.Fatalf("request %d did not sign the OSS conditional header", index)
		}
	}
}

func TestOSSCreateReusesAccessibleBucketWithoutCreateRequest(t *testing.T) {
	methods := make([]string, 0, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.Method)
		if !strings.HasPrefix(request.Host, "rootfs.localhost:") || request.URL.Path != "/" {
			t.Errorf("bucket request address = %s%s, want OSS virtual-host root", request.Host, request.URL.Path)
		}
		response.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	endpoint := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	createdStore, err := Create(Config{
		Type: TypeOSS, Bucket: "rootfs", Region: "us-east-1", Endpoint: endpoint,
		AccessKey: "test-access-key", SecretKey: "test-secret-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := createdStore.Create(); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if len(methods) != 1 || methods[0] != http.MethodHead {
		t.Fatalf("bucket methods = %#v, want one HEAD", methods)
	}
}

func TestPrefixedStoreListDoesNotInjectStartAfterForEmptyCursor(t *testing.T) {
	t.Parallel()

	recorder := &recordingListStore{Store: NewMemoryStore(t.Name())}
	store := Prefix(recorder, "tenant-a/volume-a")
	if _, _, _, err := store.List("dir/", "", "", "/", 100); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if recorder.lastPrefix != "tenant-a/volume-a/dir/" {
		t.Fatalf("List prefix = %q, want tenant-a/volume-a/dir/", recorder.lastPrefix)
	}
	if recorder.lastStartAfter != "" {
		t.Fatalf("List startAfter = %q, want empty", recorder.lastStartAfter)
	}
}

type recordingListStore struct {
	Store

	lastPrefix     string
	lastStartAfter string
}

func (s *recordingListStore) List(prefix, startAfter, token, delimiter string, limit int64) ([]Info, bool, string, error) {
	s.lastPrefix = prefix
	s.lastStartAfter = startAfter
	return s.Store.List(prefix, startAfter, token, delimiter, limit)
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
