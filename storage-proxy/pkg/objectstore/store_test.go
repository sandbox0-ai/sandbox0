package objectstore

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"io"
	"sort"
	"testing"
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

func TestCountingReaderForPreservesReadSeeker(t *testing.T) {
	reader := countingReaderFor(bytes.NewReader([]byte("hello")))

	readSeeker, ok := reader.(io.ReadSeeker)
	if !ok {
		t.Fatal("counting reader should preserve io.ReadSeeker")
	}

	buf := make([]byte, 2)
	n, err := readSeeker.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 2 || string(buf) != "he" {
		t.Fatalf("Read() = %d, %q; want 2, he", n, string(buf))
	}
	if reader.BytesRead() != 2 {
		t.Fatalf("BytesRead() = %d, want 2", reader.BytesRead())
	}

	if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	n, err = readSeeker.Read(buf)
	if err != nil {
		t.Fatalf("Read() after seek error = %v", err)
	}
	if n != 2 || string(buf) != "he" {
		t.Fatalf("Read() after seek = %d, %q; want 2, he", n, string(buf))
	}
	if reader.BytesRead() != 4 {
		t.Fatalf("BytesRead() after seek = %d, want 4", reader.BytesRead())
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
