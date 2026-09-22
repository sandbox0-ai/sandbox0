package runtimecheckpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

// CaptureScope identifies one disposable capture, before its RootFS cut exists.
// It deliberately cannot be used as a Binding or an execution reference. The
// existing regional operation and exact source incarnation own its lifetime.
type CaptureScope struct{ source Binding }

// NewCaptureScope validates a retained source identity without a final RootFS
// cut. It conveys identity only; the regional transaction grants upload/GC.
func NewCaptureScope(source Binding) (CaptureScope, error) {
	s := CaptureScope{source: source}
	_, err := s.Digest()
	return s, err
}

func (s CaptureScope) OperationID() string { return s.source.OperationID }

func (s CaptureScope) Digest() (string, error) {
	if err := s.source.validateSource(); err != nil {
		return "", err
	}
	if s.source.RootFSGenerationID != "" || s.source.RootFSDescriptorDigest != "" {
		return "", fmt.Errorf("capture scope contains a final RootFS binding")
	}
	payload, err := json.Marshal(s.source)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}

func captureScopeForBinding(binding Binding) (CaptureScope, error) {
	if err := binding.Validate(); err != nil {
		return CaptureScope{}, err
	}
	binding.RootFSGenerationID, binding.RootFSDescriptorDigest = "", ""
	return CaptureScope{source: binding}, nil
}

func (s CaptureScope) matches(binding Binding) bool {
	other, err := captureScopeForBinding(binding)
	return err == nil && s == other
}

func capturePrefix(scope CaptureScope) (string, error) {
	d, err := scope.Digest()
	if err != nil {
		return "", err
	}
	return "runtime-checkpoints/capture-v1/" + strings.TrimPrefix(d, "sha256:") + "/", nil
}

func (m Manifest) chunkPrefix() (string, error) {
	if m.Version == ManifestVersion {
		d, err := m.Binding.Digest()
		return imagePrefix(d) + "chunks/", err
	}
	if m.Version != StagedManifestVersion {
		return "", fmt.Errorf("unsupported checkpoint manifest version")
	}
	scope, err := captureScopeForBinding(m.Binding)
	if err != nil {
		return "", err
	}
	prefix, err := capturePrefix(scope)
	return prefix + "chunks/", err
}

// CaptureStager uploads tentative chunks under exclusive node capture custody.
// Its caller must serialize ownership across processes/restarts using the
// existing journal and primary fence. A recovered stager lists its entire
// bounded scope instead of resetting its budget. Every distinct object consumes
// one full ChunkBytes slot, including short chunks and discarded candidates.
// No object is moved: envelope encryption remains bound to its original key.
type CaptureStager struct {
	store    *Store
	scope    CaptureScope
	prefix   string
	maxSlots int
	gate     chan struct{}
	uploadMu sync.Mutex
	uploaded map[string]bool
	pending  map[string]chan struct{}
	bound    string
}

// OpenCaptureStaging starts or recovers disposable upload custody. The immutable
// reservation prevents a retry from expanding the regionally granted budget.
// The budget measures plaintext chunk slots; regional capacity accounting must
// also reserve bounded envelope and metadata overhead. This method does not
// allocate that budget or authorize capture by itself.
func (s *Store) OpenCaptureStaging(ctx context.Context, scope CaptureScope, maxStagedBytes int64) (*CaptureStager, error) {
	prefix, err := capturePrefix(scope)
	if err != nil {
		return nil, err
	}
	if maxStagedBytes < ChunkBytes || maxStagedBytes > MaxImageBytes || maxStagedBytes%ChunkBytes != 0 {
		return nil, fmt.Errorf("capture staging budget must be bounded whole chunk slots")
	}
	if !objectstore.SupportsContextCleanup(s.objects) {
		return nil, fmt.Errorf("capture staging requires bounded context-aware listing")
	}
	reservation, err := json.Marshal(struct {
		Version  int     `json:"version"`
		Source   Binding `json:"source"`
		MaxBytes int64   `json:"max_bytes"`
	}{1, scope.source, maxStagedBytes})
	if err != nil {
		return nil, err
	}
	if err := s.putImmutable(ctx, prefix+"reservation.json", reservation); err != nil {
		return nil, err
	}
	stager := &CaptureStager{store: s, scope: scope, prefix: prefix, maxSlots: int(maxStagedBytes / ChunkBytes),
		gate: make(chan struct{}, 1), uploaded: make(map[string]bool)}
	objects := s.objects.(objectstore.ContextCleanupStore)
	after := ""
	for {
		page, more, _, err := objects.ListContext(ctx, prefix, after, "", "", cleanupBatchSize)
		if err != nil {
			return nil, err
		}
		if len(page) > cleanupBatchSize || len(page) == 0 && more {
			return nil, fmt.Errorf("invalid capture staging listing")
		}
		for _, object := range page {
			name, err := captureObjectName(prefix, after, object)
			if err != nil {
				return nil, err
			}
			after = object.Key
			if strings.HasPrefix(name, "chunks/") {
				if len(stager.uploaded) >= stager.maxSlots {
					return nil, fmt.Errorf("recovered capture exceeds its staging budget")
				}
				// A recovered object is checked through putImmutable before reuse.
				stager.uploaded[strings.TrimPrefix(name, "chunks/")] = false
			}
		}
		if !more {
			break
		}
	}
	bound, err := s.readObject(ctx, prefix+"publication.json", MaxManifestBytes)
	if err != nil && !objectstore.IsNotFound(err) {
		return nil, err
	}
	if err == nil {
		var binding Binding
		if json.Unmarshal(bound, &binding) != nil || !scope.matches(binding) {
			return nil, fmt.Errorf("capture publication belongs to another source")
		}
		canonical, _ := json.Marshal(binding)
		if string(canonical) != string(bound) {
			return nil, fmt.Errorf("capture publication binding is not canonical")
		}
		stager.bound = string(bound)
	}
	return stager, nil
}

func (s *CaptureStager) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// StageChunk accepts tentative bytes, not evidence that the producer finished.
// Publish rehashes the completed image; changed candidates are never selected.
func (s *CaptureStager) StageChunk(ctx context.Context, payload []byte) (Chunk, error) {
	if err := s.acquire(ctx); err != nil {
		return Chunk{}, err
	}
	defer func() { <-s.gate }()
	if s.bound != "" {
		return Chunk{}, fmt.Errorf("capture is already bound for publication")
	}
	return s.stageChunk(ctx, payload)
}

func (s *CaptureStager) stageChunk(ctx context.Context, payload []byte) (Chunk, error) {
	if err := ctx.Err(); err != nil {
		return Chunk{}, err
	}
	if len(payload) == 0 || len(payload) > ChunkBytes {
		return Chunk{}, fmt.Errorf("invalid tentative capture chunk size")
	}
	chunk := Chunk{Digest: digest.FromBytes(payload).String(), Size: int64(len(payload))}
	if err := s.stageVerifiedChunk(ctx, chunk, payload); err != nil {
		return Chunk{}, err
	}
	return chunk, nil
}

// The hash comes either from StageChunk's owned call or scanImageFile's checked
// buffer. Reusing that hash avoids hashing every completed image chunk twice.
func (s *CaptureStager) stageVerifiedChunk(ctx context.Context, chunk Chunk, payload []byte) error {
	name := strings.TrimPrefix(chunk.Digest, "sha256:")
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.uploadMu.Lock()
		complete, found := s.uploaded[name]
		if complete {
			s.uploadMu.Unlock()
			return nil
		}
		if pending := s.pending[name]; pending != nil {
			s.uploadMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-pending:
				continue
			}
		}
		if !found {
			if len(s.uploaded) >= s.maxSlots {
				s.uploadMu.Unlock()
				return fmt.Errorf("capture staging budget exhausted")
			}
			// Charge before dispatch, including ambiguous failures. Recovery
			// lists actual objects rather than forgetting successful writes.
			s.uploaded[name] = false
		}
		if s.pending == nil {
			s.pending = make(map[string]chan struct{})
		}
		done := make(chan struct{})
		s.pending[name] = done
		s.uploadMu.Unlock()

		// Independent verified chunks retain scanImageFile's bounded upload
		// concurrency. Equal hashes join one in-flight immutable write.
		err := s.store.putImmutable(ctx, s.prefix+"chunks/"+name, payload)
		s.uploadMu.Lock()
		s.uploaded[name] = err == nil
		delete(s.pending, name)
		close(done)
		s.uploadMu.Unlock()
		return err
	}
}

// bindPublication runs under gate. Persist the exact final choice before either
// speculative peer exposure or publication, so retries cannot select a new cut.
func (s *CaptureStager) bindPublication(ctx context.Context, binding Binding) error {
	if !s.scope.matches(binding) {
		return fmt.Errorf("final checkpoint changed its capture source")
	}
	bound, err := json.Marshal(binding)
	if err != nil {
		return err
	}
	if s.bound != "" && s.bound != string(bound) {
		return fmt.Errorf("capture is bound to another filesystem cut")
	}
	s.bound = string(bound)
	return s.store.putImmutable(ctx, s.prefix+"publication.json", bound)
}

// Publish seals tentative admission, verifies the complete retained local image
// and binds it to its final filesystem cut. The caller must separately prove
// producer completion and retain source custody, as for Store.Publish. Partial
// failure may be retried with the same binding; another binding is rejected.
func (s *CaptureStager) Publish(ctx context.Context, binding Binding, directory string) (Reference, error) {
	if err := s.acquire(ctx); err != nil {
		return Reference{}, err
	}
	defer func() { <-s.gate }()
	if err := s.bindPublication(ctx, binding); err != nil {
		return Reference{}, err
	}
	root, err := openPrivateDirectory(directory)
	if err != nil {
		return Reference{}, err
	}
	defer root.Close()
	files, err := s.store.imageFiles(ctx, root)
	if err != nil {
		return Reference{}, err
	}
	for i, file := range files {
		files[i], err = scanImageFile(ctx, root, file, func(ctx context.Context, expected Chunk, payload []byte) error {
			return s.stageVerifiedChunk(ctx, expected, payload)
		})
		if err != nil {
			return Reference{}, err
		}
	}
	manifest := Manifest{Version: StagedManifestVersion, Binding: binding, Files: files}
	payload, err := manifest.Encode(s.store.maxBytes)
	if err != nil {
		return Reference{}, err
	}
	bd, err := binding.Digest()
	if err != nil {
		return Reference{}, err
	}
	if err := s.store.putImmutable(ctx, manifestKey(bd), payload); err != nil {
		return Reference{}, err
	}
	return Reference{BindingDigest: bd, ManifestDigest: digest.FromBytes(payload).String()}, nil
}

func captureObjectName(prefix, previous string, object objectstore.Info) (string, error) {
	if object.IsPrefix || !strings.HasPrefix(object.Key, prefix) || object.Key <= previous {
		return "", fmt.Errorf("capture listing escaped its exact scope")
	}
	name := strings.TrimPrefix(object.Key, prefix)
	if name == "reservation.json" || name == "publication.json" {
		return name, nil
	}
	if strings.HasPrefix(name, "chunks/") && validateDigest("sha256:"+strings.TrimPrefix(name, "chunks/")) == nil {
		return name, nil
	}
	return "", fmt.Errorf("capture listing contains an unknown object")
}
