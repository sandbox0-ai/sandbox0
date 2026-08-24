package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

type rootFSCompositeMaterializerTestStore struct{}

func (rootFSCompositeMaterializerTestStore) ListCompositeRootFSGenerations(context.Context, int) ([]sandboxstore.RootFSGeneration, error) {
	return nil, nil
}

func (rootFSCompositeMaterializerTestStore) GetOldestUploadingRootFSGenerationMaterializationBatch(context.Context) (*sandboxstore.RootFSGenerationMaterializationBatch, error) {
	return nil, nil
}

func (rootFSCompositeMaterializerTestStore) BeginRootFSGenerationMaterializationBatch(
	context.Context, *sandboxstore.BeginRootFSGenerationMaterializationBatchRequest,
) (*sandboxstore.RootFSGenerationMaterializationBatch, error) {
	return nil, nil
}

func (rootFSCompositeMaterializerTestStore) RegisterRootFSGenerationMaterializationBatchObject(
	context.Context, string, rootfsblock.ObjectReference,
) error {
	return nil
}

func (rootFSCompositeMaterializerTestStore) MarkRootFSGenerationMaterializationBatchObjectUploaded(
	context.Context, string, string,
) error {
	return nil
}

func (rootFSCompositeMaterializerTestStore) PublishRootFSGenerationMaterializationBatch(
	context.Context, *sandboxstore.PublishRootFSGenerationMaterializationBatchRequest,
) error {
	return nil
}

func (rootFSCompositeMaterializerTestStore) ReconcileRootFSGenerationMaterializationGarbage(
	context.Context, time.Duration, time.Duration, int,
) (*sandboxstore.RootFSGenerationMaterializationGarbageResult, error) {
	return &sandboxstore.RootFSGenerationMaterializationGarbageResult{}, nil
}

type objectStoreWithoutContextualConditionalAccess struct{ objectstore.Store }

func (s objectStoreWithoutContextualConditionalAccess) PutIfAbsent(key string, reader io.Reader) (bool, error) {
	return s.Store.(objectstore.ConditionalStore).PutIfAbsent(key, reader)
}

func TestBuildRootFSCompositeMaterializerUsesContextualConditionalObjectStore(t *testing.T) {
	cfg := &config.ManagerConfig{RootFSMaintenance: config.RootFSMaintenanceConfig{
		MaterializerInterval: config.Duration{Duration: 20 * time.Millisecond}, MaterializerScanLimit: 8,
	}}
	worker, err := buildRootFSCompositeMaterializer(
		cfg, rootFSCompositeMaterializerTestStore{}, objectstore.NewMemoryStore(t.Name()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if worker == nil {
		t.Fatal("materializer was not constructed")
	}

	_, err = buildRootFSCompositeMaterializer(cfg, rootFSCompositeMaterializerTestStore{},
		objectStoreWithoutContextualConditionalAccess{Store: objectstore.NewMemoryStore(t.Name() + "-plain")})
	if err == nil || !strings.Contains(err.Error(), "contextual conditional create") {
		t.Fatalf("non-contextual object store error = %v", err)
	}
}

func TestConfigureRootFSCompositeMaterializerRequiresWorker(t *testing.T) {
	for name, cfg := range map[string]*config.ManagerConfig{
		"disabled": {
			RootFSMaintenance: config.RootFSMaintenanceConfig{MaterializerDisabled: true},
		},
		"missing object store": {},
	} {
		t.Run(name, func(t *testing.T) {
			worker, err := configureRootFSCompositeMaterializer(cfg, rootFSCompositeMaterializerTestStore{}, nil)
			if err == nil || worker != nil || !strings.Contains(err.Error(), "Nomad") {
				t.Fatalf("configure materializer = %v, %v", worker, err)
			}
		})
	}
}
