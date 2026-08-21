package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

type objectStoreWithoutConditionalCreate struct{ objectstore.Store }

func TestBuildRootFSCompositeMaterializerUsesConditionalObjectStore(t *testing.T) {
	cfg := &config.ManagerConfig{RootFSMaintenance: config.RootFSMaintenanceConfig{
		MaterializerInterval: metav1.Duration{Duration: 20 * time.Millisecond}, MaterializerScanLimit: 8,
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
		objectStoreWithoutConditionalCreate{Store: objectstore.NewMemoryStore(t.Name() + "-plain")})
	if err == nil || !strings.Contains(err.Error(), "conditional create") {
		t.Fatalf("non-conditional object store error = %v", err)
	}
}

func TestConfigureRootFSCompositeMaterializerRequiresWorkerForNomad(t *testing.T) {
	for name, cfg := range map[string]*config.ManagerConfig{
		"disabled": {
			SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad,
			RootFSMaintenance:     config.RootFSMaintenanceConfig{MaterializerDisabled: true},
		},
		"missing object store": {SandboxRuntimeBackend: config.SandboxRuntimeBackendNomad},
	} {
		t.Run(name, func(t *testing.T) {
			worker, err := configureRootFSCompositeMaterializer(cfg, rootFSCompositeMaterializerTestStore{}, nil)
			if err == nil || worker != nil || !strings.Contains(err.Error(), "Nomad") {
				t.Fatalf("configure materializer = %v, %v", worker, err)
			}
		})
	}
}

func TestConfigureRootFSCompositeMaterializerAllowsDisabledKubernetesPath(t *testing.T) {
	worker, err := configureRootFSCompositeMaterializer(&config.ManagerConfig{
		SandboxRuntimeBackend: config.SandboxRuntimeBackendKubernetes,
		RootFSMaintenance:     config.RootFSMaintenanceConfig{MaterializerDisabled: true},
	}, rootFSCompositeMaterializerTestStore{}, nil)
	if err != nil || worker != nil {
		t.Fatalf("configure materializer = %v, %v", worker, err)
	}
}
