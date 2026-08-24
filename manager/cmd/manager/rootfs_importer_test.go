package main

import (
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
)

func TestConfigureRootFSImportWorkerFailsClosed(t *testing.T) {
	for name, test := range map[string]struct {
		cfg     *config.ManagerConfig
		objects objectstore.Store
		want    string
	}{
		"disabled": {
			cfg: &config.ManagerConfig{
				RootFSImporter: config.RootFSImporterConfig{Disabled: true},
			},
			want: "requires the durable RootFS importer",
		},
		"database": {
			cfg:  &config.ManagerConfig{},
			want: "requires PostgreSQL",
		},
	} {
		t.Run(name, func(t *testing.T) {
			worker, err := configureRootFSImportWorker(test.cfg, nil, test.objects)
			if err == nil || worker != nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("configure importer = %v, %v", worker, err)
			}
		})
	}
}

func TestRootFSImportWorkerIDIsCanonicalAndUnique(t *testing.T) {
	first, err := newRootFSImportWorkerID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRootFSImportWorkerID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "manager.rootfs.import.") || strings.Contains(first, "-") {
		t.Fatalf("worker IDs = %q, %q", first, second)
	}
}
