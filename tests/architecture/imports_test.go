package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/sandbox0-ai/sandbox0/"

func TestRuntimeServicesDoNotImportManagerImplementations(t *testing.T) {
	root := repositoryRoot(t)
	services := []string{
		"cluster-gateway",
		"regional-gateway",
		"scheduler",
		"ctld",
		"ssh-gateway",
		"global-gateway",
	}
	forbidden := []string{
		modulePath + "manager/pkg/service",
		modulePath + "manager/pkg/controller",
	}

	for _, service := range services {
		service := service
		t.Run(service, func(t *testing.T) {
			assertNoProductionImports(t, filepath.Join(root, service), forbidden)
		})
	}
}

func TestNetworkingIsInternalToCtld(t *testing.T) {
	root := repositoryRoot(t)
	if _, err := os.Stat(filepath.Join(root, "netd")); !os.IsNotExist(err) {
		t.Fatalf("top-level netd package must not exist: %v", err)
	}
	if info, err := os.Stat(filepath.Join(root, "ctld", "internal", "ctld", "networking")); err != nil || !info.IsDir() {
		t.Fatalf("ctld networking package is missing: %v", err)
	}
}

func TestRegionalGatewayUsesSharedPackages(t *testing.T) {
	assertNoProductionImports(t, filepath.Join(repositoryRoot(t), "regional-gateway"), []string{
		modulePath + "cluster-gateway/pkg/",
		modulePath + "manager/pkg/registry",
	})
}

func TestNomadRuntimeHasNoSupersededOrchestratorDependencies(t *testing.T) {
	root := repositoryRoot(t)
	assertNoProductionImports(t, root, []string{
		"k8s.io/",
		"sigs.k8s.io/controller-runtime",
	})

	for _, relativePath := range []string{
		"infra-operator",
		"sandbox0-operator",
		"internal/framework",
		"manager/pkg/apis",
		"manager/pkg/controller",
		"manager/pkg/generated",
		"netd",
		"tests/e2e",
	} {
		if _, err := os.Stat(filepath.Join(root, relativePath)); !os.IsNotExist(err) {
			t.Errorf("superseded architecture path %s must not exist: %v", relativePath, err)
		}
	}

	for _, relativePath := range []string{"go.mod", "nomad-driver-sandbox0/go.mod"} {
		payload, err := os.ReadFile(filepath.Join(root, relativePath))
		if err != nil {
			t.Fatalf("read %s: %v", relativePath, err)
		}
		for _, forbidden := range []string{"k8s.io/", "sigs.k8s.io/controller-runtime"} {
			if strings.Contains(string(payload), forbidden) {
				t.Errorf("%s retains forbidden module %q", relativePath, forbidden)
			}
		}
	}
}

func TestReadyRootFSArtifactsHaveNoUnfencedProductionPublicationPath(t *testing.T) {
	root := repositoryRoot(t)
	err := filepath.WalkDir(filepath.Join(root, "manager"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(payload), "PutReadyRootFSBaseArtifact") {
			t.Errorf("%s retains an unfenced ready-artifact publication API", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoProductionImports(t *testing.T, root string, forbidden []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			for _, prefix := range forbidden {
				if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") || strings.HasSuffix(prefix, "/") && strings.HasPrefix(importPath, prefix) {
					t.Errorf("%s imports forbidden implementation package %q", path, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("inspect imports under %s: %v", root, err)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve architecture test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
