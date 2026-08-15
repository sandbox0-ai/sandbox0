package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
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
		"netd",
		"ssh-gateway",
		"global-gateway",
		"infra-operator",
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

func TestRegionalGatewayUsesSharedPackages(t *testing.T) {
	assertNoProductionImports(t, filepath.Join(repositoryRoot(t), "regional-gateway"), []string{
		modulePath + "cluster-gateway/pkg/",
		modulePath + "manager/pkg/registry",
	})
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
