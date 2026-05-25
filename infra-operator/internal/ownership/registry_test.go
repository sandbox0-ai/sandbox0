package ownership

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func TestLookupPrefersMostSpecificEntry(t *testing.T) {
	entry, ok := Lookup("spec.services.clusterGateway.service.port")
	if !ok {
		t.Fatal("expected exact ownership entry for cluster-gateway service.port")
	}
	if entry.Owner != "plan" {
		t.Fatalf("expected owner %q, got %q", "plan", entry.Owner)
	}
	if entry.UpdateSemantics != UpdateSemanticsDeclarative {
		t.Fatalf("expected declarative semantics, got %q", entry.UpdateSemantics)
	}
	if !contains(entry.CompiledInto, "InfraPlan.RegionalGateway.DefaultClusterGatewayURL") {
		t.Fatalf("expected regional-gateway projection in compiled outputs, got %#v", entry.CompiledInto)
	}
}

func TestLookupReturnsPrefixCoverageForSubtreeLeaves(t *testing.T) {
	entry, ok := Lookup("spec.storage.s3.credentialsSecret.secretKeyKey")
	if !ok {
		t.Fatal("expected prefix ownership entry for storage.s3 subtree")
	}
	if entry.Path != "spec.storage.s3" {
		t.Fatalf("expected storage.s3 prefix owner, got %q", entry.Path)
	}
	if entry.Owner != "storage-proxy" {
		t.Fatalf("expected storage-proxy owner, got %q", entry.Owner)
	}
}

func TestLookupTracksCrossServiceDerivedFields(t *testing.T) {
	cases := []struct {
		path      string
		owner     string
		consumers []string
	}{
		{
			path:      "spec.services.clusterGateway.config.authMode",
			owner:     "plan",
			consumers: []string{"cluster-gateway", "manager"},
		},
		{
			path:      "spec.services.manager.config.httpPort",
			owner:     "plan",
			consumers: []string{"manager", "netd"},
		},
		{
			path:      "spec.publicExposure.rootDomain",
			owner:     "plan",
			consumers: []string{"global-gateway", "cluster-gateway", "manager"},
		},
	}

	for _, tc := range cases {
		entry, ok := Lookup(tc.path)
		if !ok {
			t.Fatalf("expected ownership entry for %s", tc.path)
		}
		if entry.Owner != tc.owner {
			t.Fatalf("%s: expected owner %q, got %q", tc.path, tc.owner, entry.Owner)
		}
		for _, consumer := range tc.consumers {
			if !contains(entry.Consumers, consumer) {
				t.Fatalf("%s: expected consumer %q in %#v", tc.path, consumer, entry.Consumers)
			}
		}
	}
}

func TestRegistryCoversAllSandbox0InfraSpecLeafPaths(t *testing.T) {
	paths := collectLeafJSONPaths(reflect.TypeOf(infrav1alpha1.Sandbox0InfraSpec{}), "spec")
	sort.Strings(paths)

	var missing []string
	for _, path := range paths {
		if _, ok := Lookup(path); !ok {
			missing = append(missing, path)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("missing ownership entries for spec leaf paths:\n%s", strings.Join(missing, "\n"))
	}
}

func collectLeafJSONPaths(t reflect.Type, prefix string) []string {
	t = derefType(t)
	if !shouldRecurseInto(t) {
		return []string{prefix}
	}

	var paths []string
	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" {
				continue
			}

			name, inline, ok := jsonFieldName(field)
			if !ok {
				continue
			}

			fieldType := derefType(field.Type)
			if inline {
				paths = append(paths, collectLeafJSONPaths(fieldType, prefix)...)
				continue
			}

			fieldPrefix := prefix + "." + name
			if fieldType.Kind() == reflect.Slice {
				elem := derefType(fieldType.Elem())
				if shouldRecurseInto(elem) {
					paths = append(paths, collectLeafJSONPaths(elem, fieldPrefix)...)
					continue
				}
				paths = append(paths, fieldPrefix)
				continue
			}

			if fieldType.Kind() == reflect.Map {
				paths = append(paths, fieldPrefix)
				continue
			}

			paths = append(paths, collectLeafJSONPaths(fieldType, fieldPrefix)...)
		}
	default:
		return []string{prefix}
	}

	return paths
}

func shouldRecurseInto(t reflect.Type) bool {
	t = derefType(t)
	if t.Kind() != reflect.Struct {
		return false
	}
	if t == reflect.TypeOf(corev1.Toleration{}) {
		return false
	}
	apiPkg := reflect.TypeOf(infrav1alpha1.Sandbox0InfraSpec{}).PkgPath()
	return t.PkgPath() == apiPkg
}

func derefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func jsonFieldName(field reflect.StructField) (name string, inline bool, ok bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", false, false
	}
	if tag == "" {
		if field.Anonymous {
			return "", true, true
		}
		return lowerCamel(field.Name), false, true
	}

	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, part := range parts[1:] {
		if part == "inline" {
			return "", true, true
		}
	}
	if name == "" {
		if field.Anonymous {
			return "", true, true
		}
		return "", false, false
	}
	return name, false, true
}

func lowerCamel(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
