package migrations

import (
	"strconv"
	"strings"
	"testing"
)

// Independently merged migrations must remain loadable as one Goose sequence.
func TestEmbeddedMigrationVersionsAreUnique(t *testing.T) {
	entries, err := FS.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	versions := make(map[int64]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		version, err := strconv.ParseInt(prefix, 10, 64)
		if !ok || err != nil || version <= 0 {
			t.Fatalf("invalid migration filename %q", name)
		}
		if previous, exists := versions[version]; exists {
			t.Fatalf("duplicate migration version %d: %s and %s", version, previous, name)
		}
		versions[version] = name
	}
}
