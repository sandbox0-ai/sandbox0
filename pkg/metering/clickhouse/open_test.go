package clickhouse

import "testing"

func TestOpenDeferredDoesNotRequireReachableClickHouse(t *testing.T) {
	db, repo, err := OpenDeferred(OpenConfig{
		DSN: "clickhouse://127.0.0.1:1/default",
		Schema: Config{
			Database: "sandbox0_metering",
		},
		Migrate: true,
	})
	if err != nil {
		t.Fatalf("OpenDeferred: %v", err)
	}
	defer db.Close()
	if repo == nil {
		t.Fatal("OpenDeferred returned nil repository")
	}
}
