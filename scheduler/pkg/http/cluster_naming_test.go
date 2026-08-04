package http

import (
	"strings"
	"testing"

	sharednaming "github.com/sandbox0-ai/sandbox0/pkg/naming"
)

func TestClusterIDFromName(t *testing.T) {
	clusterID, err := clusterIDFromName("My Cluster East 1")
	if err != nil {
		t.Fatalf("clusterIDFromName: %v", err)
	}
	if len(clusterID) > sharednaming.ClusterIDMaxLen {
		t.Fatalf("cluster ID too long: %d", len(clusterID))
	}
	if want := "my-cluster--820f8178"; clusterID != want {
		t.Fatalf("clusterID = %q, want %q", clusterID, want)
	}
}

func TestValidateClusterName(t *testing.T) {
	for _, name := range []string{"", "  ", "team/cluster", strings.Repeat("a", maxClusterNameLength+1)} {
		if err := validateClusterName(name); err == nil {
			t.Fatalf("validateClusterName(%q) succeeded, want error", name)
		}
	}
	if err := validateClusterName("My Cluster East 1"); err != nil {
		t.Fatalf("validateClusterName valid name: %v", err)
	}
}
