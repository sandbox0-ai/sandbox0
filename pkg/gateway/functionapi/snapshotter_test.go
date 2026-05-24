package functionapi

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestHTTPVolumeSnapshotterPrepareArtifactMounts(t *testing.T) {
	var gotAccessMode string
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/artifacts/artifact-1/volume" {
			t.Errorf("path = %s, want artifact volume path", r.URL.Path)
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("internal auth header is empty")
		}
		var req createArtifactVolumeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		gotAccessMode = req.AccessMode
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"success":true,"data":{"id":"prepared-volume"}}`))
	}))
	defer clusterGateway.Close()

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	snapshotter := NewHTTPVolumeSnapshotter(StaticClusterGatewayURLResolver(clusterGateway.URL), internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceRegionalGateway,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	}), clusterGateway.Client(), zap.NewNop())

	mounts, cleanup, err := snapshotter.PrepareArtifactMounts(context.Background(), &authn.AuthContext{TeamID: "team-1", UserID: "user-1"}, functions.FunctionRevisionSpec{
		Mounts: []functions.FunctionRevisionMount{{
			MountPoint: "/workspace/app",
			Mode:       functions.FunctionRevisionMountModeReadWrite,
			Source: functions.FunctionRevisionMountSource{
				Type:       functions.FunctionRevisionMountSourceArtifact,
				ArtifactID: "artifact-1",
			},
		}},
	}, functionruntime.Metadata{FunctionID: "fn-1"})
	if err != nil {
		t.Fatalf("PrepareArtifactMounts() error = %v", err)
	}
	if gotAccessMode != "RWX" {
		t.Fatalf("access mode = %q, want RWX", gotAccessMode)
	}
	if len(mounts) != 1 || mounts[0].Source.SandboxVolumeID != "prepared-volume" {
		t.Fatalf("mounts = %+v, want prepared volume", mounts)
	}
	if len(cleanup) != 1 || cleanup[0].SandboxVolumeID != "prepared-volume" || cleanup[0].MountPoint != "/workspace/app" {
		t.Fatalf("cleanup = %+v, want prepared volume cleanup", cleanup)
	}
}
