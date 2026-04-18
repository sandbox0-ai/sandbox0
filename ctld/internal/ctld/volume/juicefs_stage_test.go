package volume

import (
	"path/filepath"
	"testing"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

func TestJuiceFSStageProviderBuildsPerTeamVolumeMount(t *testing.T) {
	provider := NewJuiceFSStageProvider(&apiconfig.StorageProxyConfig{
		ObjectStorageType:      "s3",
		MetaURL:                "postgres://juicefs",
		S3Bucket:               "sandbox0-data",
		S3Endpoint:             "http://minio:9000",
		JuiceFSAttrTimeout:     "1s",
		JuiceFSEntryTimeout:    "2s",
		JuiceFSDirEntryTimeout: "3s",
	}, "juicefs", t.TempDir(), t.TempDir(), &recordingRunner{})

	stageDir := filepath.Join(provider.StagingRoot, "vol-1")
	args, err := provider.mountArgs(ctldapi.VolumeAttachRequest{
		TeamID:          "team-1",
		SandboxVolumeID: "vol-1",
		CacheSize:       "256",
		Prefetch:        4,
		BufferSize:      "512",
		Writeback:       true,
	}, stageDir)
	if err != nil {
		t.Fatalf("mountArgs() error = %v", err)
	}

	assertContainsString(t, args, "--subdir")
	assertContainsString(t, args, "/volumes/vol-1")
	assertContainsString(t, args, "--bucket")
	assertContainsString(t, args, "sandbox0-data")
	assertContainsString(t, args, "--endpoint")
	assertContainsString(t, args, "http://minio:9000")
	assertContainsString(t, args, "--object-prefix")
	assertContainsString(t, args, "sandboxvolumes/team-1/vol-1")
	assertContainsString(t, args, "--cache-size")
	assertContainsString(t, args, "256")
	assertContainsString(t, args, "--prefetch")
	assertContainsString(t, args, "4")
	assertContainsString(t, args, "--writeback")
	if args[len(args)-2] != "postgres://juicefs" || args[len(args)-1] != stageDir {
		t.Fatalf("mount tail = %#v, want meta url and stage dir", args[len(args)-2:])
	}
}

func assertContainsString(t *testing.T, values []string, want string) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("expected %#v to contain %q", values, want)
}
