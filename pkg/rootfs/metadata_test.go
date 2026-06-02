package rootfs

import "testing"

func TestMetadataFromAnnotations(t *testing.T) {
	meta := MetadataFromAnnotations(map[string]string{
		AnnotationSandboxID: " sandbox-a ",
		AnnotationTeamID:    " team-a ",
		AnnotationMode:      " " + ModeS0FSUpperdir + " ",
		AnnotationVolumeID:  " rootfs-a ",
		AnnotationCtldPort:  "8095",
	})
	if meta.SandboxID != "sandbox-a" || meta.TeamID != "team-a" || meta.Mode != ModeS0FSUpperdir || meta.VolumeID != "rootfs-a" || meta.CtldPort != 8095 {
		t.Fatalf("metadata = %+v, want sandbox-a team-a s0fs upperdir rootfs-a 8095", meta)
	}
	if !meta.UsesS0FSUpperdir() {
		t.Fatal("UsesS0FSUpperdir() = false, want true")
	}
	if !meta.CanPrepareS0FSUpperdir() {
		t.Fatal("CanPrepareS0FSUpperdir() = false, want true")
	}
	req, ok := meta.PrepareRootFSRequest()
	if !ok {
		t.Fatal("PrepareRootFSRequest() ok = false, want true")
	}
	if req.SandboxID != "sandbox-a" || req.TeamID != "team-a" || req.RootFSVolumeID != "rootfs-a" {
		t.Fatalf("prepare request = %+v, want sandbox-a team-a rootfs-a", req)
	}
}

func TestMetadataFromAnnotationsIgnoresInvalidCtldPort(t *testing.T) {
	meta := MetadataFromAnnotations(map[string]string{
		AnnotationMode:     ModeS0FSUpperdir,
		AnnotationVolumeID: "rootfs-a",
		AnnotationCtldPort: "not-a-port",
	})
	if meta.CtldPort != 0 {
		t.Fatalf("ctld port = %d, want 0", meta.CtldPort)
	}
}
