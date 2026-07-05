package cases

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	mountpointS3CompatEnvVar = "E2E_MOUNTPOINT_S3_COMPAT"
	mountpointS3Bucket       = "sandbox0"
	mountpointS3MountPath    = "/workspace/mountpoint-s3"
)

var mountpointS3CompatSourceCases = []string{
	"semantics_doc_test::basic_directory_structure_s3",
	"semantics_doc_test::keys_ending_in_delimiter_s3",
	"semantics_doc_test::files_shadowed_by_directories_s3",
	"lookup_test::lookup_previously_shadowed_file_test_s3",
	"mkdir_test::mkdir_visible_locally_test_s3",
	"readdir_test::readdir_while_writing_s3",
	"write_test::sequential_write_test_s3",
	"write_test::flush_test_s3",
	"write_test::out_of_order_write_test_s3",
	"write_test::overwrite_disallowed_on_concurrent_read_test_s3",
	"write_test::overwrite_truncate_test_s3",
	"unlink_test::simple_unlink_test_s3",
	"unlink_test::unlink_writehandle_test_s3",
	"setattr_test::setattr_test_s3",
	"rename_test::general-purpose-bucket-renames-rejected",
}

type mountpointS3CompatStore struct {
	scoped      objectstore.Store
	prefix      string
	stopForward func()
}

func assertMountpointS3Compatibility(env *framework.ScenarioEnv, session *e2eutils.Session) {
	if !mountpointS3CompatEnabled() {
		Skip(fmt.Sprintf("set %s=true to run mountpoint-s3 compatibility probes", mountpointS3CompatEnvVar))
	}
	runtimeClass := strings.ToLower(strings.TrimSpace(env.Config.SandboxRuntimeClassName))
	Expect(runtimeClass == "" || strings.Contains(runtimeClass, "runc")).To(BeTrue(),
		"mountpoint-s3 compatibility requires a runc-compatible sandbox runtime, got %q", env.Config.SandboxRuntimeClassName)
	for _, sourceCase := range mountpointS3CompatSourceCases {
		GinkgoWriter.Printf("mountpoint-s3 compat source: %s\n", sourceCase)
	}

	store := openMountpointS3CompatStore(env)
	DeferCleanup(store.cleanup)
	seedMountpointS3CompatObjects(store.scoped)

	backend := apispec.S3
	accessMode := apispec.RWO
	provider := apispec.CreateSandboxVolumeS3ConfigProviderAws
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{
		Backend:    &backend,
		AccessMode: &accessMode,
		S3: &apispec.CreateSandboxVolumeS3Config{
			Provider: &provider,
			Bucket:   mountpointS3Bucket,
			Prefix:   &store.prefix,
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "s3 volume id")
	DeferCleanup(func() {
		deleteSandboxVolumeForCleanup(env, session, volumeID)
	})

	templateID := createVolumePortalTemplate(env, session, mountpointS3MountPath)
	template, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), templateID)
	Expect(err).NotTo(HaveOccurred())
	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(template.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())

	claimResp, err := session.ClaimSandboxWithRequest(env.TestCtx.Context, GinkgoT(), apispec.ClaimRequest{
		Template: &templateID,
		Mounts: &[]apispec.ClaimMountRequest{{
			SandboxvolumeId: volumeID,
			MountPoint:      mountpointS3MountPath,
		}},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(claimResp).NotTo(BeNil())
	sandboxID := claimResp.SandboxId
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})
	Expect(claimResp.BootstrapMounts).NotTo(BeNil())
	Expect(*claimResp.BootstrapMounts).NotTo(BeEmpty())
	Expect((*claimResp.BootstrapMounts)[0].State).To(Equal(apispec.MountStatusStateMounted))

	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	podName := sandbox.PodName
	Expect(podName).NotTo(BeEmpty())

	assertMountpointS3Projection(env, templateNamespace, podName, store.scoped)
	assertMountpointS3ExternalUpdates(env, templateNamespace, podName, store.scoped)
	assertMountpointS3WriteLifecycle(env, templateNamespace, podName, store.scoped)
	assertMountpointS3UnsupportedAndOverwrite(env, templateNamespace, podName, store.scoped)
	assertMountpointS3Deletes(env, templateNamespace, podName, store.scoped)
}

func mountpointS3CompatEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(mountpointS3CompatEnvVar))) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func openMountpointS3CompatStore(env *framework.ScenarioEnv) *mountpointS3CompatStore {
	serviceName := env.Infra.Name + "-rustfs"
	endpoint, stopForward, err := framework.PortForwardService(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName, 9000)
	Expect(err).NotTo(HaveOccurred())

	secretName := env.Infra.Name + "-sandbox0-rustfs-credentials"
	accessKey, err := framework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, secretName, "RUSTFS_ACCESS_KEY")
	Expect(err).NotTo(HaveOccurred())
	secretKey, err := framework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, secretName, "RUSTFS_SECRET_KEY")
	Expect(err).NotTo(HaveOccurred())

	base, err := objectstore.Create(objectstore.Config{
		Type:      objectstore.TypeS3,
		Bucket:    mountpointS3Bucket,
		Region:    "us-east-1",
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	})
	Expect(err).NotTo(HaveOccurred())

	prefix := fmt.Sprintf("e2e/mountpoint-s3-compat/%d", time.Now().UnixNano())
	return &mountpointS3CompatStore{
		scoped:      objectstore.Prefix(base, prefix),
		prefix:      prefix,
		stopForward: stopForward,
	}
}

func (s *mountpointS3CompatStore) cleanup() {
	if s == nil {
		return
	}
	if s.scoped != nil {
		cleanupMountpointS3Prefix(s.scoped)
	}
	if s.stopForward != nil {
		s.stopForward()
	}
}

func cleanupMountpointS3Prefix(store objectstore.Store) {
	token := ""
	for {
		infos, more, next, err := store.List("", "", token, "", 1000)
		if err != nil {
			GinkgoWriter.Printf("cleanup mountpoint-s3 compat prefix list failed: %v\n", err)
			return
		}
		for _, info := range infos {
			if info.IsPrefix || strings.TrimSpace(info.Key) == "" {
				continue
			}
			if err := store.Delete(info.Key); err != nil && !objectstore.IsNotFound(err) {
				GinkgoWriter.Printf("cleanup mountpoint-s3 compat object %q failed: %v\n", info.Key, err)
			}
		}
		if !more || next == "" {
			return
		}
		token = next
	}
}

func seedMountpointS3CompatObjects(store objectstore.Store) {
	putMountpointS3Object(store, "colors/blue/image.jpg", "blue image")
	putMountpointS3Object(store, "colors/red/image.jpg", "red image")
	putMountpointS3Object(store, "colors/list.txt", "list")
	putMountpointS3Object(store, "blue", "shadowed")
	putMountpointS3Object(store, "blue/image.jpg", "nested")
	putMountpointS3Object(store, "marker/", "")
	putMountpointS3Object(store, "external/before.txt", "external-before")
	putMountpointS3Object(store, "random/read.txt", "0123456789abcdef")
}

func assertMountpointS3Projection(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("projecting S3 object keys using mountpoint directory semantics")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
test -d "$M/colors"
test -d "$M/colors/blue"
test -d "$M/colors/red"
test "$(cat "$M/colors/list.txt")" = "list"
test "$(cat "$M/colors/blue/image.jpg")" = "blue image"
test "$(cat "$M/colors/red/image.jpg")" = "red image"
test -d "$M/blue"
test "$(cat "$M/blue/image.jpg")" = "nested"
test ! -f "$M/blue"
test -d "$M/marker"
slice="$(dd if="$M/random/read.txt" bs=1 skip=5 count=8 2>/dev/null)"
test "$slice" = "56789abc"
mkdir "$M/local-only"
test -d "$M/local-only"
if rmdir "$M/marker"; then
  echo "rmdir unexpectedly removed S3 directory marker"
  exit 1
fi
rmdir "$M/local-only"
test ! -e "$M/local-only"
rm "$M/blue/image.jpg"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if [ -f "$M/blue" ] && [ "$(cat "$M/blue")" = "shadowed" ]; then
    exit 0
  fi
  sleep 1
done
echo "shadowed file did not reappear after deleting directory child"
exit 1
`, shellQuote(mountpointS3MountPath)))
	expectMountpointS3ObjectMissingEventually(store, "local-only/")
	expectMountpointS3ObjectMissingEventually(store, "blue/image.jpg")
	expectMountpointS3ObjectEventually(store, "blue", []byte("shadowed"))
}

func assertMountpointS3ExternalUpdates(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("making external S3 object writes visible inside the sandbox")
	putMountpointS3Object(store, "external/after.txt", "external-after")
	waitMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
test "$(cat "$M/external/before.txt")" = "external-before"
test "$(cat "$M/external/after.txt")" = "external-after"
`, shellQuote(mountpointS3MountPath)), 30*time.Second)
}

func assertMountpointS3WriteLifecycle(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("deferring S3 visibility until writer close and blocking readers while writing")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
mkdir -p "$M/write-lifecycle"
rm -f /tmp/s3-compat-writer-ready /tmp/s3-compat-writer-done /tmp/s3-compat-writer.log
nohup sh -c 'exec 3> "$1"; printf "%%s" "before-close" >&3; echo ready > /tmp/s3-compat-writer-ready; sleep 20; exec 3>&-; echo done > /tmp/s3-compat-writer-done' sh "$M/write-lifecycle/open.txt" >/tmp/s3-compat-writer.log 2>&1 &
	`, shellQuote(mountpointS3MountPath)))
	waitMountpointS3Script(env, namespace, podName, "test -f /tmp/s3-compat-writer-ready", 20*time.Second)
	expectMountpointS3ObjectMissingConsistently(store, "write-lifecycle/open.txt", 3*time.Second)

	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if cat "$M/write-lifecycle/open.txt" >/tmp/s3-compat-reader-while-writer 2>/tmp/s3-compat-reader-while-writer.err; then
  echo "read unexpectedly succeeded while writer was open"
  exit 1
fi
if rm "$M/write-lifecycle/open.txt" 2>/tmp/s3-compat-unlink-while-writer.err; then
  echo "unlink unexpectedly succeeded while writer was open"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))

	waitMountpointS3Script(env, namespace, podName, "test -f /tmp/s3-compat-writer-done", 30*time.Second)
	expectMountpointS3ObjectEventually(store, "write-lifecycle/open.txt", []byte("before-close"))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
test "$(cat %s)" = "before-close"
`, shellQuote(mountpointS3MountPath+"/write-lifecycle/open.txt")))

	By("blocking overwrite while a reader is open")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
rm -f /tmp/s3-compat-reader-ready /tmp/s3-compat-reader-done /tmp/s3-compat-reader.log
nohup sh -c 'exec 3< "$1"; echo ready > /tmp/s3-compat-reader-ready; sleep 20; cat <&3 >/dev/null; exec 3<&-; echo done > /tmp/s3-compat-reader-done' sh "$M/write-lifecycle/open.txt" >/tmp/s3-compat-reader.log 2>&1 &
`, shellQuote(mountpointS3MountPath)))
	waitMountpointS3Script(env, namespace, podName, "test -f /tmp/s3-compat-reader-ready", 20*time.Second)
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if sh -c 'printf "%%s" "blocked" > "$1"' sh "$M/write-lifecycle/open.txt" 2>/tmp/s3-compat-overwrite-while-reader.err; then
  echo "overwrite unexpectedly succeeded while reader was open"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))
	waitMountpointS3Script(env, namespace, podName, "test -f /tmp/s3-compat-reader-done", 30*time.Second)
	expectMountpointS3ObjectEventually(store, "write-lifecycle/open.txt", []byte("before-close"))
}

func assertMountpointS3UnsupportedAndOverwrite(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("rejecting unsupported mountpoint metadata and nonsequential write operations")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
file="$M/write-lifecycle/open.txt"
if sh -c 'printf "%%s" "append" >> "$1"' sh "$file" 2>/tmp/s3-compat-append.err; then
  echo "append unexpectedly succeeded for a general-purpose S3 bucket"
  exit 1
fi
if sh -c 'printf x | dd of="$1" bs=1 seek=1 count=1 2>/tmp/s3-compat-nonseq.err' sh "$M/write-lifecycle/non-sequential.txt"; then
  echo "nonsequential write unexpectedly succeeded"
  exit 1
fi
test ! -e "$M/write-lifecycle/non-sequential.txt"
if chmod 600 "$file" 2>/tmp/s3-compat-chmod.err; then
  echo "chmod unexpectedly succeeded"
  exit 1
fi
if ln "$file" "$M/write-lifecycle/hardlink.txt" 2>/tmp/s3-compat-link.err; then
  echo "hard link unexpectedly succeeded"
  exit 1
fi
if ln -s "$file" "$M/write-lifecycle/symlink.txt" 2>/tmp/s3-compat-symlink.err; then
  echo "symlink unexpectedly succeeded"
  exit 1
fi
if mv "$file" "$M/write-lifecycle/renamed.txt" 2>/tmp/s3-compat-rename.err; then
  echo "rename unexpectedly succeeded for a general-purpose S3 bucket"
  exit 1
fi
if command -v python3 >/dev/null 2>&1; then
  if python3 - "$file" <<'PY'
import os
import sys
os.listxattr(sys.argv[1])
PY
  then
    echo "listxattr unexpectedly succeeded"
    exit 1
  fi
fi
truncate -s 0 "$file"
test ! -s "$file"
printf "%%s" "replacement" > "$file"
sync
test "$(cat "$file")" = "replacement"
`, shellQuote(mountpointS3MountPath)))
	expectMountpointS3ObjectMissingEventually(store, "write-lifecycle/non-sequential.txt")
	expectMountpointS3ObjectEventually(store, "write-lifecycle/open.txt", []byte("replacement"))
}

func assertMountpointS3Deletes(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("reflecting sandbox deletes in S3 and external S3 deletes in the sandbox")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
rm "$M/write-lifecycle/open.txt"
`, shellQuote(mountpointS3MountPath)))
	expectMountpointS3ObjectMissingEventually(store, "write-lifecycle/open.txt")

	Expect(store.Delete("external/after.txt")).To(Succeed())
	waitMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if ! ls "$M/external/after.txt" >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done
echo "externally deleted object remained visible in sandbox"
exit 1
`, shellQuote(mountpointS3MountPath)), 20*time.Second)
}

func runMountpointS3Script(env *framework.ScenarioEnv, namespace, podName, script string) {
	output, err := execInSandboxPod(env, namespace, podName, script)
	Expect(err).NotTo(HaveOccurred(), "script failed with output:\n%s", output)
}

func waitMountpointS3Script(env *framework.ScenarioEnv, namespace, podName, script string, timeout time.Duration) {
	Eventually(func() error {
		_, err := execInSandboxPod(env, namespace, podName, script)
		return err
	}).WithTimeout(timeout).WithPolling(1 * time.Second).Should(Succeed())
}

func putMountpointS3Object(store objectstore.Store, key, value string) {
	Expect(store.Put(key, strings.NewReader(value))).To(Succeed(), "put S3 compat object %q", key)
}

func expectMountpointS3ObjectEventually(store objectstore.Store, key string, want []byte) {
	Eventually(func(g Gomega) {
		got, err := readMountpointS3Object(store, key)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got).To(Equal(want))
	}).WithTimeout(30*time.Second).WithPolling(1*time.Second).Should(Succeed(), "object %q should match expected content", key)
}

func expectMountpointS3ObjectMissingEventually(store objectstore.Store, key string) {
	Eventually(func(g Gomega) {
		reader, err := store.Get(key, 0, -1)
		if err == nil {
			_ = reader.Close()
		}
		g.Expect(err).To(HaveOccurred())
		g.Expect(objectstore.IsNotFound(err)).To(BeTrue(), "object %q error = %v", key, err)
	}).WithTimeout(30*time.Second).WithPolling(1*time.Second).Should(Succeed(), "object %q should be missing", key)
}

func expectMountpointS3ObjectMissingConsistently(store objectstore.Store, key string, duration time.Duration) {
	Consistently(func(g Gomega) {
		reader, err := store.Get(key, 0, -1)
		if err == nil {
			_ = reader.Close()
		}
		g.Expect(err).To(HaveOccurred())
		g.Expect(objectstore.IsNotFound(err)).To(BeTrue(), "object %q error = %v", key, err)
	}).WithTimeout(duration).WithPolling(500*time.Millisecond).Should(Succeed(), "object %q should stay missing", key)
}

func readMountpointS3Object(store objectstore.Store, key string) ([]byte, error) {
	reader, err := store.Get(key, 0, -1)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
