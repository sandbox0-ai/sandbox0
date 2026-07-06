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
	mountpointS3Bucket       = "sandbox0-mountpoint-compat"
	mountpointS3MountPath    = "/workspace/mountpoint-s3"
	mountpointS3FixtureImage = "sandbox0ai/otemplates:default-v0.2.0"
	mountpointS3FixtureName  = "mountpoint-s3-compat-s3"
	mountpointS3AccessKey    = "mountpointcompat"
	mountpointS3SecretKey    = "mountpointcompat-secret"
	mountpointS3Region       = "us-east-1"
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
	scoped         objectstore.Store
	prefix         string
	endpointURL    string
	accessKey      string
	secretKey      string
	stopForward    func()
	cleanupFixture func()
}

func assertMountpointS3Compatibility(env *framework.ScenarioEnv, session *e2eutils.Session) {
	if !mountpointS3CompatEnabled() {
		Skip(fmt.Sprintf("set %s=true to run mountpoint-s3 compatibility probes", mountpointS3CompatEnvVar))
	}
	for _, sourceCase := range mountpointS3CompatSourceCases {
		GinkgoWriter.Printf("mountpoint-s3 compat source: %s\n", sourceCase)
	}

	store := openMountpointS3CompatStore(env)
	DeferCleanup(store.cleanup)
	seedMountpointS3CompatObjects(store.scoped)

	backend := apispec.S3
	accessMode := apispec.RWO
	provider := apispec.CreateSandboxVolumeS3ConfigProviderAws
	region := mountpointS3Region
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{
		Backend:    &backend,
		AccessMode: &accessMode,
		S3: &apispec.CreateSandboxVolumeS3Config{
			Provider:    &provider,
			Bucket:      mountpointS3Bucket,
			Prefix:      &store.prefix,
			Region:      &region,
			EndpointUrl: &store.endpointURL,
			AccessKey:   &store.accessKey,
			SecretKey:   &store.secretKey,
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
	assertMountpointS3Overwrite(env, templateNamespace, podName, store.scoped)
	assertMountpointS3WriteLifecycle(env, templateNamespace, podName, store.scoped)
	assertMountpointS3Deletes(env, templateNamespace, podName, store.scoped)
	assertMountpointS3UnsupportedOperations(env, templateNamespace, podName, store.scoped)
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
	cleanupFixture := ensureMountpointS3Fixture(env)
	endpoint, stopForward, err := framework.PortForwardService(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, mountpointS3FixtureName, 9000)
	Expect(err).NotTo(HaveOccurred())

	base, err := objectstore.Create(objectstore.Config{
		Type:      objectstore.TypeS3,
		Bucket:    mountpointS3Bucket,
		Region:    mountpointS3Region,
		Endpoint:  endpoint,
		AccessKey: mountpointS3AccessKey,
		SecretKey: mountpointS3SecretKey,
	})
	Expect(err).NotTo(HaveOccurred())
	createMountpointS3Bucket(base)

	prefix := fmt.Sprintf("e2e/mountpoint-s3-compat/%d", time.Now().UnixNano())
	return &mountpointS3CompatStore{
		scoped:         objectstore.Prefix(base, prefix),
		prefix:         prefix,
		endpointURL:    fmt.Sprintf("http://%s.%s.svc.cluster.local:9000", mountpointS3FixtureName, env.Infra.Namespace),
		accessKey:      mountpointS3AccessKey,
		secretKey:      mountpointS3SecretKey,
		stopForward:    stopForward,
		cleanupFixture: cleanupFixture,
	}
}

func ensureMountpointS3Fixture(env *framework.ScenarioEnv) func() {
	manifest := fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s
  namespace: %[2]s
data:
  fake_s3.py: |
    from email.utils import formatdate
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.parse import parse_qs, unquote, urlparse
    from xml.sax.saxutils import escape

    objects = {}
    buckets = set()

    def http_date():
        return formatdate(usegmt=True)

    def s3_time():
        return "2026-01-01T00:00:00.000Z"

    def split_path(path):
        raw = unquote(urlparse(path).path).lstrip("/")
        if not raw:
            return "", ""
        parts = raw.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def send_empty(self, status):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_HEAD(self):
            bucket, key = split_path(self.path)
            if bucket and not key:
                buckets.add(bucket)
                self.send_empty(200)
                return
            body = objects.get((bucket, key))
            if body is None:
                self.send_empty(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Last-Modified", http_date())
            self.send_header("ETag", "\"test\"")
            self.end_headers()

        def do_PUT(self):
            bucket, key = split_path(self.path)
            length = int(self.headers.get("Content-Length") or "0")
            body = self.rfile.read(length) if length > 0 else b""
            if bucket and not key:
                buckets.add(bucket)
                self.send_empty(200)
                return
            buckets.add(bucket)
            objects[(bucket, key)] = body
            self.send_response(200)
            self.send_header("ETag", "\"test\"")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_DELETE(self):
            bucket, key = split_path(self.path)
            objects.pop((bucket, key), None)
            self.send_empty(204)

        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if query.get("list-type", [""])[0] == "2":
                self.list_objects_v2(query)
                return
            bucket, key = split_path(self.path)
            body = objects.get((bucket, key))
            if body is None:
                self.send_empty(404)
                return
            start = 0
            end = len(body) - 1
            status = 200
            range_header = self.headers.get("Range", "")
            if range_header.startswith("bytes="):
                status = 206
                raw_start, _, raw_end = range_header[len("bytes="):].partition("-")
                start = int(raw_start or "0")
                end = int(raw_end) if raw_end else len(body) - 1
                if end >= len(body):
                    end = len(body) - 1
            chunk = body[start:end + 1] if body else b""
            self.send_response(status)
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Last-Modified", http_date())
            self.send_header("ETag", "\"test\"")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            self.end_headers()
            self.wfile.write(chunk)

        def list_objects_v2(self, query):
            bucket, _ = split_path(self.path)
            prefix = query.get("prefix", [""])[0]
            delimiter = query.get("delimiter", [""])[0]
            start_after = query.get("start-after", [""])[0]
            max_keys = int(query.get("max-keys", ["1000"])[0])
            contents = []
            prefixes = set()
            for obj_bucket, key in sorted(objects.keys()):
                if obj_bucket != bucket or not key.startswith(prefix) or key <= start_after:
                    continue
                suffix = key[len(prefix):]
                if delimiter and delimiter in suffix:
                    prefixes.add(prefix + suffix.split(delimiter, 1)[0] + delimiter)
                    continue
                contents.append(key)
                if len(contents) + len(prefixes) >= max_keys:
                    break
            body = ['<?xml version="1.0" encoding="UTF-8"?>',
                    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
                    f'<Name>{escape(bucket)}</Name>',
                    f'<Prefix>{escape(prefix)}</Prefix>',
                    f'<KeyCount>{len(contents) + len(prefixes)}</KeyCount>',
                    f'<MaxKeys>{max_keys}</MaxKeys>',
                    '<IsTruncated>false</IsTruncated>']
            for key in contents:
                value = objects[(bucket, key)]
                body.extend(['<Contents>',
                             f'<Key>{escape(key)}</Key>',
                             f'<LastModified>{s3_time()}</LastModified>',
                             '<ETag>"test"</ETag>',
                             f'<Size>{len(value)}</Size>',
                             '<StorageClass>STANDARD</StorageClass>',
                             '</Contents>'])
            for common_prefix in sorted(prefixes):
                body.extend(['<CommonPrefixes>',
                             f'<Prefix>{escape(common_prefix)}</Prefix>',
                             '</CommonPrefixes>'])
            body.append('</ListBucketResult>')
            payload = ''.join(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app.kubernetes.io/name: %[1]s
  ports:
    - name: s3
      port: 9000
      targetPort: 9000
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: %[1]s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: %[1]s
    spec:
      containers:
        - name: fake-s3
          image: %[3]s
          imagePullPolicy: IfNotPresent
          command:
            - python3
            - /app/fake_s3.py
          ports:
            - name: s3
              containerPort: 9000
          readinessProbe:
            tcpSocket:
              port: 9000
            periodSeconds: 2
            failureThreshold: 30
          volumeMounts:
            - name: script
              mountPath: /app
      volumes:
        - name: script
          configMap:
            name: %[1]s
`, mountpointS3FixtureName, env.Infra.Namespace, mountpointS3FixtureImage)
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-mountpoint-s3-fixture-", manifest)).To(Succeed())
	Expect(framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, mountpointS3FixtureName, "3m")).To(Succeed())
	return func() {
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "deployment/"+mountpointS3FixtureName, "service/"+mountpointS3FixtureName, "configmap/"+mountpointS3FixtureName, "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
	}
}

func createMountpointS3Bucket(store objectstore.Store) {
	err := store.Create()
	if err == nil {
		return
	}
	message := strings.ToLower(err.Error())
	Expect(message).To(Or(ContainSubstring("already"), ContainSubstring("bucketalready")), "create bucket failed: %v", err)
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
	if s.cleanupFixture != nil {
		s.cleanupFixture()
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
S0_STEP=start
trap 'code=$?; echo "failed at: $S0_STEP"; ls -la "$M" || true; [ -e "$M/blue" ] && ls -la "$M/blue" || true; [ -e "$M/marker" ] && ls -la "$M/marker" || true; exit $code' EXIT
step() { S0_STEP="$1"; }
step "colors directory"
test -d "$M/colors"
step "colors blue directory"
test -d "$M/colors/blue"
step "colors red directory"
test -d "$M/colors/red"
step "colors list content"
test "$(cat "$M/colors/list.txt")" = "list"
step "colors blue image content"
test "$(cat "$M/colors/blue/image.jpg")" = "blue image"
step "colors red image content"
test "$(cat "$M/colors/red/image.jpg")" = "red image"
step "shadowed blue directory"
test -d "$M/blue"
step "shadowed blue child content"
test "$(cat "$M/blue/image.jpg")" = "nested"
step "shadowed blue is not file"
test ! -f "$M/blue"
step "delimiter marker directory"
test -d "$M/marker"
step "random ranged read"
slice="$(dd if="$M/random/read.txt" bs=1 skip=5 count=8 2>/dev/null)"
test "$slice" = "56789abc"
step "local mkdir"
mkdir "$M/local-only"
test -d "$M/local-only"
step "remote marker rmdir rejected"
if rmdir "$M/marker"; then
  echo "rmdir unexpectedly removed S3 directory marker"
  exit 1
fi
step "local rmdir"
rmdir "$M/local-only"
step "local rmdir hidden"
test ! -e "$M/local-only"
step "remove shadowing child"
rm "$M/blue/image.jpg"
step "shadowed file reappears"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if [ -f "$M/blue" ] && [ "$(cat "$M/blue")" = "shadowed" ]; then
    trap - EXIT
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
rm -f /tmp/s3-compat-reader-ready /tmp/s3-compat-reader-done /tmp/s3-compat-reader-byte /tmp/s3-compat-reader.log
nohup sh -c 'exec 3< "$1"; dd bs=1 count=1 <&3 >/tmp/s3-compat-reader-byte 2>/dev/null; echo ready > /tmp/s3-compat-reader-ready; sleep 20; cat <&3 >/dev/null; exec 3<&-; echo done > /tmp/s3-compat-reader-done' sh "$M/write-lifecycle/open.txt" >/tmp/s3-compat-reader.log 2>&1 &
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

func assertMountpointS3Overwrite(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("overwriting existing S3 objects through truncate semantics")
	putMountpointS3Object(store, "write-lifecycle/truncate.txt", "before-truncate")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
file="$M/write-lifecycle/truncate.txt"
: > "$file"
test ! -s "$file"
printf "%%s" "replacement" > "$file"
sync
test "$(cat "$file")" = "replacement"
`, shellQuote(mountpointS3MountPath)))
	expectMountpointS3ObjectEventually(store, "write-lifecycle/truncate.txt", []byte("replacement"))
}

func assertMountpointS3UnsupportedOperations(env *framework.ScenarioEnv, namespace, podName string, store objectstore.Store) {
	By("rejecting unsupported mountpoint metadata and nonsequential write operations")
	putMountpointS3Object(store, "write-lifecycle/append.txt", "append-base")
	putMountpointS3Object(store, "write-lifecycle/chmod.txt", "chmod-base")
	putMountpointS3Object(store, "write-lifecycle/link-source.txt", "link-base")
	putMountpointS3Object(store, "write-lifecycle/rename-source.txt", "rename-base")
	putMountpointS3Object(store, "write-lifecycle/xattr.txt", "xattr-base")
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
chmod_file="$M/write-lifecycle/chmod.txt"
mode_before="$(stat -c %%a "$chmod_file")"
if chmod 600 "$chmod_file" 2>/tmp/s3-compat-chmod.err; then
  mode_after="$(stat -c %%a "$chmod_file")"
  if [ "$mode_after" != "$mode_before" ]; then
    echo "chmod unexpectedly changed mode from $mode_before to $mode_after"
    exit 1
  fi
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if ln "$M/write-lifecycle/link-source.txt" "$M/write-lifecycle/hardlink.txt" 2>/tmp/s3-compat-link.err; then
  echo "hard link unexpectedly succeeded"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if ln -s "$M/write-lifecycle/link-source.txt" "$M/write-lifecycle/symlink.txt" 2>/tmp/s3-compat-symlink.err; then
  echo "symlink unexpectedly succeeded"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if command -v python3 >/dev/null 2>&1; then
  python3 - "$M/write-lifecycle/xattr.txt" 2>/tmp/s3-compat-listxattr.err <<'PY'
import os
import sys
attrs = os.listxattr(sys.argv[1])
if attrs:
    raise SystemExit(f"unexpected xattrs: {attrs!r}")
PY
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for direct rename syscall coverage"
  exit 1
fi
if python3 - "$M/write-lifecycle/rename-source.txt" "$M/write-lifecycle/renamed.txt" 2>/tmp/s3-compat-rename.err <<'PY'
import os
import sys
os.rename(sys.argv[1], sys.argv[2])
PY
then
  echo "rename unexpectedly succeeded for a general-purpose S3 bucket"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if sh -c 'printf "%%s" "append" >> "$1"' sh "$M/write-lifecycle/append.txt" 2>/tmp/s3-compat-append.err; then
  echo "append unexpectedly succeeded for a general-purpose S3 bucket"
  exit 1
fi
`, shellQuote(mountpointS3MountPath)))
	runMountpointS3Script(env, namespace, podName, fmt.Sprintf(`
set -eu
M=%s
if sh -c 'printf x | dd of="$1" bs=1 seek=1 count=1 2>/tmp/s3-compat-nonseq.err' sh "$M/write-lifecycle/non-sequential.txt"; then
  echo "nonsequential write unexpectedly succeeded"
  exit 1
fi
test ! -e "$M/write-lifecycle/non-sequential.txt"
`, shellQuote(mountpointS3MountPath)))
	expectMountpointS3ObjectMissingEventually(store, "write-lifecycle/non-sequential.txt")
	expectMountpointS3ObjectEventually(store, "write-lifecycle/append.txt", []byte("append-base"))
	expectMountpointS3ObjectEventually(store, "write-lifecycle/chmod.txt", []byte("chmod-base"))
	expectMountpointS3ObjectEventually(store, "write-lifecycle/link-source.txt", []byte("link-base"))
	expectMountpointS3ObjectEventually(store, "write-lifecycle/rename-source.txt", []byte("rename-base"))
	expectMountpointS3ObjectMissingEventually(store, "write-lifecycle/renamed.txt")
	expectMountpointS3ObjectEventually(store, "write-lifecycle/xattr.txt", []byte("xattr-base"))
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
	if err != nil {
		output += mountpointS3PodLogs(env, namespace, podName)
	}
	Expect(err).NotTo(HaveOccurred(), "script failed with output:\n%s", output)
}

func waitMountpointS3Script(env *framework.ScenarioEnv, namespace, podName, script string, timeout time.Duration) {
	Eventually(func() error {
		_, err := execInSandboxPod(env, namespace, podName, script)
		return err
	}).WithTimeout(timeout).WithPolling(1 * time.Second).Should(Succeed())
}

func mountpointS3PodLogs(env *framework.ScenarioEnv, namespace, podName string) string {
	var builder strings.Builder
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"logs", podName,
		"--namespace", namespace,
		"-c", "procd",
		"--tail", "200",
	)
	if err != nil {
		builder.WriteString(fmt.Sprintf("\nprocd logs unavailable: %v\n%s", err, output))
	} else {
		builder.WriteString("\nprocd logs:\n")
		builder.WriteString(output)
	}
	ctldOutput, ctldErr := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"logs", "daemonset/"+env.Infra.Name+"-ctld",
		"--namespace", env.Infra.Namespace,
		"--all-containers",
		"--tail", "300",
	)
	if ctldErr != nil {
		builder.WriteString(fmt.Sprintf("\nctld logs unavailable: %v\n%s", ctldErr, ctldOutput))
	} else {
		builder.WriteString("\nctld logs:\n")
		builder.WriteString(ctldOutput)
	}
	return builder.String()
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
