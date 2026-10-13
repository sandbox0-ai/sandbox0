//go:build linux

package nomadruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/sys/mountinfo"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const autogrowWriterSource = `#define _GNU_SOURCE
#include <sys/random.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
static void die(const char *op) {
 int e=errno; int fd=open("/evidence/error.txt",O_WRONLY|O_CREAT|O_TRUNC,0600);
 if(fd>=0) { dprintf(fd,"%s: errno=%d\n",op,e); close(fd); } _exit(1);
}
static void all(int fd,const unsigned char *p,size_t n) {
 while(n) { ssize_t w=write(fd,p,n); if(w<=0) die("write"); p+=w; n-=w; }
}
int main(void) {
 unsigned char token[16]; if(getrandom(token,sizeof(token),0)!=sizeof(token)) die("token");
 char text[33]; for(int i=0;i<16;i++) sprintf(text+2*i,"%02x",token[i]);
 unsigned char *chunk=malloc(1048576),*marker=malloc(1048576); if(!chunk||!marker) die("malloc");
 memset(marker,0x5a,1048576);
 int fd=open("/growing.bin",O_RDWR|O_CREAT|O_TRUNC,0600); if(fd<0) die("open");
 struct stat st; if(fstat(fd,&st)) die("fstat");
 for(int i=0;i<600;i++) {
  memset(chunk,i%251,1048576); all(fd,chunk,1048576); if(fsync(fd)) die("fsync");
  for(int j=0;j<1048576;j++) if(marker[j]!=0x5a) die("heap-marker");
  off_t off=lseek(fd,0,SEEK_CUR); if(off!=(off_t)(i+1)*1048576) die("open-offset");
  int proof=open("/evidence/state.new",O_WRONLY|O_CREAT|O_TRUNC,0600); if(proof<0) die("proof");
  dprintf(proof,"{\"pid\":%d,\"token\":\"%s\",\"chunks\":%d,\"offset\":%lld,\"inode\":%llu,\"marker_ok\":true}\n",getpid(),text,i+1,(long long)off,(unsigned long long)st.st_ino);
  if(close(proof)||rename("/evidence/state.new","/evidence/state.json")) die("proof-publish");
  usleep(5000);
 }
 if(close(fd)) die("close"); return 0;
}`

type autogrowGuestEvidence struct {
	PID      int    `json:"pid"`
	Token    string `json:"token"`
	Chunks   int    `json:"chunks"`
	Offset   int64  `json:"offset"`
	Inode    uint64 `json:"inode"`
	MarkerOK bool   `json:"marker_ok"`
}

func TestPrivilegedRemoteAutogrowPreservesRunscWriterAndRegionalCustody(t *testing.T) {
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REMOTE") != "1" {
		t.Skip("requires dedicated remote automatic-growth acceptance services")
	}
	for _, key := range []string{"INTEGRATION_DATABASE_URL", "SANDBOX0_RUSTFS_ENDPOINT", "SANDBOX0_PRIVILEGED_NBD_DEVICE", "SANDBOX0_TEST_RUNSC"} {
		require.NotEmpty(t, os.Getenv(key), "missing explicit acceptance input %s", key)
	}
	require.Zero(t, os.Geteuid())
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "sandbox0-remote-autogrow-")
	require.NoError(t, err)
	t.Cleanup(func() {
		mounts, err := mountinfo.GetMounts(nil)
		if err != nil {
			t.Errorf("retain fixture: %v", err)
			return
		}
		for _, mount := range mounts {
			if mount.Mountpoint == root || strings.HasPrefix(mount.Mountpoint, root+"/") {
				t.Errorf("retain fixture with mount %s", mount.Mountpoint)
				return
			}
		}
		require.NoError(t, os.RemoveAll(root))
	})
	for _, dir := range []string{"source", "evidence", "bundle"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0700))
	}
	program := filepath.Join(root, "writer.c")
	require.NoError(t, os.WriteFile(program, []byte(autogrowWriterSource), 0600))
	output, err := exec.CommandContext(ctx, "gcc", "-static", "-O2", "-Wall", "-Wextra", "-o", filepath.Join(root, "source/writer"), program).CombinedOutput()
	require.NoError(t, err, "compile guest: %s", output)
	pool := autogrowRemotePool(t)
	store := sandboxstore.NewPGSandboxStore(pool)
	runID := digest.FromString(fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())).Encoded()
	// A unique bucket keeps test cleanup independent of every other fixture.
	bucket := "s0-autogrow-" + runID[:20]
	t.Logf("isolated_object_bucket=%s", bucket)
	endpoint, outage := autogrowObjectEndpoint(t, os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"))
	objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY")})
	require.NoError(t, err)
	require.NoError(t, objects.Create())
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("retain failed fixture objects for custody diagnosis")
			return
		}
		for {
			items, more, _, err := objects.List("", "", "", "", 1000)
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, objects.Delete(item.Key))
			}
			if !more {
				break
			}
		}
	})
	artifact := autogrowRemoteBase(t, root, runID, store, conditional)
	stage := autogrowRemoteStage(t, store, artifact)
	client, loss := autogrowRemoteAuthority(t, root, store)
	_, err = client.ConsumeWriterGrant(ctx, stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	host, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: []string{os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE")}})
	require.NoError(t, err)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}
	m, err := rootfssession.New(rootfssession.Config{StatePath: filepath.Join(root, "sessions.db"), BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"),
		MaxDirtyTailBytes: 32 << 20, MaxNodeDirtyTailBytes: 64 << 20, DirtyTailRetirementReserveBytes: 8 << 20,
		ReadCacheBytes: 64 << 20, ReadDiskCache: rootfsblock.DiskCacheConfig{Directory: filepath.Join(root, "read-cache"), MaxBytes: 32 << 20},
		Source: conditional, Publisher: publisher, Runtime: host, LiveCommitAuthority: liveCommitAuthorityAdapter{client: client},
		PublisherForOperation: func(s rootfshandoff.StageRequest, op string) rootfsblock.ImmutableObjectPublisher {
			return nodeJournalPublisher{authority: client, stage: s, operation: op, publisher: publisher}
		}})
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	mount, err := m.Ensure(ctx, stage)
	require.NoError(t, err)
	runtime := &rootfsRuntime{sessions: m, authority: client, liveCommits: true, autoGrow: true, logger: newLogger(zap.NewNop())}
	retired := false
	defer func() {
		if retired {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if err := m.SettleLiveCommit(cleanup, stage); err != nil {
			t.Errorf("retain live receipt: %v", err)
			return
		}
		if err := m.BeginRetire(stage.Parent, stage.Identity, rootfshandoff.PlannedRetireOperationID(stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)); err != nil {
			t.Errorf("retain retirement: %v", err)
			return
		}
		if err := m.Release(cleanup, stage.Identity); err != nil {
			t.Errorf("retain physical fixture: %v", err)
		}
	}()
	runscRoot := filepath.Join(root, "runsc")
	runner := gvisorcli.New(gvisorcli.Config{Path: os.Getenv("SANDBOX0_TEST_RUNSC"), Root: runscRoot, Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true, RootFSMountRoot: filepath.Join(root, "mounts")})
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		if err := runner.Delete(cleanup, "writer", true); err != nil {
			t.Errorf("retain runsc fixture: %v", err)
		}
		if err := unix.Unmount(filepath.Join(runscRoot, "null-netns"), 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("retain null network namespace: %v", err)
		}
	}()
	spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: mount.Source}, Process: &specs.Process{Cwd: "/", Args: []string{"/writer"}, Env: []string{"PATH=/"}},
		Mounts: []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
			{Destination: "/evidence", Type: "bind", Source: filepath.Join(root, "evidence"), Options: []string{"rbind", "rw"}}},
		Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}}}}
	config, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "bundle/config.json"), config, 0600))
	require.NoError(t, runner.Create(ctx, filepath.Join(root, "bundle"), "writer"))
	require.NoError(t, runner.Start(ctx, "writer"))
	d := &nodeRuntime{runtime: runtime, logger: newLogger(zap.NewNop())}
	startControl := func() context.CancelFunc {
		control, stop := context.WithCancel(ctx)
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-control.Done():
					return
				case <-tick.C:
					d.scanLiveCommits(control)
					d.scanRootFSGrowth(control)
				}
			}
		}()
		return stop
	}
	stopControl := startControl()
	defer func() { stopControl(); d.wg.Wait() }()
	finished := make(chan error, 1)
	go func() {
		result, err := runner.Wait(ctx, "writer")
		if err == nil && result.ExitStatus != 0 {
			detail, _ := os.ReadFile(filepath.Join(root, "evidence/error.txt"))
			err = fmt.Errorf("guest exit=%d %s", result.ExitStatus, detail)
		}
		finished <- err
	}()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var first, last autogrowGuestEvidence
	peakWAL := int64(0)
	freezeChecked := false
	outageChecked := false
	running := true
	for running {
		select {
		case err := <-finished:
			require.NoError(t, err)
			running = false
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
			peakWAL = max(peakWAL, m.NodeDirtyTailUsage().UsedBytes)
			data, err := os.ReadFile(filepath.Join(root, "evidence/state.json"))
			if os.IsNotExist(err) {
				continue
			}
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &last))
			require.True(t, last.MarkerOK)
			if first.Token == "" {
				first = last
			}
			require.Equal(t, first.Token, last.Token)
			require.Equal(t, first.PID, last.PID)
			require.Equal(t, first.Inode, last.Inode)
			require.Equal(t, int64(last.Chunks)<<20, last.Offset)
			if outage != nil && !outageChecked && last.Chunks >= 80 {
				func() {
					outage.down.Store(true)
					defer outage.down.Store(false)
					// The proxy continues serving the real S3 backend before and
					// after this interval. During it, every storage request fails.
					require.Eventually(t, func() bool {
						peakWAL = max(peakWAL, m.NodeDirtyTailUsage().UsedBytes)
						pressure, err := m.DirtyTailPressureSessions()
						require.NoError(t, err)
						return outage.rejected.Load() > 0 && len(pressure) > 0
					}, 25*time.Second, 20*time.Millisecond)
					grant, err := store.GetRootFSWriterGrant(ctx, stage.Identity.WriterGrantID)
					require.NoError(t, err)
					require.Equal(t, sandboxstore.RootFSWriterGrantStateConsumed, grant.State,
						"storage pressure must not retire the live writer")
					blocked, err := os.ReadFile(filepath.Join(root, "evidence/state.json"))
					require.NoError(t, err)
					var proof autogrowGuestEvidence
					require.NoError(t, json.Unmarshal(blocked, &proof))
					require.Equal(t, first.Token, proof.Token)
					require.Equal(t, first.PID, proof.PID)
					timer := time.NewTimer(5 * time.Second)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}()
				outageChecked = true
			}
			if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_FREEZE") == "1" && !freezeChecked && last.Chunks >= 100 {
				// Stop and join the normal scheduler, leaving the same guest
				// running until its normal WAL admission blocks. Only the freeze
				// owner's progress guardian can reclaim during the kernel drain.
				stopControl()
				d.wg.Wait()
				require.Eventually(t, func() bool {
					pressure, err := m.DirtyTailPressureSessions()
					require.NoError(t, err)
					return len(pressure) > 0
				}, 10*time.Second, 10*time.Millisecond)
				for {
					err := m.CommitLive(ctx, stage)
					var capacity *rootfsblock.DirtyTailCapacityError
					if err == nil {
						break
					}
					if !errdefs.IsUnavailable(err) && !errors.As(err, &capacity) {
						require.NoError(t, err)
					}
					require.NoError(t, ctx.Err())
				}
				// Guest writes continue immediately after that cut and fill the
				// WAL again before the freeze begins.
				require.Eventually(t, func() bool {
					pressure, err := m.DirtyTailPressureSessions()
					require.NoError(t, err)
					return len(pressure) > 0
				}, 10*time.Second, 10*time.Millisecond)
				fork := rootfshandoff.RunningForkCheckpointRequest{OperationID: "autogrow-freeze-boundary",
					SourceSandboxID: "autogrow-sandbox", TargetSandboxID: "autogrow-freeze-target", TargetGenerationID: "autogrow-freeze-generation"}
				checkpoint, err := m.CaptureRunningFork(ctx, stage, fork)
				require.NoError(t, err)
				require.NoError(t, checkpoint.Validate())
				// This gate validates a local running-fork cut and authenticated
				// uploads, not the regional target-creation transaction.
				require.NoError(t, m.AcknowledgeRunningFork(stage, fork.OperationID, checkpoint.ProofDigest))
				freezeChecked = true
				stopControl = startControl()
			}
		}
	}
	stopControl()
	d.wg.Wait()
	data, err := os.ReadFile(filepath.Join(root, "evidence/state.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &last))
	require.Equal(t, 600, last.Chunks)
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_FREEZE") == "1" {
		require.True(t, freezeChecked)
	}
	if outage != nil {
		require.True(t, outageChecked)
		require.Positive(t, outage.rejected.Load())
	}
	require.NoError(t, m.SettleLiveCommit(ctx, stage))
	require.True(t, loss.publish.Load())
	require.True(t, loss.install.Load())
	usage, err := host.InspectXFS(os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE"), filepath.Dir(mount.Source)+"/xfs")
	require.NoError(t, err)
	require.GreaterOrEqual(t, usage.SizeBytes, int64(1200<<20))
	require.LessOrEqual(t, peakWAL, int64(40<<20), "includes at most 8 MiB of charged rotation suffix duplication")
	file, err := os.Open(filepath.Join(mount.Source, "growing.bin"))
	require.NoError(t, err)
	for i := range 600 {
		var chunk [1 << 20]byte
		_, err = file.ReadAt(chunk[:], int64(i)<<20)
		require.NoError(t, err)
		require.True(t, bytes.Equal(chunk[:], bytes.Repeat([]byte{byte(i % 251)}, len(chunk))), "incorrect chunk %d", i)
	}
	require.NoError(t, file.Close())
	observation, err := client.ObserveLiveCommit(ctx, stage)
	require.NoError(t, err)
	require.Positive(t, observation.Progress.InstalledSequence)
	filesystem, err := store.GetRootFSFilesystem(ctx, stage.Identity.RootFSID)
	require.NoError(t, err)
	require.Equal(t, stage.InitialGeneration, filesystem.HeadGenerationID)
	require.NoError(t, runner.Delete(ctx, "writer", true))
	operation := rootfshandoff.PlannedRetireOperationID(stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)
	require.NoError(t, runtime.PlanRetire(ctx, stage, operation))
	_, err = runtime.Retire(ctx, stage, operation)
	require.NoError(t, err)
	retired = true
	grant, err := store.GetRootFSWriterGrant(ctx, stage.Identity.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateRetired, grant.State)
	t.Logf("real_mtls_region_postgres_s3_runsc=true bytes_written=%d initial_xfs_bytes=%d final_xfs_bytes=%d peak_node_wal_payload_bytes=%d installed_sequence=%d guest_pid=%d lost_publish_ack=true lost_install_ack=true running_fork_freeze_checked=%t storage_outage_checked=%t retired=true", 600<<20, 300<<20, usage.SizeBytes, peakWAL, observation.Progress.InstalledSequence, first.PID, freezeChecked, outageChecked)
}
