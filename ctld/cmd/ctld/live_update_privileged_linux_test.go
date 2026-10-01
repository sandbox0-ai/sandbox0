//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/livehandoff"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This opt-in fixture runs the deployable embedded-procd ctld, with real HA,
// services and listener ownership. External authority/Nomad endpoints are
// supplied by the isolated fixture; this test intentionally has no guest owners.
func TestLiveCtldRejectAndLostAcknowledgement(t *testing.T) {
	root := os.Getenv("S0_CTLD_LIVE_FIXTURE_ROOT")
	if root == "" {
		t.Skip("requires isolated ctld service fixture")
	}
	require.Zero(t, os.Geteuid())
	require.Equal(t, "/opt/s0-live-update-test/f", root)
	_, err := os.Stat(filepath.Join(root, "../isolated-fixture"))
	require.NoError(t, err)
	t.Setenv("CONFIG_PATH", filepath.Join(root, "ctld.json"))
	priorState, priorNode, priorHTTP := stateRoot, nodeName, httpAddr
	priorNetwork, priorSocket, priorNS := networkRuntimeConfigPath, runtimeSlotNetworkSocket, runtimeSlotNetNSRoot
	stateRoot, nodeName, httpAddr = filepath.Join(root, "state"), "fixture-node", "127.0.0.1:19095"
	networkRuntimeConfigPath, runtimeSlotNetworkSocket, runtimeSlotNetNSRoot = filepath.Join(root, "network.json"), filepath.Join(root, "network.sock"), filepath.Join(root, "netns")
	t.Cleanup(func() {
		stateRoot, nodeName, httpAddr = priorState, priorNode, priorHTTP
		networkRuntimeConfigPath, runtimeSlotNetworkSocket, runtimeSlotNetNSRoot = priorNetwork, priorSocket, priorNS
	})
	cfg, err := config.LoadCtldConfigStrict()
	require.NoError(t, err)
	network, err := loadNetworkRuntimeConfig(networkRuntimeConfigPath)
	require.NoError(t, err)
	configuration, err := liveConfigurationDigest(cfg, network)
	require.NoError(t, err)
	binary := filepath.Join(root, "bin/ctld")
	binaryData, err := os.ReadFile(binary)
	require.NoError(t, err)
	peerDigest := fmt.Sprintf("%x", sha256.Sum256(binaryData))
	probePath := filepath.Join(root, "protocol-source.sock")
	output, err := os.Create(filepath.Join(root, "protocol-source.log"))
	require.NoError(t, err)
	defer output.Close()
	command := exec.Command(binary, "--node-name="+nodeName, "--state-root="+stateRoot, "--ha-slot=protocol-source",
		"--ha-probe-socket="+probePath, "--http-addr="+httpAddr, "--procd-cache-dir="+filepath.Join(root, "procd"),
		"--ctld-networking-config-path="+networkRuntimeConfigPath, "--runtime-slot-network-socket="+runtimeSlotNetworkSocket,
		"--runtime-slot-netns-root="+runtimeSlotNetNSRoot)
	command.Stdout, command.Stderr = output, output
	require.NoError(t, command.Start())
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
	ready := func() bool {
		connection, err := net.DialTimeout("unix", probePath, time.Second)
		if err != nil {
			return false
		}
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		var response struct {
			State ctldha.State `json:"state"`
			Ready bool         `json:"service_ready"`
		}
		if json.NewDecoder(connection).Decode(&response) != nil || response.State.Role != ctldha.RolePrimary || !response.Ready {
			return false
		}
		responseHTTP, err := (&http.Client{Timeout: time.Second}).Get("http://" + httpAddr + "/readyz")
		if err != nil {
			return false
		}
		defer responseHTTP.Body.Close()
		return responseHTTP.StatusCode == http.StatusOK
	}
	require.Eventually(t, ready, 30*time.Second, 20*time.Millisecond)
	prefix := fmt.Sprintf("protocol-%d-", time.Now().UnixNano())
	for i, scenario := range []string{"bad-config", "invalid-operation", "request-fds", "disconnect-prepared", "wrong-accept"} {
		t.Run(scenario, func(t *testing.T) {
			connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: liveUpdateSocket(), Net: "unix"})
			require.NoError(t, err)
			defer connection.Close()
			require.NoError(t, connection.SetDeadline(time.Now().Add(10*time.Second)))
			operation := fmt.Sprintf("%s%d", prefix, i)
			request := liveUpdateMessage{Version: 1, Phase: "prepare", Operation: operation, Configuration: configuration, CandidateBinary: peerDigest}
			if scenario == "bad-config" {
				request.Configuration = "changed"
			}
			if scenario == "invalid-operation" {
				request.Operation = "../escape"
			}
			var sent []*os.File
			if scenario == "request-fds" {
				sent = []*os.File{output}
			}
			require.NoError(t, livehandoff.Send(connection, request, sent))
			var envelope liveUpdateMessage
			files, err := livehandoff.Receive(connection, &envelope)
			defer livehandoff.CloseFiles(files)
			if scenario == "disconnect-prepared" || scenario == "wrong-accept" {
				require.NoError(t, err)
				require.Equal(t, "prepared", envelope.Phase)
				require.Empty(t, envelope.Runtime.Sessions.Sessions)
				if scenario == "wrong-accept" {
					require.NoError(t, livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "accept", Operation: "foreign", Configuration: configuration}, nil))
				}
			} else {
				require.Error(t, err)
			}
			require.NoError(t, connection.Close())
			require.Eventually(t, ready, 10*time.Second, 20*time.Millisecond)
			_, err = os.Stat(liveReceiptPath(operation))
			require.ErrorIs(t, err, os.ErrNotExist)
			competitor, err := os.OpenFile(filepath.Join(stateRoot, "ha/primary.lock"), os.O_RDWR, 0)
			require.NoError(t, err)
			defer competitor.Close()
			require.ErrorIs(t, unix.Flock(int(competitor.Fd()), unix.LOCK_EX|unix.LOCK_NB), unix.EWOULDBLOCK)
		})
	}
	// A candidate retains all received descriptors and accepts, then loses the
	// control channel. Source commit is recovered from the exact durable receipt.
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: liveUpdateSocket(), Net: "unix"})
	require.NoError(t, err)
	require.NoError(t, connection.SetDeadline(time.Now().Add(10*time.Second)))
	operation := prefix + "lost-ack"
	require.NoError(t, livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "prepare", Operation: operation, Configuration: configuration, CandidateBinary: peerDigest}, nil))
	var envelope liveUpdateMessage
	files, err := livehandoff.Receive(connection, &envelope)
	require.NoError(t, err)
	defer livehandoff.CloseFiles(files)
	require.NoError(t, livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "accept", Operation: operation, Configuration: configuration}, nil))
	require.NoError(t, connection.Close())
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("committed predecessor did not exit")
	}
	receipt, err := readLiveReceipt(operation)
	require.NoError(t, err)
	require.Equal(t, "committed", receipt.Phase)
	require.Equal(t, envelope.Epoch, receipt.Epoch)
	coordinator, err := ctldha.NewCoordinator(ctldha.Config{RootDir: stateRoot, Slot: "lost-ack-successor"})
	require.NoError(t, err)
	lease, err := coordinator.AdoptTransferred(files[0], receipt.Epoch)
	require.NoError(t, err)
	require.Equal(t, receipt.Epoch+1, lease.Epoch)
	require.NoError(t, lease.Close())
}
