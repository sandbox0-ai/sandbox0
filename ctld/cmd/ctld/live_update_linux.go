//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	ctldha "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/ha"
	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/proxy"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/livehandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

const liveUpdateTimeout = 15 * time.Second

type liveUpdateMessage struct {
	Version         int                             `json:"version"`
	Phase           string                          `json:"phase"`
	Operation       string                          `json:"operation"`
	Configuration   string                          `json:"configuration"`
	SourceBinary    string                          `json:"source_binary,omitempty"`
	CandidateBinary string                          `json:"candidate_binary,omitempty"`
	Epoch           uint64                          `json:"epoch,omitempty"`
	NetworkFiles    int                             `json:"network_files,omitempty"`
	Runtime         nomadruntime.LiveRuntimeHandoff `json:"runtime,omitempty"`
}

type liveUpdateReceipt struct {
	Version         int    `json:"version"`
	Phase           string `json:"phase"`
	Operation       string `json:"operation"`
	ManifestDigest  string `json:"manifest_digest"`
	SourceBinary    string `json:"source_binary"`
	CandidateBinary string `json:"candidate_binary"`
	Epoch           uint64 `json:"epoch"`
	Sessions        int    `json:"sessions"`
}

type liveCandidate struct {
	lease        *ctldha.PrimaryLease
	networkFiles []*os.File
	runtime      nomadruntime.LiveRuntimeHandoff
	files        []*os.File
	closeOnce    sync.Once
}

func (c *liveCandidate) closeCopies() {
	if c != nil {
		c.closeOnce.Do(func() { _ = livehandoff.CloseFiles(c.files) })
	}
}

type liveTransferred struct {
	drain     *primaryServiceHandle
	operation string
}

func (e *liveTransferred) Error() string { return "live node runtime transferred: " + e.operation }

func liveUpdateSocket() string { return filepath.Join(stateRoot, "ha", "live-update.sock") }

func liveConfigurationDigest(cfg *config.CtldConfig, network *config.NetworkRuntimeConfig) (string, error) {
	data, err := yaml.Marshal(struct {
		Ctld                                                    *config.CtldConfig
		Network                                                 *config.NetworkRuntimeConfig
		Node, StateRoot, HTTP, NetworkSocket, NetworkNamespaces string
	}{cfg, network, nodeName, stateRoot, httpAddr, runtimeSlotNetworkSocket, runtimeSlotNetNSRoot})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func binaryDigest() (string, error) {
	file, err := os.Open("/proc/self/exe")
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func startLiveUpdateServer(ctx context.Context, lease *ctldha.PrimaryLease) (*net.UnixListener, <-chan *net.UnixConn, error) {
	if lease == nil {
		return nil, nil, nil
	}
	path := liveUpdateSocket()
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, nil, fmt.Errorf("live update socket path is not a socket")
		}
		if err := os.Remove(path); err != nil {
			return nil, nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	requests := make(chan *net.UnixConn)
	go func() {
		for {
			connection, err := listener.AcceptUnix()
			if err != nil {
				return
			}
			if livehandoff.RequireRoot(connection) != nil {
				_ = connection.Close()
				continue
			}
			select {
			case requests <- connection:
			case <-ctx.Done():
				_ = connection.Close()
				return
			}
		}
	}()
	return listener, requests, nil
}

func executeLiveUpdate(parent context.Context, connection *net.UnixConn, options primaryRunOptions, networkService, runtimeService primaryService, httpServer *http.Server, listener *net.UnixListener) (operation string, committed bool, result error) {
	defer connection.Close()
	ctx, cancel := context.WithTimeout(parent, liveUpdateTimeout)
	defer cancel()
	_ = connection.SetDeadline(time.Now().Add(liveUpdateTimeout))
	var request liveUpdateMessage
	unexpected, err := livehandoff.Receive(connection, &request)
	_ = livehandoff.CloseFiles(unexpected)
	if err != nil || len(unexpected) != 0 {
		return "", false, fmt.Errorf("read live update request: %w", err)
	}
	if request.Version != 1 || request.Phase != "prepare" || !validLiveOperation(request.Operation) || !validLiveDigest(request.CandidateBinary) {
		return "", false, fmt.Errorf("invalid live update request")
	}
	operation = request.Operation
	if _, err := readLiveReceipt(operation); err == nil {
		return operation, false, fmt.Errorf("live update operation already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return operation, false, err
	}
	network, ok := networkService.(*networkRuntimeService)
	if !ok {
		return operation, false, fmt.Errorf("network service cannot transfer")
	}
	runtime, ok := runtimeService.(*nomadruntime.Service)
	if !ok {
		return operation, false, fmt.Errorf("runtime service cannot transfer")
	}
	networkConfig, err := loadNetworkRuntimeConfig(networkRuntimeConfigPath)
	if err != nil {
		return operation, false, err
	}
	configuration, err := liveConfigurationDigest(options.ctldConfig, networkConfig)
	if err != nil || configuration != request.Configuration {
		return operation, false, fmt.Errorf("candidate configuration is incompatible: %w", err)
	}
	sourceDigest, err := binaryDigest()
	if err != nil {
		return operation, false, err
	}
	networkFiles, err := network.daemon.PrepareLiveUpdate()
	if err != nil {
		return operation, false, err
	}
	defer livehandoff.CloseFiles(networkFiles)
	prepared, err := runtime.PrepareLiveUpdate(ctx)
	if err != nil {
		return operation, false, err
	}
	irreversible := false
	defer func() {
		if !irreversible {
			prepared.Abort()
		}
	}()
	defer livehandoff.CloseFiles(prepared.Handoff.Files)
	lock, err := options.lease.ExportFile()
	if err != nil {
		return operation, false, err
	}
	defer lock.Close()
	envelope := liveUpdateMessage{Version: 1, Phase: "prepared", Operation: operation, Configuration: configuration, SourceBinary: sourceDigest, CandidateBinary: request.CandidateBinary, Epoch: options.lease.Epoch, NetworkFiles: len(networkFiles), Runtime: prepared.Handoff}
	files := append([]*os.File{lock}, networkFiles...)
	files = append(files, prepared.Handoff.Files...)
	if err := livehandoff.Send(connection, envelope, files); err != nil {
		return operation, false, err
	}
	var accepted liveUpdateMessage
	unwanted, err := livehandoff.Receive(connection, &accepted)
	_ = livehandoff.CloseFiles(unwanted)
	if err != nil || len(unwanted) != 0 || accepted.Version != 1 || accepted.Phase != "accept" || accepted.Operation != operation || accepted.Configuration != configuration {
		return operation, false, fmt.Errorf("candidate did not accept live update: %w", err)
	}
	if err := prepared.ValidateCommit(ctx); err != nil {
		return operation, false, err
	}
	payload, _ := json.Marshal(envelope)
	digest := sha256.Sum256(payload)
	receipt := liveUpdateReceipt{Version: 1, Phase: "committing", Operation: operation, ManifestDigest: hex.EncodeToString(digest[:]), SourceBinary: sourceDigest, CandidateBinary: request.CandidateBinary, Epoch: options.lease.Epoch, Sessions: len(envelope.Runtime.Sessions.Sessions)}
	if err := writeLiveReceipt(receipt); err != nil {
		return operation, false, err
	}
	irreversible = true
	if err := prepared.Commit(); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	if err := network.daemon.CommitLiveUpdate(ctx); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	if err := httpServer.Shutdown(ctx); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	if err := listener.Close(); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	receipt.Phase = "committed"
	if err := writeLiveReceipt(receipt); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	if err := options.lease.Relinquish(); err != nil {
		return operation, false, errors.Join(errPrimaryShutdownIncomplete, err)
	}
	// Lost delivery is resolved by the exact fsynced receipt, never by retrying
	// a transfer or reopening an already detached source.
	_ = livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "commit", Operation: operation}, nil)
	return operation, true, nil
}

func receiveLiveUpdate(ctx context.Context, coordinator *ctldha.Coordinator, cfg *config.CtldConfig, network *config.NetworkRuntimeConfig) (_ *liveCandidate, result error) {
	if liveUpdateFrom == "" {
		return nil, nil
	}
	if !validLiveOperation(liveUpdateID) || liveUpdateFrom != liveUpdateSocket() {
		return nil, fmt.Errorf("live update requires the exact node socket and operation ID")
	}
	// Restarting an installed candidate must use ordinary HA recovery rather
	// than replaying a previously committed ownership transfer.
	if receipt, err := readLiveReceipt(liveUpdateID); err == nil && receipt.Phase == "committed" {
		binary, err := binaryDigest()
		if err != nil || binary != receipt.CandidateBinary {
			return nil, fmt.Errorf("live update operation belongs to another binary")
		}
		return nil, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	configuration, err := liveConfigurationDigest(cfg, network)
	if err != nil {
		return nil, err
	}
	binary, err := binaryDigest()
	if err != nil {
		return nil, err
	}
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: liveUpdateFrom, Net: "unix"})
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if err := livehandoff.RequireRoot(connection); err != nil {
		return nil, err
	}
	deadline := time.Now().Add(liveUpdateTimeout)
	if existing, ok := ctx.Deadline(); ok && existing.Before(deadline) {
		deadline = existing
	}
	_ = connection.SetDeadline(deadline)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "prepare", Operation: liveUpdateID, Configuration: configuration, CandidateBinary: binary}, nil); err != nil {
		return nil, err
	}
	var envelope liveUpdateMessage
	files, err := livehandoff.Receive(connection, &envelope)
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			_ = livehandoff.CloseFiles(files)
		}
	}()
	if envelope.Version != 1 || envelope.Phase != "prepared" || envelope.Operation != liveUpdateID || envelope.Configuration != configuration || envelope.CandidateBinary != binary || !validLiveDigest(envelope.SourceBinary) || envelope.Epoch == 0 || envelope.Runtime.Sessions.Version != 1 || (envelope.NetworkFiles != 3 && envelope.NetworkFiles != 4) || len(files) != 1+envelope.NetworkFiles+2*len(envelope.Runtime.Sessions.Sessions) || len(envelope.Runtime.Requests) != len(envelope.Runtime.Sessions.Sessions) {
		return nil, fmt.Errorf("candidate rejected live update envelope")
	}
	for _, request := range envelope.Runtime.Requests {
		if err := request.Validate(); err != nil {
			return nil, fmt.Errorf("candidate rejected writer binding: %w", err)
		}
	}
	if err := proxy.ValidateLiveListeners(network, files[1:1+envelope.NetworkFiles]); err != nil {
		return nil, fmt.Errorf("candidate proxy preflight: %w", err)
	}
	factory, err := configuredNomadRuntimeFactory(cfg, runtimeSlotNetworkSocket, network)
	if err != nil {
		return nil, err
	}
	service, err := factory(nil)
	if err != nil {
		return nil, err
	}
	preflight, ok := service.(*nomadruntime.Service)
	if !ok {
		return nil, fmt.Errorf("candidate runtime cannot preflight writers")
	}
	if err := preflight.PreflightLiveStorage(ctx, envelope.Runtime, files[1+envelope.NetworkFiles:]); err != nil {
		return nil, fmt.Errorf("candidate storage preflight: %w", err)
	}
	guard, err := preflight.StartLiveWriterGuard(ctx, envelope.Runtime)
	if err != nil {
		return nil, err
	}
	envelope.Runtime.Guard = guard
	defer func() {
		if result != nil {
			guard.Close()
		}
	}()
	if err := livehandoff.Send(connection, liveUpdateMessage{Version: 1, Phase: "accept", Operation: liveUpdateID, Configuration: configuration}, nil); err != nil {
		return nil, err
	}
	var committed liveUpdateMessage
	unwanted, receiveErr := livehandoff.Receive(connection, &committed)
	_ = livehandoff.CloseFiles(unwanted)
	receipt, err := readLiveReceipt(liveUpdateID)
	payload, _ := json.Marshal(envelope)
	digest := sha256.Sum256(payload)
	if err != nil || receipt.Phase != "committed" || receipt.ManifestDigest != hex.EncodeToString(digest[:]) || receipt.Epoch != envelope.Epoch || receipt.CandidateBinary != binary || (receiveErr == nil && (len(unwanted) != 0 || committed.Version != 1 || committed.Phase != "commit" || committed.Operation != liveUpdateID)) {
		return nil, fmt.Errorf("live update has no exact committed receipt: %w", errors.Join(err, receiveErr))
	}
	lease, err := coordinator.AdoptTransferred(files[0], envelope.Epoch)
	if err != nil {
		return nil, err
	}
	envelope.Runtime.Files = files[1+envelope.NetworkFiles:]
	return &liveCandidate{lease: lease, networkFiles: files[1 : 1+envelope.NetworkFiles], runtime: envelope.Runtime, files: files[1:]}, nil
}

var liveOperationPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var liveDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validLiveDigest(digest string) bool { return liveDigestPattern.MatchString(digest) }

func validLiveOperation(operation string) bool { return liveOperationPattern.MatchString(operation) }
func liveReceiptPath(operation string) string {
	return filepath.Join(stateRoot, "ha", "live-updates", operation+".json")
}
func readLiveReceipt(operation string) (liveUpdateReceipt, error) {
	var receipt liveUpdateReceipt
	if !validLiveOperation(operation) {
		return receipt, fmt.Errorf("invalid live update operation")
	}
	fd, err := unix.Open(liveReceiptPath(operation), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return receipt, err
	}
	file := os.NewFile(uintptr(fd), "live update receipt")
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 4096 {
		return receipt, fmt.Errorf("unsafe live update receipt: %w", err)
	}
	data, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(data) > 4096 {
		return receipt, fmt.Errorf("invalid live update receipt length: %w", err)
	}
	err = json.Unmarshal(data, &receipt)
	if err == nil && (receipt.Version != 1 || receipt.Operation != operation ||
		(receipt.Phase != "committing" && receipt.Phase != "committed") || receipt.Epoch == 0 ||
		receipt.Sessions < 0 || receipt.Sessions > 512 || !validLiveDigest(receipt.ManifestDigest) ||
		!validLiveDigest(receipt.SourceBinary) || !validLiveDigest(receipt.CandidateBinary)) {
		err = fmt.Errorf("invalid live update receipt")
	}
	return receipt, err
}
func writeLiveReceipt(receipt liveUpdateReceipt) error {
	path := liveReceiptPath(receipt.Operation)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".live-update-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
