//go:build linux

package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ctldportal "github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/portal"
	"go.uber.org/zap"
)

type SnapshotProvider func(context.Context, ctldportal.PortalReplicator) error

type Replicator struct {
	epoch    uint64
	socket   string
	listener *net.UnixListener
	logger   *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc

	opMu sync.Mutex
	mu   sync.RWMutex

	clients      map[*standbyClient]struct{}
	provider     SnapshotProvider
	providerSet  chan struct{}
	providerOnce sync.Once
	onReady      func(int)
	sequence     atomic.Uint64
	wg           sync.WaitGroup
	closeOnce    sync.Once
}

type standbyClient struct {
	conn  *net.UnixConn
	ready bool
}

func newReplicator(socket string, epoch uint64, logger *zap.Logger, onReady func(int)) (*Replicator, error) {
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale ctld HA socket: %w", err)
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		return nil, fmt.Errorf("listen for ctld HA standby: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod ctld HA socket: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replicator{
		epoch:       epoch,
		socket:      socket,
		listener:    listener,
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		clients:     make(map[*standbyClient]struct{}),
		providerSet: make(chan struct{}),
		onReady:     onReady,
	}
	r.wg.Add(1)
	go r.acceptLoop()
	return r, nil
}

func (r *Replicator) SetSnapshotProvider(provider SnapshotProvider) {
	if r == nil || provider == nil {
		return
	}
	r.mu.Lock()
	r.provider = provider
	r.mu.Unlock()
	r.providerOnce.Do(func() { close(r.providerSet) })
}

func (r *Replicator) Ready() bool {
	return r.StandbyCount() > 0
}

func (r *Replicator) StandbyCount() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for client := range r.clients {
		if client.ready {
			count++
		}
	}
	return count
}

func (r *Replicator) Publish(ctx context.Context, manifest ctldportal.RecoveryManifest, channel *os.File) error {
	if channel == nil {
		return fmt.Errorf("FUSE channel is required")
	}
	return r.broadcast(ctx, wireMessage{Type: messagePublish, Manifest: &manifest}, channel)
}

func (r *Replicator) Update(ctx context.Context, manifest ctldportal.RecoveryManifest) error {
	return r.broadcast(ctx, wireMessage{Type: messageUpdate, Manifest: &manifest}, nil)
}

func (r *Replicator) Remove(ctx context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return nil
	}
	return r.broadcast(ctx, wireMessage{Type: messageRemove, Key: key}, nil)
}

func (r *Replicator) broadcast(ctx context.Context, message wireMessage, file *os.File) error {
	if r == nil {
		return ErrStandbyUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.opMu.Lock()
	defer r.opMu.Unlock()
	clients := r.readyClients()
	if len(clients) == 0 {
		return ErrStandbyUnavailable
	}
	succeeded := 0
	for _, client := range clients {
		if err := r.send(ctx, client, message, file); err != nil {
			r.removeClient(client, err)
			continue
		}
		succeeded++
	}
	if succeeded == 0 {
		return ErrStandbyUnavailable
	}
	return nil
}

func (r *Replicator) readyClients() []*standbyClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	clients := make([]*standbyClient, 0, len(r.clients))
	for client := range r.clients {
		if client.ready {
			clients = append(clients, client)
		}
	}
	return clients
}

func (r *Replicator) acceptLoop() {
	defer r.wg.Done()
	for {
		conn, err := r.listener.AcceptUnix()
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			r.logger.Warn("accept ctld HA standby failed", zap.Error(err))
			continue
		}
		client := &standbyClient{conn: conn}
		r.mu.Lock()
		r.clients[client] = struct{}{}
		r.mu.Unlock()
		r.wg.Add(1)
		go r.initializeClient(client)
	}
}

func (r *Replicator) initializeClient(client *standbyClient) {
	defer r.wg.Done()
	select {
	case <-r.ctx.Done():
		r.removeClient(client, r.ctx.Err())
		return
	case <-r.providerSet:
	}
	r.opMu.Lock()
	provider := r.snapshotProvider()
	if provider == nil {
		r.opMu.Unlock()
		r.removeClient(client, fmt.Errorf("ctld HA snapshot provider unavailable"))
		return
	}
	target := &singleClientReplicator{parent: r, client: client}
	if err := provider(r.ctx, target); err != nil {
		r.opMu.Unlock()
		r.removeClient(client, fmt.Errorf("sync ctld HA standby: %w", err))
		return
	}
	if err := r.send(r.ctx, client, wireMessage{Type: messageSnapshotEnd}, nil); err != nil {
		r.opMu.Unlock()
		r.removeClient(client, err)
		return
	}
	r.mu.Lock()
	if _, ok := r.clients[client]; ok {
		client.ready = true
	}
	count := r.readyCountLocked()
	r.mu.Unlock()
	r.opMu.Unlock()
	r.notifyReady(count)
	r.logger.Info("ctld HA standby synchronized", zap.Uint64("epoch", r.epoch), zap.Int("standbys", count))

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			r.removeClient(client, r.ctx.Err())
			return
		case <-ticker.C:
			r.opMu.Lock()
			err := r.send(r.ctx, client, wireMessage{Type: messagePing}, nil)
			r.opMu.Unlock()
			if err != nil {
				r.removeClient(client, err)
				return
			}
		}
	}
}

func (r *Replicator) snapshotProvider() SnapshotProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.provider
}

func (r *Replicator) send(ctx context.Context, client *standbyClient, message wireMessage, file *os.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	message.Version = protocolVersion
	message.Epoch = r.epoch
	message.Sequence = r.sequence.Add(1)
	if deadline, ok := ctx.Deadline(); ok {
		_ = client.conn.SetDeadline(deadline)
	} else {
		_ = client.conn.SetDeadline(time.Now().Add(5 * time.Second))
	}
	defer client.conn.SetDeadline(time.Time{})
	if err := writePacket(client.conn, message, file); err != nil {
		return err
	}
	payload := make([]byte, maxPacketSize)
	n, _, _, _, err := client.conn.ReadMsgUnix(payload, nil)
	if err != nil {
		return fmt.Errorf("read ctld HA acknowledgement: %w", err)
	}
	var ack wireMessage
	if err := json.Unmarshal(payload[:n], &ack); err != nil {
		return fmt.Errorf("decode ctld HA acknowledgement: %w", err)
	}
	if ack.Version != protocolVersion || ack.Type != messageAck || ack.Sequence != message.Sequence || ack.Epoch != r.epoch {
		return fmt.Errorf("invalid ctld HA acknowledgement")
	}
	if ack.Error != "" {
		return errors.New(ack.Error)
	}
	return nil
}

func (r *Replicator) removeClient(client *standbyClient, reason error) {
	if r == nil || client == nil {
		return
	}
	r.mu.Lock()
	_, existed := r.clients[client]
	if existed {
		delete(r.clients, client)
	}
	count := r.readyCountLocked()
	r.mu.Unlock()
	_ = client.conn.Close()
	if existed {
		r.notifyReady(count)
		if reason != nil && r.ctx.Err() == nil {
			r.logger.Warn("ctld HA standby removed", zap.Error(reason), zap.Int("standbys", count))
		}
	}
}

func (r *Replicator) readyCountLocked() int {
	count := 0
	for client := range r.clients {
		if client.ready {
			count++
		}
	}
	return count
}

func (r *Replicator) notifyReady(count int) {
	if r.onReady != nil {
		r.onReady(count)
	}
}

func (r *Replicator) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.cancel()
		_ = r.listener.Close()
		r.mu.Lock()
		clients := make([]*standbyClient, 0, len(r.clients))
		for client := range r.clients {
			clients = append(clients, client)
			delete(r.clients, client)
		}
		r.mu.Unlock()
		for _, client := range clients {
			_ = client.conn.Close()
		}
		r.notifyReady(0)
		r.wg.Wait()
		_ = os.Remove(r.socket)
	})
}

type singleClientReplicator struct {
	parent *Replicator
	client *standbyClient
}

func (r *singleClientReplicator) Ready() bool { return r != nil && r.client != nil }

func (r *singleClientReplicator) Publish(ctx context.Context, manifest ctldportal.RecoveryManifest, channel *os.File) error {
	return r.parent.send(ctx, r.client, wireMessage{Type: messagePublish, Manifest: &manifest}, channel)
}

func (r *singleClientReplicator) Update(ctx context.Context, manifest ctldportal.RecoveryManifest) error {
	return r.parent.send(ctx, r.client, wireMessage{Type: messageUpdate, Manifest: &manifest}, nil)
}

func (r *singleClientReplicator) Remove(ctx context.Context, key string) error {
	return r.parent.send(ctx, r.client, wireMessage{Type: messageRemove, Key: key}, nil)
}

var _ ctldportal.PortalReplicator = (*Replicator)(nil)
var _ ctldportal.PortalReplicator = (*singleClientReplicator)(nil)
