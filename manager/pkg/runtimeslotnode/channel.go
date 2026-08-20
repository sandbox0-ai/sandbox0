package runtimeslotnode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	nodeChannelHelloTimeout = 5 * time.Second
	maxNodeChannelPending   = 1024
)

// ChannelHub accepts mutually authenticated node-initiated streams and routes
// transient claim/command-ready plus durable cleanup operations. The hub never
// persists messages; exact retry remains anchored in PostgreSQL and the node
// cleanup journal.
type ChannelHub struct {
	verifier nodeauth.Verifier

	mu     sync.Mutex
	routes map[nodeChannelKey]*nodeChannelRoute
	closed bool
}

type nodeChannelKey struct {
	clusterID  string
	nodeID     string
	nodeUID    string
	nodeBootID string
}

type nodeChannelRoute struct {
	mu   sync.Mutex
	conn *nodeChannelConnection
}

type nodeChannelConnection struct {
	websocket *websocket.Conn
	hello     protocol.NodeChannelHello

	writePermit chan struct{}
	mu          sync.Mutex
	pending     map[string]nodeChannelPending
	done        chan struct{}
	err         error
	once        sync.Once
}

type nodeChannelPending struct {
	command   protocol.NodeChannelCommand
	result    chan nodeChannelOutcome
	abandoned bool
	completed bool
}

type nodeChannelOutcome struct {
	result protocol.NodeChannelResult
	err    error
}

var (
	_ http.Handler                     = (*ChannelHub)(nil)
	_ Transport                        = (*ChannelHub)(nil)
	_ runtimeslotclaim.NodeExecutor    = (*ChannelHub)(nil)
	_ runtimeslotclaim.NetworkPreparer = (*ChannelHub)(nil)
)

// NewChannelHub constructs an authenticated regional node channel registry.
func NewChannelHub(verifier nodeauth.Verifier) (*ChannelHub, error) {
	if verifier == nil {
		return nil, errors.New("runtime slot node channel verifier is required")
	}
	return &ChannelHub{verifier: verifier, routes: make(map[nodeChannelKey]*nodeChannelRoute)}, nil
}

// Prepare applies one exact network policy through a node channel that
// explicitly advertises a ctld-owned network executor.
func (h *ChannelHub) Prepare(
	ctx context.Context,
	request runtimeslotclaim.NetworkPrepareRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	controlRequest := protocol.NodeNetworkPrepareControlRequest{
		OperationID: request.OperationID, ClaimID: request.ClaimID,
		SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, NetworkPolicy: request.NetworkPolicy,
		PolicyDigest: request.PolicyDigest,
	}
	target := protocol.NodeChannelTarget{
		SlotID: request.SlotID, ClusterID: request.ClusterID,
		AllocationID: request.AllocationID, NodeID: request.NodeID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
	}
	command, err := protocol.NewNodeChannelNetworkPrepareCommand(target, controlRequest)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("build node network-prepare command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	return *result.NetworkPolicyToken, nil
}

// ServeHTTP upgrades one verified node request and registers its exact boot
// incarnation. The TLS listener must require and verify client certificates;
// this handler checks that condition again to fail closed if it is miswired.
func (h *ChannelHub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h == nil || h.verifier == nil {
		http.Error(writer, "node channel is unavailable", http.StatusServiceUnavailable)
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.TLS == nil || len(request.TLS.PeerCertificates) == 0 || len(request.TLS.VerifiedChains) == 0 {
		http.Error(writer, "node channel requires verified mTLS", http.StatusUnauthorized)
		return
	}
	if request.Header.Get("Origin") != "" {
		http.Error(writer, "node channel forbids browser origins", http.StatusForbidden)
		return
	}
	identity, err := h.verifier.Verify(request.Context(), request.Header.Get("Authorization"))
	if err != nil || strings.TrimSpace(identity.ClusterID) == "" ||
		strings.TrimSpace(identity.NodeID) == "" || strings.TrimSpace(identity.NodeUID) == "" {
		http.Error(writer, "node channel identity is unauthorized", http.StatusUnauthorized)
		return
	}
	upgrader := websocket.Upgrader{
		HandshakeTimeout: nodeChannelHelloTimeout,
		Subprotocols:     []string{protocol.NodeChannelSubprotocol},
		CheckOrigin:      func(*http.Request) bool { return true },
	}
	connection, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}
	if connection.Subprotocol() != protocol.NodeChannelSubprotocol {
		_ = connection.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseProtocolError, "node channel subprotocol is required"),
			time.Now().Add(time.Second))
		_ = connection.Close()
		return
	}
	connection.SetReadLimit(protocol.NodeChannelMaxBytes)
	_ = connection.SetReadDeadline(time.Now().Add(nodeChannelHelloTimeout))
	hello, err := readNodeChannelHello(connection)
	_ = connection.SetReadDeadline(time.Time{})
	if err != nil || !nodeChannelIdentityMatches(identity, hello) {
		_ = connection.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "node channel identity is invalid"),
			time.Now().Add(time.Second))
		_ = connection.Close()
		return
	}
	registered := &nodeChannelConnection{
		websocket: connection, hello: hello,
		writePermit: make(chan struct{}, 1),
		pending:     make(map[string]nodeChannelPending), done: make(chan struct{}),
	}
	go registered.readLoop()
	if err := h.register(registered); err != nil {
		registered.close(err)
		return
	}
	<-registered.done
	h.unregister(registered)
}

func nodeChannelIdentityMatches(identity nodeauth.Identity, hello protocol.NodeChannelHello) bool {
	return hello.ClusterID == identity.ClusterID && hello.NodeID == identity.NodeID &&
		hello.NodeUID == identity.NodeUID
}

func readNodeChannelHello(connection *websocket.Conn) (protocol.NodeChannelHello, error) {
	messageType, payload, err := connection.ReadMessage()
	if err != nil {
		return protocol.NodeChannelHello{}, err
	}
	if messageType != websocket.TextMessage || len(payload) > protocol.NodeChannelMaxBytes {
		return protocol.NodeChannelHello{}, errors.New("node channel hello must be bounded text JSON")
	}
	var hello protocol.NodeChannelHello
	if err := decodeNodeChannelJSON(payload, &hello); err != nil {
		return protocol.NodeChannelHello{}, err
	}
	if err := hello.Validate(); err != nil {
		return protocol.NodeChannelHello{}, err
	}
	return hello, nil
}

func (h *ChannelHub) register(connection *nodeChannelConnection) error {
	key := nodeChannelKey{
		clusterID: connection.hello.ClusterID, nodeID: connection.hello.NodeID,
		nodeUID: connection.hello.NodeUID, nodeBootID: connection.hello.NodeBootID,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return fmt.Errorf("runtime slot node channel hub is closed: %w", errdefs.ErrUnavailable)
	}
	route := h.routes[key]
	if route == nil {
		route = &nodeChannelRoute{}
		h.routes[key] = route
	}
	route.mu.Lock()
	previous := route.conn
	if previous != nil && !previous.isClosed() &&
		previous.hello.AgentInstanceID != connection.hello.AgentInstanceID {
		route.mu.Unlock()
		return fmt.Errorf("another runtime slot node agent owns this boot: %w", errdefs.ErrAlreadyExists)
	}
	route.conn = connection
	route.mu.Unlock()
	if previous != nil {
		previous.close(fmt.Errorf("runtime slot node channel was superseded: %w", errdefs.ErrUnavailable))
	}
	return nil
}

func (h *ChannelHub) unregister(connection *nodeChannelConnection) {
	key := nodeChannelKey{
		clusterID: connection.hello.ClusterID, nodeID: connection.hello.NodeID,
		nodeUID: connection.hello.NodeUID, nodeBootID: connection.hello.NodeBootID,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	route := h.routes[key]
	if route == nil {
		return
	}
	route.mu.Lock()
	if route.conn == connection {
		route.conn = nil
		delete(h.routes, key)
	}
	route.mu.Unlock()
}

// Connected reports whether the exact authenticated node boot has a live
// stream. It is intended for readiness and bounded tests, not as durable
// liveness authority.
func (h *ChannelHub) Connected(clusterID, nodeID, nodeUID, nodeBootID string) bool {
	if h == nil {
		return false
	}
	key := nodeChannelKey{
		clusterID: clusterID, nodeID: nodeID, nodeUID: nodeUID, nodeBootID: nodeBootID,
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	route := h.routes[key]
	if route == nil {
		return false
	}
	route.mu.Lock()
	defer route.mu.Unlock()
	return route.conn != nil && !route.conn.isClosed()
}

// SelectPausedRebaseNode deterministically chooses one currently connected
// rebase-capable node. The caller must persist NodeID and NodeUID in
// PostgreSQL before dispatching any worker side effect.
func (h *ChannelHub) SelectPausedRebaseNode(clusterID, operationID string) (protocol.NodeChannelTarget, error) {
	clusterID = strings.TrimSpace(clusterID)
	operationID = strings.TrimSpace(operationID)
	if h == nil || clusterID == "" || operationID == "" {
		return protocol.NodeChannelTarget{}, fmt.Errorf("rebase worker cluster and operation are required: %w", errdefs.ErrInvalidArgument)
	}
	candidates := h.pausedRebaseNodes(clusterID, "", "")
	if len(candidates) == 0 {
		return protocol.NodeChannelTarget{}, fmt.Errorf("cluster has no authenticated rebase worker: %w", errdefs.ErrUnavailable)
	}
	sum := sha256.Sum256([]byte(operationID))
	index := binary.BigEndian.Uint64(sum[:8]) % uint64(len(candidates))
	return candidates[index], nil
}

// ResolvePausedRebaseNode returns the connected boot for one PostgreSQL-bound
// durable worker identity. It never fails over to another node.
func (h *ChannelHub) ResolvePausedRebaseNode(
	clusterID, nodeID, nodeUID string,
) (protocol.NodeChannelTarget, error) {
	clusterID = strings.TrimSpace(clusterID)
	nodeID = strings.TrimSpace(nodeID)
	nodeUID = strings.TrimSpace(nodeUID)
	if h == nil || clusterID == "" || nodeID == "" || nodeUID == "" {
		return protocol.NodeChannelTarget{}, fmt.Errorf("rebase worker identity is required: %w", errdefs.ErrInvalidArgument)
	}
	candidates := h.pausedRebaseNodes(clusterID, nodeID, nodeUID)
	if len(candidates) == 0 {
		return protocol.NodeChannelTarget{}, fmt.Errorf("exact rebase worker has no authenticated channel: %w", errdefs.ErrUnavailable)
	}
	return candidates[len(candidates)-1], nil
}

func (h *ChannelHub) pausedRebaseNodes(clusterID, nodeID, nodeUID string) []protocol.NodeChannelTarget {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	result := make([]protocol.NodeChannelTarget, 0)
	for key, route := range h.routes {
		if key.clusterID != clusterID || (nodeID != "" && key.nodeID != nodeID) ||
			(nodeUID != "" && key.nodeUID != nodeUID) {
			continue
		}
		route.mu.Lock()
		connection := route.conn
		available := connection != nil && !connection.isClosed() &&
			connection.hello.Supports(protocol.NodeChannelCommandPausedRebase)
		route.mu.Unlock()
		if available {
			result = append(result, protocol.NodeChannelTarget{
				ClusterID: key.clusterID, NodeID: key.nodeID,
				NodeUID: key.nodeUID, NodeBootID: key.nodeBootID,
			})
		}
	}
	sort.Slice(result, func(left, right int) bool {
		a, b := result[left], result[right]
		if a.NodeID != b.NodeID {
			return a.NodeID < b.NodeID
		}
		if a.NodeUID != b.NodeUID {
			return a.NodeUID < b.NodeUID
		}
		return a.NodeBootID < b.NodeBootID
	})
	return result
}

// Close disconnects every node and rejects later registrations or dispatches.
func (h *ChannelHub) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	routes := make([]*nodeChannelRoute, 0, len(h.routes))
	for _, route := range h.routes {
		routes = append(routes, route)
	}
	h.routes = make(map[nodeChannelKey]*nodeChannelRoute)
	h.mu.Unlock()
	for _, route := range routes {
		route.mu.Lock()
		if route.conn != nil {
			route.conn.close(fmt.Errorf("runtime slot node channel hub is closed: %w", errdefs.ErrUnavailable))
			route.conn = nil
		}
		route.mu.Unlock()
	}
	return nil
}

// Claim implements the regional claim planner's node boundary.
func (h *ChannelHub) Claim(
	ctx context.Context,
	target runtimeslotclaim.NodeTarget,
	request protocol.NodeClaimControlRequest,
) (protocol.NodeControlResponse, error) {
	command, err := protocol.NewNodeChannelClaimCommand(nodeChannelTarget(target), request)
	if err != nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("build node claim command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return protocol.NodeControlResponse{}, err
	}
	return *result.ControlResponse, nil
}

// CommandReady implements the regional claim planner's final node commit.
func (h *ChannelHub) CommandReady(
	ctx context.Context,
	target runtimeslotclaim.NodeTarget,
	request protocol.CommandReadyControlRequest,
) (protocol.NodeControlResponse, error) {
	command, err := protocol.NewNodeChannelCommandReadyCommand(nodeChannelTarget(target), request)
	if err != nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("build node command-ready command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return protocol.NodeControlResponse{}, err
	}
	return *result.ControlResponse, nil
}

// RunningFork dispatches an exact live RootFS checkpoint to the source node.
// The node publishes the checkpoint through writer authority before replying.
func (h *ChannelHub) RunningFork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	command, err := protocol.NewNodeChannelRunningForkCommand(target, request)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("build node running-fork command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	return *result.RunningFork, nil
}

// PausedRebase dispatches an exact three-device offline merge to one
// authenticated node boot. PostgreSQL authority remains outside the hub.
func (h *ChannelHub) PausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	if request.Reject || request.AcknowledgeProofDigest != "" {
		return rootfsrebase.WorkerResult{}, fmt.Errorf("paused-rebase execute request contains an acknowledgement: %w", errdefs.ErrInvalidArgument)
	}
	command, err := protocol.NewNodeChannelPausedRebaseCommand(target, request)
	if err != nil {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("build node paused-rebase command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return rootfsrebase.WorkerResult{}, err
	}
	return *result.PausedRebase, nil
}

// RejectPausedRebase permanently fences an exact worker operation and returns
// either a no-output rejection proof or its already-produced output proof.
func (h *ChannelHub) RejectPausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	if !request.Reject || request.AcknowledgeProofDigest != "" {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("paused-rebase rejection mode is required: %w", errdefs.ErrInvalidArgument)
	}
	command, err := protocol.NewNodeChannelPausedRebaseCommand(target, request)
	if err != nil {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("build node paused-rebase rejection: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return rootfsrebase.WorkerRejection{}, err
	}
	return *result.PausedRebaseReject, nil
}

// AcknowledgePausedRebase tells the exact worker node to discard a cached
// output after PostgreSQL has committed or permanently rejected it.
func (h *ChannelHub) AcknowledgePausedRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerAcknowledgement, error) {
	if request.Reject || request.AcknowledgeProofDigest == "" {
		return rootfsrebase.WorkerAcknowledgement{}, fmt.Errorf("paused-rebase acknowledgement proof is required: %w", errdefs.ErrInvalidArgument)
	}
	command, err := protocol.NewNodeChannelPausedRebaseCommand(target, request)
	if err != nil {
		return rootfsrebase.WorkerAcknowledgement{},
			fmt.Errorf("build node paused-rebase acknowledgement: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return rootfsrebase.WorkerAcknowledgement{}, err
	}
	return *result.PausedRebaseAck, nil
}

// CleanupRuntimeSlot implements the terminal controller's authenticated node
// transport without dialing a task-driver Unix socket from the region.
func (h *ChannelHub) CleanupRuntimeSlot(
	ctx context.Context,
	target Target,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	commandTarget := protocol.NodeChannelTarget{
		SlotID: request.SlotID, ClusterID: target.ClusterID, AllocationID: request.AllocationID,
		NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
	}
	command, err := protocol.NewNodeChannelCleanupCommand(commandTarget, request)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("build node cleanup command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	result, err := h.dispatch(ctx, command)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	return *result.CleanupProof, nil
}

func nodeChannelTarget(target runtimeslotclaim.NodeTarget) protocol.NodeChannelTarget {
	return protocol.NodeChannelTarget{
		SlotID: target.SlotID, ClusterID: target.ClusterID, AllocationID: target.AllocationID,
		NodeID: target.NodeID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		ControlEndpoint: target.ControlEndpoint,
	}
}

func (h *ChannelHub) dispatch(
	ctx context.Context,
	command protocol.NodeChannelCommand,
) (protocol.NodeChannelResult, error) {
	if err := command.Validate(); err != nil {
		return protocol.NodeChannelResult{}, fmt.Errorf("validate node channel command: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	key := nodeChannelKey{
		clusterID: command.Target.ClusterID, nodeID: command.Target.NodeID,
		nodeUID: command.Target.NodeUID, nodeBootID: command.Target.NodeBootID,
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return protocol.NodeChannelResult{}, fmt.Errorf("runtime slot node channel hub is closed: %w", errdefs.ErrUnavailable)
	}
	route := h.routes[key]
	h.mu.Unlock()
	if route == nil {
		return protocol.NodeChannelResult{}, fmt.Errorf("exact node boot has no authenticated channel: %w", errdefs.ErrUnavailable)
	}
	route.mu.Lock()
	connection := route.conn
	route.mu.Unlock()
	if connection == nil || connection.isClosed() {
		return protocol.NodeChannelResult{}, fmt.Errorf("exact node boot channel is disconnected: %w", errdefs.ErrUnavailable)
	}
	if !connection.hello.Supports(command.Kind) {
		return protocol.NodeChannelResult{}, fmt.Errorf("node channel does not support %s: %w", command.Kind, errdefs.ErrFailedPrecondition)
	}
	result, err := connection.request(ctx, command)
	if err != nil {
		return protocol.NodeChannelResult{}, err
	}
	if result.Error != "" {
		return protocol.NodeChannelResult{}, nodeChannelRemoteError(result.Error, result.ErrorClass)
	}
	return result, nil
}

func (c *nodeChannelConnection) request(
	ctx context.Context,
	command protocol.NodeChannelCommand,
) (protocol.NodeChannelResult, error) {
	pending := nodeChannelPending{command: command, result: make(chan nodeChannelOutcome, 1)}
	reuseInFlight := false
	c.mu.Lock()
	if c.err != nil {
		err := c.err
		c.mu.Unlock()
		return protocol.NodeChannelResult{}, err
	}
	if existing, exists := c.pending[command.RequestID]; exists {
		if !existing.abandoned {
			c.mu.Unlock()
			return protocol.NodeChannelResult{}, fmt.Errorf("node command is already in flight: %w", errdefs.ErrAlreadyExists)
		}
		existing.abandoned = false
		pending = existing
		reuseInFlight = true
	} else if len(c.pending) >= maxNodeChannelPending {
		c.mu.Unlock()
		return protocol.NodeChannelResult{}, fmt.Errorf("node channel has too many in-flight commands: %w", errdefs.ErrResourceExhausted)
	}
	c.pending[command.RequestID] = pending
	c.mu.Unlock()

	if !reuseInFlight {
		payload, err := json.Marshal(command)
		if err != nil || len(payload) > protocol.NodeChannelMaxBytes {
			c.removePending(command.RequestID, pending.result)
			return protocol.NodeChannelResult{}, fmt.Errorf("encode node channel command: %w: %w", err, errdefs.ErrInvalidArgument)
		}
		select {
		case c.writePermit <- struct{}{}:
		case <-ctx.Done():
			c.removePending(command.RequestID, pending.result)
			return protocol.NodeChannelResult{}, ctx.Err()
		case <-c.done:
			c.removePending(command.RequestID, pending.result)
			return protocol.NodeChannelResult{}, c.connectionError()
		}
		if err := ctx.Err(); err != nil {
			<-c.writePermit
			c.removePending(command.RequestID, pending.result)
			return protocol.NodeChannelResult{}, err
		}
		_ = c.websocket.SetWriteDeadline(time.Now().Add(nodeChannelHelloTimeout))
		err = c.websocket.WriteMessage(websocket.TextMessage, payload)
		_ = c.websocket.SetWriteDeadline(time.Time{})
		<-c.writePermit
		if err != nil {
			c.removePending(command.RequestID, pending.result)
			c.close(fmt.Errorf("write runtime slot node command: %w: %w", err, errdefs.ErrUnavailable))
			return protocol.NodeChannelResult{}, fmt.Errorf("write runtime slot node command: %w: %w", err, errdefs.ErrUnavailable)
		}
	}
	select {
	case outcome := <-pending.result:
		c.removePending(command.RequestID, pending.result)
		return outcome.result, outcome.err
	case <-ctx.Done():
		c.abandonPending(command.RequestID, pending.result)
		return protocol.NodeChannelResult{}, ctx.Err()
	case <-c.done:
		select {
		case outcome := <-pending.result:
			c.removePending(command.RequestID, pending.result)
			return outcome.result, outcome.err
		default:
		}
		return protocol.NodeChannelResult{}, c.connectionError()
	}
}

func (c *nodeChannelConnection) readLoop() {
	for {
		messageType, payload, err := c.websocket.ReadMessage()
		if err != nil {
			c.close(fmt.Errorf("read runtime slot node result: %w: %w", err, errdefs.ErrUnavailable))
			return
		}
		if messageType != websocket.TextMessage || len(payload) > protocol.NodeChannelMaxBytes {
			c.close(fmt.Errorf("runtime slot node result must be bounded text JSON: %w", errdefs.ErrUnavailable))
			return
		}
		var result protocol.NodeChannelResult
		if err := decodeNodeChannelJSON(payload, &result); err != nil {
			c.close(fmt.Errorf("decode runtime slot node result: %w: %w", err, errdefs.ErrUnavailable))
			return
		}
		c.mu.Lock()
		pending, ok := c.pending[result.RequestID]
		if !ok {
			c.mu.Unlock()
			c.close(fmt.Errorf("runtime slot node returned an unsolicited result: %w", errdefs.ErrUnavailable))
			return
		}
		if pending.completed {
			c.mu.Unlock()
			c.close(fmt.Errorf("runtime slot node returned a duplicate result: %w", errdefs.ErrUnavailable))
			return
		}
		if err := result.ValidateFor(pending.command); err != nil {
			delete(c.pending, result.RequestID)
			c.mu.Unlock()
			if pending.abandoned {
				c.close(fmt.Errorf("runtime slot node returned an invalid abandoned result: %w", errdefs.ErrUnavailable))
				return
			}
			pending.result <- nodeChannelOutcome{err: fmt.Errorf("validate runtime slot node result: %w: %w", err, errdefs.ErrUnavailable)}
			c.close(fmt.Errorf("runtime slot node returned an invalid result: %w", errdefs.ErrUnavailable))
			return
		}
		pending.completed = true
		c.pending[result.RequestID] = pending
		select {
		case pending.result <- nodeChannelOutcome{result: result}:
			c.mu.Unlock()
		default:
			c.mu.Unlock()
			c.close(fmt.Errorf("runtime slot node result buffer is full: %w", errdefs.ErrUnavailable))
			return
		}
	}
}

func (c *nodeChannelConnection) removePending(requestID string, result chan nodeChannelOutcome) {
	c.mu.Lock()
	pending, ok := c.pending[requestID]
	if ok && pending.result == result {
		delete(c.pending, requestID)
	}
	c.mu.Unlock()
}

func (c *nodeChannelConnection) abandonPending(requestID string, result chan nodeChannelOutcome) {
	c.mu.Lock()
	pending, ok := c.pending[requestID]
	if ok && pending.result == result {
		pending.abandoned = true
		c.pending[requestID] = pending
	}
	c.mu.Unlock()
}

func (c *nodeChannelConnection) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err != nil
}

func (c *nodeChannelConnection) connectionError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	return fmt.Errorf("runtime slot node channel disconnected: %w", errdefs.ErrUnavailable)
}

func (c *nodeChannelConnection) close(err error) {
	if err == nil {
		err = fmt.Errorf("runtime slot node channel closed: %w", errdefs.ErrUnavailable)
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		pending := c.pending
		c.pending = make(map[string]nodeChannelPending)
		c.mu.Unlock()
		_ = c.websocket.Close()
		for _, request := range pending {
			if !request.abandoned && !request.completed {
				request.result <- nodeChannelOutcome{err: err}
			}
		}
		close(c.done)
	})
}

func decodeNodeChannelJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("node channel message must contain exactly one JSON value")
	}
	return nil
}

func nodeChannelRemoteError(message string, class protocol.NodeChannelErrorClass) error {
	base := map[protocol.NodeChannelErrorClass]error{
		protocol.NodeChannelErrorInvalidArgument:    errdefs.ErrInvalidArgument,
		protocol.NodeChannelErrorNotFound:           errdefs.ErrNotFound,
		protocol.NodeChannelErrorAlreadyExists:      errdefs.ErrAlreadyExists,
		protocol.NodeChannelErrorFailedPrecondition: errdefs.ErrFailedPrecondition,
		protocol.NodeChannelErrorPermissionDenied:   errdefs.ErrPermissionDenied,
		protocol.NodeChannelErrorResourceExhausted:  errdefs.ErrResourceExhausted,
		protocol.NodeChannelErrorUnavailable:        errdefs.ErrUnavailable,
		protocol.NodeChannelErrorInternal:           errdefs.ErrUnavailable,
	}[class]
	if base == nil {
		base = errdefs.ErrUnavailable
	}
	return fmt.Errorf("node channel %s: %w", strings.TrimSpace(message), base)
}
