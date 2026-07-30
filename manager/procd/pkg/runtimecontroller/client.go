package runtimecontroller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"go.uber.org/zap"
)

const (
	initialReconnectDelay = 100 * time.Millisecond
	maxReconnectDelay     = 5 * time.Second
	maxSnapshotBytes      = 1 << 20
	observationWriteLimit = 5 * time.Second
)

type Identity struct {
	Namespace string
	PodName   string
	PodUID    string
	NodeHost  string
	WatchPort int
}

func IdentityFromEnv() (Identity, error) {
	port := runtimecontrol.DefaultCtldWatchPort
	if raw := strings.TrimSpace(os.Getenv(runtimecontrol.EnvCtldRuntimeWatchPort)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return Identity{}, fmt.Errorf("parse CTLD port: %w", err)
		}
		port = parsed
	}
	identity := Identity{
		Namespace: strings.TrimSpace(os.Getenv(runtimecontrol.EnvPodNamespace)),
		PodName:   strings.TrimSpace(os.Getenv(runtimecontrol.EnvPodName)),
		PodUID:    strings.TrimSpace(os.Getenv(runtimecontrol.EnvPodUID)),
		NodeHost:  strings.TrimSpace(os.Getenv(runtimecontrol.EnvNodeHostIP)),
		WatchPort: port,
	}
	return identity, identity.Validate()
}

func (i Identity) Validate() error {
	switch {
	case i.Namespace == "":
		return errors.New("pod namespace is required")
	case i.PodName == "":
		return errors.New("pod name is required")
	case i.PodUID == "":
		return errors.New("pod uid is required")
	case net.ParseIP(i.NodeHost) == nil:
		return errors.New("node host IP is invalid")
	case i.WatchPort <= 0 || i.WatchPort > 65535:
		return errors.New("CTLD runtime watch port is invalid")
	default:
		return nil
	}
}

type Client struct {
	identity   Identity
	controller *Controller
	dialer     *websocket.Dialer
	logger     *zap.Logger
}

func NewClient(identity Identity, controller *Controller, logger *zap.Logger) (*Client, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	if controller == nil {
		return nil, errors.New("runtime controller is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Client{
		identity:   identity,
		controller: controller,
		dialer: &websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			NetDialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
		logger: logger,
	}, nil
}

// Run blocks on CTLD snapshot events. It uses timers only while the control
// connection is unavailable; an idle healthy connection has no polling loop.
func (c *Client) Run(ctx context.Context) {
	delay := initialReconnectDelay
	for ctx.Err() == nil {
		connected, err := c.runConnection(ctx)
		if ctx.Err() != nil {
			return
		}
		if connected {
			delay = initialReconnectDelay
		}
		c.controller.MarkDisconnected("runtime control stream is disconnected")
		c.logger.Warn("Runtime control stream disconnected", zap.Error(err), zap.Duration("retry_after", delay))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay *= 2
		if delay > maxReconnectDelay {
			delay = maxReconnectDelay
		}
	}
}

func (c *Client) runConnection(ctx context.Context) (bool, error) {
	endpoint := url.URL{
		Scheme: "ws",
		Host:   net.JoinHostPort(c.identity.NodeHost, strconv.Itoa(c.identity.WatchPort)),
		Path:   runtimecontrol.WatchPath,
	}
	query := endpoint.Query()
	query.Set("namespace", c.identity.Namespace)
	query.Set("name", c.identity.PodName)
	query.Set("uid", c.identity.PodUID)
	endpoint.RawQuery = query.Encode()

	conn, _, err := c.dialer.DialContext(ctx, endpoint.String(), nil)
	if err != nil {
		return false, fmt.Errorf("connect runtime control stream: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(maxSnapshotBytes)
	connectionDone := make(chan struct{})
	defer close(connectionDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-connectionDone:
		}
	}()

	for {
		var snapshot runtimecontrol.Snapshot
		if err := conn.ReadJSON(&snapshot); err != nil {
			return true, err
		}
		report := func(observation runtimecontrol.Observation) error {
			if err := conn.SetWriteDeadline(time.Now().Add(observationWriteLimit)); err != nil {
				return err
			}
			return conn.WriteJSON(observation)
		}
		if err := c.controller.HandleSnapshot(ctx, snapshot, report); err != nil {
			return true, err
		}
	}
}
