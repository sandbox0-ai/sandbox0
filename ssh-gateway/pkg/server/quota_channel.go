package server

import (
	"context"
	"io"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"golang.org/x/crypto/ssh"
)

type quotaSSHChannel struct {
	ssh.Channel
	reader io.Reader
	writer io.Writer
	stderr io.ReadWriter
}

func newQuotaSSHChannel(
	ctx context.Context,
	channel ssh.Channel,
	teamID string,
	quota networkByteQuota,
) ssh.Channel {
	if channel == nil || quota == nil {
		return channel
	}
	stderr := channel.Stderr()
	return &quotaSSHChannel{
		Channel: channel,
		reader: quota.Reader(
			ctx,
			teamID,
			teamquota.KeyNetworkIngressBytes,
			channel,
		),
		writer: quota.Writer(
			ctx,
			teamID,
			teamquota.KeyNetworkEgressBytes,
			channel,
		),
		stderr: &quotaReadWriter{
			Reader: quota.Reader(
				ctx,
				teamID,
				teamquota.KeyNetworkIngressBytes,
				stderr,
			),
			Writer: quota.Writer(
				ctx,
				teamID,
				teamquota.KeyNetworkEgressBytes,
				stderr,
			),
		},
	}
}

func (c *quotaSSHChannel) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *quotaSSHChannel) Write(p []byte) (int, error) {
	return c.writer.Write(p)
}

func (c *quotaSSHChannel) Stderr() io.ReadWriter {
	return c.stderr
}

type quotaReadWriter struct {
	io.Reader
	io.Writer
}
