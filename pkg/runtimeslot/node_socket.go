package runtimeslot

import (
	"context"
	"fmt"
	"net"

	"github.com/containerd/errdefs"
)

func dialSecureNodeSocket(ctx context.Context, path string, expectedUID uint32) (net.Conn, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	if err := validateNodeSocketPeer(connection, expectedUID); err != nil {
		_ = connection.Close()
		return nil, err
	}
	// Recheck the pathname after connecting. Peer credentials are authoritative
	// for the connection; this second check also rejects an unsafe replacement
	// from becoming the endpoint used by the next request.
	if err := validateSecureNodeSocket(path, expectedUID); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("revalidate connected node control socket: %w: %w", err, errdefs.ErrPermissionDenied)
	}
	return connection, nil
}
