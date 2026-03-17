package proxy

import (
	"bytes"
	"net"
)

type prefixedConn struct {
	net.Conn
	prefix *bytes.Reader
}

func newPrefixedConn(conn net.Conn, prefix []byte) net.Conn {
	if conn == nil || len(prefix) == 0 {
		return conn
	}
	return &prefixedConn{
		Conn:   conn,
		prefix: bytes.NewReader(prefix),
	}
}

func (c *prefixedConn) Read(p []byte) (int, error) {
	if c == nil {
		return 0, net.ErrClosed
	}
	if c.prefix != nil && c.prefix.Len() > 0 {
		return c.prefix.Read(p)
	}
	return c.Conn.Read(p)
}
