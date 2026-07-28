// Package httpattrs contains shared HTTP semantic-convention helpers.
package httpattrs

import (
	"net"
	"strconv"
	"strings"
)

// SplitHostPort separates a host label into its address and numeric port.
func SplitHostPort(host string) (string, int) {
	host = strings.TrimSpace(host)
	if host == "" {
		return "", 0
	}
	if address, portText, err := net.SplitHostPort(host); err == nil {
		port, _ := strconv.Atoi(portText)
		return address, port
	}
	if strings.Count(host, ":") == 1 {
		if address, portText, ok := strings.Cut(host, ":"); ok {
			port, err := strconv.Atoi(portText)
			if err == nil {
				return address, port
			}
		}
	}
	return host, 0
}
