// Package streaming provides transport helpers for long-lived HTTP streams.
package streaming

import (
	"errors"
	"net"
	"net/http"
	"time"
)

// DisableResponseWriteDeadline clears the server-managed write deadline for a
// long-lived response. It must be called before the handler writes headers.
func DisableResponseWriteDeadline(w http.ResponseWriter) error {
	if w == nil {
		return nil
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// DisableResponseDeadlines clears server-managed read and write deadlines for
// an upgraded long-lived connection. It must be called before hijacking.
func DisableResponseDeadlines(w http.ResponseWriter) error {
	if w == nil {
		return nil
	}
	controller := http.NewResponseController(w)
	var errs []error
	if err := controller.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		errs = append(errs, err)
	}
	if err := DisableResponseWriteDeadline(w); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// DisableConnectionDeadlines clears read and write deadlines on a hijacked or
// upgraded connection used for long-lived streams.
func DisableConnectionDeadlines(conn net.Conn) error {
	if conn == nil {
		return nil
	}
	return conn.SetDeadline(time.Time{})
}
