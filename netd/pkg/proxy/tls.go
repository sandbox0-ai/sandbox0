package proxy

import (
	"bytes"
	"fmt"
	"io"
	"net"
)

// TLS ClientHello parsing

type tlsClientHello struct {
	ServerName string
}

// peekTLSClientHello reads the TLS ClientHello and returns the SNI
func peekTLSClientHello(conn net.Conn) (*tlsClientHello, io.Reader, error) {
	const maxClientHelloBytes = 64 * 1024

	var captured bytes.Buffer
	var handshakeBuf []byte

	for captured.Len() < maxClientHelloBytes {
		recordHeader := make([]byte, 5)
		if _, err := io.ReadFull(conn, recordHeader); err != nil {
			return nil, nil, err
		}
		captured.Write(recordHeader)

		if recordHeader[0] != 22 {
			return nil, nil, fmt.Errorf("not a TLS handshake record")
		}
		recordLen := int(recordHeader[3])<<8 | int(recordHeader[4])
		if recordLen <= 0 || recordLen > maxClientHelloBytes {
			return nil, nil, fmt.Errorf("invalid TLS record length")
		}
		recordPayload := make([]byte, recordLen)
		if _, err := io.ReadFull(conn, recordPayload); err != nil {
			return nil, nil, err
		}
		captured.Write(recordPayload)
		handshakeBuf = append(handshakeBuf, recordPayload...)

		for len(handshakeBuf) >= 4 {
			hsType := handshakeBuf[0]
			hsLen := int(handshakeBuf[1])<<16 | int(handshakeBuf[2])<<8 | int(handshakeBuf[3])
			if hsLen <= 0 || hsLen > maxClientHelloBytes {
				return nil, nil, fmt.Errorf("invalid TLS handshake length")
			}
			if len(handshakeBuf) < 4+hsLen {
				break
			}
			if hsType == 1 {
				sni := extractSNIFromClientHello(handshakeBuf[4 : 4+hsLen])
				reader := io.MultiReader(bytes.NewReader(captured.Bytes()), conn)
				return &tlsClientHello{ServerName: sni}, reader, nil
			}
			handshakeBuf = handshakeBuf[4+hsLen:]
		}
	}

	return nil, nil, fmt.Errorf("TLS ClientHello too large")
}

// extractSNIFromClientHello extracts the SNI from a TLS ClientHello payload.
func extractSNIFromClientHello(data []byte) string {
	// ClientHello: https://www.rfc-editor.org/rfc/rfc5246#section-7.4.1.2
	if len(data) < 34 {
		return ""
	}
	idx := 2 + 32 // Version + Random
	if idx >= len(data) {
		return ""
	}
	sessionIDLen := int(data[idx])
	idx++
	idx += sessionIDLen
	if idx+2 > len(data) {
		return ""
	}
	cipherSuitesLen := int(data[idx])<<8 | int(data[idx+1])
	idx += 2 + cipherSuitesLen
	if idx >= len(data) {
		return ""
	}
	compressionMethodsLen := int(data[idx])
	idx++
	idx += compressionMethodsLen
	if idx+2 > len(data) {
		return ""
	}
	extensionsLen := int(data[idx])<<8 | int(data[idx+1])
	idx += 2
	if idx+extensionsLen > len(data) {
		return ""
	}

	extensionsEnd := idx + extensionsLen
	for idx+4 <= extensionsEnd {
		extType := int(data[idx])<<8 | int(data[idx+1])
		extLen := int(data[idx+2])<<8 | int(data[idx+3])
		idx += 4
		if idx+extLen > extensionsEnd {
			return ""
		}
		if extType == 0 { // server_name
			if extLen < 2 || idx+2 > len(data) {
				return ""
			}
			listLen := int(data[idx])<<8 | int(data[idx+1])
			idx += 2
			listEnd := idx + listLen
			for idx+3 <= listEnd {
				nameType := data[idx]
				nameLen := int(data[idx+1])<<8 | int(data[idx+2])
				idx += 3
				if idx+nameLen > listEnd {
					return ""
				}
				if nameType == 0 {
					return string(data[idx : idx+nameLen])
				}
				idx += nameLen
			}
			return ""
		}
		idx += extLen
	}
	return ""
}
