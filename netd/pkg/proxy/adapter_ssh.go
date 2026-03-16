package proxy

import (
	"bytes"
	"strings"
)

func parseSSHBanner(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	line, _, _ := bytes.Cut(data, []byte("\n"))
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return ""
	}
	banner := string(line)
	if !strings.HasPrefix(banner, "SSH-") {
		return ""
	}
	return banner
}
