package proxy

import (
	"net"
	"net/http"
	"strings"
)

var forwardedIdentityHeaders = []string{
	"Forwarded",
	"X-Forwarded-For",
	"X-Forwarded-Host",
	"X-Forwarded-Proto",
	"X-Real-IP",
	"X-Client-IP",
	"X-Cluster-Client-IP",
	"CF-Connecting-IP",
	"True-Client-IP",
	"Fastly-Client-IP",
}

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

func setForwardedHeaders(out, in *http.Request, trustPrior bool) {
	if out == nil || in == nil {
		return
	}
	removeForwardedIdentityHeaders(out.Header)

	priorFor := ""
	priorHost := ""
	priorProto := ""
	if trustPrior {
		priorFor = strings.TrimSpace(in.Header.Get("X-Forwarded-For"))
		priorHost = strings.TrimSpace(in.Header.Get("X-Forwarded-Host"))
		priorProto = strings.TrimSpace(in.Header.Get("X-Forwarded-Proto"))
	}

	if clientIP, _, err := net.SplitHostPort(in.RemoteAddr); err == nil && clientIP != "" {
		if priorFor != "" {
			clientIP = priorFor + ", " + clientIP
		}
		out.Header.Set("X-Forwarded-For", clientIP)
	}
	if priorHost != "" {
		out.Header.Set("X-Forwarded-Host", priorHost)
	} else if strings.TrimSpace(in.Host) != "" {
		out.Header.Set("X-Forwarded-Host", in.Host)
	}
	if priorProto != "" {
		out.Header.Set("X-Forwarded-Proto", priorProto)
	} else if in.TLS != nil {
		out.Header.Set("X-Forwarded-Proto", "https")
	} else {
		out.Header.Set("X-Forwarded-Proto", "http")
	}
}

func removeForwardedIdentityHeaders(header http.Header) {
	for _, name := range forwardedIdentityHeaders {
		header.Del(name)
	}
}

func removeHopByHopHeaders(header http.Header, keepUpgrade bool) {
	removeConnectionHeaderTokens(header, keepUpgrade)
	for _, name := range hopByHopHeaders {
		if keepUpgrade && (strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Upgrade")) {
			continue
		}
		header.Del(name)
	}
	if keepUpgrade {
		header.Set("Connection", "Upgrade")
	}
}

func removeConnectionHeaderTokens(header http.Header, keepUpgrade bool) {
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			token = strings.TrimSpace(token)
			if token == "" {
				continue
			}
			if keepUpgrade && strings.EqualFold(token, "upgrade") {
				continue
			}
			header.Del(token)
		}
	}
}
