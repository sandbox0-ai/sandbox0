package proxy

import "testing"

func TestParseSSHBanner(t *testing.T) {
	banner := parseSSHBanner([]byte("SSH-2.0-OpenSSH_9.0\r\n"))
	if banner != "SSH-2.0-OpenSSH_9.0" {
		t.Fatalf("unexpected banner: %q", banner)
	}
}

func TestParseSSHBannerRejectsNonSSH(t *testing.T) {
	if parseSSHBanner([]byte("GET / HTTP/1.1\r\n")) != "" {
		t.Fatalf("expected non-ssh payload to be rejected")
	}
}
