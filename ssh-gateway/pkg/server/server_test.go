package server

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/sftp"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	procdfile "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	sshcrypto "golang.org/x/crypto/ssh"
)

type staticAuthorizer struct {
	target *SessionTarget
	err    error
}

func (a *staticAuthorizer) Authenticate(context.Context, string, sshcrypto.PublicKey) (*SessionTarget, error) {
	if a.err != nil {
		return nil, a.err
	}
	return a.target, nil
}

func TestServerInteractiveShellBridge(t *testing.T) {
	internalAuthGen := newTestInternalAuthGenerator(t)
	hostKeyPath := writeTestHostKey(t)

	upgrader := websocket.Upgrader{}
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/contexts":
			if got := r.Header.Get(internalauth.DefaultTokenHeader); got == "" {
				t.Fatalf("missing %s header on create context", internalauth.DefaultTokenHeader)
			}
			if got := r.Header.Get(internalauth.TokenForProcdHeader); got == "" {
				t.Fatalf("missing %s header on create context", internalauth.TokenForProcdHeader)
			}
			var req createContextRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode create context request: %v", err)
			}
			if req.Type != "repl" || req.Repl == nil || req.Repl.Alias != "bash" {
				t.Fatalf("unexpected create context request: %+v", req)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"success":true,"data":{"id":"ctx-1"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contexts/ctx-1/ws":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Fatalf("upgrade websocket: %v", err)
			}
			defer conn.Close()
			for {
				var msg procdWSControlMessage
				if err := conn.ReadJSON(&msg); err != nil {
					return
				}
				if msg.Type == "input" {
					_ = conn.WriteJSON(procdWSMessage{Type: "output", Source: "stdout", Data: strings.ToUpper(msg.Data)})
					_ = conn.WriteJSON(procdWSMessage{Type: "done", ExitCode: intPtr(0), State: "finished"})
					return
				}
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer procd.Close()

	cfg := &config.SSHGatewayConfig{
		SSHHostKeyPath:          hostKeyPath,
		ProcdStoragePermissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
	}
	authorizer := &staticAuthorizer{target: &SessionTarget{SandboxID: "sb_123", UserID: "user-1", TeamID: "team-1", ProcdURL: procd.URL}}

	server, err := NewServer(cfg, authorizer, internalAuthGen, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx, listener) }()
	defer func() {
		cancel()
		_ = listener.Close()
		if err := <-errCh; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve() error = %v", err)
		}
	}()

	clientSigner, clientConfig := newSSHClientConfig(t)
	_ = clientSigner
	client, err := sshcrypto.Dial("tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		t.Fatalf("ssh.Dial() error = %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	defer session.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	stdin, err := session.StdinPipe()
	if err != nil {
		t.Fatalf("StdinPipe() error = %v", err)
	}
	if err := session.RequestPty("xterm-256color", 24, 80, sshcrypto.TerminalModes{}); err != nil {
		t.Fatalf("RequestPty() error = %v", err)
	}
	if err := session.Shell(); err != nil {
		t.Fatalf("Shell() error = %v", err)
	}
	if _, err := io.WriteString(stdin, "echo hi\n"); err != nil {
		t.Fatalf("stdin write error = %v", err)
	}
	_ = stdin.Close()
	if err := session.Wait(); err != nil {
		t.Fatalf("session.Wait() error = %v, stderr=%s", err, stderr.String())
	}
	if stdout.String() != "ECHO HI\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "ECHO HI\\n")
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestServerExecBridge(t *testing.T) {
	internalAuthGen := newTestInternalAuthGenerator(t)
	hostKeyPath := writeTestHostKey(t)

	upgrader := websocket.Upgrader{}
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/contexts":
			var req createContextRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode create context request: %v", err)
			}
			if req.Type != "cmd" || req.Cmd == nil {
				t.Fatalf("unexpected exec context request: %+v", req)
			}
			if got, want := req.Cmd.Command, []string{"/bin/sh", "-lc", "printf hello"}; strings.Join(got, "\x00") != strings.Join(want, "\x00") {
				t.Fatalf("cmd command = %#v, want %#v", got, want)
			}
			if req.PTYSize != nil {
				t.Fatalf("exec context should not allocate a PTY by default: %+v", req.PTYSize)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(w, `{"success":true,"data":{"id":"ctx-exec"}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contexts/ctx-exec/ws":
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Fatalf("upgrade websocket: %v", err)
			}
			defer conn.Close()
			_ = conn.WriteJSON(procdWSMessage{Type: "output", Source: "stdout", Data: "hello"})
			_ = conn.WriteJSON(procdWSMessage{Type: "done", ExitCode: intPtr(0), State: "finished"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer procd.Close()

	_, listener, stop := startTestSSHGateway(t, hostKeyPath, procd.URL, internalAuthGen, zaptest.NewLogger(t))
	defer stop()

	_, clientConfig := newSSHClientConfig(t)
	client, err := sshcrypto.Dial("tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		t.Fatalf("ssh.Dial() error = %v", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		t.Fatalf("NewSession() error = %v", err)
	}
	defer session.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	if err := session.Run("printf hello"); err != nil {
		t.Fatalf("session.Run() error = %v, stderr=%s", err, stderr.String())
	}
	if stdout.String() != "hello" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "hello")
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestServerSFTPSubsystem(t *testing.T) {
	logger := zaptest.NewLogger(t)
	internalAuthGen := newTestInternalAuthGenerator(t)
	hostKeyPath := writeTestHostKey(t)

	rootDir := t.TempDir()
	fileManager, err := procdfile.NewManager(rootDir)
	if err != nil {
		t.Fatalf("file.NewManager() error = %v", err)
	}

	procd := newTestProcdFileServer(t, fileManager)
	defer procd.Close()

	_, listener, stop := startTestSSHGateway(t, hostKeyPath, procd.URL, internalAuthGen, logger)
	defer stop()

	_, clientConfig := newSSHClientConfig(t)
	sshClient, err := sshcrypto.Dial("tcp", listener.Addr().String(), clientConfig)
	if err != nil {
		t.Fatalf("ssh.Dial() error = %v", err)
	}
	defer sshClient.Close()

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		t.Fatalf("sftp.NewClient() error = %v", err)
	}

	if err := sftpClient.MkdirAll("workspace/data"); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	writer, err := sftpClient.Create("workspace/data/hello.txt")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if _, err := io.WriteString(writer, "hello over sftp\n"); err != nil {
		t.Fatalf("WriteString() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(writer) error = %v", err)
	}

	reader, err := sftpClient.Open("workspace/data/hello.txt")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	content, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(content) != "hello over sftp\n" {
		t.Fatalf("downloaded content = %q", string(content))
	}

	if err := sftpClient.Rename("workspace/data/hello.txt", "workspace/data/renamed.txt"); err != nil {
		t.Fatalf("Rename() error = %v", err)
	}
	entries, err := sftpClient.ReadDir("workspace/data")
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "renamed.txt" {
		t.Fatalf("ReadDir() entries = %#v", entries)
	}
	if err := sftpClient.Remove("workspace/data/renamed.txt"); err != nil {
		t.Fatalf("Remove(file) error = %v", err)
	}
	if err := sftpClient.Remove("workspace/data"); err != nil {
		t.Fatalf("Remove(dir) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootDir, "workspace", "data", "renamed.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected file to be removed, stat err = %v", err)
	}
}

func TestServerSCPUpload(t *testing.T) {
	logger := zaptest.NewLogger(t)
	internalAuthGen := newTestInternalAuthGenerator(t)
	hostKeyPath := writeTestHostKey(t)

	rootDir := t.TempDir()
	fileManager, err := procdfile.NewManager(rootDir)
	if err != nil {
		t.Fatalf("file.NewManager() error = %v", err)
	}

	procd := newTestProcdFileServer(t, fileManager)
	defer procd.Close()

	_, listener, stop := startTestSSHGateway(t, hostKeyPath, procd.URL, internalAuthGen, logger)
	defer stop()

	keyPath := writeOpenSSHClientKey(t)
	sourcePath := filepath.Join(t.TempDir(), "scp-payload.txt")
	if err := os.WriteFile(sourcePath, []byte("hello over scp\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(source) error = %v", err)
	}

	cmd := exec.Command(
		"scp",
		"-v",
		"-P", fmt.Sprintf("%d", listener.Addr().(*net.TCPAddr).Port),
		"-i", keyPath,
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "PreferredAuthentications=publickey",
		"-o", "PubkeyAuthentication=yes",
		sourcePath,
		"sb_123@127.0.0.1:scp-payload.txt",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("scp error = %v\noutput:\n%s", err, output)
	}

	content, err := os.ReadFile(filepath.Join(rootDir, "scp-payload.txt"))
	if err != nil {
		t.Fatalf("ReadFile(uploaded) error = %v", err)
	}
	if string(content) != "hello over scp\n" {
		t.Fatalf("uploaded content = %q", string(content))
	}
}

func startTestSSHGateway(t *testing.T, hostKeyPath, procdURL string, internalAuthGen *internalauth.Generator, logger *zap.Logger) (*Server, net.Listener, func()) {
	t.Helper()
	server, err := NewServer(&config.SSHGatewayConfig{
		SSHHostKeyPath:          hostKeyPath,
		ProcdStoragePermissions: []string{"sandboxvolume:read", "sandboxvolume:write"},
	}, &staticAuthorizer{target: &SessionTarget{SandboxID: "sb_123", UserID: "user-1", TeamID: "team-1", ProcdURL: procdURL}}, internalAuthGen, logger)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx, listener) }()
	stop := func() {
		cancel()
		_ = listener.Close()
		if err := <-errCh; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Serve() error = %v", err)
		}
	}
	return server, listener, stop
}

func newTestInternalAuthGenerator(t *testing.T) *internalauth.Generator {
	t.Helper()
	internalPrivatePEM, _, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair() error = %v", err)
	}
	internalPrivateKey, err := internalauth.LoadEd25519PrivateKey(internalPrivatePEM)
	if err != nil {
		t.Fatalf("LoadEd25519PrivateKey() error = %v", err)
	}
	return internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "ssh-gateway", PrivateKey: internalPrivateKey, TTL: 30 * time.Second})
}

func writeTestHostKey(t *testing.T) string {
	t.Helper()
	hostKeyPath := filepath.Join(t.TempDir(), "ssh_host_ed25519_key")
	hostKeyPEM, _, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatalf("GenerateEd25519KeyPair() error = %v", err)
	}
	if err := os.WriteFile(hostKeyPath, hostKeyPEM, 0o600); err != nil {
		t.Fatalf("WriteFile(host key) error = %v", err)
	}
	return hostKeyPath
}

func newTestProcdFileServer(t *testing.T, fileManager *procdfile.Manager) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(internalauth.DefaultTokenHeader); got == "" {
			t.Fatalf("missing %s header on %s", internalauth.DefaultTokenHeader, r.URL.Path)
		}
		if got := r.Header.Get(internalauth.TokenForProcdHeader); got == "" {
			t.Fatalf("missing %s header on %s", internalauth.TokenForProcdHeader, r.URL.Path)
		}
		filePath := translateTestProcdPath(r.URL.Query().Get("path"))
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files":
			data, err := fileManager.ReadFile(filePath)
			if err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/files" && r.URL.Query().Get("mkdir") == "true":
			if err := fileManager.MakeDir(filePath, 0o755, r.URL.Query().Get("recursive") == "true"); err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusCreated, map[string]bool{"created": true})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/files":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				_ = spec.WriteError(w, http.StatusBadRequest, "read_failed", err.Error())
				return
			}
			if err := fileManager.WriteFile(filePath, data, 0o644); err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"written": true})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/files":
			if err := fileManager.Remove(filePath); err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"deleted": true})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files/stat":
			info, err := fileManager.Stat(filePath)
			if err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, info)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/files/list":
			entries, err := fileManager.ListDir(filePath)
			if err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"entries": entries})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/files/move":
			var body struct {
				Source      string `json:"source"`
				Destination string `json:"destination"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				_ = spec.WriteError(w, http.StatusBadRequest, "invalid_request", err.Error())
				return
			}
			if err := fileManager.Move(translateTestProcdPath(body.Source), translateTestProcdPath(body.Destination)); err != nil {
				writeTestProcdFileError(w, err)
				return
			}
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]bool{"moved": true})
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeTestProcdFileError(w http.ResponseWriter, err error) {
	switch err {
	case procdfile.ErrFileNotFound:
		_ = spec.WriteError(w, http.StatusNotFound, "file_not_found", err.Error())
	case procdfile.ErrDirNotFound:
		_ = spec.WriteError(w, http.StatusNotFound, "directory_not_found", err.Error())
	case procdfile.ErrFileTooLarge:
		_ = spec.WriteError(w, http.StatusRequestEntityTooLarge, "file_too_large", err.Error())
	case procdfile.ErrPermissionDenied:
		_ = spec.WriteError(w, http.StatusForbidden, "permission_denied", err.Error())
	case procdfile.ErrPathAlreadyExists:
		_ = spec.WriteError(w, http.StatusConflict, "path_exists", err.Error())
	case procdfile.ErrPathNotDir:
		_ = spec.WriteError(w, http.StatusConflict, "path_not_directory", err.Error())
	default:
		_ = spec.WriteError(w, http.StatusInternalServerError, "operation_failed", err.Error())
	}
}

func translateTestProcdPath(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "/")
	if trimmed == "" {
		return "."
	}
	return trimmed
}

func newSSHClientConfig(t *testing.T) (sshcrypto.Signer, *sshcrypto.ClientConfig) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	signer, err := sshcrypto.NewSignerFromKey(privateKey)
	if err != nil {
		t.Fatalf("NewSignerFromKey() error = %v", err)
	}
	return signer, &sshcrypto.ClientConfig{
		User:            "sb_123",
		Auth:            []sshcrypto.AuthMethod{sshcrypto.PublicKeys(signer)},
		HostKeyCallback: sshcrypto.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}
}

func writeOpenSSHClientKey(t *testing.T) string {
	t.Helper()
	keyPath := filepath.Join(t.TempDir(), "id_ed25519")
	cmd := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", keyPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen error = %v\noutput:\n%s", err, output)
	}
	return keyPath
}

func intPtr(v int) *int { return &v }
