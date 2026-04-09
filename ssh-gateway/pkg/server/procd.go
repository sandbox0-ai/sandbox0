package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/sftp"
	procdfile "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/file"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"golang.org/x/crypto/ssh"
)

type createContextRequest struct {
	Type    string                `json:"type"`
	Repl    *createREPLRequest    `json:"repl,omitempty"`
	Cmd     *createCMDRequest     `json:"cmd,omitempty"`
	EnvVars map[string]string     `json:"env_vars,omitempty"`
	PTYSize *createContextPTYSize `json:"pty_size,omitempty"`
}

type createREPLRequest struct {
	Alias string `json:"alias"`
}

type createCMDRequest struct {
	Command []string `json:"command"`
}

type createContextPTYSize struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

type procdContextResponse struct {
	ID string `json:"id"`
}

type procdStatResponse struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Type       string    `json:"type"`
	Size       int64     `json:"size"`
	Mode       string    `json:"mode"`
	ModTime    time.Time `json:"mod_time"`
	IsLink     bool      `json:"is_link"`
	LinkTarget string    `json:"link_target,omitempty"`
}

type procdListResponse struct {
	Entries []procdStatResponse `json:"entries"`
}

type procdWSControlMessage struct {
	Type   string `json:"type"`
	Data   string `json:"data,omitempty"`
	Rows   uint16 `json:"rows,omitempty"`
	Cols   uint16 `json:"cols,omitempty"`
	Signal string `json:"signal,omitempty"`
}

type procdWSMessage struct {
	Type     string `json:"type"`
	Source   string `json:"source,omitempty"`
	Data     string `json:"data,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	State    string `json:"state,omitempty"`
}

type ptySpec struct {
	Term string
	Rows uint16
	Cols uint16
}

type bridgeResult struct {
	exitStatus uint32
	err        error
}

type shellBridge struct {
	channel ssh.Channel
	wsConn  *websocket.Conn
	done    chan bridgeResult

	writeMu sync.Mutex
	closeMu sync.Once
}

func newShellBridge(channel ssh.Channel, wsConn *websocket.Conn) *shellBridge {
	return &shellBridge{
		channel: channel,
		wsConn:  wsConn,
		done:    make(chan bridgeResult, 1),
	}
}

func (b *shellBridge) start() {
	go b.readSSHInput()
	go b.readWSOutput()
}

func (b *shellBridge) Done() <-chan bridgeResult {
	return b.done
}

func (b *shellBridge) Resize(rows, cols uint16) error {
	if rows == 0 || cols == 0 {
		return nil
	}
	return b.writeJSON(procdWSControlMessage{Type: "resize", Rows: rows, Cols: cols})
}

func (b *shellBridge) Signal(signal string) error {
	if signal == "" {
		return nil
	}
	return b.writeJSON(procdWSControlMessage{Type: "signal", Signal: signal})
}

func (b *shellBridge) Close() {
	b.closeMu.Do(func() {
		_ = b.wsConn.Close()
		_ = b.channel.Close()
	})
}

func (b *shellBridge) readSSHInput() {
	buf := make([]byte, 8192)
	for {
		n, err := b.channel.Read(buf)
		if n > 0 {
			if writeErr := b.writeJSON(procdWSControlMessage{Type: "input", Data: string(buf[:n])}); writeErr != nil {
				b.finish(bridgeResult{err: writeErr})
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (b *shellBridge) readWSOutput() {
	for {
		var msg procdWSMessage
		if err := b.wsConn.ReadJSON(&msg); err != nil {
			b.finish(bridgeResult{err: err})
			return
		}
		switch msg.Type {
		case "output":
			var writer io.Writer = b.channel
			if msg.Source == "stderr" {
				writer = b.channel.Stderr()
			}
			if _, err := writer.Write([]byte(msg.Data)); err != nil {
				b.finish(bridgeResult{err: err})
				return
			}
		case "done":
			status := uint32(0)
			if msg.ExitCode != nil && *msg.ExitCode > 0 {
				status = uint32(*msg.ExitCode)
			}
			b.finish(bridgeResult{exitStatus: status})
			return
		}
	}
}

func (b *shellBridge) finish(result bridgeResult) {
	b.closeMu.Do(func() {
		select {
		case b.done <- result:
		default:
		}
		close(b.done)
		_ = b.wsConn.Close()
	})
}

func (b *shellBridge) writeJSON(msg procdWSControlMessage) error {
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return b.wsConn.WriteJSON(msg)
}

func (s *Server) openShell(ctx context.Context, target *SessionTarget, pty ptySpec, channel ssh.Channel) (*shellBridge, error) {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return nil, err
	}

	contextID, err := s.createShellContext(ctx, target, pty, headers)
	if err != nil {
		return nil, err
	}

	wsConn, err := s.dialContextWS(ctx, target.ProcdURL, contextID, headers)
	if err != nil {
		return nil, err
	}

	bridge := newShellBridge(channel, wsConn)
	bridge.start()
	return bridge, nil
}

func (s *Server) openExec(ctx context.Context, target *SessionTarget, pty ptySpec, command string, channel ssh.Channel) (*shellBridge, error) {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return nil, err
	}

	contextID, err := s.createExecContext(ctx, target, pty, command, headers)
	if err != nil {
		return nil, err
	}

	wsConn, err := s.dialContextWS(ctx, target.ProcdURL, contextID, headers)
	if err != nil {
		return nil, err
	}

	bridge := newShellBridge(channel, wsConn)
	bridge.start()
	return bridge, nil
}

func (s *Server) createShellContext(ctx context.Context, target *SessionTarget, pty ptySpec, headers http.Header) (string, error) {
	request := createContextRequest{
		Type:    "repl",
		Repl:    &createREPLRequest{Alias: "bash"},
		PTYSize: &createContextPTYSize{Rows: pty.Rows, Cols: pty.Cols},
	}
	if pty.Term != "" {
		request.EnvVars = map[string]string{"TERM": pty.Term}
	}

	body, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal create context request: %w", err)
	}

	requestURL := strings.TrimRight(target.ProcdURL, "/") + "/api/v1/contexts"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create procd request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("call procd create context: %w", err)
	}
	defer resp.Body.Close()

	data, apiErr, err := spec.DecodeResponse[procdContextResponse](resp.Body)
	if err != nil {
		return "", fmt.Errorf("decode procd create context response: %w", err)
	}
	if apiErr != nil {
		return "", fmt.Errorf("procd create context failed: %s", apiErr.Message)
	}
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("procd create context failed with status %d", resp.StatusCode)
	}
	if data == nil || data.ID == "" {
		return "", fmt.Errorf("procd create context returned empty context id")
	}
	return data.ID, nil
}

func (s *Server) createExecContext(ctx context.Context, target *SessionTarget, pty ptySpec, command string, headers http.Header) (string, error) {
	request := createContextRequest{
		Type: "cmd",
		Cmd: &createCMDRequest{
			Command: []string{"/bin/sh", "-lc", command},
		},
	}
	if pty.Term != "" {
		request.EnvVars = map[string]string{"TERM": pty.Term}
	}
	if pty.Rows > 0 && pty.Cols > 0 {
		request.PTYSize = &createContextPTYSize{Rows: pty.Rows, Cols: pty.Cols}
	}

	body, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal exec context request: %w", err)
	}

	requestURL := strings.TrimRight(target.ProcdURL, "/") + "/api/v1/contexts"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create exec procd request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("call procd create exec context: %w", err)
	}
	defer resp.Body.Close()

	data, apiErr, err := spec.DecodeResponse[procdContextResponse](resp.Body)
	if err != nil {
		return "", fmt.Errorf("decode procd create exec context response: %w", err)
	}
	if apiErr != nil {
		return "", fmt.Errorf("procd create exec context failed: %s", apiErr.Message)
	}
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("procd create exec context failed with status %d", resp.StatusCode)
	}
	if data == nil || data.ID == "" {
		return "", fmt.Errorf("procd create exec context returned empty context id")
	}
	return data.ID, nil
}

func (s *Server) readFile(ctx context.Context, target *SessionTarget, filePath string) ([]byte, error) {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return nil, err
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files", filePath, nil)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create procd file read request: %w", err)
	}
	httpReq.Header = headers.Clone()

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call procd read file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeProcdError(resp, "read file")
	}
	return io.ReadAll(resp.Body)
}

func (s *Server) writeFile(ctx context.Context, target *SessionTarget, filePath string, data []byte) error {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return err
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files", filePath, nil)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create procd file write request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("call procd write file: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decodeProcdError(resp, "write file")
	}
	return nil
}

func (s *Server) statFile(ctx context.Context, target *SessionTarget, filePath string) (*procdStatResponse, error) {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return nil, err
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files/stat", filePath, nil)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create procd stat request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call procd stat: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeProcdError(resp, "stat file")
	}
	data, apiErr, err := spec.DecodeResponse[procdStatResponse](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode procd stat response: %w", err)
	}
	if apiErr != nil {
		return nil, mapSpecError(apiErr)
	}
	return data, nil
}

func (s *Server) listDir(ctx context.Context, target *SessionTarget, dirPath string) ([]procdStatResponse, error) {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return nil, err
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files/list", dirPath, nil)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create procd list request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call procd list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeProcdError(resp, "list dir")
	}
	data, apiErr, err := spec.DecodeResponse[procdListResponse](resp.Body)
	if err != nil {
		return nil, fmt.Errorf("decode procd list response: %w", err)
	}
	if apiErr != nil {
		return nil, mapSpecError(apiErr)
	}
	if data == nil {
		return nil, nil
	}
	return data.Entries, nil
}

func (s *Server) makeDir(ctx context.Context, target *SessionTarget, dirPath string, recursive bool) error {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return err
	}
	query := map[string]string{"mkdir": "true"}
	if recursive {
		query["recursive"] = "true"
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files", dirPath, query)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, http.NoBody)
	if err != nil {
		return fmt.Errorf("create procd mkdir request: %w", err)
	}
	httpReq.Header = headers.Clone()

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("call procd mkdir: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return decodeProcdError(resp, "mkdir")
	}
	return nil
}

func (s *Server) removePath(ctx context.Context, target *SessionTarget, filePath string) error {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return err
	}
	requestURL, err := s.procdFileURL(target.ProcdURL, "/api/v1/files", filePath, nil)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create procd remove request: %w", err)
	}
	httpReq.Header = headers.Clone()

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("call procd remove: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decodeProcdError(resp, "remove path")
	}
	return nil
}

func (s *Server) movePath(ctx context.Context, target *SessionTarget, source, destination string) error {
	headers, err := s.procdHeaders(target)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]string{"source": source, "destination": destination})
	if err != nil {
		return fmt.Errorf("marshal move request: %w", err)
	}
	requestURL := strings.TrimRight(target.ProcdURL, "/") + "/api/v1/files/move"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create procd move request: %w", err)
	}
	httpReq.Header = headers.Clone()
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("call procd move: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return decodeProcdError(resp, "move path")
	}
	return nil
}

func (s *Server) procdFileURL(procdURL, endpointPath, filePath string, extra map[string]string) (string, error) {
	baseURL, err := url.Parse(strings.TrimRight(procdURL, "/"))
	if err != nil {
		return "", fmt.Errorf("parse procd url: %w", err)
	}
	baseURL.Path = endpointPath
	query := baseURL.Query()
	query.Set("path", normalizeRemotePath(filePath))
	for key, value := range extra {
		query.Set(key, value)
	}
	baseURL.RawQuery = query.Encode()
	return baseURL.String(), nil
}

func normalizeRemotePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "."
	}
	cleaned := path.Clean(trimmed)
	if strings.HasPrefix(trimmed, "/") {
		return cleaned
	}
	if cleaned == "." {
		return cleaned
	}
	return strings.TrimPrefix(cleaned, "./")
}

func decodeProcdError(resp *http.Response, action string) error {
	defer io.Copy(io.Discard, resp.Body)
	apiErr, ok := readSpecError(resp)
	if ok {
		return mapSpecError(apiErr)
	}
	return fmt.Errorf("%s failed with status %d", action, resp.StatusCode)
}

func readSpecError(resp *http.Response) (*spec.Error, bool) {
	var envelope spec.Response
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return nil, false
	}
	if envelope.Error == nil {
		return nil, false
	}
	return envelope.Error, true
}

func mapSpecError(apiErr *spec.Error) error {
	if apiErr == nil {
		return errors.New("unknown procd error")
	}
	switch apiErr.Code {
	case "file_not_found", "dir_not_found", spec.CodeNotFound:
		return sftp.ErrSSHFxNoSuchFile
	case "permission_denied", spec.CodeForbidden:
		return sftp.ErrSSHFxPermissionDenied
	case "method_not_allowed", "operation_not_supported":
		return sftp.ErrSSHFxOpUnsupported
	default:
		return sftp.ErrSSHFxFailure
	}
}

func parseFileMode(mode string) uint32 {
	if mode == "" {
		return 0o644
	}
	parsed, err := strconv.ParseUint(mode, 8, 32)
	if err != nil {
		return 0o644
	}
	return uint32(parsed)
}

func isDirType(fileType string) bool {
	return fileType == string(procdfile.FileTypeDir)
}

func (s *Server) dialContextWS(ctx context.Context, procdURL, contextID string, headers http.Header) (*websocket.Conn, error) {
	baseURL, err := url.Parse(strings.TrimRight(procdURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse procd url: %w", err)
	}
	switch baseURL.Scheme {
	case "http":
		baseURL.Scheme = "ws"
	case "https":
		baseURL.Scheme = "wss"
	default:
		return nil, fmt.Errorf("unsupported procd url scheme %q", baseURL.Scheme)
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/") + "/api/v1/contexts/" + contextID + "/ws"

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	wsConn, _, err := dialer.DialContext(ctx, baseURL.String(), headers)
	if err != nil {
		return nil, fmt.Errorf("dial procd websocket: %w", err)
	}
	return wsConn, nil
}

func (s *Server) procdHeaders(target *SessionTarget) (http.Header, error) {
	internalToken, err := s.dataPlaneAuthGen.Generate("procd", target.TeamID, target.UserID, internalauth.GenerateOptions{})
	if err != nil {
		return nil, fmt.Errorf("generate procd token: %w", err)
	}
	procdStorageToken, err := s.dataPlaneAuthGen.Generate("storage-proxy", target.TeamID, target.UserID, internalauth.GenerateOptions{
		Permissions: s.procdStoragePermissions,
		SandboxID:   target.SandboxID,
	})
	if err != nil {
		return nil, fmt.Errorf("generate storage-proxy token for procd: %w", err)
	}
	headers := make(http.Header)
	headers.Set(internalauth.DefaultTokenHeader, internalToken)
	headers.Set(internalauth.TokenForProcdHeader, procdStorageToken)
	return headers, nil
}
