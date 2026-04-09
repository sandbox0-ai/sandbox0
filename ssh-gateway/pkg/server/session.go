package server

import (
	"context"
	"errors"
	"io"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

type sftpExit struct {
	status uint32
	err    error
}

type sshSession struct {
	server   *Server
	ctx      context.Context
	channel  ssh.Channel
	requests <-chan *ssh.Request
	target   *SessionTarget

	pty      ptySpec
	bridge   *shellBridge
	sftpDone <-chan sftpExit
}

func newSSHSession(server *Server, ctx context.Context, channel ssh.Channel, requests <-chan *ssh.Request, target *SessionTarget) *sshSession {
	return &sshSession{
		server:   server,
		ctx:      ctx,
		channel:  channel,
		requests: requests,
		target:   target,
		pty:      ptySpec{},
	}
}

func (s *sshSession) run() {
	defer s.channel.Close()
	defer s.closeBridge()

	requests := s.requests
	for requests != nil || s.sftpDone != nil {
		select {
		case <-s.ctx.Done():
			return
		case result := <-s.sftpDone:
			s.handleSFTPExit(result)
			return
		case req, ok := <-requests:
			if !ok {
				requests = nil
				continue
			}
			handled, success := s.handleRequest(req)
			if req.WantReply && handled {
				_ = req.Reply(success, nil)
			}
		}
	}
}

func (s *sshSession) handleRequest(req *ssh.Request) (handled, success bool) {
	switch req.Type {
	case "pty-req":
		return true, s.handlePTY(req.Payload)
	case "shell":
		return true, s.handleShell()
	case "window-change":
		return true, s.handleWindowChange(req.Payload)
	case "signal":
		return true, s.handleSignal(req.Payload)
	case "subsystem":
		return s.handleSubsystem(req)
	case "exec":
		return true, s.handleExec(req.Payload)
	case "eow@openssh.com":
		return true, true
	default:
		return true, false
	}
}

func (s *sshSession) handlePTY(payload []byte) bool {
	var req ptyRequest
	if err := ssh.Unmarshal(payload, &req); err != nil {
		return false
	}
	s.pty.Term = req.Term
	if req.Rows > 0 {
		s.pty.Rows = uint16(req.Rows)
	}
	if req.Cols > 0 {
		s.pty.Cols = uint16(req.Cols)
	}
	return true
}

func (s *sshSession) handleShell() bool {
	if s.bridge != nil || s.sftpDone != nil {
		return false
	}
	bridge, err := s.server.openShell(s.ctx, s.target, s.pty, s.channel)
	if err != nil {
		_, _ = s.channel.Stderr().Write([]byte("failed to start sandbox shell\n"))
		s.server.logger.Warn("Failed to start sandbox shell",
			zap.String("sandbox_id", s.target.SandboxID),
			zap.String("user_id", s.target.UserID),
			zap.Error(err),
		)
		return false
	}
	s.setBridge(bridge)
	s.watchBridge("sandbox shell disconnected\n", "Sandbox shell bridge exited with error", nil)
	return true
}

func (s *sshSession) handleWindowChange(payload []byte) bool {
	var req windowChangeRequest
	if err := ssh.Unmarshal(payload, &req); err != nil {
		return false
	}
	if req.Rows > 0 {
		s.pty.Rows = uint16(req.Rows)
	}
	if req.Cols > 0 {
		s.pty.Cols = uint16(req.Cols)
	}
	if s.bridge != nil {
		_ = s.bridge.Resize(s.pty.Rows, s.pty.Cols)
	}
	return true
}

func (s *sshSession) handleSignal(payload []byte) bool {
	var req signalRequest
	if err := ssh.Unmarshal(payload, &req); err != nil {
		return false
	}
	if s.bridge != nil {
		_ = s.bridge.Signal(normalizeSSHSignal(req.Signal))
	}
	return true
}

func (s *sshSession) handleSubsystem(req *ssh.Request) (handled, success bool) {
	var payload sftpSubsystemRequest
	if err := ssh.Unmarshal(req.Payload, &payload); err != nil || payload.Subsystem != "sftp" {
		return true, false
	}
	if s.bridge != nil || s.sftpDone != nil {
		return true, false
	}
	done := make(chan sftpExit, 1)
	s.sftpDone = done
	go func() {
		result := sftpExit{}
		if err := s.server.serveSFTP(s.channel, s.target); err != nil && !errors.Is(err, io.EOF) {
			result.status = 1
			result.err = err
		}
		done <- result
	}()
	return true, true
}

func (s *sshSession) handleExec(payload []byte) bool {
	if s.bridge != nil || s.sftpDone != nil {
		return false
	}
	var req execRequest
	if err := ssh.Unmarshal(payload, &req); err != nil {
		return false
	}
	command := strings.TrimSpace(req.Command)
	if command == "" {
		_, _ = s.channel.Stderr().Write([]byte("empty exec command\n"))
		return false
	}
	if strings.HasPrefix(command, "scp ") {
		_, _ = s.channel.Stderr().Write([]byte("legacy scp mode is not supported; use default scp/sftp mode\n"))
		return false
	}

	bridge, err := s.server.openExec(s.ctx, s.target, s.pty, command, s.channel)
	if err != nil {
		_, _ = s.channel.Stderr().Write([]byte("failed to start sandbox command\n"))
		s.server.logger.Warn("Failed to start sandbox command",
			zap.String("sandbox_id", s.target.SandboxID),
			zap.String("user_id", s.target.UserID),
			zap.String("command", command),
			zap.Error(err),
		)
		return false
	}
	s.setBridge(bridge)
	s.watchBridge("sandbox command disconnected\n", "Sandbox command bridge exited with error", []zap.Field{zap.String("command", command)})
	return true
}

func (s *sshSession) handleSFTPExit(result sftpExit) {
	if result.err != nil {
		s.server.logger.Warn("SFTP subsystem exited with error",
			zap.String("sandbox_id", s.target.SandboxID),
			zap.Error(result.err),
		)
	}
	if err := s.channel.CloseWrite(); err != nil && !errors.Is(err, io.EOF) {
		s.server.logger.Warn("Failed to close SFTP channel write side",
			zap.String("sandbox_id", s.target.SandboxID),
			zap.Error(err),
		)
	}
	if _, err := s.channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{Status: result.status})); err != nil {
		s.server.logger.Warn("Failed to send SFTP exit status",
			zap.String("sandbox_id", s.target.SandboxID),
			zap.Uint32("exit_status", result.status),
			zap.Error(err),
		)
	}
}

func (s *sshSession) setBridge(bridge *shellBridge) {
	s.bridge = bridge
}

func (s *sshSession) closeBridge() {
	if s.bridge != nil {
		s.bridge.Close()
	}
}

func (s *sshSession) watchBridge(stderrMsg, logMsg string, extraFields []zap.Field) {
	go func() {
		result, ok := <-s.bridge.Done()
		if !ok {
			return
		}
		if result.err != nil {
			_, _ = s.channel.Stderr().Write([]byte(stderrMsg))
			fields := append([]zap.Field{zap.String("sandbox_id", s.target.SandboxID)}, extraFields...)
			fields = append(fields, zap.Error(result.err))
			s.server.logger.Warn(logMsg, fields...)
		}
		_, _ = s.channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{Status: result.exitStatus}))
		s.closeBridge()
		_ = s.channel.Close()
	}()
}
