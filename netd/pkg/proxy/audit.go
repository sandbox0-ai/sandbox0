package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

func (p *Proxy) openAuditLog() {
	if p.auditPath == "" {
		return
	}
	p.auditMu.Lock()
	defer p.auditMu.Unlock()
	if p.auditFile != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(p.auditPath), 0700); err != nil {
		p.logger.Warn("Failed to create audit log directory",
			zap.String("path", p.auditPath),
			zap.Error(err),
		)
		return
	}
	f, err := os.OpenFile(p.auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		p.logger.Warn("Failed to open audit log file",
			zap.String("path", p.auditPath),
			zap.Error(err),
		)
		return
	}
	p.auditFile = f
}

func (p *Proxy) writeAuditEntry(entry *AuditEntry) {
	if entry == nil || p.auditPath == "" {
		return
	}
	p.openAuditLog()
	p.auditMu.Lock()
	defer p.auditMu.Unlock()
	if p.auditFile == nil {
		return
	}

	data, err := json.Marshal(entry)
	if err != nil {
		p.logger.Warn("Failed to marshal audit entry", zap.Error(err))
		return
	}
	if err := p.rotateAuditIfNeeded(int64(len(data) + 1)); err != nil {
		p.logger.Warn("Failed to rotate audit log", zap.Error(err))
	}
	if p.auditFile == nil {
		return
	}
	if _, err := p.auditFile.Write(append(data, '\n')); err != nil {
		p.logger.Warn("Failed to write audit entry", zap.Error(err))
	}
}

func (p *Proxy) rotateAuditIfNeeded(incomingBytes int64) error {
	if p.auditFile == nil || p.auditMaxBytes <= 0 {
		return nil
	}
	info, err := p.auditFile.Stat()
	if err != nil {
		return err
	}
	if info.Size()+incomingBytes < p.auditMaxBytes {
		return nil
	}

	if err := p.auditFile.Close(); err != nil {
		return err
	}
	p.auditFile = nil

	rotated := fmt.Sprintf("%s.%s", p.auditPath, time.Now().UTC().Format("20060102T150405Z"))
	if err := os.Rename(p.auditPath, rotated); err != nil {
		return err
	}
	if p.auditBackups > 0 {
		p.trimAuditBackups()
	}
	f, err := os.OpenFile(p.auditPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	p.auditFile = f
	return nil
}

func (p *Proxy) trimAuditBackups() {
	dir := filepath.Dir(p.auditPath)
	base := filepath.Base(p.auditPath) + "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		p.logger.Warn("Failed to read audit log directory", zap.Error(err))
		return
	}
	backups := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, base) {
			backups = append(backups, filepath.Join(dir, name))
		}
	}
	if len(backups) <= p.auditBackups {
		return
	}
	sort.Strings(backups)
	toDelete := backups[:len(backups)-p.auditBackups]
	for _, path := range toDelete {
		if err := os.Remove(path); err != nil {
			p.logger.Warn("Failed to remove old audit log", zap.String("path", path), zap.Error(err))
		}
	}
}
