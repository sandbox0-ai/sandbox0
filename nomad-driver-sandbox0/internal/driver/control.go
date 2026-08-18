// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type statusResponse struct {
	TaskID      string    `json:"task_id"`
	AllocID     string    `json:"alloc_id"`
	ContainerID string    `json:"container_id"`
	Phase       string    `json:"phase"`
	RootMounted bool      `json:"root_mounted"`
	WriterEpoch string    `json:"writer_epoch,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type claimResponse struct {
	Phase string `json:"phase"`
}

func (h *taskHandle) ServeControl(ctx context.Context) {
	h.controlOnce.Do(func() {
		if err := os.MkdirAll(filepath.Dir(h.socketPath), 0o750); err != nil {
			h.logger.Error("cannot create slot control directory", "error", err)
			return
		}
		_ = os.Remove(h.socketPath)
		listener, err := net.Listen("unix", h.socketPath)
		if err != nil {
			h.logger.Error("cannot listen on slot control socket", "path", h.socketPath, "error", err)
			return
		}
		if err := os.Chmod(h.socketPath, 0o600); err != nil {
			_ = listener.Close()
			h.logger.Error("cannot secure slot control socket", "path", h.socketPath, "error", err)
			return
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/status", func(w http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodGet {
				writeControlError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			writeControlJSON(w, http.StatusOK, h.statusSnapshot())
		})
		mux.HandleFunc("/claim", func(w http.ResponseWriter, request *http.Request) {
			if request.Method != http.MethodPost {
				writeControlError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			var claim ClaimRequest
			decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 1<<20))
			if err := decoder.Decode(&claim); err != nil {
				writeControlError(w, http.StatusBadRequest, fmt.Sprintf("decode claim: %v", err))
				return
			}
			if err := h.Claim(claim); err != nil {
				writeControlError(w, http.StatusConflict, err.Error())
				return
			}
			writeControlJSON(w, http.StatusOK, claimResponse{Phase: string(phaseActive)})
		})

		server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		h.mu.Lock()
		h.controlServer = server
		h.mu.Unlock()

		serveDone := make(chan struct{})
		go func() {
			defer close(serveDone)
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				h.logger.Error("slot control server stopped", "error", err)
			}
		}()
		select {
		case <-ctx.Done():
			_ = server.Close()
		case <-serveDone:
		}
	})
}

func (h *taskHandle) stopControl() {
	h.mu.Lock()
	server := h.controlServer
	h.controlServer = nil
	h.mu.Unlock()
	if server != nil {
		_ = server.Close()
	}
}

func writeControlJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func writeControlError(w http.ResponseWriter, code int, message string) {
	writeControlJSON(w, code, map[string]string{"error": message})
}
