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

package soakstate

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	maxVerificationRecordBytes = 8 << 20
	maxVerificationRecords     = 2_000_000
)

type VerifyOptions struct {
	Path                     string
	ExpectedConfigSHA256     string
	ExpectedExecutableSHA256 string
	RequireFinal             bool
}

type Verification struct {
	RunID            string          `json:"run_id"`
	ConfigSHA256     string          `json:"config_sha256"`
	ExecutableSHA256 string          `json:"executable_sha256"`
	Records          uint64          `json:"records"`
	FirstAt          time.Time       `json:"first_at"`
	LastAt           time.Time       `json:"last_at"`
	ActiveElapsed    time.Duration   `json:"active_elapsed_ns"`
	LastType         string          `json:"last_type"`
	Config           json.RawMessage `json:"config"`
	LastData         json.RawMessage `json:"last_data"`
	LastCheckpoint   json.RawMessage `json:"last_checkpoint"`
}

// VerifyFile independently validates a completed soak evidence chain without
// requiring the original executable to run. It holds a non-blocking shared
// lock so an in-progress writer cannot be mistaken for final evidence.
func VerifyFile(options VerifyOptions) (Verification, error) {
	path := filepath.Clean(strings.TrimSpace(options.Path))
	if !filepath.IsAbs(path) || path == string(filepath.Separator) {
		return Verification{}, fmt.Errorf("soak evidence path must be a non-root absolute path")
	}
	if err := validateOptionalSHA256(options.ExpectedConfigSHA256, "expected configuration"); err != nil {
		return Verification{}, err
	}
	if err := validateOptionalSHA256(options.ExpectedExecutableSHA256, "expected executable"); err != nil {
		return Verification{}, err
	}

	pathInfo, err := os.Lstat(path)
	if err != nil {
		return Verification{}, fmt.Errorf("inspect soak evidence: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() {
		return Verification{}, fmt.Errorf("soak evidence must be a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return Verification{}, fmt.Errorf("open soak evidence for verification: %w", err)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return Verification{}, fmt.Errorf("stat opened soak evidence: %w", err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(pathInfo, openedInfo) {
		return Verification{}, fmt.Errorf("soak evidence changed while opening")
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		return Verification{}, fmt.Errorf("lock soak evidence for verification: %w", err)
	}
	defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN) //nolint:errcheck
	if openedInfo.Size() == 0 {
		return Verification{}, fmt.Errorf("soak evidence is empty")
	}
	var tail [1]byte
	if _, err := file.ReadAt(tail[:], openedInfo.Size()-1); err != nil {
		return Verification{}, fmt.Errorf("read soak evidence tail: %w", err)
	}
	if tail[0] != '\n' {
		return Verification{}, fmt.Errorf("soak evidence has an incomplete trailing record")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Verification{}, fmt.Errorf("rewind soak evidence: %w", err)
	}

	var result Verification
	state := &Log{}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), maxVerificationRecordBytes)
	for scanner.Scan() {
		if result.Records >= maxVerificationRecords {
			return Verification{}, fmt.Errorf("soak evidence exceeds %d records", maxVerificationRecords)
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			return Verification{}, fmt.Errorf("soak evidence contains an empty event at line %d", result.Records+1)
		}
		var current record
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&current); err != nil {
			return Verification{}, fmt.Errorf("decode soak evidence line %d: %w", result.Records+1, err)
		}
		if err := requireJSONEOF(decoder); err != nil {
			return Verification{}, fmt.Errorf("decode soak evidence line %d: %w", result.Records+1, err)
		}
		sequence := result.Records + 1
		if err := validateRecord(current, state, sequence); err != nil {
			return Verification{}, fmt.Errorf("validate soak evidence line %d: %w", sequence, err)
		}
		if sequence == 1 {
			configDigest := sha256.Sum256(current.Data)
			if current.ConfigSHA256 != hex.EncodeToString(configDigest[:]) {
				return Verification{}, fmt.Errorf("validate soak evidence line 1: configuration digest mismatch")
			}
			if err := validateRequiredSHA256(current.ConfigSHA256, "configuration"); err != nil {
				return Verification{}, err
			}
			if err := validateRequiredSHA256(current.ExecutableSHA256, "executable"); err != nil {
				return Verification{}, err
			}
			result.RunID = current.RunID
			result.ConfigSHA256 = current.ConfigSHA256
			result.ExecutableSHA256 = current.ExecutableSHA256
			result.FirstAt = current.At
			result.Config = append(json.RawMessage(nil), current.Data...)
		}
		state.runID = current.RunID
		state.configSHA256 = current.ConfigSHA256
		state.executableSHA256 = current.ExecutableSHA256
		state.previousSHA256 = current.EventSHA256
		state.activeElapsed = time.Duration(current.ActiveElapsedNS)
		result.Records = sequence
		result.LastAt = current.At
		result.ActiveElapsed = time.Duration(current.ActiveElapsedNS)
		result.LastType = current.Type
		result.LastData = append(result.LastData[:0], current.Data...)
		result.LastCheckpoint = append(result.LastCheckpoint[:0], current.Checkpoint...)
	}
	if err := scanner.Err(); err != nil {
		return Verification{}, fmt.Errorf("scan soak evidence: %w", err)
	}
	if result.Records == 0 {
		return Verification{}, fmt.Errorf("soak evidence has no records")
	}
	if options.ExpectedConfigSHA256 != "" && result.ConfigSHA256 != options.ExpectedConfigSHA256 {
		return Verification{}, fmt.Errorf("soak configuration digest does not match expected value")
	}
	if options.ExpectedExecutableSHA256 != "" && result.ExecutableSHA256 != options.ExpectedExecutableSHA256 {
		return Verification{}, fmt.Errorf("soak executable digest does not match expected value")
	}
	if options.RequireFinal && result.LastType != "final" {
		return Verification{}, fmt.Errorf("soak evidence does not end with a final event")
	}
	return result, nil
}

func validateOptionalSHA256(value, name string) error {
	if value == "" {
		return nil
	}
	return validateRequiredSHA256(value, name)
}

func validateRequiredSHA256(value, name string) error {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", name)
	}
	payload, err := hex.DecodeString(value)
	if err != nil || len(payload) != sha256.Size {
		return fmt.Errorf("%s SHA-256 must be 64 lowercase hexadecimal characters", name)
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("multiple JSON values are not allowed")
}
