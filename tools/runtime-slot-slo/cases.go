package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Cases share the same synchronized barrier, transport budgets, identity set,
// and cleanup path. Selection is deterministic, before any request is sent.
type claimCase struct {
	Name      string          `json:"name"`
	ClaimBody json.RawMessage `json:"claim_body"`
	Workload  workload        `json:"workload"`
}

type caseEvidence struct {
	Name            string   `json:"name"`
	ClaimBodySHA256 string   `json:"claim_body_sha256"`
	WorkloadSHA256  string   `json:"workload_sha256"`
	Workload        workload `json:"workload"`
	ExpectedSamples int      `json:"expected_samples"`
}

func digestBytes(payload []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(payload))
}

func workloadSHA(work workload) string {
	payload, _ := json.Marshal(work)
	return digestBytes(payload)
}

func loadCases(path string) ([]claimCase, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > 4<<20 {
		return nil, errors.New("cases file exceeds 4 MiB")
	}
	var document struct {
		Version int         `json:"version"`
		Cases   []claimCase `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode cases: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF || document.Version != 1 {
		return nil, errors.New("cases file must contain exactly one version 1 object")
	}
	// Canonicalize only whitespace; this exact byte slice is hashed and sent.
	for index := range document.Cases {
		var body bytes.Buffer
		if err := json.Compact(&body, document.Cases[index].ClaimBody); err != nil {
			return nil, fmt.Errorf("case %d claim body: %w", index, err)
		}
		document.Cases[index].ClaimBody = body.Bytes()
	}
	if err := (config{cases: document.Cases, concurrency: 1024}).validateCases(); err != nil {
		return nil, err
	}
	if len(document.Cases) == 0 {
		return nil, errors.New("cases file is empty")
	}
	return document.Cases, nil
}

func (c config) validateCases() error {
	if len(c.cases) == 0 {
		return nil
	}
	if len(c.cases) < 2 || len(c.cases) > 64 || len(c.cases) > c.concurrency {
		return errors.New("mixed runs need 2..64 cases and concurrency at least the case count")
	}
	names := make(map[string]bool)
	for _, current := range c.cases {
		if strings.TrimSpace(current.Name) != current.Name || current.Name == "" || len(current.Name) > 128 || names[current.Name] {
			return errors.New("case names must be non-empty, unique, trimmed, and at most 128 bytes")
		}
		names[current.Name] = true
		body := bytes.TrimSpace(current.ClaimBody)
		if len(body) == 0 || len(body) > 1<<20 || body[0] != '{' || !json.Valid(body) {
			return fmt.Errorf("case %s needs a claim JSON object no larger than 1 MiB", current.Name)
		}
		if err := current.Workload.validate(); err != nil {
			return fmt.Errorf("case %s: %w", current.Name, err)
		}
		if len(current.Workload.Steps) == 0 {
			return errors.New("mixed runs require an executable workload in every case")
		}
	}
	return nil
}

func (c config) forSample(index int) config {
	if len(c.cases) > 0 {
		selected := c.cases[index%len(c.cases)]
		c.body, c.workload, c.caseName = selected.ClaimBody, selected.Workload, selected.Name
		c.cases = nil
	}
	return c
}

func (c config) caseInventory() []caseEvidence {
	if len(c.cases) == 0 {
		return nil
	}
	result := make([]caseEvidence, len(c.cases))
	for index, current := range c.cases {
		result[index] = caseEvidence{Name: current.Name, ClaimBodySHA256: digestBytes(current.ClaimBody),
			WorkloadSHA256: workloadSHA(current.Workload), Workload: current.Workload}
	}
	for index := 0; index < c.batches*c.concurrency; index++ {
		result[index%len(result)].ExpectedSamples++
	}
	return result
}

func annotateCase(result *sample, cfg config) {
	if cfg.caseName != "" {
		result.CaseName = cfg.caseName
		result.ClaimBodySHA256 = digestBytes(cfg.body)
		result.WorkloadSHA256 = workloadSHA(cfg.workload)
	}
}
