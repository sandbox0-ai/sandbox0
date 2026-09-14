// Package nomadinventory reads bounded Nomad allocation catalogs without embedding jobs.
package nomadinventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Allocation retains only identity and scheduling state needed by runtime recovery.
type Allocation struct {
	NodeID        string `json:"NodeID"`
	ID            string `json:"ID"`
	JobID         string `json:"JobID"`
	Namespace     string `json:"Namespace"`
	ClientStatus  string `json:"ClientStatus"`
	DesiredStatus string `json:"DesiredStatus"`
}

const (
	// PageSize bounds one decoded response independently of cluster density.
	PageSize = 64
	// MaxPages bounds work even when the server never completes pagination.
	MaxPages = 128
)

// List reads bounded summaries. The node-specific allocations endpoint
// embeds the complete Job in every result, amplifying large warm-carrier jobs
// across both the Nomad server and this client before any cleanup can proceed.
func List(ctx context.Context, client *http.Client, baseURL *url.URL, nodeID, namespace string, headers http.Header) ([]Allocation, error) {
	if client == nil || baseURL == nil || nodeID == "" || strings.TrimSpace(nodeID) != nodeID || namespace == "" {
		return nil, errors.New("Nomad allocation inventory requires an exact node ID")
	}
	query := url.Values{
		"filter":    {"NodeID == " + strconv.Quote(nodeID)},
		"namespace": {namespace}, "per_page": {strconv.Itoa(PageSize)},
		"resources": {"false"}, "task_states": {"false"},
	}
	var allocations []Allocation
	seenIDs := make(map[string]struct{})
	seenTokens := make(map[string]struct{})
	for page := 0; page < MaxPages; page++ {
		var batch []Allocation
		target := *baseURL
		target.Path = strings.TrimSuffix(target.Path, "/") + "/v1/allocations"
		target.RawQuery = query.Encode()
		next, err := readPage(ctx, client, target.String(), headers, &batch)
		if err != nil {
			return nil, err
		}
		if len(batch) > PageSize {
			return nil, errors.New("Nomad allocation inventory exceeded its page bound")
		}
		for _, allocation := range batch {
			if allocation.NodeID != nodeID || allocation.ID == "" || strings.TrimSpace(allocation.ID) != allocation.ID {
				return nil, errors.New("Nomad allocation inventory returned an inexact identity")
			}
			if _, exists := seenIDs[allocation.ID]; exists {
				return nil, errors.New("Nomad allocation inventory repeated an allocation")
			}
			seenIDs[allocation.ID] = struct{}{}
			allocations = append(allocations, allocation)
		}
		if next == "" {
			return allocations, nil
		}
		if len(next) > 4096 {
			return nil, errors.New("Nomad allocation inventory token exceeded its bound")
		}
		if _, exists := seenTokens[next]; exists {
			return nil, errors.New("Nomad allocation inventory repeated a page token")
		}
		seenTokens[next] = struct{}{}
		query.Set("next_token", next)
	}
	return nil, errors.New("Nomad allocation inventory exceeded its page count bound")
}

// readPage rejects oversized and truncated responses before exposing an absence
// decision. Authentication and TLS remain owned by the caller's existing client.
func readPage(ctx context.Context, client *http.Client, target string, headers http.Header, batch *[]Allocation) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	request.Header = headers.Clone()
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("Nomad allocation inventory returned HTTP %d", response.StatusCode)
	}
	const maxBytes = 2 << 20
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return "", err
	}
	if len(payload) > maxBytes {
		return "", errors.New("Nomad allocation inventory response exceeded its bound")
	}
	if err := json.Unmarshal(payload, batch); err != nil {
		return "", err
	}
	if len(response.Header.Values("X-Nomad-NextToken")) > 1 {
		return "", errors.New("Nomad allocation inventory returned ambiguous page tokens")
	}
	return response.Header.Get("X-Nomad-NextToken"), nil
}
