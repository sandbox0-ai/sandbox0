package nodepoollifecycle

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type NomadConfig struct {
	Address        string
	Region         string
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
	TokenFile      string
	WarmJobID      string
}

type NomadClient struct {
	baseURL   *url.URL
	region    string
	tokenFile string
	warmJobID string
	http      *http.Client
}

func NewNomadClient(config NomadConfig) (*NomadClient, error) {
	baseURL, err := url.Parse(strings.TrimSpace(config.Address))
	if err != nil || baseURL.Scheme != "https" || baseURL.Host == "" ||
		baseURL.Path != "" || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, errors.New("nomad lifecycle address must be one HTTPS origin")
	}
	caPEM, err := os.ReadFile(config.CACertFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("nomad lifecycle CA is invalid")
	}
	certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
	if err != nil {
		return nil, err
	}
	config.Region = strings.TrimSpace(config.Region)
	config.TokenFile = strings.TrimSpace(config.TokenFile)
	config.WarmJobID = strings.TrimSpace(config.WarmJobID)
	if config.Region == "" || config.TokenFile == "" || config.WarmJobID == "" {
		return nil, errors.New("nomad lifecycle identity config is incomplete")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		RootCAs:      roots,
		Certificates: []tls.Certificate{certificate},
	}
	return &NomadClient{
		baseURL: baseURL, region: config.Region, tokenFile: config.TokenFile,
		warmJobID: config.WarmJobID,
		http:      &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}, nil
}

func (n *NomadClient) FenceAndStopWarmAllocations(ctx context.Context, nodeID string) error {
	request := struct {
		NodeID      string
		Eligibility string
	}{NodeID: nodeID, Eligibility: "ineligible"}
	if err := n.request(ctx, http.MethodPut, "/v1/node/"+url.PathEscape(nodeID)+"/eligibility", request, nil); err != nil {
		return fmt.Errorf("fence Nomad node: %w", err)
	}
	allocations, err := n.allocations(ctx, nodeID)
	if err != nil {
		return err
	}
	for _, allocation := range allocations {
		if allocation.terminal() {
			continue
		}
		if allocation.JobID != n.warmJobID ||
			(allocation.Namespace != "" && allocation.Namespace != "default") {
			return fmt.Errorf("nomad node %s has non-warm allocation %s from job %s",
				nodeID, allocation.ID, allocation.JobID)
		}
	}
	// Validate the complete inventory before stopping any allocation. A later
	// page may expose another job that must block this node's removal.
	for _, allocation := range allocations {
		if allocation.terminal() {
			continue
		}
		if err := n.request(ctx, http.MethodPut,
			"/v1/allocation/"+url.PathEscape(allocation.ID)+"/stop", nil, nil); err != nil {
			return fmt.Errorf("stop warm allocation %s: %w", allocation.ID, err)
		}
	}
	return nil
}

func (n *NomadClient) NodeHasNonterminalAllocations(ctx context.Context, nodeID string) (bool, error) {
	allocations, err := n.allocations(ctx, nodeID)
	if err != nil {
		return false, err
	}
	for _, allocation := range allocations {
		if !allocation.terminal() {
			return true, nil
		}
	}
	return false, nil
}

func (n *NomadClient) PurgeNode(ctx context.Context, nodeID string) error {
	return n.request(ctx, http.MethodPut, "/v1/node/"+url.PathEscape(nodeID)+"/purge", nil, nil)
}

type nomadAllocation struct {
	NodeID        string `json:"NodeID"`
	ID            string `json:"ID"`
	JobID         string `json:"JobID"`
	Namespace     string `json:"Namespace"`
	ClientStatus  string `json:"ClientStatus"`
	DesiredStatus string `json:"DesiredStatus"`
}

func (a nomadAllocation) terminal() bool {
	// ClientStatus is Nomad's execution-state truth. A one-shot warm carrier
	// normally completes while DesiredStatus remains "run"; requiring a stop
	// desire would keep an already dead allocation blocking scale-in forever.
	return a.ClientStatus == "complete" || a.ClientStatus == "failed" || a.ClientStatus == "lost"
}

const (
	nomadAllocationPageSize = 64
	nomadAllocationMaxPages = 128
)

// allocations reads bounded summaries. The node-specific allocations endpoint
// embeds the complete Job in every result, amplifying large warm-carrier jobs
// across both the Nomad server and this client before any cleanup can proceed.
func (n *NomadClient) allocations(ctx context.Context, nodeID string) ([]nomadAllocation, error) {
	if nodeID == "" || strings.TrimSpace(nodeID) != nodeID {
		return nil, errors.New("Nomad allocation inventory requires an exact node ID")
	}
	query := url.Values{
		"filter":    {"NodeID == " + strconv.Quote(nodeID)},
		"namespace": {"*"}, "per_page": {strconv.Itoa(nomadAllocationPageSize)},
		"resources": {"false"}, "task_states": {"false"},
	}
	var allocations []nomadAllocation
	seenIDs := make(map[string]struct{})
	seenTokens := make(map[string]struct{})
	for page := 0; page < nomadAllocationMaxPages; page++ {
		var batch []nomadAllocation
		headers, err := n.requestQuery(ctx, http.MethodGet, "/v1/allocations", query, nil, &batch)
		if err != nil {
			return nil, err
		}
		if len(batch) > nomadAllocationPageSize {
			return nil, errors.New("Nomad allocation inventory exceeded its page bound")
		}
		for _, allocation := range batch {
			if allocation.NodeID != nodeID || strings.TrimSpace(allocation.ID) == "" {
				return nil, errors.New("Nomad allocation inventory returned an inexact identity")
			}
			if _, exists := seenIDs[allocation.ID]; exists {
				return nil, errors.New("Nomad allocation inventory repeated an allocation")
			}
			seenIDs[allocation.ID] = struct{}{}
			allocations = append(allocations, allocation)
		}
		next := headers.Get("X-Nomad-NextToken")
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

func (n *NomadClient) request(
	ctx context.Context,
	method, requestPath string,
	requestBody, responseBody any,
) error {
	_, err := n.requestQuery(ctx, method, requestPath, nil, requestBody, responseBody)
	return err
}

func (n *NomadClient) requestQuery(
	ctx context.Context,
	method, requestPath string,
	query url.Values,
	requestBody, responseBody any,
) (http.Header, error) {
	tokenBytes, err := os.ReadFile(n.tokenFile)
	token := strings.TrimSpace(string(tokenBytes))
	if err != nil || token == "" || len(tokenBytes) > 64<<10 || len(strings.Fields(token)) != 1 {
		return nil, errors.New("nomad lifecycle token file is invalid")
	}
	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(payload)
	}
	target := *n.baseURL
	target.Path = path.Join(target.Path, requestPath)
	target.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-Nomad-Token", token)
	request.Header.Set("X-Nomad-Region", n.region)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := n.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 2<<20)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		payload, _ := io.ReadAll(limited)
		return nil, fmt.Errorf("nomad returned %s: %s", response.Status,
			strings.TrimSpace(string(payload)))
	}
	if responseBody == nil {
		_, err = io.Copy(io.Discard, limited)
		return nil, err
	}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(responseBody); err != nil {
		return nil, err
	}
	return response.Header, nil
}
