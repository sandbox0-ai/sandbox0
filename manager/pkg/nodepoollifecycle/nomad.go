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
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
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
		if !nomadinventory.IsWarmJob(n.warmJobID, allocation.JobID) ||
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

func (n *NomadClient) NodeHasNonWarmNonterminalAllocations(ctx context.Context, nodeID string) (bool, error) {
	allocations, err := n.allocations(ctx, nodeID)
	if err != nil {
		return false, err
	}
	for _, allocation := range allocations {
		if allocation.terminal() ||
			(nomadinventory.IsWarmJob(n.warmJobID, allocation.JobID) &&
				(allocation.Namespace == "" || allocation.Namespace == "default")) {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (n *NomadClient) PurgeNode(ctx context.Context, nodeID string) error {
	nodePath := "/v1/node/" + url.PathEscape(nodeID)
	err := n.request(ctx, http.MethodPut, nodePath+"/purge", nil, nil)
	if err == nil {
		return nil
	}
	// Nomad returns HTTP 500 for purging a node that is already absent. Only
	// accept that failure after a separate read confirms the node is gone.
	var statusErr *nomadHTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.code != http.StatusInternalServerError {
		return err
	}
	probeErr := n.request(ctx, http.MethodGet, nodePath, nil, nil)
	if errors.As(probeErr, &statusErr) && statusErr.code == http.StatusNotFound {
		return nil
	}
	return err
}

type nomadHTTPStatusError struct{ code int }

func (e *nomadHTTPStatusError) Error() string {
	return fmt.Sprintf("nomad returned HTTP %d", e.code)
}

type nomadAllocation nomadinventory.Allocation

func (a nomadAllocation) terminal() bool {
	// ClientStatus is Nomad's execution-state truth. A one-shot warm carrier
	// normally completes while DesiredStatus remains "run"; requiring a stop
	// desire would keep an already dead allocation blocking scale-in forever.
	return a.ClientStatus == "complete" || a.ClientStatus == "failed" || a.ClientStatus == "lost"
}

func (n *NomadClient) allocations(ctx context.Context, nodeID string) ([]nomadAllocation, error) {
	tokenBytes, err := os.ReadFile(n.tokenFile)
	token := strings.TrimSpace(string(tokenBytes))
	if err != nil || token == "" || len(tokenBytes) > 64<<10 || len(strings.Fields(token)) != 1 {
		return nil, errors.New("nomad lifecycle token file is invalid")
	}
	headers := http.Header{"X-Nomad-Token": {token}, "X-Nomad-Region": {n.region}}
	records, err := nomadinventory.List(ctx, n.http, n.baseURL, nodeID, "*", headers)
	if err != nil {
		return nil, err
	}
	result := make([]nomadAllocation, len(records))
	for i, record := range records {
		result[i] = nomadAllocation(record)
	}
	return result, nil
}

func (n *NomadClient) request(
	ctx context.Context,
	method, requestPath string,
	requestBody, responseBody any,
) error {
	tokenBytes, err := os.ReadFile(n.tokenFile)
	token := strings.TrimSpace(string(tokenBytes))
	if err != nil || token == "" || len(tokenBytes) > 64<<10 || len(strings.Fields(token)) != 1 {
		return errors.New("nomad lifecycle token file is invalid")
	}
	var body io.Reader
	if requestBody != nil {
		payload, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(payload)
	}
	target := *n.baseURL
	target.Path = path.Join(target.Path, requestPath)
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return err
	}
	request.Header.Set("X-Nomad-Token", token)
	request.Header.Set("X-Nomad-Region", n.region)
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := n.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, (2<<20)+1)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &nomadHTTPStatusError{code: response.StatusCode}
	}
	if response.Header.Get("X-Nomad-NextToken") != "" ||
		(response.Header.Get("X-Nomad-Results-Filtered-By-ACLs") != "" && response.Header.Get("X-Nomad-Results-Filtered-By-ACLs") != "false") {
		return errors.New("nomad returned incomplete catalog")
	}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(payload) > 2<<20 {
		return errors.New("nomad response exceeds bound")
	}
	if responseBody == nil {
		return nil
	}
	return json.Unmarshal(payload, responseBody)
}
