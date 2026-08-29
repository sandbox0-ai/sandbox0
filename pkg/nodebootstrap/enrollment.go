package nodebootstrap

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
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
)

const (
	metadataOrigin          = "http://100.100.100.200"
	maxMetadataResponse     = 64 << 10
	maxEnrollmentResponse   = 16 << 20
	metadataTokenTTLSeconds = "600"
)

type metadataIdentity struct {
	InstanceID  string `json:"instance-id"`
	PrivateIPv4 string `json:"private-ipv4"`
}

type signedIdentity struct {
	instanceID string
	document   []byte
	signature  string
	privateIP  string
}

type MetadataClient struct {
	http   *http.Client
	origin string
}

func NewMetadataClient() *MetadataClient {
	return &MetadataClient{http: &http.Client{Timeout: 10 * time.Second}, origin: metadataOrigin}
}

func (m *MetadataClient) InstanceID(ctx context.Context) (string, error) {
	token, err := m.token(ctx)
	if err != nil {
		return "", err
	}
	payload, err := m.get(ctx, token, "/latest/meta-data/instance-id")
	if err != nil {
		return "", err
	}
	instanceID := strings.TrimSpace(string(payload))
	if instanceID == "" || len(instanceID) > 256 || strings.ContainsAny(instanceID, " \t\r\n/") {
		return "", errors.New("metadata instance ID is invalid")
	}
	return instanceID, nil
}

func (m *MetadataClient) SignedIdentity(ctx context.Context, audience string) (signedIdentity, error) {
	if strings.TrimSpace(audience) != audience || audience == "" || len(audience) > 512 {
		return signedIdentity{}, errors.New("metadata identity audience is invalid")
	}
	token, err := m.token(ctx)
	if err != nil {
		return signedIdentity{}, err
	}
	document, err := m.get(ctx, token, "/latest/dynamic/instance-identity/document")
	if err != nil {
		return signedIdentity{}, err
	}
	signature, err := m.get(ctx, token, "/latest/dynamic/instance-identity/pkcs7?audience="+url.QueryEscape(audience))
	if err != nil {
		return signedIdentity{}, err
	}
	var identity metadataIdentity
	decoder := json.NewDecoder(bytes.NewReader(document))
	if err := decoder.Decode(&identity); err != nil {
		return signedIdentity{}, fmt.Errorf("decode metadata identity: %w", err)
	}
	identity.InstanceID = strings.TrimSpace(identity.InstanceID)
	identity.PrivateIPv4 = strings.TrimSpace(identity.PrivateIPv4)
	if identity.InstanceID == "" || identity.PrivateIPv4 == "" {
		return signedIdentity{}, errors.New("metadata identity is incomplete")
	}
	value := strings.Join(strings.Fields(string(signature)), "")
	if value == "" || len(value) > maxMetadataResponse {
		return signedIdentity{}, errors.New("metadata identity signature is invalid")
	}
	return signedIdentity{instanceID: identity.InstanceID, document: bytes.TrimSpace(document),
		signature: value, privateIP: identity.PrivateIPv4}, nil
}

func (m *MetadataClient) token(ctx context.Context) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, m.origin+"/latest/api/token", nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("X-aliyun-ecs-metadata-token-ttl-seconds", metadataTokenTTLSeconds)
	response, err := m.http.Do(request)
	if err != nil {
		return "", fmt.Errorf("request metadata token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token returned HTTP %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(payload) > 4096 {
		return "", errors.New("metadata token response is invalid")
	}
	token := strings.TrimSpace(string(payload))
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return "", errors.New("metadata token is empty")
	}
	return token, nil
}

func (m *MetadataClient) get(ctx context.Context, token, path string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.origin+path, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("X-aliyun-ecs-metadata-token", token)
	response, err := m.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request instance metadata: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("instance metadata returned HTTP %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxMetadataResponse+1))
	if err != nil || len(payload) == 0 || len(payload) > maxMetadataResponse {
		return nil, errors.New("instance metadata response is invalid")
	}
	return payload, nil
}

type EnrollmentClient struct {
	origin string
	http   *http.Client
}

func NewEnrollmentClient(origin, caFile string) (*EnrollmentClient, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("node enrollment CA file contains no certificates")
	}
	return &EnrollmentClient{origin: strings.TrimRight(origin, "/"), http: &http.Client{
		Timeout: 2 * time.Minute,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{ //nolint:gosec
			MinVersion: tls.VersionTLS12, RootCAs: roots,
		}},
	}}, nil
}

func (c *EnrollmentClient) Challenge(ctx context.Context, instanceID string) (nodeenrollment.Challenge, error) {
	var response nodeenrollment.Challenge
	err := c.post(ctx, "/internal/v1/node-enrollment/challenge",
		map[string]string{"provider_instance_id": instanceID}, &response)
	return response, err
}

func (c *EnrollmentClient) Finalize(
	ctx context.Context,
	identity signedIdentity,
	challenge nodeenrollment.Challenge,
	nodeID string,
	nomadCSR, authorityCSR []byte,
) (*nodeenrollment.FinalizeResponse, error) {
	request := struct {
		ProviderInstanceID string `json:"provider_instance_id"`
		nodeenrollment.FinalizeRequest
	}{ProviderInstanceID: identity.instanceID, FinalizeRequest: nodeenrollment.FinalizeRequest{
		Challenge: challenge.Audience, Document: identity.document, SignatureBase64: identity.signature,
		NomadNodeID: nodeID, NomadCSRPEM: nomadCSR, AuthorityCSRPEM: authorityCSR,
	}}
	var response nodeenrollment.FinalizeResponse
	if err := c.post(ctx, "/internal/v1/node-enrollment/finalize", request, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *EnrollmentClient) Admit(
	ctx context.Context,
	identity signedIdentity,
	challenge nodeenrollment.Challenge,
	nodeID string,
) (*nodeenrollment.AdmitResponse, error) {
	request := struct {
		ProviderInstanceID string `json:"provider_instance_id"`
		nodeenrollment.AdmitRequest
	}{ProviderInstanceID: identity.instanceID, AdmitRequest: nodeenrollment.AdmitRequest{
		Challenge: challenge.Audience, Document: identity.document,
		SignatureBase64: identity.signature, NomadNodeID: nodeID,
	}}
	var response nodeenrollment.AdmitResponse
	if err := c.post(ctx, "/internal/v1/node-enrollment/admit", request, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c *EnrollmentClient) post(ctx context.Context, path string, requestValue, responseValue any) error {
	payload, err := json.Marshal(requestValue)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("call node enrollment endpoint: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("node enrollment endpoint returned HTTP %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxEnrollmentResponse+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(responseValue); err != nil {
		return fmt.Errorf("decode node enrollment response: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("node enrollment response contains trailing data")
	}
	return nil
}
