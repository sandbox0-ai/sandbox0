package nodeenrollment

import (
	"bytes"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/smallstep/pkcs7"
)

// AliyunInstanceIdentity is the signed ECS instance identity document.
type AliyunInstanceIdentity struct {
	ZoneID         string `json:"zone-id"`
	SerialNumber   string `json:"serial-number"`
	InstanceID     string `json:"instance-id"`
	RegionID       string `json:"region-id"`
	PrivateIPv4    string `json:"private-ipv4"`
	OwnerAccountID string `json:"owner-account-id"`
	MAC            string `json:"mac"`
	ImageID        string `json:"image-id"`
	InstanceType   string `json:"instance-type"`
}

// AliyunIdentityPolicy pins every cloud attribute that may admit an elastic
// worker. ESS membership is checked separately against live provider state.
type AliyunIdentityPolicy struct {
	RegionID       string
	OwnerAccountID string
	ImageID        string
	InstanceTypes  []string
	SignerCertPEM  []byte
}

type AliyunIdentityVerifier struct {
	policy AliyunIdentityPolicy
	signer *x509.Certificate
}

func NewAliyunIdentityVerifier(policy AliyunIdentityPolicy) (*AliyunIdentityVerifier, error) {
	policy.RegionID = strings.TrimSpace(policy.RegionID)
	policy.OwnerAccountID = strings.TrimSpace(policy.OwnerAccountID)
	policy.ImageID = strings.TrimSpace(policy.ImageID)
	for index := range policy.InstanceTypes {
		policy.InstanceTypes[index] = strings.TrimSpace(policy.InstanceTypes[index])
	}
	block, rest := pem.Decode(policy.SignerCertPEM)
	if block == nil || block.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("aliyun identity signer must contain one PEM certificate")
	}
	signer, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse Aliyun identity signer: %w", err)
	}
	if policy.RegionID == "" || policy.OwnerAccountID == "" || policy.ImageID == "" ||
		len(policy.InstanceTypes) == 0 || slices.Contains(policy.InstanceTypes, "") {
		return nil, errors.New("aliyun identity policy is incomplete")
	}
	return &AliyunIdentityVerifier{policy: policy, signer: signer}, nil
}

// Verify validates the detached PKCS#7 signature over the exact document plus
// the server-provided audience, then enforces the cloud admission policy.
func (v *AliyunIdentityVerifier) Verify(
	document []byte,
	signatureBase64, audience, remoteIP string,
) (AliyunInstanceIdentity, error) {
	if v == nil || v.signer == nil {
		return AliyunInstanceIdentity{}, errors.New("aliyun identity verifier is unavailable")
	}
	document = bytes.TrimSpace(document)
	if len(document) == 0 || len(document) > 16<<10 || document[len(document)-1] != '}' ||
		strings.TrimSpace(audience) != audience || audience == "" || len(audience) > 512 {
		return AliyunInstanceIdentity{}, errors.New("aliyun identity document or audience is invalid")
	}
	var identity AliyunInstanceIdentity
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return AliyunInstanceIdentity{}, fmt.Errorf("decode Aliyun identity document: %w", err)
	}
	audienceJSON, _ := json.Marshal(audience)
	signedContent := append(append([]byte(nil), document[:len(document)-1]...), []byte(`,"audience":`)...)
	signedContent = append(signedContent, audienceJSON...)
	signedContent = append(signedContent, '}')
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(signatureBase64))
	if err != nil || len(signature) == 0 || len(signature) > 64<<10 {
		return AliyunInstanceIdentity{}, errors.New("decode Aliyun identity signature")
	}
	p7, err := pkcs7.Parse(signature)
	if err != nil {
		return AliyunInstanceIdentity{}, fmt.Errorf("parse Aliyun identity signature: %w", err)
	}
	p7.Content = signedContent
	signer := p7.GetOnlySigner()
	if signer == nil || subtle.ConstantTimeCompare(signer.Raw, v.signer.Raw) != 1 {
		return AliyunInstanceIdentity{}, errors.New("aliyun identity signature uses an untrusted signer")
	}
	if err := p7.Verify(); err != nil {
		return AliyunInstanceIdentity{}, fmt.Errorf("verify Aliyun identity signature: %w", err)
	}
	if identity.RegionID != v.policy.RegionID || identity.OwnerAccountID != v.policy.OwnerAccountID ||
		identity.ImageID != v.policy.ImageID || !slices.Contains(v.policy.InstanceTypes, identity.InstanceType) {
		return AliyunInstanceIdentity{}, errors.New("aliyun instance identity violates enrollment policy")
	}
	ip, err := netip.ParseAddr(identity.PrivateIPv4)
	if err != nil || !ip.Is4() || !ip.IsPrivate() || ip.String() != identity.PrivateIPv4 ||
		identity.PrivateIPv4 != remoteIP {
		return AliyunInstanceIdentity{}, errors.New("aliyun identity private address does not match the caller")
	}
	if strings.TrimSpace(identity.InstanceID) == "" || len(identity.InstanceID) > 256 {
		return AliyunInstanceIdentity{}, errors.New("aliyun identity instance ID is invalid")
	}
	return identity, nil
}

// OfficialAliyunInstanceIdentitySignerPEM is published in Alibaba Cloud's ECS
// instance identity documentation. It expires in 2038; rotation must be an
// explicit reviewed deployment change.
const OfficialAliyunInstanceIdentitySignerPEM = `-----BEGIN CERTIFICATE-----
MIIDdzCCAl+gAwIBAgIEZmbRhzANBgkqhkiG9w0BAQsFADBsMRAwDgYDVQQGEwdV
bmtub3duMRAwDgYDVQQIEwdVbmtub3duMRAwDgYDVQQHEwdVbmtub3duMRAwDgYD
VQQKEwdVbmtub3duMRAwDgYDVQQLEwdVbmtub3duMRAwDgYDVQQDEwdVbmtub3du
MB4XDTE4MDIyMzAxMjkzOFoXDTM4MDIxODAxMjkzOFowbDEQMA4GA1UEBhMHVW5r
bm93bjEQMA4GA1UECBMHVW5rbm93bjEQMA4GA1UEBxMHVW5rbm93bjEQMA4GA1UE
ChMHVW5rbm93bjEQMA4GA1UECxMHVW5rbm93bjEQMA4GA1UEAxMHVW5rbm93bjCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAIJwy5sbZDiNyX4mvdP32pqM
YMK4k7+5lRnVR2Fky/5uwyGSPbddNXaXzwEm+u4wIsJiaAN3OZgJpYIoCGik+9lG
5gVAIr0+/3rZ61IbeVE+vDenDd8g/m/YIdYBfC2IbzgS9EVGAf/gJdtDODXrDfQj
Fk2rQsvpftVOUs3Vpl9O+jeCQLoRbZYm0c5v7jP/L2lK0MjhiywPF2kpDeisMtnD
/ArkSPIlg1qVYm3F19v3pa6ZioM2hnwXg5DibYlgVvsIBGhvYqdQ1KosNVcVGGQa
HCUuVGdS7vHJYp3byH0vQYYygzxUJT2TqvK7pD57eYMN5drc7e19oyRQvbPQ3kkC
AwEAAaMhMB8wHQYDVR0OBBYEFAwwrnHlRgFvPGo+UD5zS1xAkC91MA0GCSqGSIb3
DQEBCwUAA4IBAQBBLhDRgezd/OOppuYEVNB9+XiJ9dNmcuHUhjNTnjiKQWVk/YDA
v+T2V3t9yl8L8o61tRIVKQ++lDhjlVmur/mbBN25/UNRpJllfpUH6oOaqvQAze4a
nRgyTnBwVBZkdJ0d1sivL9NZ4pKelJF3Ylw6rp0YMqV+cwkt/vRtzRJ31ZEeBhs7
vKh7F6BiGCHL5ZAwEUYe8O3akQwjgrMUcfuiFs4/sAeDMnmgN6Uq8DFEBXDpAxVN
sV/6Hockdfinx85RV2AUwJGfClcVcu4hMhOvKROpcH27xu9bBIeMuY0vvzP2VyOm
DoJeqU7qZjyCaUBkPimsz/1eRod6d4P5qxTj
-----END CERTIFICATE-----
`
