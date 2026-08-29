package nodeenrollment

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/smallstep/pkcs7"
	"github.com/stretchr/testify/require"
)

func testAliyunIdentitySigner(t *testing.T) ([]byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "identity.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certificate, err := x509.ParseCertificate(raw)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}), certificate, key
}

func signAliyunIdentity(t *testing.T, content []byte, certificate *x509.Certificate, key *rsa.PrivateKey) string {
	t.Helper()
	signed, err := pkcs7.NewSignedData(content)
	require.NoError(t, err)
	require.NoError(t, signed.AddSigner(certificate, key, pkcs7.SignerInfoConfig{}))
	signed.Detach()
	raw, err := signed.Finish()
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(raw)
}

func TestAliyunIdentityVerifierBindsAudienceAndCallerAddress(t *testing.T) {
	pemBytes, certificate, key := testAliyunIdentitySigner(t)
	verifier, err := NewAliyunIdentityVerifier(AliyunIdentityPolicy{
		RegionID: "us-east-1", OwnerAccountID: "1234", ImageID: "image-1",
		InstanceTypes: []string{"ecs.test"}, SignerCertPEM: pemBytes,
	})
	require.NoError(t, err)
	document := []byte(`{"zone-id":"us-east-1a","serial-number":"serial","instance-id":"i-test","region-id":"us-east-1","private-ipv4":"172.16.1.4","owner-account-id":"1234","mac":"00:11:22:33:44:55","image-id":"image-1","instance-type":"ecs.test"}`)
	audience := "challenge-1"
	signed := append(append([]byte(nil), document[:len(document)-1]...), []byte(`,"audience":"challenge-1"}`)...)
	signature := signAliyunIdentity(t, signed, certificate, key)

	identity, err := verifier.Verify(document, signature, audience, "172.16.1.4")
	require.NoError(t, err)
	require.Equal(t, "i-test", identity.InstanceID)
	_, err = verifier.Verify(document, signature, "challenge-2", "172.16.1.4")
	require.ErrorContains(t, err, "signature")
	_, err = verifier.Verify(document, signature, audience, "172.16.1.5")
	require.ErrorContains(t, err, "caller")
}

func TestAliyunIdentityVerifierRejectsAnotherSigner(t *testing.T) {
	pemBytes, _, _ := testAliyunIdentitySigner(t)
	_, otherCertificate, otherKey := testAliyunIdentitySigner(t)
	verifier, err := NewAliyunIdentityVerifier(AliyunIdentityPolicy{
		RegionID: "us-east-1", OwnerAccountID: "1234", ImageID: "image-1",
		InstanceTypes: []string{"ecs.test"}, SignerCertPEM: pemBytes,
	})
	require.NoError(t, err)
	document := []byte(`{"zone-id":"us-east-1a","serial-number":"serial","instance-id":"i-test","region-id":"us-east-1","private-ipv4":"172.16.1.4","owner-account-id":"1234","mac":"00:11:22:33:44:55","image-id":"image-1","instance-type":"ecs.test"}`)
	signed := append(append([]byte(nil), document[:len(document)-1]...), []byte(`,"audience":"challenge-1"}`)...)
	signature := signAliyunIdentity(t, signed, otherCertificate, otherKey)
	_, err = verifier.Verify(document, signature, "challenge-1", "172.16.1.4")
	require.ErrorContains(t, err, "untrusted signer")
}
