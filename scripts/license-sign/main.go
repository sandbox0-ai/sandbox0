package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/license"
	"github.com/sandbox0-ai/infra/pkg/licensing"
)

const (
	defaultLicenseSubject = "sandbox0-enterprise"
)

func main() {
	var (
		privateKeyFile string
		outFile        string
		keyID          string
		subject        string
		version        string
		featuresCSV    string
		notBeforeStr   string
		expiresAtStr   string
		expiresIn      time.Duration
	)

	flag.StringVar(&privateKeyFile, "private-key-file", "", "Path to Ed25519 private key PEM file (required)")
	flag.StringVar(&outFile, "out", "license.lic", "Output license file path")
	flag.StringVar(&keyID, "kid", license.CurrentKeyID, "Signing key id embedded in license envelope")
	flag.StringVar(&subject, "subject", defaultLicenseSubject, "License subject/customer identifier")
	flag.StringVar(&version, "version", "v1", "License payload version")
	flag.StringVar(&featuresCSV, "features", string(licensing.FeatureMultiCluster), "Comma-separated feature list (e.g. multi_cluster)")
	flag.StringVar(&notBeforeStr, "not-before", "", "RFC3339 timestamp for license activation time (default: now)")
	flag.StringVar(&expiresAtStr, "expires-at", "", "RFC3339 timestamp for license expiration time")
	flag.DurationVar(&expiresIn, "expires-in", 365*24*time.Hour, "Relative expiration from now (ignored when -expires-at is provided)")
	flag.Parse()

	if strings.TrimSpace(privateKeyFile) == "" {
		fatalf("missing required flag: -private-key-file")
	}
	if strings.TrimSpace(keyID) == "" {
		fatalf("missing required flag: -kid")
	}

	now := time.Now().UTC()

	notBefore := now
	if strings.TrimSpace(notBeforeStr) != "" {
		parsed, err := time.Parse(time.RFC3339, notBeforeStr)
		if err != nil {
			fatalf("parse -not-before: %v", err)
		}
		notBefore = parsed.UTC()
	}

	var expiresAt time.Time
	if strings.TrimSpace(expiresAtStr) != "" {
		parsed, err := time.Parse(time.RFC3339, expiresAtStr)
		if err != nil {
			fatalf("parse -expires-at: %v", err)
		}
		expiresAt = parsed.UTC()
	} else {
		expiresAt = now.Add(expiresIn).UTC()
	}

	if !expiresAt.After(notBefore) {
		fatalf("invalid expiration: expires_at must be after not_before")
	}

	privateKey, err := internalauth.LoadEd25519PrivateKeyFromFile(privateKeyFile)
	if err != nil {
		fatalf("load private key: %v", err)
	}

	claims := license.Claims{
		Version:   strings.TrimSpace(version),
		Subject:   strings.TrimSpace(subject),
		IssuedAt:  now.Unix(),
		NotBefore: notBefore.Unix(),
		ExpiresAt: expiresAt.Unix(),
		Features:  parseFeatures(featuresCSV),
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		fatalf("marshal claims: %v", err)
	}

	signature := ed25519.Sign(privateKey, payload)

	env := license.Envelope{
		KeyID:     keyID,
		Payload:   base64.RawURLEncoding.EncodeToString(payload),
		Signature: base64.RawURLEncoding.EncodeToString(signature),
	}
	data, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		fatalf("marshal envelope: %v", err)
	}
	data = append(data, '\n')

	if err := os.WriteFile(outFile, data, 0o600); err != nil {
		fatalf("write output file: %v", err)
	}

	fmt.Printf("License written to %s\n", outFile)
	fmt.Printf("kid=%s subject=%s expires_at=%s features=%s\n",
		keyID, claims.Subject, expiresAt.Format(time.RFC3339), strings.Join(claims.Features, ","))
}

func parseFeatures(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	seen := make(map[string]struct{}, len(parts))
	features := make([]string, 0, len(parts))
	for _, part := range parts {
		feature := strings.TrimSpace(part)
		if feature == "" {
			continue
		}
		if _, ok := seen[feature]; ok {
			continue
		}
		seen[feature] = struct{}{}
		features = append(features, feature)
	}
	return features
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "license-sign: "+format+"\n", args...)
	os.Exit(1)
}
