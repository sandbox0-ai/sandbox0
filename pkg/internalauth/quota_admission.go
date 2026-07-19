package internalauth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
)

const (
	maxQuotaAdmissionBodyDigestSize = 1 << 20
)

// QuotaAdmissionClass identifies why an upstream service may have admitted
// quota on behalf of a downstream service.
type QuotaAdmissionClass string

const (
	// QuotaAdmissionClassEdgeAdmitted is used for an authenticated public
	// request admitted at the regional edge.
	QuotaAdmissionClassEdgeAdmitted QuotaAdmissionClass = "edge_admitted"

	// QuotaAdmissionClassSystem is reserved for explicit service/route
	// allowlists. IsSystem alone never makes a request quota-exempt.
	QuotaAdmissionClassSystem QuotaAdmissionClass = "system"
)

// QuotaAdmissionProof is a signed, short-lived, request-bound statement that
// the listed keys are already owned by a previous hop. A receiver must
// additionally consume ProofID through the region-shared one-time store before
// trusting any key.
type QuotaAdmissionProof struct {
	ProofID          string              `json:"proof_id"`
	IssuedAtMS       int64               `json:"issued_at_ms"`
	ExpiresAtMS      int64               `json:"expires_at_ms"`
	EnforcementEpoch int64               `json:"enforcement_epoch"`
	RedisGeneration  int64               `json:"redis_generation"`
	Class            QuotaAdmissionClass `json:"class"`
	TeamID           string              `json:"team_id"`
	Method           string              `json:"method"`
	Path             string              `json:"path"`
	RawQuerySHA256   string              `json:"raw_query_sha256"`
	ContentLength    int64               `json:"content_length"`
	TransferEncoding []string            `json:"transfer_encoding"`
	Range            string              `json:"range"`
	ContentType      string              `json:"content_type"`
	ContentEncoding  string              `json:"content_encoding"`
	Accept           string              `json:"accept"`
	IdempotencyKey   string              `json:"idempotency_key"`
	LastEventID      string              `json:"last_event_id"`
	BodySHA256       string              `json:"body_sha256,omitempty"`
	OperationID      string              `json:"operation_id"`
	RequestID        string              `json:"request_id"`
	Keys             []coreteamquota.Key `json:"keys"`
	Origin           string              `json:"origin"`
}

// NewQuotaAdmissionProof constructs and validates a proof for req. Callers may
// pass any Team Quota keys supported by this version; unknown keys fail closed.
func NewQuotaAdmissionProof(
	class QuotaAdmissionClass,
	req *http.Request,
	teamID string,
	operationID string,
	requestID string,
	origin string,
	keys []coreteamquota.Key,
	version guard.Version,
) (*QuotaAdmissionProof, error) {
	proofID, err := newQuotaAdmissionProofID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	binding := quotaAdmissionRequestBinding(req, true)
	proof := &QuotaAdmissionProof{
		ProofID:          proofID,
		IssuedAtMS:       now.UnixMilli(),
		ExpiresAtMS:      now.Add(coreteamquota.MaxAdmissionProofLifetime).UnixMilli(),
		EnforcementEpoch: version.EnforcementEpoch,
		RedisGeneration:  version.RedisGeneration,
		Class:            class,
		TeamID:           strings.TrimSpace(teamID),
		Method:           canonicalRequestMethod(req),
		Path:             CanonicalRequestPath(req),
		RawQuerySHA256:   binding.rawQuerySHA256,
		ContentLength:    binding.contentLength,
		TransferEncoding: binding.transferEncoding,
		Range:            binding.rangeHeader,
		ContentType:      binding.contentType,
		ContentEncoding:  binding.contentEncoding,
		Accept:           binding.accept,
		IdempotencyKey:   binding.idempotencyKey,
		LastEventID:      binding.lastEventID,
		BodySHA256:       binding.bodySHA256,
		OperationID:      strings.TrimSpace(operationID),
		RequestID:        strings.TrimSpace(requestID),
		Keys:             append([]coreteamquota.Key(nil), keys...),
		Origin:           strings.TrimSpace(origin),
	}
	if err := proof.Validate(); err != nil {
		return nil, err
	}
	return proof, nil
}

// Validate checks the proof payload without making it an authentication
// requirement. A signed token with an invalid proof remains an authenticated
// token, but the proof must not grant a quota bypass.
func (p *QuotaAdmissionProof) Validate() error {
	if p == nil {
		return fmt.Errorf("quota admission proof is required")
	}
	if !validFixedHex(p.ProofID, 16) {
		return fmt.Errorf("quota admission proof requires a 128-bit proof_id")
	}
	if p.IssuedAtMS <= 0 ||
		p.ExpiresAtMS <= p.IssuedAtMS ||
		p.ExpiresAtMS-p.IssuedAtMS > coreteamquota.MaxAdmissionProofLifetime.Milliseconds() {
		return fmt.Errorf("quota admission proof validity must be positive and at most %s", coreteamquota.MaxAdmissionProofLifetime)
	}
	if err := p.PolicyVersion().Validate(); err != nil {
		return fmt.Errorf("quota admission proof policy version is invalid: %w", err)
	}
	switch p.Class {
	case QuotaAdmissionClassEdgeAdmitted:
		if p.TeamID == "" {
			return fmt.Errorf("edge quota admission proof requires team_id")
		}
	case QuotaAdmissionClassSystem:
	default:
		return fmt.Errorf("unknown quota admission class %q", p.Class)
	}
	if p.Method == "" || p.Method != strings.ToUpper(strings.TrimSpace(p.Method)) {
		return fmt.Errorf("quota admission proof requires a canonical HTTP method")
	}
	if p.Path == "" || p.Path[0] != '/' {
		return fmt.Errorf("quota admission proof requires a canonical path")
	}
	if !validFixedHex(p.RawQuerySHA256, sha256.Size) {
		return fmt.Errorf("quota admission proof requires a SHA-256 query fingerprint")
	}
	if p.ContentLength < -1 {
		return fmt.Errorf("quota admission proof content_length must be at least -1")
	}
	for _, encoding := range p.TransferEncoding {
		if strings.TrimSpace(encoding) == "" {
			return fmt.Errorf("quota admission proof transfer_encoding contains an empty value")
		}
	}
	if p.BodySHA256 != "" && !validFixedHex(p.BodySHA256, sha256.Size) {
		return fmt.Errorf("quota admission proof body_sha256 must be a SHA-256 digest")
	}
	if p.OperationID == "" || p.RequestID == "" {
		return fmt.Errorf("quota admission proof requires operation and request IDs")
	}
	if p.Origin == "" {
		return fmt.Errorf("quota admission proof requires origin")
	}
	if len(p.Keys) == 0 {
		return fmt.Errorf("quota admission proof requires at least one key")
	}
	seen := make(map[coreteamquota.Key]struct{}, len(p.Keys))
	for _, key := range p.Keys {
		if !coreteamquota.KnownKey(key) {
			return fmt.Errorf("unknown quota admission key %q", key)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate quota admission key %q", key)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// MatchesRequest checks the signed team, method, path, correlation identity,
// and origin against the current signed claims and HTTP request.
func (p *QuotaAdmissionProof) MatchesRequest(claims *Claims, req *http.Request) bool {
	if p == nil || claims == nil || req == nil || claims.Audit == nil {
		return false
	}
	if err := p.Validate(); err != nil {
		return false
	}
	nowMS := time.Now().UTC().UnixMilli()
	if nowMS < p.IssuedAtMS || nowMS >= p.ExpiresAtMS {
		return false
	}
	if claims.ExpiresAt != nil &&
		p.ExpiresAtMS > claims.ExpiresAt.Time.UTC().UnixMilli() {
		return false
	}
	binding := quotaAdmissionRequestBinding(req, p.BodySHA256 != "")
	if p.TeamID != claims.TeamID ||
		p.Method != canonicalRequestMethod(req) ||
		p.Path != CanonicalRequestPath(req) ||
		p.RawQuerySHA256 != binding.rawQuerySHA256 ||
		p.ContentLength != binding.contentLength ||
		!equalStrings(p.TransferEncoding, binding.transferEncoding) ||
		p.Range != binding.rangeHeader ||
		p.ContentType != binding.contentType ||
		p.ContentEncoding != binding.contentEncoding ||
		p.Accept != binding.accept ||
		p.IdempotencyKey != binding.idempotencyKey ||
		p.LastEventID != binding.lastEventID ||
		p.BodySHA256 != binding.bodySHA256 ||
		p.OperationID != claims.Audit.OperationID ||
		p.RequestID != claims.Audit.RequestID ||
		p.Origin != claims.Audit.Origin {
		return false
	}
	switch p.Class {
	case QuotaAdmissionClassEdgeAdmitted:
		return !claims.IsSystem && p.Origin == ServiceRegionalGateway
	case QuotaAdmissionClassSystem:
		return claims.IsSystem
	default:
		return false
	}
}

// PolicyVersion returns the exact distributed policy generation observed when
// the upstream hop signed the proof.
func (p *QuotaAdmissionProof) PolicyVersion() guard.Version {
	if p == nil {
		return guard.Version{}
	}
	return guard.Version{
		EnforcementEpoch: p.EnforcementEpoch,
		RedisGeneration:  p.RedisGeneration,
	}
}

// ContainsKey reports whether key is explicitly covered by this proof.
func (p *QuotaAdmissionProof) ContainsKey(key coreteamquota.Key) bool {
	if p == nil || !coreteamquota.KnownKey(key) {
		return false
	}
	for _, admitted := range p.Keys {
		if admitted == key {
			return true
		}
	}
	return false
}

// CanonicalRequestPath returns the escaped request path used by quota proofs.
// RawQuery is bound separately by an exact SHA-256 fingerprint.
func CanonicalRequestPath(req *http.Request) string {
	if req == nil || req.URL == nil {
		return ""
	}
	path := req.URL.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

func canonicalRequestMethod(req *http.Request) string {
	if req == nil {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(req.Method))
}

func cloneQuotaAdmissionProof(value *QuotaAdmissionProof) *QuotaAdmissionProof {
	if value == nil {
		return nil
	}
	copyValue := *value
	copyValue.Keys = append([]coreteamquota.Key(nil), value.Keys...)
	copyValue.TransferEncoding = append([]string(nil), value.TransferEncoding...)
	return &copyValue
}

func prepareQuotaAdmissionProof(
	value *QuotaAdmissionProof,
	tokenExpiresAt time.Time,
) *QuotaAdmissionProof {
	proof := cloneQuotaAdmissionProof(value)
	if proof == nil {
		return proof
	}
	maxExpiresAtMS := proof.IssuedAtMS + coreteamquota.MaxAdmissionProofLifetime.Milliseconds()
	tokenExpiresAtMS := tokenExpiresAt.UTC().UnixMilli()
	if proof.ExpiresAtMS > maxExpiresAtMS {
		proof.ExpiresAtMS = maxExpiresAtMS
	}
	if proof.ExpiresAtMS > tokenExpiresAtMS {
		proof.ExpiresAtMS = tokenExpiresAtMS
	}
	return proof
}

type quotaAdmissionBinding struct {
	rawQuerySHA256   string
	contentLength    int64
	transferEncoding []string
	rangeHeader      string
	contentType      string
	contentEncoding  string
	accept           string
	idempotencyKey   string
	lastEventID      string
	bodySHA256       string
}

func quotaAdmissionRequestBinding(
	req *http.Request,
	includeReplayableBody bool,
) quotaAdmissionBinding {
	if req == nil {
		return quotaAdmissionBinding{
			rawQuerySHA256: hashQuotaAdmissionValue(""),
			contentLength:  -1,
		}
	}
	rawQuery := ""
	if req.URL != nil {
		rawQuery = req.URL.RawQuery
	}
	binding := quotaAdmissionBinding{
		rawQuerySHA256:   hashQuotaAdmissionValue(rawQuery),
		contentLength:    req.ContentLength,
		transferEncoding: append([]string(nil), req.TransferEncoding...),
		rangeHeader:      stableQuotaAdmissionHeader(req, "Range"),
		contentType:      stableQuotaAdmissionHeader(req, "Content-Type"),
		contentEncoding:  stableQuotaAdmissionHeader(req, "Content-Encoding"),
		accept:           stableQuotaAdmissionHeader(req, "Accept"),
		idempotencyKey:   stableQuotaAdmissionHeader(req, "Idempotency-Key"),
		lastEventID:      stableQuotaAdmissionHeader(req, "Last-Event-ID"),
	}
	if includeReplayableBody {
		binding.bodySHA256 = replayableQuotaAdmissionBodySHA256(req)
	}
	return binding
}

// replayableQuotaAdmissionBodySHA256 never reads req.Body. Most inbound
// streaming requests therefore bind exact length, transfer encoding, and
// semantic headers, but do not claim body-content equivalence. Fixed-cost
// Fixed-cost keys are independent of content; when GetBody safely exposes an
// already replayable, bounded payload, its content digest is also bound.
func replayableQuotaAdmissionBodySHA256(req *http.Request) string {
	if req == nil ||
		req.GetBody == nil ||
		req.ContentLength <= 0 ||
		req.ContentLength > maxQuotaAdmissionBodyDigestSize {
		return ""
	}
	body, err := req.GetBody()
	if err != nil {
		return ""
	}
	defer body.Close()
	payload, err := io.ReadAll(io.LimitReader(body, maxQuotaAdmissionBodyDigestSize+1))
	if err != nil ||
		int64(len(payload)) != req.ContentLength ||
		len(payload) > maxQuotaAdmissionBodyDigestSize {
		return ""
	}
	return hashQuotaAdmissionValue(string(payload))
}

func stableQuotaAdmissionHeader(req *http.Request, name string) string {
	if req == nil {
		return ""
	}
	return strings.Join(req.Header.Values(name), "\n")
}

func hashQuotaAdmissionValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func newQuotaAdmissionProofID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate quota admission proof ID: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func validFixedHex(value string, bytes int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
