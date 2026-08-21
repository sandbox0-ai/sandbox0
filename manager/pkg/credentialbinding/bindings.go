// Package credentialbinding converts and fingerprints credential binding
// policy without resolving secret-bearing source versions.
package credentialbinding

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

const EmptyDigest = "sha256:4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"

// ToStore converts public policy bindings into source-version-independent
// storage bindings. SourceID and SourceVersion are intentionally unresolved.
func ToStore(in []v1alpha1.CredentialBinding) []egressauthstore.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]egressauthstore.CredentialBinding, 0, len(in))
	for _, binding := range in {
		out = append(out, egressauthstore.CredentialBinding{
			Ref: binding.Ref, SourceRef: binding.SourceRef,
			Projection:  toStoreProjection(binding.Projection),
			CachePolicy: toStoreCachePolicy(binding.CachePolicy),
		})
	}
	return out
}

// FromStore converts materialized bindings back into public policy bindings.
func FromStore(in []egressauthstore.CredentialBinding) []v1alpha1.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.CredentialBinding, 0, len(in))
	for _, binding := range in {
		out = append(out, v1alpha1.CredentialBinding{
			Ref: binding.Ref, SourceRef: binding.SourceRef,
			Projection:  fromStoreProjection(binding.Projection),
			CachePolicy: fromStoreCachePolicy(binding.CachePolicy),
		})
	}
	return out
}

// CloneStore deep-copies materialized or semantic storage bindings.
func CloneStore(in []egressauthstore.CredentialBinding) []egressauthstore.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]egressauthstore.CredentialBinding, 0, len(in))
	for _, binding := range in {
		out = append(out, egressauthstore.CredentialBinding{
			Ref: binding.Ref, SourceRef: binding.SourceRef,
			SourceID: binding.SourceID, SourceVersion: binding.SourceVersion,
			Projection:  cloneStoreProjection(binding.Projection),
			CachePolicy: cloneStoreCachePolicy(binding.CachePolicy),
		})
	}
	return out
}

// DigestPublic returns the canonical source-version-independent binding
// digest used to bind network policy, durable admission, and runtime caches.
func DigestPublic(in []v1alpha1.CredentialBinding) string {
	return DigestStore(ToStore(in))
}

// DigestStore returns the canonical source-version-independent binding
// digest. Materialized source IDs and versions are deliberately excluded so
// source rotation does not change network policy identity.
func DigestStore(in []egressauthstore.CredentialBinding) string {
	canonical := CloneStore(in)
	for index := range canonical {
		canonical[index].SourceID = 0
		canonical[index].SourceVersion = 0
	}
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].Ref != canonical[j].Ref {
			return canonical[i].Ref < canonical[j].Ref
		}
		return canonical[i].SourceRef < canonical[j].SourceRef
	})
	if canonical == nil {
		canonical = []egressauthstore.CredentialBinding{}
	}
	payload, _ := json.Marshal(canonical)
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// EqualStoreSemantic compares binding semantics while ignoring materialized
// source IDs, source versions, and caller ordering.
func EqualStoreSemantic(left, right []egressauthstore.CredentialBinding) bool {
	return DigestStore(left) == DigestStore(right) &&
		string(canonicalStorePayload(left)) == string(canonicalStorePayload(right))
}

func canonicalStorePayload(in []egressauthstore.CredentialBinding) []byte {
	canonical := CloneStore(in)
	for index := range canonical {
		canonical[index].SourceID = 0
		canonical[index].SourceVersion = 0
	}
	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].Ref != canonical[j].Ref {
			return canonical[i].Ref < canonical[j].Ref
		}
		return canonical[i].SourceRef < canonical[j].SourceRef
	})
	if canonical == nil {
		canonical = []egressauthstore.CredentialBinding{}
	}
	payload, _ := json.Marshal(canonical)
	return payload
}

func toStoreProjection(in v1alpha1.ProjectionSpec) egressauthstore.ProjectionSpec {
	out := egressauthstore.ProjectionSpec{Type: egressauthstore.CredentialProjectionType(in.Type)}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &egressauthstore.HTTPHeadersProjection{
			Headers: make([]egressauthstore.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, egressauthstore.ProjectedHeader{
				Name: header.Name, ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.PlaceholderSubstitution != nil {
		out.PlaceholderSubstitution = &egressauthstore.PlaceholderSubstitutionProjection{
			Replacements: make([]egressauthstore.PlaceholderReplacement, 0, len(in.PlaceholderSubstitution.Replacements)),
		}
		for _, replacement := range in.PlaceholderSubstitution.Replacements {
			locations := make([]egressauth.PlaceholderSubstitutionLocation, 0, len(replacement.Locations))
			for _, location := range replacement.Locations {
				locations = append(locations, egressauth.PlaceholderSubstitutionLocation(location))
			}
			out.PlaceholderSubstitution.Replacements = append(out.PlaceholderSubstitution.Replacements,
				egressauthstore.PlaceholderReplacement{
					Placeholder: replacement.Placeholder, ValueTemplate: replacement.ValueTemplate,
					Locations: locations,
				})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &egressauthstore.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &egressauthstore.UsernamePasswordProjection{}
	}
	if in.SSHProxy != nil {
		out.SSHProxy = &egressauthstore.SSHProxyProjection{
			SandboxPublicKeys: append([]string(nil), in.SSHProxy.SandboxPublicKeys...),
			UpstreamUsername:  in.SSHProxy.UpstreamUsername,
			KnownHosts:        append([]string(nil), in.SSHProxy.KnownHosts...),
		}
	}
	return out
}

func fromStoreProjection(in egressauthstore.ProjectionSpec) v1alpha1.ProjectionSpec {
	out := v1alpha1.ProjectionSpec{Type: v1alpha1.CredentialProjectionType(in.Type)}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &v1alpha1.HTTPHeadersProjection{
			Headers: make([]v1alpha1.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, v1alpha1.ProjectedHeader{
				Name: header.Name, ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.PlaceholderSubstitution != nil {
		out.PlaceholderSubstitution = &v1alpha1.PlaceholderSubstitutionProjection{
			Replacements: make([]v1alpha1.PlaceholderReplacement, 0, len(in.PlaceholderSubstitution.Replacements)),
		}
		for _, replacement := range in.PlaceholderSubstitution.Replacements {
			locations := make([]v1alpha1.PlaceholderSubstitutionLocation, 0, len(replacement.Locations))
			for _, location := range replacement.Locations {
				locations = append(locations, v1alpha1.PlaceholderSubstitutionLocation(location))
			}
			out.PlaceholderSubstitution.Replacements = append(out.PlaceholderSubstitution.Replacements,
				v1alpha1.PlaceholderReplacement{
					Placeholder: replacement.Placeholder, ValueTemplate: replacement.ValueTemplate,
					Locations: locations,
				})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &v1alpha1.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &v1alpha1.UsernamePasswordProjection{}
	}
	if in.SSHProxy != nil {
		out.SSHProxy = &v1alpha1.SSHProxyProjection{
			SandboxPublicKeys: append([]string(nil), in.SSHProxy.SandboxPublicKeys...),
			UpstreamUsername:  in.SSHProxy.UpstreamUsername,
			KnownHosts:        append([]string(nil), in.SSHProxy.KnownHosts...),
		}
	}
	return out
}

func cloneStoreProjection(in egressauthstore.ProjectionSpec) egressauthstore.ProjectionSpec {
	return toStoreProjection(fromStoreProjection(in))
}

func toStoreCachePolicy(in *v1alpha1.CachePolicySpec) *egressauthstore.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauthstore.CachePolicySpec{TTL: in.TTL}
}

func fromStoreCachePolicy(in *egressauthstore.CachePolicySpec) *v1alpha1.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &v1alpha1.CachePolicySpec{TTL: in.TTL}
}

func cloneStoreCachePolicy(in *egressauthstore.CachePolicySpec) *egressauthstore.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauthstore.CachePolicySpec{TTL: in.TTL}
}
