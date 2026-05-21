package functionruntime

import (
	"net/http"
	"strings"
)

const OwnerKind = "function-runtime"

const (
	HeaderOwnerKind                 = "X-Sandbox0-Function-Runtime-Owner-Kind"
	HeaderFunctionID                = "X-Sandbox0-Function-ID"
	HeaderFunctionRevisionID        = "X-Sandbox0-Function-Revision-ID"
	HeaderFunctionRuntimeInstanceID = "X-Sandbox0-Function-Runtime-Instance-ID"
)

type Metadata struct {
	OwnerKind                 string
	FunctionID                string
	FunctionRevisionID        string
	FunctionRuntimeInstanceID string
}

func SetHeaders(header http.Header, metadata Metadata) {
	if header == nil {
		return
	}
	setHeaderValue(header, HeaderOwnerKind, metadata.OwnerKind)
	setHeaderValue(header, HeaderFunctionID, metadata.FunctionID)
	setHeaderValue(header, HeaderFunctionRevisionID, metadata.FunctionRevisionID)
	setHeaderValue(header, HeaderFunctionRuntimeInstanceID, metadata.FunctionRuntimeInstanceID)
}

func FromHeaders(header http.Header) *Metadata {
	if header == nil {
		return nil
	}
	metadata := &Metadata{
		OwnerKind:                 strings.TrimSpace(header.Get(HeaderOwnerKind)),
		FunctionID:                strings.TrimSpace(header.Get(HeaderFunctionID)),
		FunctionRevisionID:        strings.TrimSpace(header.Get(HeaderFunctionRevisionID)),
		FunctionRuntimeInstanceID: strings.TrimSpace(header.Get(HeaderFunctionRuntimeInstanceID)),
	}
	if metadata.OwnerKind == "" && metadata.FunctionID == "" && metadata.FunctionRevisionID == "" && metadata.FunctionRuntimeInstanceID == "" {
		return nil
	}
	return metadata
}

func ClearHeaders(header http.Header) {
	if header == nil {
		return
	}
	header.Del(HeaderOwnerKind)
	header.Del(HeaderFunctionID)
	header.Del(HeaderFunctionRevisionID)
	header.Del(HeaderFunctionRuntimeInstanceID)
}

func setHeaderValue(header http.Header, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		header.Del(key)
		return
	}
	header.Set(key, value)
}
