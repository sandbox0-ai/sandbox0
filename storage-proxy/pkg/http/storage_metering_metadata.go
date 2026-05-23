package http

import (
	"net/http"

	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func storageObservationMetadataFromHeaders(header http.Header) *meteringpkg.StorageObservation {
	metadata := functionruntime.FromHeaders(header)
	if metadata == nil {
		return nil
	}
	return &meteringpkg.StorageObservation{
		Product:                   meteringpkg.ProductFunction,
		OwnerKind:                 metadata.OwnerKind,
		FunctionID:                metadata.FunctionID,
		FunctionRevisionID:        metadata.FunctionRevisionID,
		FunctionRuntimeInstanceID: metadata.FunctionRuntimeInstanceID,
	}
}
