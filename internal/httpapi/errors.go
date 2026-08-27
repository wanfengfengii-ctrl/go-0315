package httpapi

import (
	"errors"
	"net/http"

	"archival-replica-integrity-recovery/internal/service"
	"archival-replica-integrity-recovery/internal/store"
)

// mapError translates domain and persistence errors into the stable HTTP error
// envelope. Database errors are never mapped to success.
func mapError(err error) (int, ErrorCode, string) {
	switch {
	case errors.Is(err, service.ErrBatchNotFound),
		errors.Is(err, store.ErrNotFound),
		errors.Is(err, service.ErrRepairNotFound):
		return http.StatusNotFound, CodeNotFound, "resource not found"

	case errors.Is(err, service.ErrIdempotencyConflict):
		return http.StatusConflict, CodeIdempotencyConflict, "idempotency conflict"

	case errors.Is(err, service.ErrQuorumConflict):
		return http.StatusConflict, CodeQuorumConflict, "quorum conflict"

	case errors.Is(err, service.ErrStaleGeneration):
		return http.StatusConflict, CodeStaleGeneration, "stale generation"

	case errors.Is(err, service.ErrTerminalConflict):
		return http.StatusConflict, CodeTerminalConflict, "terminal conflict"

	case errors.Is(err, service.ErrLeaseConflict):
		return http.StatusConflict, CodeConflict, "lease conflict"

	case errors.Is(err, service.ErrNotQualified):
		return http.StatusForbidden, CodeNotQualified, "reviewer not qualified"

	case errors.Is(err, service.ErrNotReady):
		return http.StatusConflict, CodeNotReady, err.Error()

	case errors.Is(err, service.ErrCancelled):
		// 499 is the conventional status for a client that closed the
		// connection before the response was written.
		return 499, CodeCancelled, "request cancelled"

	case errors.Is(err, service.ErrInvalidPolicy),
		errors.Is(err, service.ErrInvalidCatalog):
		return http.StatusBadRequest, CodeMalformedRequest, err.Error()

	case errors.Is(err, service.ErrAlreadyExists),
		errors.Is(err, service.ErrAlreadyFrozen),
		errors.Is(err, service.ErrNotFrozen),
		errors.Is(err, store.ErrAlreadyExists),
		errors.Is(err, store.ErrAlreadyFrozen),
		errors.Is(err, store.ErrNotFrozen),
		errors.Is(err, store.ErrConflict):
		return http.StatusConflict, CodeConflict, err.Error()

	default:
		return http.StatusInternalServerError, CodeInternal, "internal error"
	}
}
