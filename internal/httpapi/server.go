// Package httpapi exposes the versioned HTTP interface for the service. It
// centralizes the stable error envelope, idempotency-key plumbing, strict JSON
// decoding and deterministic JSON encoding so that every state mutation routes
// through the domain state machine rather than ad-hoc handlers.
package httpapi

import (
	"encoding/json"
	"log"
	"net/http"

	"archival-replica-integrity-recovery/internal/service"
)

// ErrorCode is a stable, machine-readable error code returned in the error
// envelope. These codes are part of the public contract.
type ErrorCode string

const (
	CodeMalformedRequest    ErrorCode = "MALFORMED_REQUEST"
	CodeIdempotencyConflict ErrorCode = "IDEMPOTENCY_CONFLICT"
	CodeQuorumConflict      ErrorCode = "QUORUM_CONFLICT"
	CodeStaleGeneration     ErrorCode = "STALE_GENERATION"
	CodeTerminalConflict    ErrorCode = "TERMINAL_CONFLICT"
	CodeConflict            ErrorCode = "CONFLICT"
	CodeNotFound            ErrorCode = "NOT_FOUND"
	CodeNotQualified        ErrorCode = "NOT_QUALIFIED"
	CodeNotReady            ErrorCode = "NOT_READY"
	CodeCancelled           ErrorCode = "CANCELLED"
	CodeInternal            ErrorCode = "INTERNAL"
)

// errorBody is the documented error envelope {code, message, details}.
type errorBody struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Details []string  `json:"details"`
}

// Server is the root HTTP handler for the versioned API.
type Server struct {
	svc *service.Service
	mux *http.ServeMux
}

// NewServer assembles the versioned routes over the given service and returns
// a ready-to-serve handler.
func NewServer(svc *service.Service) *Server {
	s := &Server{svc: svc, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/healthz", s.handleHealth)
	s.mux.HandleFunc("/v1/batches", s.handleBatches)
	s.mux.HandleFunc("/v1/batches/", s.handleBatchSubresource)
	s.mux.HandleFunc("/v1/leases", s.handleLeases)
	s.mux.HandleFunc("/v1/leases/", s.handleLeaseSubresource)
	s.mux.HandleFunc("/v1/repairs/", s.handleRepairSubresource)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, CodeMalformedRequest, "method not allowed", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeJSON writes v as JSON with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("httpapi: encode response: %v", err)
	}
}

// writeError writes the documented error envelope.
func writeError(w http.ResponseWriter, status int, code ErrorCode, msg string, details []string) {
	writeJSON(w, status, errorBody{Code: code, Message: msg, Details: details})
}
