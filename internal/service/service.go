// Package service implements the application state machine that wires the six
// documented components together. It owns the business flows — freeze, scan
// and verdict, isolation closure, repair orchestration, stability verification
// and terminal arbitration — and calls the store for every durable transition.
// Storage-node side effects happen through the RepairAdapter outside database
// transactions, matching the documented failure boundary.
package service

import (
	"errors"

	"archival-replica-integrity-recovery/internal/store"
)

// Service is the application facade. All HTTP mutations route through its
// methods so that every state change obeys the domain state machine.
type Service struct {
	store   *store.Store
	adapter RepairAdapter
}

// NewService builds a service backed by the given store and repair adapter.
func NewService(st *store.Store, adapter RepairAdapter) *Service {
	if adapter == nil {
		adapter = SuccessAdapter{}
	}
	return &Service{store: st, adapter: adapter}
}

// Store exposes the underlying store for the HTTP layer's read paths.
func (s *Service) Store() *store.Store { return s.store }

// Service-level sentinel errors. The HTTP layer maps these to stable error
// codes and statuses.
var (
	ErrBatchNotFound       = errors.New("service: batch not found")
	ErrNotFrozen           = errors.New("service: batch is not frozen")
	ErrAlreadyFrozen       = errors.New("service: batch is already frozen")
	ErrAlreadyExists       = errors.New("service: resource already exists")
	ErrInvalidPolicy       = errors.New("service: invalid preservation policy")
	ErrInvalidCatalog      = errors.New("service: invalid object catalogue")
	ErrLeaseConflict       = errors.New("service: lease conflict")
	ErrQuorumConflict      = errors.New("service: quorum conflict")
	ErrStaleGeneration     = errors.New("service: stale generation")
	ErrTerminalConflict    = errors.New("service: terminal conflict")
	ErrIdempotencyConflict = errors.New("service: idempotency conflict")
	ErrRepairNotFound      = errors.New("service: repair task not found")
	ErrNotQualified        = errors.New("service: reviewer not qualified")
	ErrNotReady            = errors.New("service: not ready for terminal decision")
)
