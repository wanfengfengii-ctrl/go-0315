package service

import (
	"sync"

	"archival-replica-integrity-recovery/internal/domain"
)

// RepairAdapter is the seam to external storage nodes. It performs a single
// chunk copy from a trusted source node to a target node and reports the
// outcome as a failure category; an empty category means the copy succeeded.
// Calls happen outside database transactions so that external failures never
// leave partially-committed domain state.
type RepairAdapter interface {
	Copy(sourceNode, targetNode, objectID string, chunkNo int64) domain.RepairFailureCategory
}

// SuccessAdapter always reports a successful copy. It is the default adapter
// used by the executable when no external storage integration is configured.
type SuccessAdapter struct{}

// Copy always succeeds.
func (SuccessAdapter) Copy(sourceNode, targetNode, objectID string, chunkNo int64) domain.RepairFailureCategory {
	return ""
}

// ScriptedAdapter replays a fixed sequence of outcomes, then repeats the final
// outcome forever. It lets tests exercise timeout, rejection and success in a
// deterministic order.
type ScriptedAdapter struct {
	mu       sync.Mutex
	sequence []domain.RepairFailureCategory
	position int
	calls    int
}

// NewScriptedAdapter builds an adapter that returns outcomes in sequence.
func NewScriptedAdapter(sequence ...domain.RepairFailureCategory) *ScriptedAdapter {
	return &ScriptedAdapter{sequence: sequence}
}

// Copy returns the next scripted outcome, holding the final one forever.
func (a *ScriptedAdapter) Copy(sourceNode, targetNode, objectID string, chunkNo int64) domain.RepairFailureCategory {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	if len(a.sequence) == 0 {
		return ""
	}
	idx := a.position
	if idx >= len(a.sequence) {
		idx = len(a.sequence) - 1
	}
	out := a.sequence[idx]
	if a.position < len(a.sequence)-1 {
		a.position++
	}
	return out
}

// Calls returns the number of Copy invocations observed so far.
func (a *ScriptedAdapter) Calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}
