// Package lease implements the half-open lease invariant and the mutual
// exclusion rule shared by the four limited-lifetime resource leases. It is
// the in-memory substrate that the persistence layer mirrors with a database
// logical clock and optimistic version comparison.
package lease

import (
	"errors"
	"sync"

	"archival-replica-integrity-recovery/internal/domain"
)

// Sentinel errors returned by lease operations.
var (
	ErrAlreadyHeld    = errors.New("lease: resource already held by another lease")
	ErrExpired        = errors.New("lease: lease is not active")
	ErrHolderMismatch = errors.New("lease: renew/release holder mismatch")
	ErrNotFound       = errors.New("lease: lease not found")
)

// Active reports whether tick lies within the half-open interval
// [start, expires). A tick equal to expires is already outside the interval.
func Active(tick, start, expires int64) bool {
	return start <= tick && tick < expires
}

// Expired reports whether tick has reached or passed the expiry boundary.
func Expired(tick, expires int64) bool {
	return tick >= expires
}

// Overlap reports whether two half-open intervals intersect. Touching at a
// boundary (a.Expires == b.Start) does not count as overlap.
func Overlap(aStart, aExpires, bStart, bExpires int64) bool {
	return aStart < bExpires && bStart < aExpires
}

// Manager grants and renews exclusive leases over named resources in memory.
// It is safe for concurrent use and enforces the "one holder per resource"
// rule using the half-open interval invariant.
type Manager struct {
	mu     sync.Mutex
	leases map[string]*domain.ResourceLease
	nextID int64
}

// NewManager returns an empty lease manager.
func NewManager() *Manager {
	return &Manager{leases: make(map[string]*domain.ResourceLease)}
}

// resourceKey combines the resource type and key into a single map key.
func resourceKey(t domain.ResourceType, key string) string {
	return string(t) + "\x00" + key
}

// Acquire grants an exclusive lease for resource starting at start and
// expiring at expires. It fails if another active lease already holds the
// resource, and requires the interval to be active at start.
func (m *Manager) Acquire(t domain.ResourceType, key, holder string, start, expires int64) (*domain.ResourceLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rk := resourceKey(t, key)
	if existing, ok := m.leases[rk]; ok && Active(start, existing.StartTick, existing.ExpiresTick) {
		return nil, ErrAlreadyHeld
	}
	if !Active(start, start, expires) {
		return nil, ErrExpired
	}

	m.nextID++
	l := &domain.ResourceLease{
		ResourceType: t,
		ResourceKey:  key,
		LeaseID:      leaseID(m.nextID),
		Holder:       holder,
		StartTick:    start,
		ExpiresTick:  expires,
		Version:      1,
	}
	m.leases[rk] = l
	return l, nil
}

// Renew extends an existing lease to newExpires. The holder must match and the
// lease must still be active at now; renewing at or after expiry fails.
func (m *Manager) Renew(leaseID, holder string, now, newExpires int64) (*domain.ResourceLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for rk, l := range m.leases {
		if l.LeaseID != leaseID {
			continue
		}
		if l.Holder != holder {
			return nil, ErrHolderMismatch
		}
		if !Active(now, l.StartTick, l.ExpiresTick) {
			return nil, ErrExpired
		}
		l.ExpiresTick = newExpires
		l.Version++
		_ = rk
		return l, nil
	}
	return nil, ErrNotFound
}

// Release drops a lease. The holder must match.
func (m *Manager) Release(leaseID, holder string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for rk, l := range m.leases {
		if l.LeaseID != leaseID {
			continue
		}
		if l.Holder != holder {
			return ErrHolderMismatch
		}
		delete(m.leases, rk)
		return nil
	}
	return ErrNotFound
}

func leaseID(n int64) string {
	const digits = "0123456789abcdef"
	var buf [16]byte
	i := len(buf)
	for v := n; v > 0; v >>= 4 {
		i--
		buf[i] = digits[v&0xf]
	}
	if i == len(buf) {
		i--
		buf[i] = '0'
	}
	return "lease-" + string(buf[i:])
}
