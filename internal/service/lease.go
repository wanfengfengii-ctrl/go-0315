package service

import (
	"context"
	"fmt"

	"archival-replica-integrity-recovery/internal/domain"
)

// AcquireLease grants a durable exclusive lease over a named resource using
// the database logical clock. It delegates to the store's optimistic lease
// logic and maps persistence conflicts to ErrLeaseConflict.
func (s *Service) AcquireLease(ctx context.Context, resourceType domain.ResourceType, resourceKey, holder string, start, expires int64) (*domain.ResourceLease, error) {
	if start >= expires {
		return nil, ErrLeaseConflict
	}
	leaseID, err := s.store.NextTick(ctx)
	if err != nil {
		return nil, err
	}
	l := domain.ResourceLease{
		ResourceType: resourceType,
		ResourceKey:  resourceKey,
		LeaseID:      fmt.Sprintf("lease-%d", leaseID),
		Holder:       holder,
		StartTick:    start,
		ExpiresTick:  expires,
		Version:      1,
	}
	if err := s.store.AcquireLease(ctx, l); err != nil {
		return nil, ErrLeaseConflict
	}
	return &l, nil
}

// RenewLease extends a lease and returns its new version.
func (s *Service) RenewLease(ctx context.Context, leaseID, holder string, now, newExpires int64) (int64, error) {
	version, err := s.store.RenewLease(ctx, leaseID, holder, now, newExpires)
	if err != nil {
		return 0, ErrLeaseConflict
	}
	return version, nil
}

// ReleaseLease drops a lease held by holder.
func (s *Service) ReleaseLease(ctx context.Context, leaseID, holder string) error {
	if err := s.store.ReleaseLease(ctx, leaseID, holder); err != nil {
		return ErrLeaseConflict
	}
	return nil
}
