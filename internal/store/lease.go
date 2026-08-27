package store

import (
	"context"
	"database/sql"
	"errors"

	"archival-replica-integrity-recovery/internal/domain"
	"archival-replica-integrity-recovery/internal/lease"
)

// Lease errors specific to persistence.
var (
	ErrLeaseHeld     = errors.New("store: resource already leased")
	ErrLeaseExpired  = errors.New("store: lease not active")
	ErrLeaseHolder   = errors.New("store: holder mismatch")
	ErrLeaseNotFound = errors.New("store: lease not found")
)

// AcquireLease persists an exclusive lease over a resource. It fails when an
// active lease already covers the resource at start or when the requested
// interval is not active at its own start.
func (s *Store) AcquireLease(ctx context.Context, l domain.ResourceLease) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var curStart, curExpires int64
		err := tx.QueryRowContext(ctx,
			`SELECT start_tick, expires_tick FROM leases WHERE resource_type = ? AND resource_key = ?`,
			l.ResourceType, l.ResourceKey).Scan(&curStart, &curExpires)
		if err == nil && lease.Active(l.StartTick, curStart, curExpires) {
			return ErrLeaseHeld
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if !lease.Active(l.StartTick, l.StartTick, l.ExpiresTick) {
			return ErrLeaseExpired
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO leases (resource_type, resource_key, lease_id, holder, start_tick, expires_tick, version)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			l.ResourceType, l.ResourceKey, l.LeaseID, l.Holder, l.StartTick, l.ExpiresTick, l.Version); err != nil {
			if isUniqueViolation(err) {
				return ErrLeaseHeld
			}
			return err
		}
		return nil
	})
}

// RenewLease extends a lease to newExpires. The holder and lease id must match
// and the lease must still be active at now; the version is bumped atomically.
func (s *Store) RenewLease(ctx context.Context, leaseID, holder string, now, newExpires int64) (int64, error) {
	var version int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var cur domain.ResourceLease
		err := tx.QueryRowContext(ctx,
			`SELECT resource_type, resource_key, lease_id, holder, start_tick, expires_tick, version
			 FROM leases WHERE lease_id = ?`, leaseID).
			Scan(&cur.ResourceType, &cur.ResourceKey, &cur.LeaseID, &cur.Holder, &cur.StartTick, &cur.ExpiresTick, &cur.Version)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseNotFound
		}
		if err != nil {
			return err
		}
		if cur.Holder != holder {
			return ErrLeaseHolder
		}
		if !lease.Active(now, cur.StartTick, cur.ExpiresTick) {
			return ErrLeaseExpired
		}
		version = cur.Version + 1
		_, err = tx.ExecContext(ctx,
			`UPDATE leases SET expires_tick = ?, version = ? WHERE lease_id = ? AND version = ?`,
			newExpires, version, leaseID, cur.Version)
		if err != nil {
			return err
		}
		return nil
	})
	return version, err
}

// ReleaseLease drops a lease held by holder.
func (s *Store) ReleaseLease(ctx context.Context, leaseID, holder string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var curHolder string
		err := tx.QueryRowContext(ctx, `SELECT holder FROM leases WHERE lease_id = ?`, leaseID).Scan(&curHolder)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseNotFound
		}
		if err != nil {
			return err
		}
		if curHolder != holder {
			return ErrLeaseHolder
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM leases WHERE lease_id = ?`, leaseID)
		return err
	})
}

// GetLease loads a lease by id.
func (s *Store) GetLease(ctx context.Context, leaseID string) (*domain.ResourceLease, error) {
	var l domain.ResourceLease
	err := s.db.QueryRowContext(ctx,
		`SELECT resource_type, resource_key, lease_id, holder, start_tick, expires_tick, version
		 FROM leases WHERE lease_id = ?`, leaseID).
		Scan(&l.ResourceType, &l.ResourceKey, &l.LeaseID, &l.Holder, &l.StartTick, &l.ExpiresTick, &l.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrLeaseNotFound
	}
	if err != nil {
		return nil, err
	}
	return &l, nil
}
