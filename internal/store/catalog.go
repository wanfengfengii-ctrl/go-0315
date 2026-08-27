package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"archival-replica-integrity-recovery/internal/domain"
)

// PutObjects replaces the full object catalogue for a batch in one
// transaction. It is only valid while the batch is a draft.
func (s *Store) PutObjects(ctx context.Context, batchID string, objects []domain.ArchiveObject) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		var status domain.Status
		if err := tx.QueryRowContext(ctx, `SELECT status FROM batches WHERE batch_id = ?`, batchID).Scan(&status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status != domain.StatusDraft {
			return ErrAlreadyFrozen
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE batch_id = ?`, batchID); err != nil {
			return err
		}
		for _, o := range objects {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO objects (batch_id, object_id, canonical_key, expected_length, expected_root) VALUES (?, ?, ?, ?, ?)`,
				batchID, o.ObjectID, o.CanonicalKey, o.ExpectedLength, o.ExpectedRoot); err != nil {
				if isUniqueViolation(err) {
					return fmt.Errorf("%w: object %s", ErrAlreadyExists, o.ObjectID)
				}
				return err
			}
		}
		return nil
	})
}

// PutDependencies replaces the dependency edge set for a batch.
func (s *Store) PutDependencies(ctx context.Context, batchID string, deps []domain.ObjectDependency) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM dependencies WHERE batch_id = ?`, batchID); err != nil {
			return err
		}
		for _, d := range deps {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO dependencies (batch_id, from_object, to_object, reason) VALUES (?, ?, ?, ?)`,
				batchID, d.FromObject, d.ToObject, d.Reason); err != nil {
				if isUniqueViolation(err) {
					return fmt.Errorf("%w: dependency %s->%s", ErrAlreadyExists, d.FromObject, d.ToObject)
				}
				return err
			}
		}
		return nil
	})
}

// PutNodes replaces the storage node roster for a batch.
func (s *Store) PutNodes(ctx context.Context, batchID string, nodes []domain.StorageNode) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM nodes WHERE batch_id = ?`, batchID); err != nil {
			return err
		}
		for _, n := range nodes {
			enabled := 0
			if n.Enabled {
				enabled = 1
			}
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO nodes (batch_id, node_id, failure_domain, enabled) VALUES (?, ?, ?, ?)`,
				batchID, n.NodeID, n.FailureDomain, enabled); err != nil {
				if isUniqueViolation(err) {
					return fmt.Errorf("%w: node %s", ErrAlreadyExists, n.NodeID)
				}
				return err
			}
		}
		return nil
	})
}

// ListObjects returns the object catalogue sorted by canonical key.
func (s *Store) ListObjects(ctx context.Context, batchID string) ([]domain.ArchiveObject, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT object_id, canonical_key, expected_length, expected_root FROM objects WHERE batch_id = ? ORDER BY canonical_key, object_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ArchiveObject
	for rows.Next() {
		var o domain.ArchiveObject
		if err := rows.Scan(&o.ObjectID, &o.CanonicalKey, &o.ExpectedLength, &o.ExpectedRoot); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ListNodes returns the node roster sorted by node id.
func (s *Store) ListNodes(ctx context.Context, batchID string) ([]domain.StorageNode, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, failure_domain, enabled FROM nodes WHERE batch_id = ? ORDER BY node_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.StorageNode
	for rows.Next() {
		var n domain.StorageNode
		var enabled int
		if err := rows.Scan(&n.NodeID, &n.FailureDomain, &enabled); err != nil {
			return nil, err
		}
		n.Enabled = enabled == 1
		out = append(out, n)
	}
	return out, rows.Err()
}

// ListDependencies returns the dependency edges sorted by (from, to).
func (s *Store) ListDependencies(ctx context.Context, batchID string) ([]domain.ObjectDependency, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT from_object, to_object, reason FROM dependencies WHERE batch_id = ? ORDER BY from_object, to_object`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ObjectDependency
	for rows.Next() {
		var d domain.ObjectDependency
		if err := rows.Scan(&d.FromObject, &d.ToObject, &d.Reason); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetObject loads a single object's catalogue row.
func (s *Store) GetObject(ctx context.Context, batchID, objectID string) (*domain.ArchiveObject, error) {
	var o domain.ArchiveObject
	err := s.db.QueryRowContext(ctx,
		`SELECT object_id, canonical_key, expected_length, expected_root FROM objects WHERE batch_id = ? AND object_id = ?`,
		batchID, objectID).Scan(&o.ObjectID, &o.CanonicalKey, &o.ExpectedLength, &o.ExpectedRoot)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}
